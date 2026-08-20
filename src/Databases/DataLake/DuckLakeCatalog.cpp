#include <Databases/DataLake/DuckLakeCatalog.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context.h>

#include <fmt/format.h>

#include <algorithm>
#include <functional>

#if USE_SQLITE
#include <Databases/SQLite/SQLiteUtils.h>
#include <sqlite3.h>
#endif

#if USE_LIBPQXX
#include <cctype>
#include <cstdlib>

#include <pqxx/pqxx>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
extern const int SQLITE_ENGINE_ERROR;
extern const int POSTGRESQL_CONNECTION_FAILURE;
extern const int DUCKLAKE_CATALOG_ERROR;
extern const int UNSUPPORTED_METHOD;
extern const int LOGICAL_ERROR;
}

namespace
{

#if USE_LIBPQXX
/// libpq conninfo helper: substitute `password_env=VAR` tokens with the referenced
/// environment variable's value, appended as a properly quoted `password='...'` parameter.
///
/// Rationale: PGPASSWORD is process-wide, so a node attaching several DuckLake postgres
/// catalogs with different passwords cannot use it; and putting password= in
/// ducklake_connection_string would store the secret in the CREATE DATABASE DDL (system
/// tables, logs). password_env lets each catalog name its own env var instead.
/// A missing or empty variable fails the attach loudly rather than retrying doomed auth.
String resolveConnInfoPasswordFromEnv(const String & conninfo)
{
    static const String marker = "password_env=";
    if (conninfo.find(marker) == String::npos)
        return conninfo;

    String password_value;
    String result;
    result.reserve(conninfo.size());

    size_t pos = 0;
    while (pos < conninfo.size())
    {
        size_t token_end = conninfo.find_first_of(" \t", pos);
        if (token_end == String::npos)
            token_end = conninfo.size();
        String token = conninfo.substr(pos, token_end - pos);
        if (token.starts_with(marker))
        {
            String var = token.substr(marker.size());
            bool valid = !var.empty() && (std::isalpha(static_cast<unsigned char>(var[0])) || var[0] == '_');
            for (size_t i = 1; valid && i < var.size(); ++i)
                valid = std::isalnum(static_cast<unsigned char>(var[i])) || var[i] == '_';
            if (!valid)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid environment variable name '{}' in ducklake_connection_string password_env parameter",
                    var);
            const char * value = std::getenv(var.c_str()); // NOLINT(concurrency-mt-unsafe): read-only env lookup at attach time
            if (!value || !*value)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "ducklake_connection_string references environment variable '{}' via password_env, "
                    "but it is not set or empty in the server environment",
                    var);
            if (!password_value.empty() && password_value != value)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "ducklake_connection_string contains multiple password_env parameters with different values");
            password_value = value;
        }
        else
        {
            if (!result.empty())
                result.push_back(' ');
            result += token;
        }
        pos = token_end;
        while (pos < conninfo.size() && (conninfo[pos] == ' ' || conninfo[pos] == '\t'))
            ++pos;
    }

    if (!password_value.empty())
    {
        // libpq quoting: single-quote the value, backslash-escaping ' and \.
        result += " password='";
        for (char c : password_value)
        {
            if (c == '\'' || c == '\\')
                result.push_back('\\');
            result.push_back(c);
        }
        result.push_back('\'');
    }
    return result;
}
#endif

/// Quoting helpers. Identifiers and literals embedded into catalog SQL always come from
/// either the catalog itself or from already-validated settings, but we quote anyway.
String quoteLiteral(const String & value)
{
    String result;
    result.reserve(value.size() + 2);
    result.push_back('\'');
    for (char c : value)
    {
        if (c == '\'')
            result.push_back('\'');
        result.push_back(c);
    }
    result.push_back('\'');
    return result;
}

String quoteIdentifier(const String & name)
{
    String result;
    result.reserve(name.size() + 2);
    result.push_back('"');
    for (char c : name)
    {
        if (c == '"')
            result.push_back('"');
        result.push_back(c);
    }
    result.push_back('"');
    return result;
}

bool parseBool(const std::optional<String> & value)
{
    if (!value.has_value())
        return false;
    return *value == "1" || *value == "t" || *value == "true" || *value == "yes";
}

Int64 parseInt64(const std::optional<String> & value, const String & what)
{
    if (!value.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected NULL {} in the DuckLake catalog", what);
    return std::stoll(*value);
}

}

struct DuckLakeQueryResult
{
    std::vector<String> column_names;
    using Row = std::vector<std::optional<String>>;
    std::vector<Row> rows;
};

class IDuckLakeConnection;

/// One catalog transaction (write path). Statements execute inside the transaction;
/// commit throws DuckLakeCommitConflictException when another writer committed a
/// conflicting ducklake_snapshot row concurrently.
class IDuckLakeTransaction
{
public:
    virtual ~IDuckLakeTransaction() = default;

    virtual DuckLakeQueryResult exec(const String & query) = 0;
    virtual void commit() = 0;
};

class IDuckLakeConnection
{
public:
    virtual ~IDuckLakeConnection() = default;

    using Row = std::vector<std::optional<String>>;
    virtual DuckLakeQueryResult exec(const String & query) = 0;
    virtual bool tableExists(const String & name) = 0;

    /// ducklake_* table reference including the catalog schema qualifier for postgres.
    virtual String qualified(const String & table) = 0;

    /// Begin a catalog write transaction. The SQLite backend throws UNSUPPORTED_METHOD:
    /// writes are supported for PostgreSQL catalogs only.
    virtual std::unique_ptr<IDuckLakeTransaction> beginTransaction() = 0;

    /// Begin a snapshot read transaction and return a connection view whose exec() calls
    /// all run inside it (REPEATABLE READ READ ONLY on postgres). The transaction ends
    /// when the view is destroyed. Needed because visibility-filtered reads cannot see
    /// rows a concurrent flush physically removed (inlined deletes/data): only a real
    /// transaction keeps a multi-statement catalog read on one snapshot.
    virtual std::unique_ptr<IDuckLakeConnection> beginReadTx() = 0;
};

#if USE_SQLITE

/// Run one statement and collect all rows. The query is a single statement; prepare/step
/// it directly (sqlite3_exec would stringify BLOB values and truncate them at the first
/// zero byte).
static DuckLakeQueryResult sqliteExec(sqlite3 * db, const String & query)
{
    sqlite3_stmt * stmt = nullptr;
    DuckLakeQueryResult result;
    int status = sqlite3_prepare_v2(db, query.c_str(), static_cast<int>(query.size() + 1), &stmt, nullptr);
    if (status != SQLITE_OK)
        throw Exception(
            ErrorCodes::SQLITE_ENGINE_ERROR,
            "DuckLake catalog query failed to prepare. Error status: {}. Query: {}",
            status,
            query);

    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> stmt_holder(stmt, &sqlite3_finalize);

    const int col_num = sqlite3_column_count(stmt);
    while ((status = sqlite3_step(stmt)) == SQLITE_ROW)
    {
        if (result.column_names.empty())
        {
            result.column_names.reserve(col_num);
            for (int i = 0; i < col_num; ++i)
                result.column_names.emplace_back(sqlite3_column_name(stmt, i));
        }
        DuckLakeQueryResult::Row row;
        row.reserve(col_num);
        for (int i = 0; i < col_num; ++i)
        {
            if (sqlite3_column_type(stmt, i) == SQLITE_NULL)
            {
                row.emplace_back(std::nullopt);
                continue;
            }
            const char * data = reinterpret_cast<const char *>(sqlite3_column_text(stmt, i));
            const int bytes = sqlite3_column_bytes(stmt, i);
            row.emplace_back(String(data, bytes));
        }
        result.rows.push_back(std::move(row));
    }
    if (status != SQLITE_DONE)
        throw Exception(
            ErrorCodes::SQLITE_ENGINE_ERROR,
            "DuckLake catalog query failed. Error status: {}. Query: {}",
            status,
            query);
    return result;
}

class DuckLakeSQLiteConnection final : public IDuckLakeConnection
{
public:
    DuckLakeSQLiteConnection(const String & database_path_, ContextPtr context_)
        : database_path(database_path_)
        , context(std::move(context_))
    {
    }

    const String & getDatabasePath() const { return database_path; }

    DuckLakeQueryResult exec(const String & query) override
    {
        std::lock_guard lock(mutex);
        ensureOpen();
        return sqliteExec(db.get(), query);
    }

    bool tableExists(const String & name) override
    {
        return !exec(fmt::format("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = {} LIMIT 1", quoteLiteral(name))).rows.empty();
    }

    String qualified(const String & table) override { return quoteIdentifier(table); }

    std::unique_ptr<IDuckLakeTransaction> beginTransaction() override
    {
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Writes to a DuckLake catalog are supported for the PostgreSQL backend only (sqlite catalog '{}')",
            database_path);
    }

    std::unique_ptr<IDuckLakeConnection> beginReadTx() override;

private:
    String database_path;
    ContextPtr context;
    SQLitePtr db;
    std::mutex mutex;

    void ensureOpen()
    {
        if (!db)
            db = openSQLiteDB(database_path, context, /* throw_on_error */ true);
    }
};

/// Snapshot-read view over its own sqlite connection with a deferred transaction open, so
/// all statements of one logical catalog read observe one snapshot. A second connection is
/// used instead of the shared one so a long-lived snapshot read never serializes against
/// unrelated autocommit catalog lookups (and vice versa).
class DuckLakeSQLiteReadTx final : public IDuckLakeConnection
{
public:
    DuckLakeSQLiteReadTx(const String & database_path, ContextPtr context)
        : db(openSQLiteDB(database_path, context, /* throw_on_error */ true))
    {
        sqliteExec(db.get(), "BEGIN");
    }

    ~DuckLakeSQLiteReadTx() override
    {
        try
        {
            sqliteExec(db.get(), "ROLLBACK");
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("DuckLakeSQLiteReadTx"), "Failed to roll back catalog snapshot read");
        }
    }

    DuckLakeQueryResult exec(const String & query) override
    {
        /// See DuckLakePostgresReadTx::exec: reader streams can hit the catalog
        /// concurrently, and a sqlite handle is not thread-safe.
        std::lock_guard lock(exec_mutex);
        return sqliteExec(db.get(), query);
    }

    bool tableExists(const String & name) override
    {
        return !exec(fmt::format("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = {} LIMIT 1", quoteLiteral(name))).rows.empty();
    }

    String qualified(const String & table) override { return quoteIdentifier(table); }

    std::unique_ptr<IDuckLakeTransaction> beginTransaction() override
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Writes are not supported on a DuckLake snapshot read transaction");
    }

    std::unique_ptr<IDuckLakeConnection> beginReadTx() override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot nest DuckLake snapshot read transactions");
    }

private:
    SQLitePtr db;
    std::mutex exec_mutex;
};

std::unique_ptr<IDuckLakeConnection> DuckLakeSQLiteConnection::beginReadTx()
{
    return std::make_unique<DuckLakeSQLiteReadTx>(database_path, context);
}

#endif

#if USE_LIBPQXX

static DuckLakeQueryResult pqxxMapResult(const pqxx::result & res)
{
    DuckLakeQueryResult result;
    result.column_names.reserve(res.columns());
    for (pqxx::row::size_type i = 0; i < res.columns(); ++i)
        result.column_names.emplace_back(res.column_name(i));

    result.rows.reserve(res.size());
    for (const auto & prow : res)
    {
        DuckLakeQueryResult::Row row;
        row.reserve(prow.size());
        for (const auto & field : prow)
        {
            if (field.is_null())
                row.emplace_back(std::nullopt);
            else
                row.emplace_back(String(field.c_str()));
        }
        result.rows.push_back(std::move(row));
    }
    return result;
}

class DuckLakePostgresConnection final : public IDuckLakeConnection
{
public:
    DuckLakePostgresConnection(const String & conninfo_, const String & catalog_schema_)
        : conninfo(conninfo_)
        , catalog_schema(catalog_schema_)
    {
    }

    DuckLakeQueryResult exec(const String & query) override
    {
        std::lock_guard lock(mutex);
        try
        {
            ensureOpen();
            pqxx::nontransaction tx(*connection);
            return pqxxMapResult(tx.exec(query));
        }
        catch (const pqxx::broken_connection & e)
        {
            connection.reset();
            throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "DuckLake catalog connection broken: {}", e.what());
        }
    }

    bool tableExists(const String & name) override
    {
        const auto result = exec(fmt::format(
            "SELECT to_regclass({}) IS NOT NULL",
            quoteLiteral(fmt::format("{}.{}", catalog_schema, name))));
        return !result.rows.empty() && parseBool(result.rows[0][0]);
    }

    String qualified(const String & table) override
    {
        return fmt::format("{}.{}", quoteIdentifier(catalog_schema), quoteIdentifier(table));
    }

    std::unique_ptr<IDuckLakeTransaction> beginTransaction() override;

    std::unique_ptr<IDuckLakeConnection> beginReadTx() override;

private:
    friend class DuckLakePostgresTransaction;

    String conninfo;
    String catalog_schema;
    std::unique_ptr<pqxx::connection> connection;
    std::mutex mutex;

    void ensureOpen()
    {
        if (!connection || !connection->is_open())
            connection = std::make_unique<pqxx::connection>(conninfo);
    }
};

/// Snapshot-read view over a dedicated connection running one REPEATABLE READ READ ONLY
/// transaction for its whole lifetime. A dedicated connection (rather than the shared
/// autocommit one) is what lets a query hold its snapshot from pinning through its last
/// catalog read without serializing against other queries' catalog access. Postgres
/// guarantees the snapshot stays fixed for the transaction, so rows physically removed by
/// a concurrent flush (inlined deletes/data) remain visible for their pinned snapshot.
class DuckLakePostgresReadTx final : public IDuckLakeConnection
{
public:
    using Transaction = pqxx::transaction<pqxx::isolation_level::repeatable_read, pqxx::write_policy::read_only>;

    DuckLakePostgresReadTx(const String & conninfo_, const String & catalog_schema_)
        : connection(std::make_unique<pqxx::connection>(conninfo_))
        , tx(std::make_unique<Transaction>(*connection))
        , catalog_schema(catalog_schema_)
    {
    }

    ~DuckLakePostgresReadTx() override
    {
        try
        {
            if (tx)
                tx->abort();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("DuckLakePostgresReadTx"), "Failed to abort catalog snapshot read");
        }
    }

    DuckLakeQueryResult exec(const String & query) override
    try
    {
        /// Multiple reader streams can execute catalog statements concurrently
        /// (e.g. several inlined-data objects materializing in parallel); libpqxx
        /// forbids overlapping commands on one transaction, so serialize them.
        std::lock_guard lock(exec_mutex);
        return pqxxMapResult(tx->exec(query));
    }
    catch (const pqxx::broken_connection & e)
    {
        tx.reset();
        connection.reset();
        throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "DuckLake catalog connection broken: {}", e.what());
    }

    bool tableExists(const String & name) override
    {
        const auto result = exec(fmt::format(
            "SELECT to_regclass({}) IS NOT NULL",
            quoteLiteral(fmt::format("{}.{}", catalog_schema, name))));
        return !result.rows.empty() && parseBool(result.rows[0][0]);
    }

    String qualified(const String & table) override
    {
        return fmt::format("{}.{}", quoteIdentifier(catalog_schema), quoteIdentifier(table));
    }

    std::unique_ptr<IDuckLakeTransaction> beginTransaction() override
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Writes are not supported on a DuckLake snapshot read transaction");
    }

    std::unique_ptr<IDuckLakeConnection> beginReadTx() override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot nest DuckLake snapshot read transactions");
    }

private:
    std::unique_ptr<pqxx::connection> connection;
    std::unique_ptr<Transaction> tx;
    String catalog_schema;
    std::mutex exec_mutex;
};

std::unique_ptr<IDuckLakeConnection> DuckLakePostgresConnection::beginReadTx()
{
    return std::make_unique<DuckLakePostgresReadTx>(conninfo, catalog_schema);
}

/// Catalog write transaction over pqxx::work. Holds the connection's mutex for its whole
/// lifetime: a commit is a short, fixed sequence of statements, so readers only ever wait
/// for one commit.
class DuckLakePostgresTransaction final : public IDuckLakeTransaction
{
public:
    explicit DuckLakePostgresTransaction(DuckLakePostgresConnection & connection_)
        : connection(connection_)
        , lock(connection.mutex)
    {
        try
        {
            connection.ensureOpen();
            work = std::make_unique<pqxx::work>(*connection.connection);
        }
        catch (const pqxx::broken_connection & e)
        {
            connection.connection.reset();
            throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "DuckLake catalog connection broken: {}", e.what());
        }
        catch (const pqxx::sql_error & e)
        {
            throw Exception(ErrorCodes::DUCKLAKE_CATALOG_ERROR, "Failed to begin a DuckLake catalog transaction: {}", e.what());
        }
    }

    DuckLakeQueryResult exec(const String & query) override
    {
        try
        {
            return convert(work->exec(query));
        }
        catch (const pqxx::sql_error & e)
        {
            throwConflictOrError(e, query);
        }
    }

    void commit() override
    {
        try
        {
            work->commit();
        }
        catch (const pqxx::sql_error & e)
        {
            throwConflictOrError(e, "COMMIT");
        }
    }

private:
    DuckLakePostgresConnection & connection;
    std::unique_lock<std::mutex> lock;
    std::unique_ptr<pqxx::work> work;

    static DuckLakeQueryResult convert(const pqxx::result & res)
    {
        DuckLakeQueryResult result;
        result.column_names.reserve(res.columns());
        for (pqxx::row::size_type i = 0; i < res.columns(); ++i)
            result.column_names.emplace_back(res.column_name(i));
        result.rows.reserve(res.size());
        for (const auto & prow : res)
        {
            DuckLakeQueryResult::Row row;
            row.reserve(prow.size());
            for (const auto & field : prow)
            {
                if (field.is_null())
                    row.emplace_back(std::nullopt);
                else
                    row.emplace_back(String(field.c_str()));
            }
            result.rows.push_back(std::move(row));
        }
        return result;
    }

    [[noreturn]] static void throwConflictOrError(const pqxx::sql_error & e, const String & query)
    {
        /// 23505 unique_violation: another writer inserted the same ducklake_snapshot id.
        /// 40001 serialization_failure: lost a serializable race.
        const String sqlstate = e.sqlstate();
        if (sqlstate == "23505" || sqlstate == "40001")
            throw DuckLakeCommitConflictException(
                ErrorCodes::DUCKLAKE_CATALOG_ERROR,
                "DuckLake catalog commit conflicted with a concurrent writer (sqlstate {}): {}",
                sqlstate,
                e.what());
        throw Exception(
            ErrorCodes::DUCKLAKE_CATALOG_ERROR,
            "DuckLake catalog write failed (sqlstate {}). Error: {}. Query: {}",
            sqlstate,
            e.what(),
            query);
    }
};

std::unique_ptr<IDuckLakeTransaction> DuckLakePostgresConnection::beginTransaction()
{
    return std::make_unique<DuckLakePostgresTransaction>(*this);
}

#endif

namespace
{

String visibilityPredicate(Int64 snapshot_id, const String & alias)
{
    return fmt::format(
        "({0} >= {1}.begin_snapshot AND ({0} < {1}.end_snapshot OR {1}.end_snapshot IS NULL))", snapshot_id, alias);
}

/// DuckLake does not vend storage credentials. Satisfy the requiresCredentials() request with
/// a no-op credential set: local (file://) tables need no credentials at all, and remote
/// object storage (s3://) falls through to the environment credential chain — the pod's IRSA
/// identity in the managed-warehouse deployment — since the engine args carry no keys.
class NoCredentials final : public DataLake::IStorageCredentials
{
public:
    void addCredentialsToEngineArgs(DB::ASTs &) const override { }
};

}

DuckLakeCatalog::DuckLakeCatalog(
    const std::string & warehouse_,
    const std::string & backend_,
    const std::string & connection_string_,
    const std::string & catalog_schema_,
    ContextPtr context_)
    : ICatalog(warehouse_)
{
    if (backend_ == "sqlite")
    {
#if USE_SQLITE
        sqlite_database_path = connection_string_;
        connection = std::make_unique<DuckLakeSQLiteConnection>(connection_string_, context_);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ClickHouse was compiled without SQLite support");
#endif
    }
    else if (backend_ == "postgres")
    {
#if USE_LIBPQXX
        connection = std::make_unique<DuckLakePostgresConnection>(
            resolveConnInfoPasswordFromEnv(connection_string_), catalog_schema_);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ClickHouse was compiled without PostgreSQL support");
#endif
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unknown ducklake_backend '{}': expected 'postgres' or 'sqlite'",
            backend_);
    }

    /// Validate the catalog and read global metadata.
    String catalog_version;
    bool encrypted = false;
    for (const auto & row : connection->exec(fmt::format("SELECT key, value FROM {} WHERE scope IS NULL", connection->qualified("ducklake_metadata"))).rows)
    {
        const auto & key = row.at(0);
        const auto & value = row.at(1);
        if (!key.has_value())
            continue;
        if (*key == "version")
            catalog_version = value.value_or("");
        else if (*key == "data_path")
            data_path = value.value_or("");
        else if (*key == "encrypted")
            encrypted = parseBool(value);
    }

    if (catalog_version != "1.0")
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "DuckLake catalog schema version '{}' is not supported (only '1.0')",
            catalog_version.empty() ? "<missing>" : catalog_version);
    if (encrypted)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "DuckLake catalog encryption is not supported");
    if (data_path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake catalog has no 'data_path' in ducklake_metadata");

    if (data_path.find("://") == String::npos && !data_path.starts_with("/"))
    {
        if (sqlite_database_path.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DuckLake catalog data_path '{}' is relative; relative data paths are only supported with the sqlite backend",
                data_path);
        /// Resolve against the catalog file location, like DuckDB does. openSQLiteDB resolves
        /// relative database paths against the user_files directory, so mirror that here.
        String base = sqlite_database_path;
        if (!base.starts_with("/"))
            base = context_->getUserFilesPath() + base;
        const auto slash = base.find_last_of('/');
        data_path = (slash == String::npos ? String() : base.substr(0, slash + 1)) + data_path;
    }
}

DuckLakeCatalog::~DuckLakeCatalog() = default;

bool DuckLakeCatalog::supportsWrites() const
{
    return isPostgres();
}

DuckLakeCurrentPartitionSpec DuckLakeCatalog::getCurrentPartitionSpec(Int64 table_id, Int64 snapshot_id) const
{
    const auto info = connection->exec(fmt::format(
        "SELECT partition_id FROM {} WHERE table_id = {} AND {}",
        connection->qualified("ducklake_partition_info"),
        table_id,
        visibilityPredicate(snapshot_id, "ducklake_partition_info")));

    DuckLakeCurrentPartitionSpec spec;
    if (info.rows.empty())
        return spec;

    if (info.rows.size() != 1)
        throw Exception(
            ErrorCodes::DUCKLAKE_CATALOG_ERROR,
            "DuckLake table (id {}) has {} partition specs visible at snapshot {}, expected at most one",
            table_id,
            info.rows.size(),
            snapshot_id);
    spec.partition_id = parseInt64(info.rows[0][0], "partition_id");

    const auto fields = connection->exec(fmt::format(
        "SELECT partition_key_index, column_id, transform FROM {} WHERE partition_id = {} AND table_id = {} "
        "ORDER BY partition_key_index",
        connection->qualified("ducklake_partition_column"),
        *spec.partition_id,
        table_id));
    for (const auto & row : fields.rows)
    {
        spec.fields.push_back(DuckLakePartitionField{
            .partition_key_index = parseInt64(row[0], "partition_key_index"),
            .column_id = parseInt64(row[1], "column_id"),
            .transform = row[2].value_or(""),
        });
    }
    return spec;
}

namespace
{

String sqlInt64(Int64 value)
{
    return fmt::format("{}", value);
}

String sqlNullableString(const std::optional<String> & value)
{
    return value.has_value() ? quoteLiteral(*value) : String("NULL");
}

}

Int64 DuckLakeCatalog::appendDataFiles(
    Int64 table_id,
    const std::optional<Int64> & partition_id,
    const std::vector<DuckLakeNewDataFile> & files)
{
    if (files.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No data files to register in the DuckLake catalog");
    if (!supportsWrites())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Writes to a DuckLake catalog are supported for the PostgreSQL backend only");

    Int64 total_records = 0;
    Int64 total_bytes = 0;
    for (const auto & file : files)
    {
        total_records += file.record_count;
        total_bytes += file.file_size_bytes;
    }

    /// column_id -> type of every visible column, for type-aware stats comparisons below.
    const auto column_rows = getColumnRows(*connection, table_id);
    const auto column_tree = DuckLake::buildColumnTree(column_rows, pinSnapshot(*connection));
    std::unordered_map<Int64, NameAndTypePair> column_types;
    std::function<void(const DuckLake::ColumnNode &)> collect_types = [&](const DuckLake::ColumnNode & node)
    {
        column_types.emplace(node.info.column_id, NameAndTypePair(node.info.name, DuckLake::getColumnType(node)));
        for (const auto & child : node.children)
            collect_types(child);
    };
    for (const auto & root : column_tree)
        collect_types(root);

    /// Optimistic concurrency: two writers picking the same next snapshot id collide on the
    /// ducklake_snapshot primary key; the loser retries the whole commit. The table_stats
    /// row lock below serializes writers to the same table, so the retry only fires for
    /// writes to *other* tables of the same catalog.
    static constexpr size_t max_commit_attempts = 10;
    for (size_t attempt = 0;; ++attempt)
    {
        try
        {
            auto transaction = connection->beginTransaction();

            const auto snapshot_rows = transaction->exec(fmt::format(
                "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id FROM {} "
                "ORDER BY snapshot_id DESC LIMIT 1",
                connection->qualified("ducklake_snapshot")));
            if (snapshot_rows.rows.empty())
                throw Exception(ErrorCodes::DUCKLAKE_CATALOG_ERROR, "DuckLake catalog has no snapshots");
            const auto & current = snapshot_rows.rows[0];
            const Int64 new_snapshot_id = parseInt64(current[0], "snapshot_id") + 1;
            const Int64 schema_version = parseInt64(current[1], "schema_version");
            const Int64 next_catalog_id = parseInt64(current[2], "next_catalog_id");
            const Int64 next_file_id = parseInt64(current[3], "next_file_id");

            /// Lock the table's stats row for the rest of the transaction (serializes
            /// concurrent writers to this table) and get the row id counter.
            const auto stats_rows = transaction->exec(fmt::format(
                "SELECT record_count, next_row_id, file_size_bytes FROM {} WHERE table_id = {} FOR UPDATE",
                connection->qualified("ducklake_table_stats"),
                table_id));
            Int64 next_row_id = 0;
            bool have_stats_row = !stats_rows.rows.empty();
            if (have_stats_row)
                next_row_id = stats_rows.rows[0][1].has_value() ? std::stoll(*stats_rows.rows[0][1]) : 0;

            transaction->exec(fmt::format(
                "INSERT INTO {} (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) "
                "VALUES ({}, now(), {}, {}, {})",
                connection->qualified("ducklake_snapshot"),
                new_snapshot_id,
                schema_version,
                next_catalog_id,
                next_file_id + files.size()));

            Int64 row_id_start = next_row_id;
            for (size_t i = 0; i < files.size(); ++i)
            {
                const auto & file = files[i];
                const Int64 data_file_id = next_file_id + static_cast<Int64>(i);
                transaction->exec(fmt::format(
                    "INSERT INTO {} (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, "
                    "path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, "
                    "partition_id, encryption_key, mapping_id, partial_max) "
                    "VALUES ({}, {}, {}, NULL, NULL, {}, true, 'parquet', {}, {}, NULL, {}, {}, NULL, NULL, NULL)",
                    connection->qualified("ducklake_data_file"),
                    data_file_id,
                    table_id,
                    new_snapshot_id,
                    quoteLiteral(file.path),
                    file.record_count,
                    file.file_size_bytes,
                    row_id_start,
                    partition_id.has_value() ? sqlInt64(*partition_id) : String("NULL")));
                row_id_start += file.record_count;

                if (!file.column_stats.empty())
                {
                    String query = fmt::format(
                        "INSERT INTO {} (data_file_id, table_id, column_id, column_size_bytes, value_count, "
                        "null_count, min_value, max_value, contains_nan, extra_stats) VALUES ",
                        connection->qualified("ducklake_file_column_stats"));
                    for (size_t j = 0; j < file.column_stats.size(); ++j)
                    {
                        const auto & stats = file.column_stats[j];
                        if (j > 0)
                            query += ", ";
                        query += fmt::format(
                            "({}, {}, {}, NULL, {}, {}, {}, {}, {}, NULL)",
                            data_file_id,
                            table_id,
                            stats.column_id,
                            stats.value_count,
                            stats.null_count,
                            sqlNullableString(stats.min_value),
                            sqlNullableString(stats.max_value),
                            stats.contains_nan ? "true" : "false");
                    }
                    transaction->exec(query);
                }

                for (size_t key_index = 0; key_index < file.partition_values.size(); ++key_index)
                {
                    transaction->exec(fmt::format(
                        "INSERT INTO {} (data_file_id, table_id, partition_key_index, partition_value) "
                        "VALUES ({}, {}, {}, {})",
                        connection->qualified("ducklake_file_partition_value"),
                        data_file_id,
                        table_id,
                        key_index,
                        sqlNullableString(file.partition_values[key_index])));
                }
            }

            if (have_stats_row)
            {
                transaction->exec(fmt::format(
                    "UPDATE {} SET record_count = record_count + {}, next_row_id = next_row_id + {}, "
                    "file_size_bytes = file_size_bytes + {} WHERE table_id = {}",
                    connection->qualified("ducklake_table_stats"),
                    total_records,
                    total_records,
                    total_bytes,
                    table_id));
            }
            else
            {
                transaction->exec(fmt::format(
                    "INSERT INTO {} (table_id, record_count, next_row_id, file_size_bytes) VALUES ({}, {}, {}, {})",
                    connection->qualified("ducklake_table_stats"),
                    table_id,
                    total_records,
                    total_records,
                    total_bytes));
            }

            /// Widen the table-level column bounds so readers relying on
            /// ducklake_table_column_stats stay correct. The comparison must be
            /// type-aware (serialized stats values do not order lexicographically),
            /// so it happens here rather than in SQL.
            {
                /// Merge the per-file stats of this commit into one bound per column.
                struct NewColumnBounds
                {
                    bool has_any = false;
                    bool contains_null = false;
                    bool contains_nan = false;
                    std::optional<String> min_value;
                    std::optional<String> max_value;
                };
                std::map<Int64, NewColumnBounds> new_bounds;
                for (const auto & file : files)
                {
                    for (const auto & stats : file.column_stats)
                    {
                        auto & bounds = new_bounds[stats.column_id];
                        bounds.contains_null |= stats.null_count > 0;
                        bounds.contains_nan |= stats.contains_nan;
                        if (stats.min_value.has_value() && stats.max_value.has_value())
                        {
                            bounds.has_any = true;
                            if (!bounds.min_value.has_value())
                            {
                                bounds.min_value = stats.min_value;
                                bounds.max_value = stats.max_value;
                                continue;
                            }
                            auto type_it = column_types.find(stats.column_id);
                            if (type_it == column_types.end())
                                continue;
                            const auto new_min = DuckLake::parseStatsValue(*stats.min_value, type_it->second.type);
                            const auto cur_min = DuckLake::parseStatsValue(*bounds.min_value, type_it->second.type);
                            const auto new_max = DuckLake::parseStatsValue(*stats.max_value, type_it->second.type);
                            const auto cur_max = DuckLake::parseStatsValue(*bounds.max_value, type_it->second.type);
                            if (new_min.has_value() && cur_min.has_value() && *new_min < *cur_min)
                                bounds.min_value = stats.min_value;
                            if (new_max.has_value() && cur_max.has_value() && *cur_max < *new_max)
                                bounds.max_value = stats.max_value;
                        }
                    }
                }

                const auto existing = transaction->exec(fmt::format(
                    "SELECT column_id, contains_null, contains_nan, min_value, max_value FROM {} WHERE table_id = {}",
                    connection->qualified("ducklake_table_column_stats"),
                    table_id));
                std::unordered_map<Int64, size_t> existing_row_by_column;
                for (size_t i = 0; i < existing.rows.size(); ++i)
                    existing_row_by_column.emplace(parseInt64(existing.rows[i][0], "column_id"), i);

                for (const auto & [column_id, bounds] : new_bounds)
                {
                    std::optional<String> new_min = bounds.min_value;
                    std::optional<String> new_max = bounds.max_value;
                    const auto existing_it = existing_row_by_column.find(column_id);
                    if (existing_it != existing_row_by_column.end())
                    {
                        const auto & row = existing.rows[existing_it->second];
                        auto type_it = column_types.find(column_id);
                        if (type_it != column_types.end())
                        {
                            if (row[3].has_value() && new_min.has_value())
                            {
                                const auto old_value = DuckLake::parseStatsValue(*row[3], type_it->second.type);
                                const auto candidate = DuckLake::parseStatsValue(*new_min, type_it->second.type);
                                if (old_value.has_value() && candidate.has_value() && !(*candidate < *old_value))
                                    new_min = row[3];
                            }
                            if (row[4].has_value() && new_max.has_value())
                            {
                                const auto old_value = DuckLake::parseStatsValue(*row[4], type_it->second.type);
                                const auto candidate = DuckLake::parseStatsValue(*new_max, type_it->second.type);
                                if (old_value.has_value() && candidate.has_value() && !(*old_value < *candidate))
                                    new_max = row[4];
                            }
                        }
                        transaction->exec(fmt::format(
                            "UPDATE {} SET min_value = {}, max_value = {}, contains_null = contains_null OR {}, "
                            "contains_nan = contains_nan OR {} WHERE table_id = {} AND column_id = {}",
                            connection->qualified("ducklake_table_column_stats"),
                            sqlNullableString(new_min),
                            sqlNullableString(new_max),
                            bounds.contains_null ? "true" : "false",
                            bounds.contains_nan ? "true" : "false",
                            table_id,
                            column_id));
                    }
                    else
                    {
                        transaction->exec(fmt::format(
                            "INSERT INTO {} (table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) "
                            "VALUES ({}, {}, {}, {}, {}, {}, NULL)",
                            connection->qualified("ducklake_table_column_stats"),
                            table_id,
                            column_id,
                            bounds.contains_null ? "true" : "false",
                            bounds.contains_nan ? "true" : "false",
                            sqlNullableString(new_min),
                            sqlNullableString(new_max)));
                    }
                }
            }

            transaction->exec(fmt::format(
                "INSERT INTO {} (snapshot_id, changes_made) VALUES ({}, {})",
                connection->qualified("ducklake_snapshot_changes"),
                new_snapshot_id,
                quoteLiteral(fmt::format("inserted_into_table:{}", table_id))));

            transaction->commit();
            return new_snapshot_id;
        }
        catch (const DuckLakeCommitConflictException &)
        {
            if (attempt + 1 >= max_commit_attempts)
                throw Exception(
                    ErrorCodes::DUCKLAKE_CATALOG_ERROR,
                    "DuckLake catalog commit kept conflicting with concurrent writers after {} attempts; retry the insert",
                    max_commit_attempts);
        }
    }
}

DB::DatabaseDataLakeCatalogType DuckLakeCatalog::getCatalogType() const
{
    return DB::DatabaseDataLakeCatalogType::DUCKLAKE;
}

bool DuckLakeCatalog::isPostgres() const
{
    return sqlite_database_path.empty();
}

Int64 DuckLakeCatalog::pinSnapshot(IDuckLakeConnection & conn) const
{
    const auto result = conn.exec(fmt::format("SELECT COALESCE(MAX(snapshot_id), 0) FROM {}", conn.qualified("ducklake_snapshot")));
    if (result.rows.empty())
        return 0;
    return parseInt64(result.rows[0][0], "snapshot_id");
}

std::shared_ptr<DuckLakeCatalog::SnapshotRead> DuckLakeCatalog::beginSnapshotRead() const
{
    auto read = std::make_shared<SnapshotRead>();
    read->conn = connection->beginReadTx();
    read->snapshot_id = pinSnapshot(*read->conn);
    return read;
}

std::shared_ptr<DuckLakeCatalog::SnapshotRead> DuckLakeCatalog::beginSnapshotReadAt(Int64 snapshot_id) const
{
    auto read = std::make_shared<SnapshotRead>();
    read->conn = connection->beginReadTx();
    read->snapshot_id = snapshot_id;
    return read;
}

bool DuckLakeCatalog::empty() const
{
    return getTables().empty();
}

DuckLakeCatalog::Namespaces DuckLakeCatalog::getNamespaces() const
{
    const Int64 snapshot = pinSnapshot(*connection);
    const auto result = connection->exec(fmt::format(
        "SELECT schema_name FROM {} WHERE {} ORDER BY schema_name",
        connection->qualified("ducklake_schema"),
        visibilityPredicate(snapshot, "ducklake_schema")));

    Namespaces namespaces;
    namespaces.reserve(result.rows.size());
    for (const auto & row : result.rows)
        namespaces.push_back(row[0].value_or(""));
    return namespaces;
}

DB::Names DuckLakeCatalog::listTablesInNamespaceDirect(const std::string & namespace_name) const
{
    const Int64 snapshot = pinSnapshot(*connection);
    const auto result = connection->exec(fmt::format(
        "SELECT t.table_name FROM {0} t "
        "JOIN {1} s ON s.schema_id = t.schema_id "
        "WHERE s.schema_name = {2} AND {3} AND {4} "
        "ORDER BY t.table_name",
        connection->qualified("ducklake_table"),
        connection->qualified("ducklake_schema"),
        quoteLiteral(namespace_name),
        visibilityPredicate(snapshot, "s"),
        visibilityPredicate(snapshot, "t")));

    DB::Names tables;
    tables.reserve(result.rows.size());
    for (const auto & row : result.rows)
        tables.push_back(namespace_name + "." + row[0].value_or(""));
    return tables;
}

DB::Names DuckLakeCatalog::getTables() const
{
    DB::Names result;
    for (const auto & namespace_name : getNamespaces())
    {
        auto tables = listTablesInNamespaceDirect(namespace_name);
        result.insert(result.end(), std::make_move_iterator(tables.begin()), std::make_move_iterator(tables.end()));
    }
    return result;
}

std::optional<std::pair<Int64, Int64>> DuckLakeCatalog::findTable(IDuckLakeConnection & conn, const String & namespace_name, const String & table_name, Int64 snapshot_id) const
{
    const auto result = conn.exec(fmt::format(
        "SELECT t.table_id, s.schema_id FROM {0} t "
        "JOIN {1} s ON s.schema_id = t.schema_id "
        "WHERE s.schema_name = {2} AND t.table_name = {3} AND {4} AND {5}",
        conn.qualified("ducklake_table"),
        conn.qualified("ducklake_schema"),
        quoteLiteral(namespace_name),
        quoteLiteral(table_name),
        visibilityPredicate(snapshot_id, "s"),
        visibilityPredicate(snapshot_id, "t")));

    if (result.rows.empty())
        return std::nullopt;
    return std::make_pair(parseInt64(result.rows[0][0], "table_id"), parseInt64(result.rows[0][1], "schema_id"));
}

bool DuckLakeCatalog::existsTable(const std::string & namespace_name, const std::string & table_name) const
{
    return findTable(*connection, namespace_name, table_name, pinSnapshot(*connection)).has_value();
}

namespace
{

String joinPaths(const String & base, const String & suffix)
{
    if (base.empty() || base.ends_with('/'))
        return base + suffix;
    return base + "/" + suffix;
}

}

String DuckLakeCatalog::getTableDataPath(IDuckLakeConnection & conn, const String & namespace_name, const String & table_name, Int64 snapshot_id) const
{
    const auto result = conn.exec(fmt::format(
        "SELECT s.path, s.path_is_relative, t.path, t.path_is_relative FROM {0} t "
        "JOIN {1} s ON s.schema_id = t.schema_id "
        "WHERE s.schema_name = {2} AND t.table_name = {3} AND {4} AND {5}",
        conn.qualified("ducklake_table"),
        conn.qualified("ducklake_schema"),
        quoteLiteral(namespace_name),
        quoteLiteral(table_name),
        visibilityPredicate(snapshot_id, "s"),
        visibilityPredicate(snapshot_id, "t")));

    if (result.rows.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake table {}.{} does not exist", namespace_name, table_name);

    const auto & row = result.rows[0];
    const String schema_path = row[0].value_or("");
    const bool schema_path_relative = parseBool(row[1]);
    const String table_path = row[2].value_or("");
    const bool table_path_relative = parseBool(row[3]);

    String location = schema_path_relative || schema_path.empty() ? joinPaths(data_path, schema_path) : schema_path;
    location = table_path_relative || table_path.empty() ? joinPaths(location, table_path) : table_path;

    if (location.find("://") == String::npos)
        location = "file://" + location;
    return location;
}

std::vector<DuckLake::ColumnInfo> DuckLakeCatalog::getColumnRows(IDuckLakeConnection & conn, Int64 table_id) const
{
    const auto result = conn.exec(fmt::format(
        "SELECT column_id, parent_column, column_order, column_name, column_type, nulls_allowed, "
        "begin_snapshot, end_snapshot FROM {} WHERE table_id = {} ORDER BY column_id",
        conn.qualified("ducklake_column"),
        table_id));

    std::vector<DuckLake::ColumnInfo> columns;
    columns.reserve(result.rows.size());
    for (const auto & row : result.rows)
    {
        DuckLake::ColumnInfo info;
        info.column_id = parseInt64(row[0], "column_id");
        if (row[1].has_value())
            info.parent_column = std::stoll(*row[1]);
        info.column_order = parseInt64(row[2], "column_order");
        info.name = row[3].value_or("");
        info.type = row[4].value_or("");
        info.nulls_allowed = parseBool(row[5]);
        info.begin_snapshot = parseInt64(row[6], "begin_snapshot");
        if (row[7].has_value())
            info.end_snapshot = std::stoll(*row[7]);
        columns.push_back(std::move(info));
    }
    return columns;
}

DuckLakeTableSnapshotInfo DuckLakeCatalog::getTableSnapshotInfo(IDuckLakeConnection & conn, const String & namespace_name, const String & table_name, Int64 snapshot_id) const
{
    const Int64 snapshot = snapshot_id;
    const auto table = findTable(conn, namespace_name, table_name, snapshot);
    if (!table.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake table {}.{} does not exist", namespace_name, table_name);
    const auto [table_id, schema_id] = *table;

    const auto column_rows = getColumnRows(conn, table_id);
    auto roots = DuckLake::buildColumnTree(column_rows, snapshot);

    std::unordered_map<Int64, NameAndTypePair> column_types;
    std::function<void(const DuckLake::ColumnNode &)> collect_types = [&](const DuckLake::ColumnNode & node)
    {
        column_types.emplace(node.info.column_id, NameAndTypePair(node.info.name, DuckLake::getColumnType(node)));
        for (const auto & child : node.children)
            collect_types(child);
    };
    for (const auto & root : roots)
        collect_types(root);

    return DuckLakeTableSnapshotInfo{
        .snapshot_id = snapshot,
        .table_id = table_id,
        .schema = DuckLake::getTableSchema(roots),
        .field_id_map = DuckLake::buildFieldIdMap(column_rows, snapshot),
        .column_types = std::move(column_types),
    };
}

DuckLakeFileListing DuckLakeCatalog::getDataFiles(IDuckLakeConnection & conn, Int64 table_id, Int64 snapshot_id, const DuckLakeListingOptions & options) const
{
    DuckLakeFileListing listing;

    const auto data_files = conn.exec(fmt::format(
        "SELECT data.data_file_id, data.path, data.path_is_relative, data.record_count, data.file_size_bytes, "
        "data.encryption_key, data.mapping_id, data.file_format, data.partition_id, "
        "del.path, del.path_is_relative, del.format, del.delete_count, del.encryption_key "
        "FROM {0} data "
        "LEFT JOIN {1} del ON del.data_file_id = data.data_file_id AND {2} "
        "WHERE data.table_id = {3} AND {4} "
        "ORDER BY data.data_file_id",
        conn.qualified("ducklake_data_file"),
        conn.qualified("ducklake_delete_file"),
        visibilityPredicate(snapshot_id, "del"),
        table_id,
        visibilityPredicate(snapshot_id, "data")));

    for (const auto & row : data_files.rows)
    {
        const Int64 data_file_id = parseInt64(row[0], "data_file_id");

        if (row[5].has_value() && !row[5]->empty())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake data file '{}' is encrypted; Parquet modular encryption is not supported",
                row[1].value_or(""));
        const String file_format = Poco::toLower(row[7].value_or("parquet"));
        if (file_format != "parquet")
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake data file '{}' has format '{}' which is not supported (only 'parquet')",
                row[1].value_or(""),
                file_format);

        if (listing.files.empty() || listing.files.back().data_file_id != data_file_id)
        {
            DuckLakeDataFileEntry entry{
                .data_file_id = data_file_id,
                .path = row[1].value_or(""),
                .path_is_relative = parseBool(row[2]),
                .record_count = row[3].has_value() ? std::stoll(*row[3]) : 0,
                .file_size_bytes = row[4].has_value() ? std::stoll(*row[4]) : 0,
                .partition_id = std::nullopt,
                .mapping_id = std::nullopt,
                .name_mapping = {},
                .delete_files = {},
                .column_stats = {},
                .partition_values = {},
                .inlined_deleted_positions = {},
            };
            if (row[8].has_value())
                entry.partition_id = std::stoll(*row[8]);
            if (row[6].has_value())
                entry.mapping_id = std::stoll(*row[6]);
            listing.files.push_back(std::move(entry));
        }

        if (row[9].has_value())
        {
            const String delete_format = Poco::toLower(row[11].value_or("parquet"));
            if (delete_format != "parquet")
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "DuckLake delete file '{}' has format '{}' (puffin deletion vectors are not supported)",
                    row[9].value_or(""),
                    delete_format);
            if (row[13].has_value() && !row[13]->empty())
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "DuckLake delete file '{}' is encrypted; Parquet modular encryption is not supported",
                    row[9].value_or(""));

            listing.files.back().delete_files.push_back(DuckLakeDeleteFileEntry{
                .path = row[9].value_or(""),
                .path_is_relative = parseBool(row[10]),
                .delete_count = row[12].has_value() ? std::stoll(*row[12]) : 0,
            });
        }
    }

    if (listing.files.empty())
        return listing;

    /// ducklake_file_column_stats / ducklake_file_partition_value historically had no index on
    /// table_id (DuckDB owns the schema), so `WHERE table_id = ...` seq-scanned every row ever
    /// written; restrict both reads to the snapshot-visible files. Batch the id list: a single
    /// IN (...) over ~10^7+ ids exceeds postgres's 1GB allocation cap during parse/plan
    /// ("invalid memory alloc request size 1073741824"), which is exactly the catalog scale
    /// this scoping exists for. 100k ids per batch keeps each statement ~1MB.
    static constexpr size_t ducklake_stats_id_batch = 100000;

    /// Stats are read only for the columns the caller can prune on; a caller with no
    /// min/max-usable filter skips this read entirely (the side table holds ~10^8 rows
    /// on a busy catalog, and fetching it dominates the listing cost there).
    if (!options.stats_column_ids.has_value() || !options.stats_column_ids->empty())
    {
        String column_filter;
        if (options.stats_column_ids.has_value())
        {
            column_filter = " AND column_id IN (";
            for (size_t i = 0; i < options.stats_column_ids->size(); ++i)
            {
                if (i > 0)
                    column_filter += ',';
                column_filter += std::to_string((*options.stats_column_ids)[i]);
            }
            column_filter += ")";
        }

        for (size_t batch_begin = 0; batch_begin < listing.files.size(); batch_begin += ducklake_stats_id_batch)
        {
            const size_t batch_end = std::min(batch_begin + ducklake_stats_id_batch, listing.files.size());
            String batch_ids = "(";
            for (size_t i = batch_begin; i < batch_end; ++i)
            {
                if (i > batch_begin)
                    batch_ids += ',';
                batch_ids += std::to_string(listing.files[i].data_file_id);
            }
            batch_ids += ")";

            const auto stats = conn.exec(fmt::format(
                "SELECT data_file_id, column_id, value_count, null_count, contains_nan, min_value, max_value "
                "FROM {} WHERE table_id = {} AND data_file_id IN {}{}",
                conn.qualified("ducklake_file_column_stats"),
                table_id,
                batch_ids,
                column_filter));
        for (const auto & row : stats.rows)
        {
            const Int64 data_file_id = parseInt64(row[0], "data_file_id");
            auto it = std::lower_bound(
                listing.files.begin(), listing.files.end(), data_file_id,
                [](const DuckLakeDataFileEntry & entry, Int64 id) { return entry.data_file_id < id; });
            if (it == listing.files.end() || it->data_file_id != data_file_id)
                continue;
            it->column_stats.push_back(DuckLakeFileColumnStats{
                .column_id = parseInt64(row[1], "column_id"),
                .value_count = row[2].has_value() ? std::stoll(*row[2]) : 0,
                .null_count = row[3].has_value() ? std::stoll(*row[3]) : 0,
                .contains_nan = parseBool(row[4]),
                .min_value = row[5],
                .max_value = row[6],
            });
        }
        }
    }

    /// Partition values feed partition pruning and the hive-partition column
    /// reconstruction for name-mapped files (ducklake_add_data_files). An unfiltered
    /// scan skips this read, but never when a listed file carries a name mapping: its
    /// is_partition entries need the values regardless of the filter.
    bool any_mapped_files = false;
    for (const auto & file : listing.files)
    {
        if (file.mapping_id.has_value())
        {
            any_mapped_files = true;
            break;
        }
    }
    if (options.fetch_partition_values || any_mapped_files)
    {
        for (size_t batch_begin = 0; batch_begin < listing.files.size(); batch_begin += ducklake_stats_id_batch)
        {
            const size_t batch_end = std::min(batch_begin + ducklake_stats_id_batch, listing.files.size());
            String batch_ids = "(";
            for (size_t i = batch_begin; i < batch_end; ++i)
            {
                if (i > batch_begin)
                    batch_ids += ',';
                batch_ids += std::to_string(listing.files[i].data_file_id);
            }
            batch_ids += ")";

            const auto partition_values = conn.exec(fmt::format(
                "SELECT data_file_id, partition_key_index, partition_value FROM {} WHERE table_id = {} AND data_file_id IN {}",
                conn.qualified("ducklake_file_partition_value"),
                table_id,
                batch_ids));
            for (const auto & row : partition_values.rows)
            {
                const Int64 data_file_id = parseInt64(row[0], "data_file_id");
                const auto partition_key_index = static_cast<size_t>(parseInt64(row[1], "partition_key_index"));
                auto it = std::lower_bound(
                    listing.files.begin(), listing.files.end(), data_file_id,
                    [](const DuckLakeDataFileEntry & entry, Int64 id) { return entry.data_file_id < id; });
                if (it == listing.files.end() || it->data_file_id != data_file_id)
                    continue;
                if (it->partition_values.size() <= partition_key_index)
                    it->partition_values.resize(partition_key_index + 1);
                it->partition_values[partition_key_index] = row[2];
            }
        }
    }

    const auto partition_specs = conn.exec(fmt::format(
        "SELECT pc.partition_id, pc.partition_key_index, pc.column_id, pc.transform "
        "FROM {0} pc "
        "JOIN {1} pi ON pi.partition_id = pc.partition_id AND {2} "
        "WHERE pc.table_id = {3} "
        "ORDER BY pc.partition_id, pc.partition_key_index",
        conn.qualified("ducklake_partition_column"),
        conn.qualified("ducklake_partition_info"),
        visibilityPredicate(snapshot_id, "pi"),
        table_id));
    for (const auto & row : partition_specs.rows)
    {
        const Int64 partition_id = parseInt64(row[0], "partition_id");
        listing.partition_specs[partition_id].push_back(DuckLakePartitionField{
            .partition_key_index = parseInt64(row[1], "partition_key_index"),
            .column_id = parseInt64(row[2], "column_id"),
            .transform = row[3].value_or(""),
        });
    }

    /// Name mappings for files added via ducklake_add_data_files (mapping_id NOT NULL).
    /// ducklake_name_mapping rows form a tree via parent_column (mapping row id of the
    /// parent entry); flatten it to dotted source paths per mapping_id.
    {
        std::unordered_map<Int64, size_t> position_by_mapping_id;
        for (size_t i = 0; i < listing.files.size(); ++i)
        {
            if (listing.files[i].mapping_id.has_value())
                position_by_mapping_id.emplace(*listing.files[i].mapping_id, i);
        }
        if (!position_by_mapping_id.empty())
        {
            const auto column_mappings = conn.exec(fmt::format(
                "SELECT mapping_id, type FROM {} WHERE table_id = {}",
                conn.qualified("ducklake_column_mapping"),
                table_id));
            for (const auto & row : column_mappings.rows)
            {
                const String mapping_type = row[1].value_or("");
                if (mapping_type != "map_by_name")
                    throw Exception(
                        ErrorCodes::SUPPORT_IS_DISABLED,
                        "DuckLake column mapping type '{}' is not supported (only 'map_by_name')",
                        mapping_type);
            }

            const auto name_mappings = conn.exec(fmt::format(
                "SELECT nm.mapping_id, nm.column_id, nm.source_name, nm.target_field_id, "
                "nm.parent_column, nm.is_partition "
                "FROM {0} nm "
                "JOIN {1} cm ON cm.mapping_id = nm.mapping_id "
                "WHERE cm.table_id = {2}",
                conn.qualified("ducklake_name_mapping"),
                conn.qualified("ducklake_column_mapping"),
                table_id));

            struct RawNameMapRow
            {
                Int64 mapping_id;
                String source_name;
                Int64 target_field_id;
                std::optional<Int64> parent_column;
                bool is_partition;
            };
            std::unordered_map<Int64, RawNameMapRow> rows_by_column_id;
            for (const auto & row : name_mappings.rows)
            {
                const Int64 column_id = parseInt64(row[1], "column_id");
                RawNameMapRow raw{
                    .mapping_id = parseInt64(row[0], "mapping_id"),
                    .source_name = row[2].value_or(""),
                    .target_field_id = parseInt64(row[3], "target_field_id"),
                    .parent_column = std::nullopt,
                    .is_partition = parseBool(row[5]),
                };
                if (row[4].has_value())
                    raw.parent_column = std::stoll(*row[4]);
                rows_by_column_id.emplace(column_id, std::move(raw));
            }

            for (const auto & [column_id, raw] : rows_by_column_id)
            {
                const auto file_it = position_by_mapping_id.find(raw.mapping_id);
                if (file_it == position_by_mapping_id.end())
                    continue;

                /// Walk the parent chain to build the dotted source path; guard against
                /// catalog corruption (cycles) with a depth limit.
                String source_path = raw.source_name;
                std::vector<Int64> ancestor_field_ids;
                auto parent = raw.parent_column;
                for (size_t depth = 0; parent.has_value() && depth < 1000; ++depth)
                {
                    const auto parent_it = rows_by_column_id.find(*parent);
                    if (parent_it == rows_by_column_id.end())
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "DuckLake name mapping (id {}) references unknown parent column {}",
                            raw.mapping_id,
                            *parent);
                    source_path = parent_it->second.source_name + "." + source_path;
                    ancestor_field_ids.push_back(parent_it->second.target_field_id);
                    parent = parent_it->second.parent_column;
                }
                if (parent.has_value())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "DuckLake name mapping (id {}) has a cycle in parent_column links",
                        raw.mapping_id);

                listing.files[file_it->second].name_mapping.push_back(DuckLakeNameMapEntry{
                    .source_path = std::move(source_path),
                    .target_field_id = raw.target_field_id,
                    .ancestor_field_ids = std::move(ancestor_field_ids),
                    .is_partition = raw.is_partition,
                });
            }
        }
    }

    /// Inlined deletions live in ducklake_inlined_delete_N (file_id, row_id, begin_snapshot),
    /// where row_id is the file-relative position (same as the pos column of delete files).
    /// ducklake_inlined_delete rows are physically removed by a flush; reading them inside
    /// the snapshot transaction is what keeps them consistent with the pinned snapshot.
    const String inlined_deletes_table = fmt::format("ducklake_inlined_delete_{}", table_id);
    if (conn.tableExists(inlined_deletes_table))
    {
        std::unordered_map<Int64, size_t> position_by_file_id;
        for (size_t i = 0; i < listing.files.size(); ++i)
            position_by_file_id.emplace(listing.files[i].data_file_id, i);

        const auto inlined_deletes = conn.exec(fmt::format(
            "SELECT file_id, row_id FROM {} WHERE begin_snapshot <= {}",
            conn.qualified(inlined_deletes_table),
            snapshot_id));
        for (const auto & row : inlined_deletes.rows)
        {
            const Int64 file_id = parseInt64(row[0], "file_id");
            const Int64 row_id = parseInt64(row[1], "row_id");
            auto it = position_by_file_id.find(file_id);
            if (it == position_by_file_id.end())
                continue;
            auto & file = listing.files[it->second];
            if (row_id < 0 || row_id >= file.record_count)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Inlined deletion at position {} is out of bounds for DuckLake data file '{}' "
                    "(record_count {})",
                    row_id,
                    file.path,
                    file.record_count);
            file.inlined_deleted_positions.push_back(static_cast<UInt64>(row_id));
        }
    }

    return listing;
}

std::vector<DuckLakeInlinedDataTable> DuckLakeCatalog::getInlinedDataTables(IDuckLakeConnection & conn, Int64 table_id) const
{
    const auto result = conn.exec(fmt::format(
        "SELECT table_name, schema_version FROM {} WHERE table_id = {}",
        conn.qualified("ducklake_inlined_data_tables"),
        table_id));

    std::vector<DuckLakeInlinedDataTable> tables;
    for (const auto & row : result.rows)
    {
        const String table_name = row[0].value_or("");
        if (table_name.empty() || !conn.tableExists(table_name))
            continue;
        tables.push_back(DuckLakeInlinedDataTable{
            .table_name = table_name,
            .schema_version = parseInt64(row[1], "schema_version"),
        });
    }
    return tables;
}

std::pair<std::vector<String>, std::vector<std::vector<std::optional<String>>>>
DuckLakeCatalog::getInlinedRows(IDuckLakeConnection & conn, const String & inlined_table, Int64 snapshot_id) const
{
    auto result = conn.exec(fmt::format(
        "SELECT * FROM {} inlined WHERE {} ORDER BY row_id",
        conn.qualified(inlined_table),
        visibilityPredicate(snapshot_id, "inlined")));
    return {std::move(result.column_names), std::move(result.rows)};
}

UInt64 DuckLakeCatalog::getInlinedRowCount(IDuckLakeConnection & conn, const String & inlined_table, Int64 snapshot_id) const
{
    const auto result = conn.exec(fmt::format(
        "SELECT COUNT(*) FROM {} inlined WHERE {}",
        conn.qualified(inlined_table),
        visibilityPredicate(snapshot_id, "inlined")));
    if (result.rows.empty())
        return 0;
    return static_cast<UInt64>(parseInt64(result.rows[0][0], "count"));
}

std::map<Int64, Int64> DuckLakeCatalog::getSchemaVersionFirstSnapshots(IDuckLakeConnection & conn) const
{
    const auto result = conn.exec(fmt::format(
        "SELECT schema_version, MIN(snapshot_id) FROM {} GROUP BY schema_version",
        conn.qualified("ducklake_snapshot")));

    std::map<Int64, Int64> versions;
    for (const auto & row : result.rows)
        versions.emplace(parseInt64(row[0], "schema_version"), parseInt64(row[1], "snapshot_id"));
    return versions;
}

void DuckLakeCatalog::getTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    DataLake::TableMetadata & result) const
{
    if (!tryGetTableMetadata(namespace_name, table_name, result))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DuckLake table {}.{} does not exist", namespace_name, table_name);
}

bool DuckLakeCatalog::tryGetTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    DataLake::TableMetadata & result) const
{
    /// One session for the whole lookup: pin + find + schema + path all observe the same
    /// catalog snapshot (previously this pinned three separate times).
    const auto read = beginSnapshotRead();
    const Int64 snapshot = read->snapshot_id;
    if (!findTable(*read->conn, namespace_name, table_name, snapshot).has_value())
        return false;

    const auto info = getTableSnapshotInfo(*read->conn, namespace_name, table_name, snapshot);
    const String location = getTableDataPath(*read->conn, namespace_name, table_name, snapshot);
    result.setLocation(location);
    result.setSchema(info.schema);
    result.setDataLakeSpecificProperties(DataLake::DataLakeSpecificProperties{
        .iceberg_metadata_file_location = "",
        .ducklake_schema_name = namespace_name,
        .ducklake_table_name = table_name,
    });
    /// DuckLake never vends credentials: local tables need none, and object-storage tables
    /// authenticate via the environment credential chain (IRSA in the mw deployment).
    if (result.requiresCredentials())
        result.setStorageCredentials(std::make_shared<NoCredentials>());
    return true;
}

}
