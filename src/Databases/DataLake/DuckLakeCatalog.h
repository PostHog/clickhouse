#pragma once

#include <Databases/DataLake/ICatalog.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeTypes.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <mutex>
#include <map>

namespace DB
{

class IDuckLakeConnection;

/// One row of ducklake_delete_file joined to its data file.
struct DuckLakeDeleteFileEntry
{
    String path;
    bool path_is_relative;
    Int64 delete_count;
};

/// One row of ducklake_file_column_stats for one data file.
struct DuckLakeFileColumnStats
{
    Int64 column_id;
    Int64 value_count;
    Int64 null_count;
    bool contains_nan;
    /// Min/max serialized by the DuckLake writer (plain numbers, ISO dates/timestamps,
    /// raw strings); empty when the file has no values for the column.
    std::optional<String> min_value;
    std::optional<String> max_value;
};

/// One row of ducklake_name_mapping, flattened to a dotted parquet-side source path.
struct DuckLakeNameMapEntry
{
    /// Dotted parquet column path (struct children joined by '.'; list/map children are
    /// structural and excluded by the metadata layer).
    String source_path;
    /// Catalog column id this path maps to.
    Int64 target_field_id;
    /// Target ids of the ancestor mapping entries (used to detect structural parents).
    std::vector<Int64> ancestor_field_ids;
    /// Value comes from the partition path, not from the parquet content.
    bool is_partition;
};

/// One row of ducklake_data_file (visible at a pinned snapshot) with its delete files,
/// column statistics, partition values and inlined deletions.
struct DuckLakeDataFileEntry
{
    Int64 data_file_id;
    String path;
    bool path_is_relative;
    Int64 record_count;
    Int64 file_size_bytes;
    /// Partition spec this file was written with (index into DuckLakeFileListing::partition_specs).
    std::optional<Int64> partition_id;
    /// ducklake_column_mapping.mapping_id when the file was added via ducklake_add_data_files
    /// and its parquet columns must be matched by name.
    std::optional<Int64> mapping_id;
    std::vector<DuckLakeNameMapEntry> name_mapping;
    std::vector<DuckLakeDeleteFileEntry> delete_files;
    std::vector<DuckLakeFileColumnStats> column_stats;
    /// Partition values serialized like stats values, indexed by partition_key_index.
    std::vector<std::optional<String>> partition_values;
    /// File-relative positions deleted via the catalog's inlined deletion table
    /// (ducklake_inlined_delete_N), already translated from global row ids.
    std::vector<UInt64> inlined_deleted_positions;
};

/// One row of ducklake_partition_column: one partition key of one partition spec.
struct DuckLakePartitionField
{
    Int64 partition_key_index;
    Int64 column_id;
    /// identity, year, month, day, hour or bucket(N).
    String transform;
};

/// Result of DuckLakeCatalog::getDataFiles: the visible files plus the partition specs
/// they reference (partition_id -> fields sorted by partition_key_index).
struct DuckLakeFileListing
{
    std::vector<DuckLakeDataFileEntry> files;
    std::unordered_map<Int64, std::vector<DuckLakePartitionField>> partition_specs;
};

/// One entry of ducklake_inlined_data_tables: an SQL table in the catalog database that
/// holds inlined insert rows for one schema version of a table.
struct DuckLakeInlinedDataTable
{
    String table_name;
    Int64 schema_version;
};

/// One new data file to register in the catalog (DuckLakeCatalog::appendDataFiles).
struct DuckLakeNewDataFile
{
    /// Per-column statistics of the file (ducklake_file_column_stats). Min/max are
    /// serialized exactly like the values the read side parses (plain numbers, ISO
    /// dates/timestamps, raw strings).
    struct ColumnStats
    {
        Int64 column_id;
        Int64 value_count;
        Int64 null_count;
        bool contains_nan;
        std::optional<String> min_value;
        std::optional<String> max_value;
    };

    /// Path relative to the table's data path (registered with path_is_relative = true).
    String path;
    Int64 record_count;
    Int64 file_size_bytes;
    std::vector<ColumnStats> column_stats;
    /// Serialized partition values indexed by partition_key_index; empty for unpartitioned
    /// tables.
    std::vector<std::optional<String>> partition_values;
};

/// Thrown when a catalog commit loses the optimistic-concurrency race on ducklake_snapshot
/// (another writer committed concurrently). Retryable: re-pin and redo the commit.
class DuckLakeCommitConflictException : public Exception
{
public:
    using Exception::Exception;
};

/// The table's partition spec visible at a snapshot: its partition_id and the fields
/// sorted by partition_key_index. partition_id is nullopt when the table is unpartitioned.
struct DuckLakeCurrentPartitionSpec
{
    std::optional<Int64> partition_id;
    std::vector<DuckLakePartitionField> fields;
};

/// Everything DuckLakeMetadata needs to serve reads at one pinned snapshot.
struct DuckLakeTableSnapshotInfo
{
    Int64 snapshot_id;
    Int64 table_id;
    NamesAndTypesList schema;
    std::unordered_map<String, Int64> field_id_map;
    /// column_id -> name+type of every currently visible column (including nested children,
    /// whose names are their bare element names; only ids and types matter for them).
    std::unordered_map<Int64, NameAndTypePair> column_types;
};

/// Read-only DuckLake catalog on top of PostgreSQL or SQLite, implementing the DuckLake 1.0
/// metadata schema (ducklake_metadata_manager.cpp in the DuckLake repository).
/// Which side-table reads DuckLakeCatalog::getDataFiles should do. On a busy catalog
/// these tables are the largest in the schema, so callers scope them to what they can
/// actually use.
struct DuckLakeListingOptions
{
    /// Per-column min/max stats to read: nullopt = all columns (default), empty =
    /// skip the ducklake_file_column_stats read entirely, non-empty = only these
    /// column ids. Fewer stats never causes wrong pruning, only less.
    std::optional<std::vector<Int64>> stats_column_ids;
    /// Skip the ducklake_file_partition_value read (callers that cannot
    /// partition-prune, e.g. unfiltered scans). Missing values never cause wrong
    /// pruning, only less.
    bool fetch_partition_values = true;
};

class DuckLakeCatalog : public DataLake::ICatalog
{
public:
    DuckLakeCatalog(
        const std::string & warehouse_,
        const std::string & backend_,
        const std::string & connection_string_,
        const std::string & catalog_schema_,
        ContextPtr context_);
    ~DuckLakeCatalog() override;

    DB::DatabaseDataLakeCatalogType getCatalogType() const override;

    bool empty() const override;
    DB::Names getTables() const override;
    Namespaces getNamespaces() const;
    bool existsTable(const std::string & namespace_name, const std::string & table_name) const override;
    void getTableMetadata(const std::string & namespace_name, const std::string & table_name, DataLake::TableMetadata & result) const override;
    bool tryGetTableMetadata(const std::string & namespace_name, const std::string & table_name, DataLake::TableMetadata & result) const override;
    std::optional<DataLake::StorageType> getStorageType() const override { return std::nullopt; }
    bool isTransactional() const override { return false; }

    /// Whether the catalog backend is PostgreSQL (affects how inlined values are serialized:
    /// strings/blobs are bytea hex, booleans are t/f, timestamps are DuckDB text).
    bool isPostgres() const;

    /// Whether this catalog accepts writes (PostgreSQL backend only; the catalog schema
    /// version was validated at construction).
    bool supportsWrites() const;

    /// The partition spec of `table_id` visible at `snapshot_id` (partition_id nullopt when
    /// the table has no partition spec there).
    DuckLakeCurrentPartitionSpec getCurrentPartitionSpec(Int64 table_id, Int64 snapshot_id) const;

    /// Atomically register `files` for `table_id` in one catalog transaction: a new
    /// ducklake_snapshot row, ducklake_data_file / ducklake_file_column_stats /
    /// ducklake_file_partition_value rows, ducklake_table_stats /
    /// ducklake_table_column_stats updates and a ducklake_snapshot_changes entry.
    /// Retries on optimistic-concurrency conflicts (DuckLakeCommitConflictException).
    /// Returns the committed snapshot_id.
    Int64 appendDataFiles(
        Int64 table_id,
        const std::optional<Int64> & partition_id,
        const std::vector<DuckLakeNewDataFile> & files);

    /// One pinned catalog snapshot read through its own transaction. `conn` is a
    /// transaction-scoped view of the catalog (REPEATABLE READ READ ONLY on postgres):
    /// every statement through it observes exactly `snapshot_id`, including rows a
    /// concurrent flush physically removed (inlined deletes/data). Visibility-filtered
    /// reads alone cannot see those — a flush between two autocommit statements both
    /// resurrects deleted rows and loses flushed ones — so a query holds one SnapshotRead
    /// from pinning through its last catalog read. Destroying it ends the transaction.
    struct SnapshotRead
    {
        std::unique_ptr<IDuckLakeConnection> conn;
        Int64 snapshot_id;
    };

    /// Open a snapshot transaction and pin MAX(snapshot_id) inside it, so the pin and all
    /// subsequent reads through the session are one consistent catalog view.
    std::shared_ptr<SnapshotRead> beginSnapshotRead() const;

    /// Same, but read at a caller-chosen snapshot id (parallel-replicas propagation /
    /// time travel): the transaction still guarantees a consistent view, the id is just
    /// not re-pinned. The id must be <= the current MAX; catalog rows are never rewritten,
    /// so any historical id remains readable (subject to external snapshot expiry).
    std::shared_ptr<SnapshotRead> beginSnapshotReadAt(Int64 snapshot_id) const;

    /// Load schema + field-id map for one table at `snapshot_id`.
    /// Throws if the table does not exist at that snapshot.
    DuckLakeTableSnapshotInfo getTableSnapshotInfo(IDuckLakeConnection & conn, const String & namespace_name, const String & table_name, Int64 snapshot_id) const;

    /// List data files (with delete files, column stats, partition values and inlined
    /// deletions) visible at `snapshot_id`, plus the partition specs they reference.
    /// Throws on unsupported per-file features (encryption, name mapping, puffin).
    DuckLakeFileListing getDataFiles(
        IDuckLakeConnection & conn, Int64 table_id, Int64 snapshot_id, const DuckLakeListingOptions & options = {}) const;

    /// Inlined data tables registered for `table_id` (ducklake_inlined_data_tables).
    /// Tables that do not exist in the catalog are skipped (already dropped by a flush).
    std::vector<DuckLakeInlinedDataTable> getInlinedDataTables(IDuckLakeConnection & conn, Int64 table_id) const;

    /// SQL column names and row values of `inlined_table` visible at `snapshot_id`
    /// (begin/end snapshot visibility), ordered by row_id.
    /// Returns empty column_names when the table does not exist.
    std::pair<std::vector<String>, std::vector<std::vector<std::optional<String>>>>
    getInlinedRows(IDuckLakeConnection & conn, const String & inlined_table, Int64 snapshot_id) const;

    /// Number of rows of `inlined_table` visible at `snapshot_id` (COUNT with the
    /// begin/end snapshot visibility predicate). 0 when the table does not exist.
    UInt64 getInlinedRowCount(IDuckLakeConnection & conn, const String & inlined_table, Int64 snapshot_id) const;

    /// Global schema_version -> first snapshot_id that has it (ducklake_snapshot is global).
    /// Inlined data tables are named with the global schema version at their creation, so
    /// this maps an inlined table's schema_version to the snapshot whose visible column
    /// names match the inlined table's SQL columns.
    std::map<Int64, Int64> getSchemaVersionFirstSnapshots(IDuckLakeConnection & conn) const;

    /// All ducklake_column rows (full history) of `table_id`, sorted by column_id.
    std::vector<DuckLake::ColumnInfo> getColumnRows(IDuckLakeConnection & conn, Int64 table_id) const;

    /// Absolute table data path (with URI scheme) as resolved from data_path + schema/table paths.
    String getTableDataPath(IDuckLakeConnection & conn, const String & namespace_name, const String & table_name, Int64 snapshot_id) const;

protected:
    DB::Names listTablesInNamespaceDirect(const std::string & namespace_name) const;

private:
    std::unique_ptr<IDuckLakeConnection> connection;
    /// From ducklake_metadata: the catalog-wide data path prefix, exactly as stored.
    String data_path;
    /// Raw sqlite database file path (sqlite backend only), used to resolve a relative data_path.
    String sqlite_database_path;

    Int64 pinSnapshot(IDuckLakeConnection & conn) const;
    std::optional<std::pair<Int64, Int64>> findTable(IDuckLakeConnection & conn, const String & namespace_name, const String & table_name, Int64 snapshot_id) const;
};

}
