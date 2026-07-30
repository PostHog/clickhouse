# DuckLake Write Path — Design

Scope: `INSERT INTO` for DuckLake tables backed by a **PostgreSQL** catalog only.
SQLite catalogs remain read-only. Everything else (CREATE/ALTER/DROP TABLE, DELETE/UPDATE,
inlined inserts, compaction) is out of scope for the first iteration.

## 1. Current state (read-only, merged in PR #1)

- Catalog: `DuckLakeCatalog` (`src/Databases/DataLake/DuckLakeCatalog.{h,cpp}`) — implements
  `DataLake::ICatalog` over `IDuckLakeConnection`, a **query-only** interface
  (`exec(SELECT ...)`, one mutex). Two backends: `DuckLakePostgresConnection` (libpqxx
  `pqxx::nontransaction`) and `DuckLakeSQLiteConnection`.
- Per-table metadata: `DuckLakeMetadata : IDataLakeMetadata`
  (`src/Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeMetadata.{h,cpp}`), pinned to one
  `snapshot_id` per query, immutable (`supportsUpdate() == false`).
- Writes are rejected in three places:
  - `IDataLakeMetadata::supportsWrites()` defaults to `false` → `StorageObjectStorage::write`
    throws `NOT_IMPLEMENTED` (`StorageObjectStorage.cpp:697`).
  - `DuckLakeMetadata::createInitial` throws `UNSUPPORTED_METHOD`.
  - `ICatalog::{createTable,updateMetadata,updateSchema,dropTable}` throw `NOT_IMPLEMENTED`.
- Read side already consumes everything a writer must produce:
  `ducklake_snapshot`, `ducklake_snapshot_changes`, `ducklake_data_file`,
  `ducklake_file_column_stats`, `ducklake_file_partition_value`, `ducklake_partition_info`,
  `ducklake_partition_column`, `ducklake_table_stats`, `ducklake_table_column_stats`.

## 2. DuckLake commit model (from DuckDB's `ducklake_metadata_manager.cpp`)

A DuckLake commit is **one SQL transaction** against the catalog database:

1. `BEGIN`.
2. Read the latest snapshot row: `SELECT snapshot_id, schema_version, next_catalog_id,
   next_file_id FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1`.
3. Insert a new snapshot: `snapshot_id = old + 1`, same `schema_version` (unchanged for pure
   data appends), `next_catalog_id` / `next_file_id` advanced by the ids this commit consumed.
4. Insert all `ducklake_data_file` / `ducklake_file_column_stats` /
   `ducklake_file_partition_value` rows with `begin_snapshot = new_snapshot_id`,
   `data_file_id` allocated from `next_file_id`.
5. Update `ducklake_table_stats` (record_count, next_row_id, file_size_bytes) and, when new
   min/max values fall outside the stored bounds, `ducklake_table_column_stats`.
6. Insert a `ducklake_snapshot_changes` row (`changes_made = 'inserted_data_file'`).
7. `COMMIT`.

**Concurrency**: optimistic, enforced by the database. `ducklake_snapshot.snapshot_id` is the
PRIMARY KEY, so two concurrent writers inserting `old + 1` conflict; exactly one commits.
The loser rolls back and retries the whole commit (re-list, re-pin, re-insert). Written data
files whose catalog transaction rolled back are orphans — acceptable (DuckLake has
`ducklake_files_scheduled_for_deletion` / cleanup tooling); never delete files written by
someone else's snapshot.

PostgreSQL only: use `pqxx::work` (real transactions). To make the conflict deterministic and
the retry cheap, also `SELECT ... FOR UPDATE` the table's row in `ducklake_table_stats`
(step 5's target) at the start of the transaction — this serializes writers to the *same
table* while allowing different tables to commit concurrently, and turns cross-table
conflicts into the primary-key retry path.

Retry policy: bounded (e.g. 10 attempts, exponential backoff with jitter), throw
`NOT_IMPLEMENTED`-style `DUCKLAKE_COMMIT_CONFLICT`-ish error after exhaustion (mirrors
Iceberg's "Metadata changed during write operation, try again",
`IcebergWrites.cpp:1181`).

## 3. Components

### 3.1 Catalog write API — `DuckLakeCatalog` + `IDuckLakeConnection`

`IDuckLakeConnection` (`DuckLakeCatalog.cpp:92`) gains a transaction interface:

```cpp
class IDuckLakeTransaction
{
public:
    virtual ~IDuckLakeTransaction() = default;
    virtual void exec(const String & query) = 0;            // no result rows
    virtual ResultSet query(const String & query) = 0;      // with result rows
    virtual void commit() = 0;                              // throws on conflict
};
virtual std::unique_ptr<IDuckLakeTransaction> beginTransaction() = 0;
```

- Postgres backend: `pqxx::work`; `commit()` maps `pqxx::sql_error` with SQLSTATE
  `23505` (unique_violation) / `40001` (serialization_failure) to a dedicated
  `CommitConflictException` so the retry loop can catch it precisely.
- SQLite backend: `beginTransaction()` throws `UNSUPPORTED_METHOD`
  ("writes require a PostgreSQL DuckLake catalog").

New `DuckLakeCatalog` methods (all take/return plain structs, no SQL leaking to callers):

```cpp
struct DuckLakeNewDataFile
{
    String path;                 // relative to the table's data path
    Int64 record_count;
    Int64 file_size_bytes;
    Int64 footer_size;
    struct ColumnStats { Int64 column_id; Int64 value_count; Int64 null_count;
                         std::optional<String> min_value, max_value; bool contains_nan; };
    std::vector<ColumnStats> column_stats;
    /// Serialized partition values indexed by partition_key_index; empty if unpartitioned.
    std::vector<std::optional<String>> partition_values;
};

/// Atomically registers `files` for `table_id` in one catalog transaction.
/// Retries on snapshot conflicts. Returns the committed snapshot_id.
Int64 appendDataFiles(Int64 table_id, const std::vector<DuckLakeNewDataFile> & files);
```

`appendDataFiles` encapsulates: snapshot pin → id allocation → INSERTs (batched,
multi-row `INSERT ... VALUES (...), (...)` to keep round-trips flat) → stats updates →
`snapshot_changes` → commit → retry loop. Column stats min/max serialization must match the
reader exactly (`DuckLakeFileColumnStats` parsing in `DuckLakeCatalog.cpp` and
`DuckLakeInlinedValues.cpp`): plain numbers, ISO dates/timestamps, strings verbatim.

Also needed: `getCurrentPartitionSpec(table_id)` — the visible `ducklake_partition_info` +
`ducklake_partition_column` rows at the pinned snapshot (read side already has the query in
`getDataFiles`; factor it out) so the sink knows the partition transforms and can emit
hive-style paths that `DuckLakePartitionConstantsTransform` reads back.

### 3.2 Sink — `DuckLakeStorageSink` (new files under `src/Storages/ObjectStorage/DataLakes/DuckLake/`)

`DuckLakeWrites.{h,cpp}`, modelled on `IcebergWrites.{h,cpp}` but much smaller:

- `DuckLakeStorageSink : SinkToStorage`
  - `consume(Chunk)`: buffer → partition (if the table has a partition spec) → write Parquet
    files via the existing `StorageObjectStorageSink` / `MultipleFileWriter` machinery.
  - File naming: `<storage_table_path>/ducklake-<uuid>.parquet`, relative path registered in
    the catalog with `path_is_relative = true` (matches what the read side resolves via
    `storage_table_path + '/' + path`).
  - Partitioning: reuse `ChunkPartitioner` (`src/Storages/ObjectStorage/DataLakes/Iceberg/ChunkPartitioner.h`)
    if it fits DuckLake transforms (identity/year/month/day/hour/bucket); otherwise a small
    DuckLake-specific partitioner keyed on the serialized partition values. Output path
    layout: hive-style `col=value/...` so `DuckLakePartitionConstantsTransform` reads
    partition constants back consistently.
  - Parquet schema must use the **catalog column ids as parquet field ids** for top-level
    and nested columns, matching the `ColumnMapper` encoding used by the read path
    (`DuckLakeMetadata.cpp:269`). This is the same mechanism as Iceberg's field-id writing
    (`parquet_write_field_ids`-style format settings); verify against
    `ColumnMapper`-based reads of the freshly written files.
  - Statistics: collect per-file record counts, null counts, min/max per column id while
    writing. Reuse `Iceberg/DataFileStatistics.{h,cpp}` where types allow; otherwise a
    DuckLake-native collector producing `DuckLakeNewDataFile::ColumnStats` with the
    reader-compatible string serialization.
- `onFinish()`: compute file sizes via object storage metadata, build
  `std::vector<DuckLakeNewDataFile>`, call `catalog->appendDataFiles(...)`. On commit
  conflict after retries: throw (files stay orphaned; log their paths).
- `onException()`: best-effort removal of files this sink wrote (fail-close: log and
  surface, never delete unknown files).

### 3.3 Metadata plumbing — `DuckLakeMetadata`

- `supportsWrites()` → `true` when the catalog `isPostgres()` **and** the gate setting is on.
- `write(...)` (`IDataLakeMetadata.h:126` signature): construct `DuckLakeStorageSink` with
  the pinned `snapshot_id`/`table_id`/schema/`column_types_by_id`, the object storage, and
  the `DuckLakeCatalog` (downcast from `DataLake::ICatalog`).
- `supportsParallelInsert()` → `true`: parallel inserts are just independent commits; the
  snapshot-conflict retry already handles them.
- Gate setting: follow the Iceberg precedent (`allow_insert_into_iceberg`). Add
  `allow_insert_into_ducklake` (or reuse a shared setting if one exists by then).

`StorageObjectStorage::write` already routes through `IDataLakeMetadata::write` with the
catalog (`StorageObjectStorage.cpp:666-701`); no routing changes needed.

### 3.4 What is deliberately NOT in this iteration

- `CREATE TABLE` / schema evolution (`ducklake_column` history, `ducklake_name_mapping`,
  `schema_version` bumps). Tables are created by DuckDB; ClickHouse only appends.
- DELETE/UPDATE (positional delete files, `ducklake_delete_file`).
- Data inlining (`ducklake_inlined_data_tables` — a DuckDB optimization; ClickHouse always
  writes files).
- Compaction / snapshot expiration / orphan cleanup.
- SQLite catalog writes.

## 4. Read-after-write consistency

`DuckLakeMetadata` is pinned per query; a query started after a commit sees the new snapshot
because `getTableSnapshotInfo` re-pins `MAX(snapshot_id)`. A query in flight during a commit
keeps reading its pinned snapshot — exactly DuckLake semantics. The new files written by this
sink must therefore be fully readable by the *existing* read path: file listing
(`getDataFiles`), stats pruning (`DuckLakePruning`), partition constants, name mapping
(none — we write field ids directly). The integration test asserts this round-trip:
write via ClickHouse → read via ClickHouse → read the same table via DuckDB (fixture
generation tooling) to prove catalog validity, and write via DuckDB → read via ClickHouse to
catch schema drift.

## 5. Testing plan

- Extend `tests/integration/test_ducklake_catalog/` (already has a `postgres1` container and
  `ducklake_pg` database):
  - `INSERT INTO ducklake_pg.table VALUES ...` → `SELECT` round-trip, incl. types:
    ints, strings, dates/timestamps, decimals, nested struct/list/map, Variant (PR #1 added
    VARIANT write support in Parquet already).
  - Partitioned table insert (identity + calendar transforms): partition pruning still
    prunes correctly after the insert.
  - Min/max stats pruning on newly written files (`WHERE` outside new file bounds skips it).
  - Concurrent inserts from two clients both succeed (one retries).
  - SQLite catalog: `INSERT` throws a clear `UNSUPPORTED_METHOD`.
- Unit tests (gtest, alongside `gtest_ducklake_types.cpp`): stats value serialization
  (CH value → catalog string) against the same cases `DuckLakeInlinedValues` parses.

## 6. Build order (PR #2 commits)

1. `IDuckLakeConnection` transaction API + Postgres implementation + `CommitConflict`
   plumbing (SQLite throws).
2. `DuckLakeCatalog::appendDataFiles` + `getCurrentPartitionSpec` (+ gtests where possible).
3. `DuckLakeStorageSink` (unpartitioned first) + `DuckLakeMetadata::write` + setting gate.
4. Partitioned writes.
5. Integration tests.
