#pragma once

#include <Core/Field.h>
#include <Formats/FormatFilterInfo.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakePartitionConstantsTransform.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// One data file of a DuckLake table, with the positional delete files bound to it
/// (DuckLake binds delete files 1:1 via ducklake_delete_file.data_file_id).
struct DuckLakeDataObjectInfo : public ObjectInfo
{
    struct PositionalDeleteFile
    {
        /// Path relative to the object storage root (the table data path).
        String path;
        Int64 delete_count;
    };

    explicit DuckLakeDataObjectInfo(
        const String & path_,
        std::vector<PositionalDeleteFile> positional_delete_files_,
        std::optional<Int64> record_count_,
        std::optional<Int64> file_size_bytes_,
        std::vector<UInt64> inlined_deleted_positions_ = {})
        : ObjectInfo(path_)
        , positional_delete_files(std::move(positional_delete_files_))
        , record_count(record_count_)
        , file_size_bytes(file_size_bytes_)
        , inlined_deleted_positions(std::move(inlined_deleted_positions_))
    {
    }

    std::optional<size_t> getFileSizeHint() const
    {
        if (file_size_bytes.has_value())
            return static_cast<size_t>(*file_size_bytes);
        return std::nullopt;
    }

    std::vector<PositionalDeleteFile> positional_delete_files;
    std::optional<Int64> record_count;
    std::optional<Int64> file_size_bytes;
    /// File-relative positions deleted via the catalog's inlined deletion table.
    std::vector<UInt64> inlined_deleted_positions;
    /// Set for files added via ducklake_add_data_files: the parquet columns are matched
    /// by name (ducklake_name_mapping) instead of by field id, so the file needs its own
    /// ColumnMapper instead of the table-wide one.
    ColumnMapperPtr column_mapper;
    /// Hive partition columns whose values come from the catalog rather than the parquet
    /// content (is_partition name mappings).
    std::vector<DuckLakePartitionConstantsTransform::ConstantColumn> partition_constants;

    /// Set when this object is not a parquet file but a DuckLake inlined-data table
    /// (rows stored in the catalog, not yet flushed): the reading node reads the rows
    /// from the catalog at the pinned snapshot instead of opening a file. Emitting
    /// inlined tables as objects is what lets distributed reads hand them to exactly
    /// one replica (an additional per-node pipe would duplicate them).
    String inlined_table_name;
    /// ducklake_inlined_data_tables.schema_version for inlined objects (column names in
    /// the inlined table are as of this global schema version).
    Int64 inlined_schema_version = -1;
};

using DuckLakeDataObjectInfoPtr = std::shared_ptr<DuckLakeDataObjectInfo>;

/// The complete per-file read state of a DuckLakeDataObjectInfo, in a form that can be
/// shipped to a parallel-replicas/cluster secondary through the cluster read-task
/// protocol. Everything a reading node needs to execute the file read exactly as the
/// initiator would — delete files, inlined deletion positions, per-file column mapping
/// (name-mapped files) and catalog-side partition constants. Without it a secondary
/// would reconstruct a bare ObjectInfo from the path and silently skip deletes.
struct DuckLakeObjectSerializableInfo
{
    struct PositionalDeleteFile
    {
        String path;
        Int64 delete_count;
    };
    struct PartitionConstant
    {
        String column_name;
        String type_name;
        Field value;
    };

    std::vector<PositionalDeleteFile> positional_delete_files;
    std::optional<Int64> record_count;
    std::optional<Int64> file_size_bytes;
    std::vector<UInt64> inlined_deleted_positions;
    /// Name -> field-id encoding of the per-file ColumnMapper (ducklake_add_data_files
    /// name-mapped files); empty = use the table-wide mapper.
    std::vector<std::pair<String, Int64>> column_mapper_encoding;
    /// Dotted parquet-side path -> clickhouse name (the other half of the per-file
    /// ColumnMapper for name-mapped files).
    std::vector<std::pair<String, String>> column_mapper_name_mapping;
    std::vector<PartitionConstant> partition_constants;
    /// Non-empty when this task is an inlined-data table, not a parquet file.
    String inlined_table_name;
    Int64 inlined_schema_version = -1;

    void serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const;
    void deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version);

private:
    void checkVersion(size_t protocol_version) const;
};

}
