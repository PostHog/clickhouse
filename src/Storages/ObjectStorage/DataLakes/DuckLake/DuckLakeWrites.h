#pragma once

#include "config.h"

#if USE_PARQUET

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Databases/DataLake/DuckLakeCatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Formats/FormatFilterInfo.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Common/DateLUTImpl.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

/// Sink writing DuckLake data files (Parquet with catalog field ids) and registering them
/// in the catalog on finish. One Parquet file per distinct partition key of the inserted
/// data (DuckLake partition values are per-file); unpartitioned tables get one file.
/// Files are written under hive-style directories (`col=value/ducklake-<uuid>.parquet`)
/// mirroring the DuckDB layout; partition values live in the catalog, not in the path
/// (no name mapping is registered, the files carry field ids).
class DuckLakeStorageSink final : public SinkToStorage
{
public:
    DuckLakeStorageSink(
        ObjectStoragePtr object_storage_,
        String storage_table_path_,
        std::shared_ptr<DuckLakeCatalog> catalog_,
        Int64 table_id_,
        DuckLakeCurrentPartitionSpec partition_spec_,
        std::unordered_map<Int64, NameAndTypePair> column_types_by_id_,
        ColumnMapperPtr column_mapper_,
        const std::optional<FormatSettings> & format_settings_,
        SharedHeader sample_block_,
        ContextPtr context_);

    String getName() const override { return "DuckLakeStorageSink"; }

    void consume(Chunk & chunk) override;
    void onFinish() override;
    void onException(std::exception_ptr exception) override;

private:
    struct PartitionFieldEval
    {
        enum class Transform
        {
            Identity,
            Year,
            Month,
            Day,
            Hour,
        };

        Int64 partition_key_index;
        size_t column_index;
        Transform transform;
        /// Column name and the hive directory key (column name for identity, transform name
        /// for calendar transforms — mirroring the DuckDB layout, e.g. `year=2023/month=6`).
        String column_name;
        String dir_name;
        DataTypePtr type;
        /// Calendar transforms only: the lookup table and DateTime64 scale.
        const DateLUTImpl * lut = nullptr;
        UInt32 scale = 0;
    };

    struct ColumnStatsAccumulator
    {
        Int64 column_id;
        size_t column_index;
        DataTypePtr type;
        Int64 value_count = 0;
        Int64 null_count = 0;
        bool contains_nan = false;
        std::optional<Field> min_value{};
        std::optional<Field> max_value{};

        void update(const IColumn & column, size_t row);
        DuckLakeNewDataFile::ColumnStats finalize() const;
    };

    using PartitionKey = std::vector<std::optional<String>>;
    struct PartitionKeyHasher
    {
        size_t operator()(const PartitionKey & key) const;
    };

    struct PartitionWriter
    {
        PartitionKey key;
        String relative_path;
        String storage_path;
        std::unique_ptr<WriteBufferFromFileBase> buffer;
        OutputFormatPtr output_format;
        Int64 num_rows = 0;
        std::vector<ColumnStatsAccumulator> stats;

        void finalizeFile();
        void cancel();
    };

    ObjectStoragePtr object_storage;
    String storage_table_path;
    std::shared_ptr<DuckLakeCatalog> catalog;
    Int64 table_id;
    DuckLakeCurrentPartitionSpec partition_spec;
    std::vector<PartitionFieldEval> partition_evals;
    ColumnMapperPtr column_mapper;
    std::optional<FormatSettings> format_settings;
    SharedHeader sample_block;
    ContextPtr context;

    /// Stats accumulators are the same for every partition writer; this is the template.
    std::vector<ColumnStatsAccumulator> stats_template;

    std::unordered_map<PartitionKey, size_t, PartitionKeyHasher> writer_index_by_key;
    std::vector<std::unique_ptr<PartitionWriter>> writers;
    bool committed = false;

    LoggerPtr log = getLogger("DuckLakeStorageSink");

    PartitionWriter & getOrCreateWriter(PartitionKey key);
};

}

#endif
