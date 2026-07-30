#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeWrites.h>

#if USE_PARQUET

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeTypes.h>

#include <Common/Exception.h>
#include <Common/NaNUtils.h>

#include <Poco/String.h>
#include <Poco/UUIDGenerator.h>

#include <fmt/format.h>

#include <numeric>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Serialize a non-NULL field exactly like the read side parses stats/partition values
/// (DuckLake::parseStatsValue): strings verbatim, everything else via the default text
/// serialization.
String serializeStatsField(const Field & value, const DataTypePtr & type_)
{
    const auto type = removeNullable(type_);
    const WhichDataType which(type);
    if (which.isString() || which.isFixedString())
        return value.safeGet<String>();

    auto column = type->createColumn();
    column->insert(value);
    WriteBufferFromOwnString buf;
    FormatSettings format_settings;
    type->getDefaultSerialization()->serializeText(*column, 0, buf, format_settings);
    return buf.str();
}

/// Floor division for possibly negative values.
Int64 floorDiv(Int64 value, Int64 divisor)
{
    return value >= 0 ? value / divisor : -((-value + divisor - 1) / divisor);
}

/// The (year, month, day) of a date/datetime field in the column's timezone.
struct YMD
{
    Int64 year;
    Int64 month;
    Int64 day;
};

YMD civilFromField(const Field & field, const DataTypePtr & type, const DateLUTImpl & lut, UInt32 scale)
{
    const auto nested = removeNullable(type);
    const WhichDataType which(nested);
    if (which.isDate())
    {
        const DayNum day(static_cast<UInt16>(field.safeGet<UInt64>()));
        return {lut.toYear(day), lut.toMonth(day), lut.toDayOfMonth(day)};
    }
    if (which.isDate32())
    {
        const ExtendedDayNum day(static_cast<Int32>(field.safeGet<Int64>()));
        return {lut.toYear(day), lut.toMonth(day), lut.toDayOfMonth(day)};
    }
    if (which.isDateTime())
    {
        const auto time = static_cast<time_t>(field.safeGet<UInt64>());
        return {lut.toYear(time), lut.toMonth(time), lut.toDayOfMonth(time)};
    }
    const auto ticks = field.safeGet<DecimalField<DateTime64>>().getValue().value;
    const Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
    const auto time = static_cast<time_t>(floorDiv(ticks, multiplier));
    return {lut.toYear(time), lut.toMonth(time), lut.toDayOfMonth(time)};
}

/// Seconds since the epoch of a datetime field (floor for sub-second precision).
Int64 epochSecondsFromField(const Field & field, const DataTypePtr & type, UInt32 scale)
{
    const auto nested = removeNullable(type);
    const WhichDataType which(nested);
    if (which.isDateTime())
        return static_cast<Int64>(field.safeGet<UInt64>());
    if (which.isDateTime64())
    {
        const auto ticks = field.safeGet<DecimalField<DateTime64>>().getValue().value;
        return floorDiv(ticks, DecimalUtils::scaleMultiplier<DateTime64>(scale));
    }
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED,
        "DuckLake partition transform 'hour' requires a timestamp column, got '{}'",
        nested->getName());
}

bool isFloatNaN(const DataTypePtr & type, const Field & value)
{
    const WhichDataType which(removeNullable(type));
    return (which.isFloat32() || which.isFloat64()) && isNaN(value.safeGet<Float64>());
}

bool isScalarStatsType(const DataTypePtr & type_)
{
    const auto type = removeNullable(type_);
    const WhichDataType which(type);
    return !which.isTuple() && !which.isArray() && !which.isMap() && !which.isDynamic() && !which.isVariant();
}

}

size_t DuckLakeStorageSink::PartitionKeyHasher::operator()(const PartitionKey & key) const
{
    SipHash hash;
    for (const auto & part : key)
    {
        hash.update(part.has_value());
        if (part.has_value())
            hash.update(*part);
    }
    return hash.get64();
}

void DuckLakeStorageSink::ColumnStatsAccumulator::update(const IColumn & column, size_t row)
{
    if (column.isNullAt(row))
    {
        ++null_count;
        return;
    }
    ++value_count;

    const Field value = column[row];
    if (isFloatNaN(type, value))
    {
        contains_nan = true;
        return; /// NaN is excluded from min/max bounds
    }
    if (!min_value.has_value() || value < *min_value)
        min_value = value;
    if (!max_value.has_value() || *max_value < value)
        max_value = value;
}

DuckLakeNewDataFile::ColumnStats DuckLakeStorageSink::ColumnStatsAccumulator::finalize() const
{
    return DuckLakeNewDataFile::ColumnStats{
        .column_id = column_id,
        .value_count = value_count,
        .null_count = null_count,
        .contains_nan = contains_nan,
        .min_value = min_value.has_value() ? std::make_optional(serializeStatsField(*min_value, type)) : std::nullopt,
        .max_value = max_value.has_value() ? std::make_optional(serializeStatsField(*max_value, type)) : std::nullopt,
    };
}

DuckLakeStorageSink::DuckLakeStorageSink(
    ObjectStoragePtr object_storage_,
    String storage_table_path_,
    std::shared_ptr<DuckLakeCatalog> catalog_,
    Int64 table_id_,
    DuckLakeCurrentPartitionSpec partition_spec_,
    std::unordered_map<Int64, NameAndTypePair> column_types_by_id_,
    ColumnMapperPtr column_mapper_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_)
    : SinkToStorage(sample_block_)
    , object_storage(std::move(object_storage_))
    , storage_table_path(std::move(storage_table_path_))
    , catalog(std::move(catalog_))
    , table_id(table_id_)
    , partition_spec(std::move(partition_spec_))
    , column_mapper(std::move(column_mapper_))
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , context(std::move(context_))
{
    if (format_settings)
    {
        format_settings->parquet.write_page_index = true;
        format_settings->parquet.bloom_filter_push_down = true;
        format_settings->parquet.filter_push_down = true;
    }

    const auto & field_id_map = column_mapper->getStorageColumnEncoding();

    for (const auto & field : partition_spec.fields)
    {
        const auto column_it = column_types_by_id_.find(field.column_id);
        if (column_it == column_types_by_id_.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DuckLake partition column (id {}) is not visible in the table schema",
                field.column_id);

        const String & column_name = column_it->second.name;
        PartitionFieldEval eval{
            .partition_key_index = field.partition_key_index,
            .column_index = sample_block->getPositionByName(column_name),
            .transform = PartitionFieldEval::Transform::Identity,
            .column_name = column_name,
            .dir_name = column_name,
            .type = column_it->second.type,
            .lut = nullptr,
            .scale = 0,
        };

        const String transform = Poco::toLower(field.transform);
        eval.dir_name = transform == "identity" ? column_name : transform;
        const auto nested = removeNullable(eval.type);
        const WhichDataType which(nested);
        if (transform == "identity")
        {
            if (!isScalarStatsType(eval.type))
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "DuckLake identity partition column '{}' has non-scalar type '{}'; writing to such tables is not supported",
                    column_name,
                    nested->getName());
        }
        else if (transform == "year" || transform == "month" || transform == "day")
        {
            if (which.isDate() || which.isDate32())
            {
                eval.lut = &DateLUT::instance();
            }
            else if (which.isDateTime())
            {
                const auto & date_time = assert_cast<const DataTypeDateTime &>(*nested);
                if (!date_time.hasExplicitTimeZone())
                    throw Exception(
                        ErrorCodes::SUPPORT_IS_DISABLED,
                        "DuckLake calendar partition column '{}' is a timestamp without an explicit timezone; "
                        "writing to such tables is not supported",
                        column_name);
                eval.lut = &date_time.getTimeZone();
            }
            else if (which.isDateTime64())
            {
                const auto & date_time = assert_cast<const DataTypeDateTime64 &>(*nested);
                if (!date_time.hasExplicitTimeZone())
                    throw Exception(
                        ErrorCodes::SUPPORT_IS_DISABLED,
                        "DuckLake calendar partition column '{}' is a timestamp without an explicit timezone; "
                        "writing to such tables is not supported",
                        column_name);
                eval.lut = &date_time.getTimeZone();
                eval.scale = date_time.getScale();
            }
            else
            {
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "DuckLake partition transform '{}' requires a date/timestamp column, got '{}'",
                    transform,
                    nested->getName());
            }

            if (transform == "year")
                eval.transform = PartitionFieldEval::Transform::Year;
            else if (transform == "month")
                eval.transform = PartitionFieldEval::Transform::Month;
            else
                eval.transform = PartitionFieldEval::Transform::Day;
        }
        else if (transform == "hour")
        {
            eval.transform = PartitionFieldEval::Transform::Hour;
        }
        else
        {
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "DuckLake partition transform '{}' is not supported by the ClickHouse DuckLake writer "
                "(identity, year, month, day and hour are)",
                field.transform);
        }
        partition_evals.push_back(std::move(eval));
    }

    /// Stats are collected for top-level scalar columns only; nested leaves are skipped
    /// (DuckDB keeps stats for them too, but readers must tolerate their absence).
    for (size_t i = 0; i < sample_block->columns(); ++i)
    {
        const auto & column = sample_block->getByPosition(i);
        const WhichDataType which(removeNullable(column.type));
        if (which.isTime() || which.isTime64())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Writing to DuckLake tables with '{}' columns (column '{}') is not supported yet: "
                "the Parquet writer has no Time support",
                column.type->getName(),
                column.name);
        const auto id_it = field_id_map.find(column.name);
        if (id_it == field_id_map.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DuckLake column '{}' is missing in the catalog field id map",
                column.name);
        if (!isScalarStatsType(column.type))
            continue;
        stats_template.push_back(ColumnStatsAccumulator{
            .column_id = id_it->second,
            .column_index = i,
            .type = column.type,
        });
    }
}

DuckLakeStorageSink::PartitionWriter & DuckLakeStorageSink::getOrCreateWriter(PartitionKey key)
{
    const auto it = writer_index_by_key.find(key);
    if (it != writer_index_by_key.end())
        return *writers[it->second];

    auto writer = std::make_unique<PartitionWriter>();
    writer->key = key;
    writer->stats = stats_template;

    String relative;
    for (size_t i = 0; i < partition_evals.size(); ++i)
    {
        if (i > 0)
            relative += "/";
        relative += fmt::format(
            "{}={}",
            partition_evals[i].dir_name,
            key[i].has_value() ? *key[i] : String("__HIVE_DEFAULT_PARTITION__"));
    }
    if (!relative.empty())
        relative += "/";
    relative += fmt::format("ducklake-{}.parquet", Poco::UUIDGenerator::defaultGenerator().createRandom().toString());

    writer->relative_path = relative;
    writer->storage_path = storage_table_path + "/" + relative;
    writer->buffer = object_storage->writeObject(
        StoredObject(writer->storage_path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    auto format_filter_info = std::make_shared<FormatFilterInfo>(nullptr, context, column_mapper, nullptr, nullptr);
    writer->output_format = FormatFactory::instance().getOutputFormatParallelIfPossible(
        "Parquet", *writer->buffer, *sample_block, context, format_settings, format_filter_info);

    writer_index_by_key.emplace(key, writers.size());
    writers.push_back(std::move(writer));
    return *writers.back();
}

void DuckLakeStorageSink::consume(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    const auto & columns = chunk.getColumns();

    if (partition_evals.empty())
    {
        auto & writer = getOrCreateWriter({});
        writer.output_format->write(sample_block->cloneWithColumns(columns));
        writer.num_rows += static_cast<Int64>(num_rows);
        for (auto & stats : writer.stats)
            for (size_t row = 0; row < num_rows; ++row)
                stats.update(*columns[stats.column_index], row);
        return;
    }

    /// Evaluate the partition key of every row, then scatter the chunk by key.
    std::vector<PartitionKey> keys(num_rows);
    std::unordered_map<PartitionKey, std::vector<UInt64>, PartitionKeyHasher> rows_by_key;
    for (size_t row = 0; row < num_rows; ++row)
    {
        PartitionKey key;
        key.reserve(partition_evals.size());
        for (const auto & eval : partition_evals)
        {
            const IColumn & column = *columns[eval.column_index];
            if (column.isNullAt(row))
            {
                key.emplace_back(std::nullopt);
                continue;
            }
            const Field value = column[row];
            switch (eval.transform)
            {
                case PartitionFieldEval::Transform::Identity:
                    key.emplace_back(serializeStatsField(value, eval.type));
                    break;
                case PartitionFieldEval::Transform::Year:
                    key.emplace_back(fmt::format("{}", civilFromField(value, eval.type, *eval.lut, eval.scale).year));
                    break;
                case PartitionFieldEval::Transform::Month:
                    key.emplace_back(fmt::format("{}", civilFromField(value, eval.type, *eval.lut, eval.scale).month));
                    break;
                case PartitionFieldEval::Transform::Day:
                    key.emplace_back(fmt::format("{}", civilFromField(value, eval.type, *eval.lut, eval.scale).day));
                    break;
                case PartitionFieldEval::Transform::Hour:
                    key.emplace_back(fmt::format("{}", floorDiv(epochSecondsFromField(value, eval.type, eval.scale), 3600)));
                    break;
            }
        }
        rows_by_key[key].push_back(row);
        keys[row] = std::move(key);
    }

    for (const auto & [key, rows] : rows_by_key)
    {
        auto indices_column = ColumnUInt64::create(rows.size());
        auto & indices = indices_column->getData();
        for (size_t i = 0; i < rows.size(); ++i)
            indices[i] = rows[i];

        Columns sub_columns;
        sub_columns.reserve(columns.size());
        for (const auto & column : columns)
            sub_columns.push_back(column->index(*indices_column, rows.size()));

        auto & writer = getOrCreateWriter(key);
        writer.output_format->write(sample_block->cloneWithColumns(sub_columns));
        writer.num_rows += static_cast<Int64>(rows.size());
        for (auto & stats : writer.stats)
            for (size_t row = 0; row < rows.size(); ++row)
                stats.update(*sub_columns[stats.column_index], row);
    }
}

void DuckLakeStorageSink::PartitionWriter::finalizeFile()
{
    output_format->flush();
    output_format->finalize();
    buffer->finalize();
    output_format.reset();
    buffer.reset();
}

void DuckLakeStorageSink::PartitionWriter::cancel()
{
    if (output_format)
        output_format->cancel();
    if (buffer)
        buffer->cancel();
}

void DuckLakeStorageSink::onFinish()
{
    if (committed)
        return;

    std::vector<DuckLakeNewDataFile> files;
    files.reserve(writers.size());
    for (auto & writer : writers)
    {
        writer->finalizeFile();

        Int64 file_size = 0;
        const auto object_metadata = object_storage->tryGetObjectMetadata(writer->storage_path, /*with_tags=*/false);
        if (object_metadata.has_value())
            file_size = static_cast<Int64>(object_metadata->size_bytes);
        DuckLakeNewDataFile file{
            .path = writer->relative_path,
            .record_count = writer->num_rows,
            .file_size_bytes = file_size,
            .column_stats = {},
            .partition_values = writer->key,
        };
        file.column_stats.reserve(writer->stats.size());
        for (const auto & stats : writer->stats)
            file.column_stats.push_back(stats.finalize());
        files.push_back(std::move(file));
    }

    if (!files.empty())
    {
        const Int64 committed_snapshot = catalog->appendDataFiles(table_id, partition_spec.partition_id, files);
        LOG_INFO(
            log,
            "DuckLake: committed {} data file(s) ({} rows) for table (id {}) at snapshot {}",
            files.size(),
            std::accumulate(writers.begin(), writers.end(), Int64(0), [](Int64 sum, const auto & w) { return sum + w->num_rows; }),
            table_id,
            committed_snapshot);
    }
    committed = true;
}

void DuckLakeStorageSink::onException(std::exception_ptr /* exception */)
{
    for (auto & writer : writers)
    {
        writer->cancel();
        /// The catalog commit either never happened or was rolled back: the files this sink
        /// wrote are unreferenced, remove them (fail-close: deletion errors surface).
        object_storage->removeObjectIfExists(StoredObject(writer->storage_path));
    }
    writers.clear();
}

}

#endif
