#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeDataObjectInfo.h>

#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_PROTOCOL;
}

void DuckLakeObjectSerializableInfo::checkVersion(size_t protocol_version) const
{
    if (protocol_version < DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DUCKLAKE_METADATA)
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "DuckLake object metadata requires cluster processing protocol version >= {}, got {}",
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DUCKLAKE_METADATA,
            protocol_version);
}

void DuckLakeObjectSerializableInfo::serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const
{
    checkVersion(protocol_version);

    writeVarUInt(positional_delete_files.size(), out);
    for (const auto & delete_file : positional_delete_files)
    {
        writeStringBinary(delete_file.path, out);
        writeVarInt(delete_file.delete_count, out);
    }

    if (record_count.has_value())
    {
        writeVarUInt(1, out);
        writeVarInt(*record_count, out);
    }
    else
    {
        writeVarUInt(0, out);
    }
    if (file_size_bytes.has_value())
    {
        writeVarUInt(1, out);
        writeVarInt(*file_size_bytes, out);
    }
    else
    {
        writeVarUInt(0, out);
    }

    writeVarUInt(inlined_deleted_positions.size(), out);
    for (const auto position : inlined_deleted_positions)
        writeVarUInt(position, out);

    writeVarUInt(column_mapper_encoding.size(), out);
    for (const auto & [name, field_id] : column_mapper_encoding)
    {
        writeStringBinary(name, out);
        writeVarInt(field_id, out);
    }

    writeVarUInt(partition_constants.size(), out);
    for (const auto & constant : partition_constants)
    {
        writeStringBinary(constant.column_name, out);
        writeStringBinary(constant.type_name, out);
        writeFieldBinary(constant.value, out);
    }
}

void DuckLakeObjectSerializableInfo::deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version)
{
    checkVersion(protocol_version);

    size_t delete_files_size = 0;
    readVarUInt(delete_files_size, in);
    positional_delete_files.reserve(delete_files_size);
    for (size_t i = 0; i < delete_files_size; ++i)
    {
        PositionalDeleteFile delete_file;
        readStringBinary(delete_file.path, in);
        readVarInt(delete_file.delete_count, in);
        positional_delete_files.push_back(std::move(delete_file));
    }

    {
        UInt64 has_record_count = 0;
        readVarUInt(has_record_count, in);
        if (has_record_count)
        {
            Int64 value = 0;
            readVarInt(value, in);
            record_count = value;
        }
        UInt64 has_file_size = 0;
        readVarUInt(has_file_size, in);
        if (has_file_size)
        {
            Int64 value = 0;
            readVarInt(value, in);
            file_size_bytes = value;
        }
    }

    size_t positions_size = 0;
    readVarUInt(positions_size, in);
    inlined_deleted_positions.reserve(positions_size);
    for (size_t i = 0; i < positions_size; ++i)
    {
        UInt64 position = 0;
        readVarUInt(position, in);
        inlined_deleted_positions.push_back(position);
    }

    size_t encoding_size = 0;
    readVarUInt(encoding_size, in);
    column_mapper_encoding.reserve(encoding_size);
    for (size_t i = 0; i < encoding_size; ++i)
    {
        String name;
        Int64 field_id = 0;
        readStringBinary(name, in);
        readVarInt(field_id, in);
        column_mapper_encoding.emplace_back(std::move(name), field_id);
    }

    size_t constants_size = 0;
    readVarUInt(constants_size, in);
    partition_constants.reserve(constants_size);
    for (size_t i = 0; i < constants_size; ++i)
    {
        PartitionConstant constant;
        readStringBinary(constant.column_name, in);
        readStringBinary(constant.type_name, in);
        constant.value = readFieldBinary(in);
        partition_constants.push_back(std::move(constant));
    }
}

}
