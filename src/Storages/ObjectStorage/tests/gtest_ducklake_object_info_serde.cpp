#include <gtest/gtest.h>

#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeDataObjectInfo.h>

using namespace DB;

namespace
{

DuckLakeObjectSerializableInfo makeInfo()
{
    DuckLakeObjectSerializableInfo info;
    info.positional_delete_files = {
        {.path = "deletes/d1.parquet", .delete_count = 3},
        {.path = "deletes/d2.parquet", .delete_count = 17},
    };
    info.record_count = 100;
    info.file_size_bytes = 4242;
    info.inlined_deleted_positions = {1, 5, 90};
    info.column_mapper_encoding = {{"person_id", 1}, {"props.os", 42}};
    info.partition_constants = {
        {.column_name = "team_id", .type_name = "Int64", .value = Field(Int64(566139))},
        {.column_name = "day", .type_name = "Date", .value = Field(UInt16(19850))},
    };
    return info;
}

void expectEqual(const DuckLakeObjectSerializableInfo & a, const DuckLakeObjectSerializableInfo & b)
{
    ASSERT_EQ(a.positional_delete_files.size(), b.positional_delete_files.size());
    for (size_t i = 0; i < a.positional_delete_files.size(); ++i)
    {
        EXPECT_EQ(a.positional_delete_files[i].path, b.positional_delete_files[i].path);
        EXPECT_EQ(a.positional_delete_files[i].delete_count, b.positional_delete_files[i].delete_count);
    }
    EXPECT_EQ(a.record_count, b.record_count);
    EXPECT_EQ(a.file_size_bytes, b.file_size_bytes);
    EXPECT_EQ(a.inlined_deleted_positions, b.inlined_deleted_positions);
    EXPECT_EQ(a.column_mapper_encoding, b.column_mapper_encoding);
    ASSERT_EQ(a.partition_constants.size(), b.partition_constants.size());
    for (size_t i = 0; i < a.partition_constants.size(); ++i)
    {
        EXPECT_EQ(a.partition_constants[i].column_name, b.partition_constants[i].column_name);
        EXPECT_EQ(a.partition_constants[i].type_name, b.partition_constants[i].type_name);
        EXPECT_EQ(a.partition_constants[i].value, b.partition_constants[i].value);
    }
}

}

TEST(DuckLakeObjectInfoSerde, RoundTrip)
{
    const auto info = makeInfo();

    WriteBufferFromOwnString out;
    info.serializeForClusterFunctionProtocol(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);

    DuckLakeObjectSerializableInfo decoded;
    ReadBufferFromString in(out.str());
    decoded.deserializeForClusterFunctionProtocol(in, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);

    expectEqual(info, decoded);
}

TEST(DuckLakeObjectInfoSerde, EmptyOptionals)
{
    DuckLakeObjectSerializableInfo info;
    /// No delete files, no record/size hints, no mapper, no constants.

    WriteBufferFromOwnString out;
    info.serializeForClusterFunctionProtocol(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);

    DuckLakeObjectSerializableInfo decoded;
    ReadBufferFromString in(out.str());
    decoded.deserializeForClusterFunctionProtocol(in, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION);

    expectEqual(info, decoded);
    EXPECT_FALSE(decoded.record_count.has_value());
    EXPECT_FALSE(decoded.file_size_bytes.has_value());
}

TEST(DuckLakeObjectInfoSerde, OldProtocolRejected)
{
    const auto info = makeInfo();
    WriteBufferFromOwnString out;
    EXPECT_THROW(
        info.serializeForClusterFunctionProtocol(out, DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_FILE_STATS),
        Exception);
}
