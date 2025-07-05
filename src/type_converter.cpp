#include "include/type_converter.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/string_util.hpp"
#include <sstream>

// TODO: Add Arrow includes once available
// #include <arrow/type.h>
// #include <arrow/array.h>

namespace duckdb {

// ===== STATIC LOOKUP TABLES =====

const std::unordered_map<LogicalTypeId, std::string> SnowflakeTypeConverter::direct_snowflake_map_ = {
    {LogicalTypeId::TINYINT, "NUMBER(3,0)"},
    {LogicalTypeId::SMALLINT, "NUMBER(5,0)"},
    {LogicalTypeId::INTEGER, "NUMBER(10,0)"},
    {LogicalTypeId::BIGINT, "NUMBER(19,0)"},
    {LogicalTypeId::FLOAT, "FLOAT"},
    {LogicalTypeId::DOUBLE, "DOUBLE"},
    {LogicalTypeId::VARCHAR, "VARCHAR"},
    {LogicalTypeId::BLOB, "BINARY"},
    {LogicalTypeId::BOOLEAN, "BOOLEAN"},
    {LogicalTypeId::DATE, "DATE"},
    {LogicalTypeId::TIME, "TIME"},
    {LogicalTypeId::TIMESTAMP, "TIMESTAMP_NTZ"},
    {LogicalTypeId::TIMESTAMP_TZ, "TIMESTAMP_TZ"}
};

const std::unordered_map<LogicalTypeId, std::string> SnowflakeTypeConverter::arrow_equivalents_ = {
    {LogicalTypeId::TINYINT, "int8"},
    {LogicalTypeId::SMALLINT, "int16"},
    {LogicalTypeId::INTEGER, "int32"},
    {LogicalTypeId::BIGINT, "int64"},
    {LogicalTypeId::FLOAT, "float32"},
    {LogicalTypeId::DOUBLE, "float64"},
    {LogicalTypeId::VARCHAR, "utf8"},
    {LogicalTypeId::BLOB, "binary"},
    {LogicalTypeId::BOOLEAN, "bool"},
    {LogicalTypeId::DATE, "date32"},
    {LogicalTypeId::TIME, "time64[us]"},
    {LogicalTypeId::TIMESTAMP, "timestamp[us]"},
    {LogicalTypeId::TIMESTAMP_TZ, "timestamp[us, UTC]"}
};

const std::unordered_map<std::string, LogicalTypeId> SnowflakeTypeConverter::reverse_type_map_ = {
    {"int8", LogicalTypeId::TINYINT},
    {"int16", LogicalTypeId::SMALLINT},
    {"int32", LogicalTypeId::INTEGER},
    {"int64", LogicalTypeId::BIGINT},
    {"float32", LogicalTypeId::FLOAT},
    {"float64", LogicalTypeId::DOUBLE},
    {"utf8", LogicalTypeId::VARCHAR},
    {"binary", LogicalTypeId::BLOB},
    {"bool", LogicalTypeId::BOOLEAN},
    {"date32", LogicalTypeId::DATE},
    {"time64[us]", LogicalTypeId::TIME},
    {"timestamp[us]", LogicalTypeId::TIMESTAMP},
    {"timestamp[us, UTC]", LogicalTypeId::TIMESTAMP_TZ}
};

// ===== PRIMARY CONVERSION FUNCTIONS =====

SnowflakeTypeConverter::ConversionResult<std::shared_ptr<arrow::DataType>>
SnowflakeTypeConverter::ConvertDuckDBToArrow(const LogicalType& duckdb_type) {
    // Lookup direct arrow mapping
    auto it = arrow_equivalents_.find(duckdb_type.id());
    if (it != arrow_equivalents_.end()) {
        // Create Arrow type based on string description
        if (it->second == "int8") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::int8()));
        if (it->second == "int16") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::int16()));
        if (it->second == "int32") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::int32()));
        if (it->second == "int64") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::int64()));
        if (it->second == "float32") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::float32()));
        if (it->second == "float64") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::float64()));
        if (it->second == "utf8") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::utf8()));
        if (it->second == "binary") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::binary()));
        if (it->second == "bool") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::boolean()));
        if (it->second == "date32") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::date32()));
        if (it->second == "time64[us]") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::time64(arrow::TimeUnit::MICRO)));
        if (it->second == "timestamp[us]") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::timestamp(arrow::TimeUnit::MICRO)));
        if (it->second == "timestamp[us, UTC]") return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(std::shared_ptr<arrow::DataType>(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")));
    }
    // DECIMAL handling
    if (duckdb_type.id() == LogicalTypeId::DECIMAL) {
        auto precision = DecimalType::GetWidth(duckdb_type);
        auto scale = DecimalType::GetScale(duckdb_type);
        if (precision > 38) {
            return ConversionResult<std::shared_ptr<arrow::DataType>>::Error(
                "Decimal precision exceeds Arrow limit"
            );
        }
        return ConversionResult<std::shared_ptr<arrow::DataType>>::Success(
            std::shared_ptr<arrow::DataType>(arrow::decimal128(precision, scale))
        );
    }
    return ConversionResult<std::shared_ptr<arrow::DataType>>::Error(
        "Unsupported DuckDB type for Arrow conversion"
    );
}

SnowflakeTypeConverter::ConversionResult<std::string>
SnowflakeTypeConverter::ConvertArrowToSnowflake(const std::string& arrow_type_desc) {
    // Primitives
    if (arrow_type_desc == "int8")   return ConversionResult<std::string>::Success("NUMBER(3,0)");
    if (arrow_type_desc == "int16")  return ConversionResult<std::string>::Success("NUMBER(5,0)");
    if (arrow_type_desc == "int32")  return ConversionResult<std::string>::Success("NUMBER(10,0)");
    if (arrow_type_desc == "int64")  return ConversionResult<std::string>::Success("NUMBER(19,0)");
    if (arrow_type_desc == "float32")return ConversionResult<std::string>::Success("FLOAT");
    if (arrow_type_desc == "float64")return ConversionResult<std::string>::Success("DOUBLE");
    if (arrow_type_desc == "utf8")   return ConversionResult<std::string>::Success("VARCHAR");
    if (arrow_type_desc == "binary") return ConversionResult<std::string>::Success("BINARY");
    if (arrow_type_desc == "bool")   return ConversionResult<std::string>::Success("BOOLEAN");
    if (arrow_type_desc == "date32") return ConversionResult<std::string>::Success("DATE");
    if (arrow_type_desc == "time64[us]") return ConversionResult<std::string>::Success("TIME");
    if (arrow_type_desc == "timestamp[us]")     return ConversionResult<std::string>::Success("TIMESTAMP_NTZ");
    if (arrow_type_desc == "timestamp[us, UTC]")return ConversionResult<std::string>::Success("TIMESTAMP_TZ");
    // Decimal128(p,s)
    if (arrow_type_desc.rfind("decimal128(", 0) == 0) {
        auto params = arrow_type_desc.substr(10, arrow_type_desc.size() - 11);
        auto comma = params.find(',');
        auto p = params.substr(0, comma), s = params.substr(comma+1);
        return ConversionResult<std::string>::Success("NUMBER(" + p + "," + s + ")");
    }
    return ConversionResult<std::string>::Error("Unsupported Arrow type: " + arrow_type_desc);
}

SnowflakeTypeConverter::ConversionResult<std::string>
SnowflakeTypeConverter::ConvertDuckDBToSnowflake(const LogicalType& duckdb_type) {
    // DECIMAL
    if (duckdb_type.id() == LogicalTypeId::DECIMAL) {
        auto p = DecimalType::GetWidth(duckdb_type);
        auto s = DecimalType::GetScale(duckdb_type);
        return ConversionResult<std::string>::Success("NUMBER(" + std::to_string(p) + "," + std::to_string(s) + ")");
    }
    // Nested
    if (duckdb_type.id() == LogicalTypeId::LIST ||
        duckdb_type.id() == LogicalTypeId::STRUCT ||
        duckdb_type.id() == LogicalTypeId::MAP ||
        duckdb_type.id() == LogicalTypeId::UNION) {
        return ConvertNestedType(duckdb_type);
    }
    // Direct mapping
    auto it = direct_snowflake_map_.find(duckdb_type.id());
    if (it != direct_snowflake_map_.end()) {
        return ConversionResult<std::string>::Success(std::string(it->second));
    }
    return ConversionResult<std::string>::Error("Unsupported DuckDB type");
}

SnowflakeTypeConverter::ConversionResult<LogicalType>
SnowflakeTypeConverter::ConvertSnowflakeToDuckDB(const std::string& snowflake_type) {
    // NUMBER(p,s)
    if (snowflake_type.rfind("NUMBER", 0) == 0) {
        auto params = snowflake_type.substr(snowflake_type.find('(')+1,
            snowflake_type.find(')') - snowflake_type.find('(') - 1);
        auto comma = params.find(',');
        if (comma != std::string::npos) {
            int p = stoi(params.substr(0,comma)), s = stoi(params.substr(comma+1));
            return ConversionResult<LogicalType>::Success(LogicalType::DECIMAL(p,s));
        }
        return ConversionResult<LogicalType>::Success(LogicalType::DECIMAL(stoi(params),0));
    }
    // Primitive reverse
    auto it = reverse_type_map_.find(snowflake_type);
    if (it != reverse_type_map_.end()) {
        return ConversionResult<LogicalType>::Success(LogicalType(it->second));
    }
    // VARIANT → VARCHAR (since JSON doesn't exist in DuckDB)
    if (snowflake_type == "VARIANT") {
        return ConversionResult<LogicalType>::Success(LogicalType::VARCHAR);
    }
    // OBJECT/ARRAY/MAP → STRUCT/LIST/MAP
    if (snowflake_type == "OBJECT") return ConversionResult<LogicalType>::Success(LogicalType::STRUCT({}));
    if (snowflake_type == "ARRAY")  return ConversionResult<LogicalType>::Success(LogicalType::LIST(LogicalType::VARCHAR));
    if (snowflake_type == "MAP")    return ConversionResult<LogicalType>::Success(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

    return ConversionResult<LogicalType>::Error("Unsupported Snowflake type: " + snowflake_type);
}

// Nested support
SnowflakeTypeConverter::ConversionResult<std::string>
SnowflakeTypeConverter::ConvertNestedType(const LogicalType& duckdb_type) {
    switch (duckdb_type.id()) {
        case LogicalTypeId::LIST:   return ConversionResult<std::string>::Success("ARRAY");
        case LogicalTypeId::STRUCT: return ConversionResult<std::string>::Success("OBJECT");
        case LogicalTypeId::MAP:    return ConversionResult<std::string>::Success("MAP");
        case LogicalTypeId::UNION:  return ConversionResult<std::string>::Success("VARIANT");
        default: return ConversionResult<std::string>::Error("Unsupported nested type");
    }
}

// Decimal adjustment
SnowflakeTypeConverter::DecimalAdjustment
SnowflakeTypeConverter::AdjustDecimalForSnowflake(uint8_t precision, uint8_t scale) {
    DecimalAdjustment a{precision, scale, false, false, std::string("")};
    if (precision > 38) {
        a.adjusted_precision = 38; a.precision_reduced = true;
        a.warning_message = "Precision reduced to 38";
    }
    return a;
}

} // namespace duckdb 