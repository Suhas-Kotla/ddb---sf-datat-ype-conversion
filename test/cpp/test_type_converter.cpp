#include <iostream>
#include <string>
#include "type_converter.hpp"

using namespace duckdb;

#define TEST_ASSERT(condition, message) \
    if (!(condition)) { \
        std::cout << "✗ FAIL: " << message << std::endl; \
        return false; \
    } else { \
        std::cout << "✓ PASS: " << message << std::endl; \
    }

bool TestIntegerTypes() {
    std::cout << "\n=== Testing Integer Types ===" << std::endl;
    
    auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::TINYINT);
    TEST_ASSERT(result.IsValid(), "TINYINT conversion");
    TEST_ASSERT(result.GetValue() == "NUMBER(3,0)", "TINYINT -> NUMBER(3,0)");

    result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::SMALLINT);
    TEST_ASSERT(result.IsValid(), "SMALLINT conversion");
    TEST_ASSERT(result.GetValue() == "NUMBER(5,0)", "SMALLINT -> NUMBER(5,0)");

    result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::INTEGER);
    TEST_ASSERT(result.IsValid(), "INTEGER conversion");
    TEST_ASSERT(result.GetValue() == "NUMBER(10,0)", "INTEGER -> NUMBER(10,0)");

    result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::BIGINT);
    TEST_ASSERT(result.IsValid(), "BIGINT conversion");
    TEST_ASSERT(result.GetValue() == "NUMBER(19,0)", "BIGINT -> NUMBER(19,0)");

    return true;
}

bool TestFloatingPointTypes() {
    std::cout << "\n=== Testing Floating Point Types ===" << std::endl;
    
    auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::FLOAT);
    TEST_ASSERT(result.IsValid(), "FLOAT conversion");
    TEST_ASSERT(result.GetValue() == "FLOAT", "FLOAT -> FLOAT");

    result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::DOUBLE);
    TEST_ASSERT(result.IsValid(), "DOUBLE conversion");
    TEST_ASSERT(result.GetValue() == "DOUBLE", "DOUBLE -> DOUBLE");

    return true;
}

bool TestTextTypes() {
    std::cout << "\n=== Testing Text Types ===" << std::endl;
    
    auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::VARCHAR);
    TEST_ASSERT(result.IsValid(), "VARCHAR conversion");
    TEST_ASSERT(result.GetValue() == "VARCHAR", "VARCHAR -> VARCHAR");

    // Note: CHAR and TEXT are not supported in DuckDB, so we skip those tests
    std::cout << "✓ SKIP: CHAR and TEXT tests (not supported in DuckDB)" << std::endl;

    return true;
}

bool TestBinaryAndBooleanTypes() {
    std::cout << "\n=== Testing Binary and Boolean Types ===" << std::endl;
    
    auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::BLOB);
    TEST_ASSERT(result.IsValid(), "BLOB conversion");
    TEST_ASSERT(result.GetValue() == "BINARY", "BLOB -> BINARY");

    result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::BOOLEAN);
    TEST_ASSERT(result.IsValid(), "BOOLEAN conversion");
    TEST_ASSERT(result.GetValue() == "BOOLEAN", "BOOLEAN -> BOOLEAN");

    return true;
}

bool TestTemporalTypes() {
    std::cout << "\n=== Testing Temporal Types ===" << std::endl;
    
    auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::DATE);
    TEST_ASSERT(result.IsValid(), "DATE conversion");
    TEST_ASSERT(result.GetValue() == "DATE", "DATE -> DATE");

    result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::TIME);
    TEST_ASSERT(result.IsValid(), "TIME conversion");
    TEST_ASSERT(result.GetValue() == "TIME", "TIME -> TIME");

    result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::TIMESTAMP);
    TEST_ASSERT(result.IsValid(), "TIMESTAMP conversion");
    TEST_ASSERT(result.GetValue() == "TIMESTAMP_NTZ", "TIMESTAMP -> TIMESTAMP_NTZ");

    result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::TIMESTAMP_TZ);
    TEST_ASSERT(result.IsValid(), "TIMESTAMP_TZ conversion");
    TEST_ASSERT(result.GetValue() == "TIMESTAMP_TZ", "TIMESTAMP_TZ -> TIMESTAMP_TZ");

    return true;
}

bool TestDecimalTypes() {
    std::cout << "\n=== Testing Decimal Types ===" << std::endl;
    
    auto decimal_type = LogicalType::DECIMAL(18, 3);
    auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(decimal_type);
    TEST_ASSERT(result.IsValid(), "DECIMAL(18,3) conversion");
    TEST_ASSERT(result.GetValue() == "NUMBER(18,3)", "DECIMAL(18,3) -> NUMBER(18,3)");

    decimal_type = LogicalType::DECIMAL(38, 10);
    result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(decimal_type);
    TEST_ASSERT(result.IsValid(), "DECIMAL(38,10) conversion");
    TEST_ASSERT(result.GetValue() == "NUMBER(38,10)", "DECIMAL(38,10) -> NUMBER(38,10)");

    return true;
}

bool TestArrowConversion() {
    std::cout << "\n=== Testing Arrow Conversion Pipeline ===" << std::endl;
    
    // Test DuckDB → Arrow → Snowflake pipeline
    auto arrow_result = SnowflakeTypeConverter::ConvertDuckDBToArrow(LogicalType::INTEGER);
    TEST_ASSERT(arrow_result.IsValid(), "DuckDB → Arrow conversion");
    // Note: We can't easily compare Arrow types as strings, so we just check it's valid
    TEST_ASSERT(arrow_result.GetValue() != nullptr, "Arrow result is not null");

    auto snowflake_result = SnowflakeTypeConverter::ConvertArrowToSnowflake("int32");
    TEST_ASSERT(snowflake_result.IsValid(), "Arrow → Snowflake conversion");
    TEST_ASSERT(snowflake_result.GetValue() == "NUMBER(10,0)", "int32 -> NUMBER(10,0)");

    auto direct_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::INTEGER);
    TEST_ASSERT(direct_result.IsValid(), "Direct DuckDB → Snowflake conversion");
    TEST_ASSERT(direct_result.GetValue() == snowflake_result.GetValue(), "Direct conversion matches pipeline");

    return true;
}

bool TestErrorHandling() {
    std::cout << "\n=== Testing Error Handling ===" << std::endl;
    
    // Test unsupported Arrow type
    auto result = SnowflakeTypeConverter::ConvertArrowToSnowflake("unsupported_arrow_type");
    TEST_ASSERT(!result.IsValid(), "Unsupported Arrow type should fail");
    TEST_ASSERT(result.GetError().find("Unsupported Arrow type") != std::string::npos, "Error message contains expected text");

    // Test unsupported DuckDB type
    auto unsupported_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::INTERVAL);
    TEST_ASSERT(!unsupported_result.IsValid(), "Unsupported DuckDB type should fail");
    TEST_ASSERT(unsupported_result.GetError().find("Unsupported DuckDB type") != std::string::npos, "Error message contains expected text");

    return true;
}

int main() {
    std::cout << "Starting SnowflakeTypeConverter tests..." << std::endl;
    
    bool all_passed = true;
    
    all_passed &= TestIntegerTypes();
    all_passed &= TestFloatingPointTypes();
    all_passed &= TestTextTypes();
    all_passed &= TestBinaryAndBooleanTypes();
    all_passed &= TestTemporalTypes();
    all_passed &= TestDecimalTypes();
    all_passed &= TestArrowConversion();
    all_passed &= TestErrorHandling();
    
    if (all_passed) {
        std::cout << "\n🎉 All tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\n❌ Some tests failed!" << std::endl;
        return 1;
    }
} 