#include "catch.hpp"
#include "type_converter.hpp"
#include "duckdb.hpp"

using namespace duckdb;

TEST_CASE("Basic Type Conversions", "[type_converter]") {
    
    SECTION("Integer Types") {
        // Test TINYINT conversion
        auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::TINYINT);
        REQUIRE(result.success);
        REQUIRE(result.result == "NUMBER(3,0)");
        
        // Test INTEGER conversion  
        result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::INTEGER);
        REQUIRE(result.success);
        REQUIRE(result.result == "NUMBER(10,0)");
    }
    
    SECTION("Decimal Precision Adjustment") {
        auto adjusted = SnowflakeTypeConverter::AdjustDecimalPrecision(45, 5);
        REQUIRE(adjusted.first == 38);  // Reduced precision
        REQUIRE(adjusted.second == 0);  // Reduced scale
    }
    
    // TODO: Add more comprehensive tests
} 