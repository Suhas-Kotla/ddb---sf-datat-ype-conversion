#include "type_converter.hpp"
#include "duckdb.hpp"
#include <iostream>
#include <vector>

using namespace duckdb;

/**
 * @brief Demonstration of DuckDB-Snowflake type conversion functionality
 * 
 * This example shows how to use the SnowflakeTypeConverter class
 * to convert between DuckDB, Arrow, and Snowflake type systems.
 */
int main() {
    std::cout << "=== DuckDB-Snowflake Type Conversion Demo ===\n\n";
    
    // ===== BASIC TYPE CONVERSIONS =====
    std::cout << "1. Basic Type Conversions:\n";
    std::cout << "   -----------------------\n";
    
    // Test integer types
    auto int_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::INTEGER);
    std::cout << "   INTEGER -> " << (int_result.success ? int_result.result : "ERROR: " + int_result.error_message) << "\n";
    
    auto bigint_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::BIGINT);
    std::cout << "   BIGINT -> " << (bigint_result.success ? bigint_result.result : "ERROR: " + bigint_result.error_message) << "\n";
    
    // Test floating point types
    auto float_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::FLOAT);
    std::cout << "   FLOAT -> " << (float_result.success ? float_result.result : "ERROR: " + float_result.error_message) << "\n";
    
    auto double_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::DOUBLE);
    std::cout << "   DOUBLE -> " << (double_result.success ? double_result.result : "ERROR: " + double_result.error_message) << "\n";
    
    // Test text types
    auto varchar_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::VARCHAR);
    std::cout << "   VARCHAR -> " << (varchar_result.success ? varchar_result.result : "ERROR: " + varchar_result.error_message) << "\n";
    
    // Test boolean
    auto bool_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::BOOLEAN);
    std::cout << "   BOOLEAN -> " << (bool_result.success ? bool_result.result : "ERROR: " + bool_result.error_message) << "\n";
    
    std::cout << "\n";
    
    // ===== TEMPORAL TYPE CONVERSIONS =====
    std::cout << "2. Temporal Type Conversions:\n";
    std::cout << "   ---------------------------\n";
    
    auto date_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::DATE);
    std::cout << "   DATE -> " << (date_result.success ? date_result.result : "ERROR: " + date_result.error_message) << "\n";
    
    auto timestamp_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::TIMESTAMP);
    std::cout << "   TIMESTAMP -> " << (timestamp_result.success ? timestamp_result.result : "ERROR: " + timestamp_result.error_message) << "\n";
    
    std::cout << "\n";
    
    // ===== DECIMAL PRECISION HANDLING =====
    std::cout << "3. Decimal Precision Handling:\n";
    std::cout << "   ----------------------------\n";
    
    // Normal decimal
    auto normal_decimal = LogicalType::DECIMAL(18, 3);
    auto normal_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(normal_decimal);
    std::cout << "   DECIMAL(18,3) -> " << (normal_result.success ? normal_result.result : "ERROR: " + normal_result.error_message) << "\n";
    
    // Large decimal requiring adjustment
    auto large_decimal = LogicalType::DECIMAL(45, 5);
    auto large_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(large_decimal);
    std::cout << "   DECIMAL(45,5) -> " << (large_result.success ? large_result.result : "ERROR: " + large_result.error_message) << "\n";
    
    // Test precision adjustment function
    auto adjustment = SnowflakeTypeConverter::AdjustDecimalForSnowflake(50, 10);
    std::cout << "   Precision adjustment: (50,10) -> (" << (int)adjustment.adjusted_precision 
              << "," << (int)adjustment.adjusted_scale << ")\n";
    if (adjustment.precision_reduced || adjustment.scale_reduced) {
        std::cout << "   Warning: " << adjustment.warning_message << "\n";
    }
    
    std::cout << "\n";
    
    // ===== COMPLEX TYPE CONVERSIONS =====
    std::cout << "4. Complex Type Conversions:\n";
    std::cout << "   --------------------------\n";
    
    // List type
    auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
    auto list_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(list_type);
    std::cout << "   LIST(VARCHAR) -> " << (list_result.success ? list_result.result : "ERROR: " + list_result.error_message) << "\n";
    
    // Nested list type
    auto nested_list = LogicalType::LIST(LogicalType::LIST(LogicalType::INTEGER));
    auto nested_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(nested_list);
    std::cout << "   LIST(LIST(INTEGER)) -> " << (nested_result.success ? nested_result.result : "ERROR: " + nested_result.error_message) << "\n";
    
    // Struct type
    auto struct_type = LogicalType::STRUCT({{"name", LogicalType::VARCHAR}, {"age", LogicalType::INTEGER}});
    auto struct_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(struct_type);
    std::cout << "   STRUCT(name:VARCHAR, age:INTEGER) -> " << (struct_result.success ? struct_result.result : "ERROR: " + struct_result.error_message) << "\n";
    
    std::cout << "\n";
    
    // ===== TYPE MAPPING INFORMATION =====
    std::cout << "5. Type Mapping Information:\n";
    std::cout << "   --------------------------\n";
    
    auto mapping_info = SnowflakeTypeConverter::GetTypeMappingInfo(LogicalType::INTEGER);
    if (mapping_info.success) {
        std::cout << "   DuckDB Type: " << mapping_info.result.duckdb_type << "\n";
        std::cout << "   Arrow Type: " << mapping_info.result.arrow_type << "\n";
        std::cout << "   Snowflake Type: " << mapping_info.result.snowflake_type << "\n";
        std::cout << "   Notes: " << mapping_info.result.conversion_notes << "\n";
        std::cout << "   Precision Loss: " << (mapping_info.result.has_precision_loss ? "Yes" : "No") << "\n";
        std::cout << "   Special Handling: " << (mapping_info.result.requires_special_handling ? "Yes" : "No") << "\n";
    } else {
        std::cout << "   Error getting mapping info: " << mapping_info.error_message << "\n";
    }
    
    std::cout << "\n";
    
    // ===== REVERSE CONVERSIONS =====
    std::cout << "6. Reverse Conversions (Snowflake -> DuckDB):\n";
    std::cout << "   -------------------------------------------\n";
    
    auto reverse_int = SnowflakeTypeConverter::ConvertSnowflakeToDuckDB("NUMBER(10,0)");
    std::cout << "   NUMBER(10,0) -> " << (reverse_int.success ? reverse_int.result.ToString() : "ERROR: " + reverse_int.error_message) << "\n";
    
    auto reverse_varchar = SnowflakeTypeConverter::ConvertSnowflakeToDuckDB("VARCHAR");
    std::cout << "   VARCHAR -> " << (reverse_varchar.success ? reverse_varchar.result.ToString() : "ERROR: " + reverse_varchar.error_message) << "\n";
    
    auto reverse_array = SnowflakeTypeConverter::ConvertSnowflakeToDuckDB("ARRAY(VARCHAR)");
    std::cout << "   ARRAY(VARCHAR) -> " << (reverse_array.success ? reverse_array.result.ToString() : "ERROR: " + reverse_array.error_message) << "\n";
    
    std::cout << "\n";
    
    // ===== ERROR HANDLING =====
    std::cout << "7. Error Handling:\n";
    std::cout << "   ----------------\n";
    
    // Test with unsupported type (placeholder for now)
    auto error_result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::UNION({{LogicalType::INTEGER, LogicalType::VARCHAR}}));
    std::cout << "   UNION(INTEGER, VARCHAR) -> " << (error_result.success ? error_result.result : "ERROR: " + error_result.error_message) << "\n";
    
    // Test invalid Snowflake type
    auto invalid_result = SnowflakeTypeConverter::ConvertSnowflakeToDuckDB("INVALID_TYPE");
    std::cout << "   INVALID_TYPE -> " << (invalid_result.success ? invalid_result.result.ToString() : "ERROR: " + invalid_result.error_message) << "\n";
    
    std::cout << "\n";
    
    // ===== PERFORMANCE DEMO =====
    std::cout << "8. Performance Demo:\n";
    std::cout << "   ------------------\n";
    
    // Test multiple conversions to show performance
    std::vector<LogicalType> test_types = {
        LogicalType::INTEGER,
        LogicalType::VARCHAR,
        LogicalType::FLOAT,
        LogicalType::BOOLEAN,
        LogicalType::DATE,
        LogicalType::TIMESTAMP,
        LogicalType::DECIMAL(18, 3),
        LogicalType::LIST(LogicalType::VARCHAR)
    };
    
    int success_count = 0;
    int error_count = 0;
    
    for (const auto& type : test_types) {
        auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(type);
        if (result.success) {
            success_count++;
        } else {
            error_count++;
        }
    }
    
    std::cout << "   Converted " << test_types.size() << " types: " 
              << success_count << " successful, " << error_count << " errors\n";
    
    std::cout << "\n=== Demo Complete ===\n";
    
    return 0;
} 