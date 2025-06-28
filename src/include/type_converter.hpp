#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
#include <memory>
#include <string>

// Forward declarations for Arrow types
namespace arrow {
    class DataType;
    class Array;
    class RecordBatch;
}

namespace duckdb {

/**
 * @brief Handles type conversion between DuckDB, Arrow, and Snowflake type systems
 * 
 * This class implements the core type mapping logic needed for the
 * DuckDB-Snowflake extension. It provides bidirectional conversion
 * between all three type systems while preserving data integrity.
 */
class SnowflakeTypeConverter {
public:
    // Type conversion results with error handling
    template<typename T>
    struct ConversionResult {
        T result;
        bool success;
        std::string error_message;
        
        static ConversionResult<T> Success(T&& value) {
            return {std::forward<T>(value), true, ""};
        }
        
        static ConversionResult<T> Error(const std::string& error) {
            return {T{}, false, error};
        }
    };

    /**
     * @brief Convert DuckDB LogicalType to Arrow DataType
     * @param duckdb_type Source DuckDB type
     * @return Arrow DataType or error
     */
    static ConversionResult<std::shared_ptr<arrow::DataType>> 
    ConvertDuckDBToArrow(const LogicalType &duckdb_type);
    
    /**
     * @brief Convert Arrow DataType to Snowflake type string
     * @param arrow_type Source Arrow type
     * @return Snowflake SQL type specification or error
     */
    static ConversionResult<std::string> 
    ConvertArrowToSnowflake(const arrow::DataType &arrow_type);
    
    /**
     * @brief Convert Snowflake type string to DuckDB LogicalType
     * @param snowflake_type Source Snowflake type specification
     * @return DuckDB LogicalType or error
     */
    static ConversionResult<LogicalType> 
    ConvertSnowflakeToDuckDB(const std::string &snowflake_type);
    
    /**
     * @brief Complete conversion pipeline: DuckDB → Arrow → Snowflake
     * @param duckdb_type Source DuckDB type
     * @return Snowflake type specification or error
     */
    static ConversionResult<std::string> 
    ConvertDuckDBToSnowflake(const LogicalType &duckdb_type);

    /**
     * @brief Validate data conversion preserves integrity
     * @param source_type Original type
     * @param target_type Converted type
     * @param source_data Original data
     * @param target_data Converted data
     * @return Success or error details
     */
    static ConversionResult<void> 
    ValidateConversion(const LogicalType &source_type,
                      const LogicalType &target_type,
                      const Vector &source_data,
                      const Vector &target_data);

private:
    /**
     * @brief Handle decimal precision adjustments for Snowflake limits
     * @param precision Source precision
     * @param scale Source scale
     * @return Adjusted precision/scale pair
     */
    static std::pair<uint8_t, uint8_t> 
    AdjustDecimalPrecision(uint8_t precision, uint8_t scale);
    
    /**
     * @brief Format detailed error messages for type conversion failures
     * @param operation Description of the failed operation
     * @param source_type Source type description
     * @param target_type Target type description
     * @param detail Additional error details
     * @return Formatted error message
     */
    static std::string 
    FormatConversionError(const std::string &operation,
                         const std::string &source_type,
                         const std::string &target_type,
                         const std::string &detail);
};

} // namespace duckdb 