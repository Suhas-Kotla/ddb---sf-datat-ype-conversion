#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
#include <arrow/type.h>
#include <arrow/array.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace duckdb {

/**
 * @brief Core type conversion engine for DuckDB-Snowflake extension
 * 
 * This class handles all type mappings between DuckDB, Arrow, and Snowflake.
 * Focus: Pure type conversion logic without connection handling.
 */
class SnowflakeTypeConverter {
public:
    // Conversion result wrapper
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
        
        bool IsValid() const { return success; }
        const T& GetValue() const { return result; }
        const std::string& GetError() const { return error_message; }
    };

    // Template specialization for void
    template<>
    struct ConversionResult<void> {
        bool success;
        std::string error_message;
        
        static ConversionResult<void> Success() {
            return {true, ""};
        }
        static ConversionResult<void> Error(const std::string& error) {
            return {false, error};
        }
        bool IsValid() const { return success; }
        const std::string& GetError() const { return error_message; }
    };

    // ===== PRIMARY CONVERSION FUNCTIONS =====
    
    /**
     * @brief Convert DuckDB LogicalType to Arrow DataType equivalent
     * @param duckdb_type Source DuckDB type
     * @return Arrow DataType representation or error details
     */
    static ConversionResult<std::shared_ptr<arrow::DataType>> 
    ConvertDuckDBToArrow(const LogicalType& duckdb_type);
    
    /**
     * @brief Convert Arrow DataType to Snowflake SQL type string
     * @param arrow_type_desc Source Arrow type description
     * @return Snowflake SQL type specification or error details
     */
    static ConversionResult<std::string> 
    ConvertArrowToSnowflake(const std::string& arrow_type_desc);
    
    /**
     * @brief Direct conversion: DuckDB → Snowflake (via Arrow)
     * @param duckdb_type Source DuckDB type
     * @return Snowflake SQL type specification or error details
     */
    static ConversionResult<std::string> 
    ConvertDuckDBToSnowflake(const LogicalType& duckdb_type);
    
    /**
     * @brief Reverse conversion: Snowflake → DuckDB (via Arrow)
     * @param snowflake_type Snowflake SQL type specification
     * @return DuckDB LogicalType or error details
     */
    static ConversionResult<LogicalType> 
    ConvertSnowflakeToDuckDB(const std::string& snowflake_type);

    // ===== TYPE MAPPING UTILITIES =====
    
    /**
     * @brief Get comprehensive type mapping information
     * @param duckdb_type Source DuckDB type
     * @return Mapping details (Arrow intermediate, Snowflake target, notes)
     */
    struct TypeMappingInfo {
        std::string duckdb_type;
        std::string arrow_type; 
        std::string snowflake_type;
        std::string conversion_notes;
        bool has_precision_loss;
        bool requires_special_handling;
    };
    
    static ConversionResult<TypeMappingInfo> 
    GetTypeMappingInfo(const LogicalType& duckdb_type);
    
    /**
     * @brief Check if two types are conversion-compatible
     * @param source_type Source type
     * @param target_type Target type
     * @return Compatibility assessment with warnings
     */
    static ConversionResult<std::string> 
    CheckTypeCompatibility(const LogicalType& source_type, 
                          const LogicalType& target_type);

    // ===== PRECISION AND SCALE HANDLING =====
    
    /**
     * @brief Handle decimal precision adjustments for Snowflake limits
     * @param precision Source precision
     * @param scale Source scale
     * @return Adjusted precision/scale with warnings
     */
    struct DecimalAdjustment {
        uint8_t adjusted_precision;
        uint8_t adjusted_scale;
        bool precision_reduced;
        bool scale_reduced;
        std::string warning_message;
    };
    
    static DecimalAdjustment 
    AdjustDecimalForSnowflake(uint8_t precision, uint8_t scale);
    
    /**
     * @brief Validate numeric range compatibility
     * @param source_type Source numeric type
     * @param target_type Target numeric type
     * @return Range compatibility assessment
     */
    static ConversionResult<std::string> 
    ValidateNumericRange(const LogicalType& source_type, 
                        const LogicalType& target_type);

    // ===== NESTED TYPE HANDLING =====
    
    /**
     * @brief Convert nested/composite types (STRUCT, LIST, MAP, UNION)
     * @param duckdb_type Source nested type
     * @return Snowflake representation strategy
     */
    static ConversionResult<std::string> 
    ConvertNestedType(const LogicalType& duckdb_type);
    
    /**
     * @brief Get flattening strategy for complex nested types
     * @param duckdb_type Source complex type
     * @return Flattening approach for Snowflake compatibility
     */
    static ConversionResult<std::string> 
    GetFlatteningStrategy(const LogicalType& duckdb_type);

    // ===== DATA VALIDATION =====
    
    /**
     * @brief Validate data conversion preserves integrity
     * @param source_type Original type
     * @param target_type Converted type
     * @param source_data Original data
     * @param target_data Converted data
     * @return Success or error details
     */
    static ConversionResult<void> 
    ValidateConversion(const LogicalType& source_type,
                      const LogicalType& target_type,
                      const Vector& source_data,
                      const Vector& target_data);

private:
    // ===== INTERNAL CONVERSION HELPERS =====
    
    /**
     * @brief Handle simple/primitive type conversions
     */
    static ConversionResult<std::string> 
    ConvertPrimitiveType(const LogicalType& duckdb_type);
    
    /**
     * @brief Handle temporal type conversions with timezone awareness
     */
    static ConversionResult<std::string> 
    ConvertTemporalType(const LogicalType& duckdb_type);
    
    /**
     * @brief Handle unsigned integer types (no Snowflake equivalent)
     */
    static ConversionResult<std::string> 
    ConvertUnsignedType(const LogicalType& duckdb_type);
    
    /**
     * @brief Format detailed error messages with context
     */
    static std::string 
    FormatConversionError(const std::string& operation,
                         const LogicalType& source_type,
                         const std::string& error_detail);
                         
    /**
     * @brief Type mapping lookup tables (populated during initialization)
     */
    static const std::unordered_map<LogicalTypeId, std::string> direct_snowflake_map_;
    static const std::unordered_map<LogicalTypeId, std::string> arrow_equivalents_;
    static const std::unordered_map<std::string, LogicalTypeId> reverse_type_map_;
};

} // namespace duckdb 