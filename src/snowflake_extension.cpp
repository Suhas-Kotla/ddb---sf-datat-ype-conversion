#include "snowflake_extension.hpp"
#include "type_converter.hpp"
#include "adbc_connector.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

void SnowflakeExtension::Load(DatabaseInstance &db) {
    // Register all extension functions
    RegisterTableFunctions(db);
    RegisterScalarFunctions(db);
}

std::string SnowflakeExtension::GetVersion() {
    return "1.0.0";
}

void SnowflakeExtension::RegisterTableFunctions(DatabaseInstance &db) {
    // TODO: Implement snowflake_scan table function
    // This will handle: SELECT * FROM snowflake_scan('connection_string', 'query')
    
    // TODO: Implement snowflake_insert table function  
    // This will handle: COPY data TO snowflake_insert('connection_string', 'table_name')
}

void SnowflakeExtension::RegisterScalarFunctions(DatabaseInstance &db) {
    // TODO: Implement type mapping utility functions
    // Example: SELECT snowflake_type_info('INTEGER') -> 'NUMBER(10,0)'
    
    // Type information function
    auto type_info_function = ScalarFunction(
        "snowflake_type_info",
        {LogicalType::VARCHAR},  // Input: DuckDB type name
        LogicalType::VARCHAR,    // Output: Snowflake type
        nullptr                  // TODO: Implement function
    );
    
    ExtensionUtil::RegisterFunction(db, type_info_function);
}

} // namespace duckdb

// Required extension entry points
extern "C" {

DUCKDB_EXTENSION_API void snowflake_init(duckdb::DatabaseInstance &db) {
    duckdb::SnowflakeExtension::Load(db);
}

DUCKDB_EXTENSION_API const char *snowflake_version() {
    return "1.0.0";
}

} 