#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

/**
 * @brief Main entry point for the Snowflake extension
 * 
 * This class handles the initialization and registration of all
 * Snowflake-related functions and types with DuckDB.
 */
class SnowflakeExtension {
public:
    /**
     * @brief Initialize the Snowflake extension
     * @param db DatabaseInstance to register functions with
     */
    static void Load(DatabaseInstance &db);
    
    /**
     * @brief Get extension version information
     * @return Version string
     */
    static std::string GetVersion();
    
private:
    /**
     * @brief Register all table functions (scan, insert, etc.)
     * @param db DatabaseInstance to register with
     */
    static void RegisterTableFunctions(DatabaseInstance &db);
    
    /**
     * @brief Register utility scalar functions
     * @param db DatabaseInstance to register with  
     */
    static void RegisterScalarFunctions(DatabaseInstance &db);
};

} // namespace duckdb

// Extension entry points - required by DuckDB
extern "C" {
    DUCKDB_EXTENSION_API void snowflake_init(duckdb::DatabaseInstance &db);
    DUCKDB_EXTENSION_API const char *snowflake_version();
} 