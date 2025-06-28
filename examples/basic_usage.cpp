#include "duckdb.hpp"
#include <iostream>

using namespace duckdb;

int main() {
    // Create DuckDB database and load extension
    DuckDB db(nullptr);
    Connection con(db);
    
    // Load the Snowflake extension
    con.Query("LOAD 'snowflake';");
    
    // Example: Get type mapping information
    auto result = con.Query("SELECT snowflake_type_info('INTEGER');");
    result->Print();
    
    // TODO: Add examples for:
    // - Connecting to Snowflake
    // - Querying Snowflake data
    // - Inserting data to Snowflake
    // - Complex type conversions
    
    return 0;
} 