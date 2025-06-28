#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include <memory>
#include <string>
#include <unordered_map>

// ADBC includes
extern "C" {
#include "adbc.h"
}

// Forward declarations
namespace arrow {
    class RecordBatch;
    class Schema;
}

namespace duckdb {

/**
 * @brief Configuration for Snowflake connection
 */
struct SnowflakeConfig {
    std::string account;
    std::string user;
    std::string password;
    std::string database;
    std::string schema;
    std::string warehouse;
    std::string role;
    
    // Optional authentication settings
    std::string private_key_path;
    std::string private_key_passphrase;
    std::string token;
    
    // Connection options
    std::unordered_map<std::string, std::string> options;
    
    /**
     * @brief Build Snowflake URI from configuration
     * @return Complete Snowflake connection URI
     */
    std::string BuildURI() const;
    
    /**
     * @brief Validate configuration completeness
     * @return True if configuration is valid
     */
    bool IsValid() const;
};

/**
 * @brief ADBC-based connector for Snowflake integration
 * 
 * This class manages the ADBC connection lifecycle and provides
 * high-level methods for querying and data ingestion with Snowflake.
 */
class SnowflakeADBCConnector {
public:
    /**
     * @brief Construct connector with configuration
     * @param config Snowflake connection configuration
     */
    explicit SnowflakeADBCConnector(const SnowflakeConfig &config);
    
    /**
     * @brief Destructor - ensures proper cleanup
     */
    ~SnowflakeADBCConnector();
    
    // Delete copy constructor and assignment (RAII)
    SnowflakeADBCConnector(const SnowflakeADBCConnector&) = delete;
    SnowflakeADBCConnector& operator=(const SnowflakeADBCConnector&) = delete;
    
    /**
     * @brief Initialize ADBC connection to Snowflake
     * @return Success or error details
     */
    string Connect();
    
    /**
     * @brief Execute SQL query and return Arrow data
     * @param sql SQL query string
     * @return Arrow RecordBatch with results or error
     */
    std::pair<std::shared_ptr<arrow::RecordBatch>, string> 
    ExecuteQuery(const std::string &sql);
    
    /**
     * @brief Insert Arrow data into Snowflake table
     * @param table_name Target table name
     * @param batch Arrow RecordBatch to insert
     * @return Success or error details
     */
    string InsertBatch(const std::string &table_name, 
                      const std::shared_ptr<arrow::RecordBatch> &batch);
    
    /**
     * @brief Get Snowflake table schema information
     * @param table_name Table to inspect
     * @return Arrow Schema or error
     */
    std::pair<std::shared_ptr<arrow::Schema>, string> 
    GetTableSchema(const std::string &table_name);
    
    /**
     * @brief Check if connection is active
     * @return True if connected
     */
    bool IsConnected() const { return connected_; }
    
    /**
     * @brief Disconnect from Snowflake
     */
    void Disconnect();

private:
    SnowflakeConfig config_;
    bool connected_;
    
    // ADBC objects
    AdbcError adbc_error_;
    AdbcDatabase adbc_database_;
    AdbcConnection adbc_connection_;
    AdbcStatement adbc_statement_;
    
    /**
     * @brief Initialize ADBC database with Snowflake driver
     * @return Success or error message
     */
    string InitializeDatabase();
    
    /**
     * @brief Initialize ADBC connection
     * @return Success or error message  
     */
    string InitializeConnection();
    
    /**
     * @brief Clean up ADBC resources
     */
    void Cleanup();
    
    /**
     * @brief Format ADBC error messages
     * @param operation Description of failed operation
     * @return Formatted error message
     */
    string FormatADBCError(const std::string &operation) const;
};

} // namespace duckdb 