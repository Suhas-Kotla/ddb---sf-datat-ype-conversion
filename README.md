# DuckDB-Snowflake Extension

A DuckDB extension that provides seamless integration with Snowflake data warehouse through ADBC (Arrow Database Connectivity) and comprehensive type conversion between DuckDB, Arrow, and Snowflake type systems.

## Overview

This extension enables:
- **Direct SQL queries** to Snowflake from DuckDB
- **Bidirectional data transfer** between DuckDB and Snowflake
- **Automatic type conversion** preserving data integrity
- **High-performance data ingestion** using Arrow format
- **Comprehensive type mapping** between all three type systems

## Project Structure

```
duckdb-snowflake-extension/
├── CMakeLists.txt              # Main build configuration
├── vcpkg.json                  # Dependency management
├── Makefile                    # Build convenience wrapper
├── src/
│   ├── snowflake_extension.cpp # Main extension entry point
│   ├── type_converter.cpp      # Type conversion implementation
│   ├── adbc_connector.cpp      # ADBC connection management
│   └── include/
│       ├── snowflake_extension.hpp
│       ├── type_converter.hpp
│       └── adbc_connector.hpp
├── test/
│   ├── sql/                    # SQL-based tests
│   └── cpp/                    # C++ unit tests
├── docs/                       # Documentation
└── examples/                   # Usage examples
```

## Features

### Type Conversion System
- **DuckDB → Arrow → Snowflake** conversion pipeline
- **Automatic precision adjustment** for decimal types
- **Comprehensive error handling** with detailed messages
- **Support for complex types** (arrays, structs, maps)

### ADBC Integration
- **Native ADBC driver support** for Snowflake
- **Connection pooling** and resource management
- **Batch data operations** for optimal performance
- **Secure authentication** (password, key-based, token)

### SQL Functions
- `snowflake_scan(connection, query)` - Query Snowflake data
- `snowflake_insert(connection, table, data)` - Insert data to Snowflake
- `snowflake_type_info(type)` - Get Snowflake type mapping

## Prerequisites

- **DuckDB** (latest version)
- **CMake** 3.15 or higher
- **C++17** compatible compiler
- **vcpkg** for dependency management
- **ADBC Snowflake driver**

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/duckdb-snowflake-extension.git
cd duckdb-snowflake-extension
```

### 2. Install Dependencies
```bash
# Install vcpkg dependencies
vcpkg install arrow arrow-adbc

# Set environment variable
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### 3. Build the Extension
```bash
# Release build
make release

# Or debug build
make debug
```

### 4. Run Tests
```bash
make test
```

## Usage

### Basic Setup
```sql
-- Load the extension
LOAD 'build/release/extension/snowflake/snowflake.duckdb_extension';

-- Check type mappings
SELECT snowflake_type_info('INTEGER');  -- Returns: NUMBER(10,0)
SELECT snowflake_type_info('DECIMAL(18,3)');  -- Returns: NUMBER(18,3)
```

### Query Snowflake Data
```sql
-- Query data from Snowflake
SELECT * FROM snowflake_scan(
    'user:pass@account/database/schema?warehouse=my_warehouse',
    'SELECT * FROM my_table LIMIT 1000'
);
```

### Insert Data to Snowflake
```sql
-- Insert DuckDB data to Snowflake
COPY (SELECT * FROM local_table) 
TO snowflake_insert(
    'user:pass@account/database/schema?warehouse=my_warehouse',
    'target_table'
);
```

## Type Mapping

| DuckDB Type | Snowflake Type | Notes |
|-------------|----------------|-------|
| TINYINT     | NUMBER(3,0)    | Direct mapping |
| INTEGER     | NUMBER(10,0)   | Direct mapping |
| BIGINT      | NUMBER(19,0)   | Direct mapping |
| DECIMAL(p,s)| NUMBER(p,s)    | Precision ≤ 38 |
| VARCHAR     | VARCHAR        | Direct mapping |
| BOOLEAN     | BOOLEAN        | Direct mapping |
| LIST        | ARRAY          | Nested types |
| STRUCT      | OBJECT         | Nested types |

## Development

### Building from Source
```bash
# Debug build with verbose output
make debug

# Run specific tests
cd build/debug && make test_type_converter
```

### Code Formatting
```bash
make format
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Update documentation
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 