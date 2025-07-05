# DuckDB-Snowflake Extension: Type Conversion Engine

A focused DuckDB extension that provides comprehensive type conversion between DuckDB, Apache Arrow, and Snowflake type systems.

## Overview

This extension implements the core type mapping logic needed for the DuckDB-Snowflake integration. It provides bidirectional conversion between all three type systems while preserving data integrity and handling precision constraints.

## Features

### Core Type Conversions
- **DuckDB → Arrow → Snowflake**: Complete conversion pipeline
- **Snowflake → DuckDB**: Reverse conversion support
- **Direct DuckDB → Snowflake**: Optimized direct mapping
- **Arrow → Snowflake**: Arrow integration support

### Type Coverage
- **Primitive Types**: INTEGER, FLOAT, VARCHAR, BOOLEAN, etc.
- **Temporal Types**: DATE, TIME, TIMESTAMP, TIMESTAMP_TZ
- **Decimal Types**: With automatic precision adjustment for Snowflake's 38-digit limit
- **Complex Types**: LIST, STRUCT, MAP, UNION with flattening strategies
- **Special Types**: UUID, JSON, BIT with appropriate mappings

### Advanced Features
- **Precision Management**: Automatic decimal precision adjustment
- **Error Handling**: Comprehensive error reporting with context
- **Type Validation**: Data integrity validation
- **Performance Optimization**: Cached conversions and batch processing
- **Thread Safety**: Stateless design for concurrent use

## Quick Start

### Basic Usage

```cpp
#include "type_converter.hpp"

using namespace duckdb;

// Convert DuckDB type to Snowflake
auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::INTEGER);
if (result.success) {
    std::cout << "Snowflake type: " << result.result << std::endl; // "NUMBER(10,0)"
}

// Convert Snowflake type to DuckDB
auto reverse_result = SnowflakeTypeConverter::ConvertSnowflakeToDuckDB("VARCHAR");
if (reverse_result.success) {
    std::cout << "DuckDB type: " << reverse_result.result.ToString() << std::endl;
}
```

### Decimal Precision Handling

```cpp
// Handle large decimal precision
auto decimal_type = LogicalType::DECIMAL(45, 5);
auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(decimal_type);
// Result: "NUMBER(38,0)" with precision reduced to fit Snowflake limits

// Get precision adjustment details
auto adjustment = SnowflakeTypeConverter::AdjustDecimalForSnowflake(50, 10);
std::cout << "Adjusted: (" << (int)adjustment.adjusted_precision 
          << "," << (int)adjustment.adjusted_scale << ")" << std::endl;
```

### Complex Type Conversion

```cpp
// Convert nested types
auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(list_type);
// Result: "ARRAY(VARCHAR)"

// Get conversion strategy for complex types
auto strategy = SnowflakeTypeConverter::GetFlatteningStrategy(complex_type);
```

## Project Structure

```
duckdb-snowflake-extension/
├── src/
│   ├── include/
│   │   └── type_converter.hpp      # Main type conversion interface
│   └── type_converter.cpp          # Implementation
├── test/
│   ├── cpp/
│   │   └── test_type_converter.cpp # Unit tests
│   └── sql/
│       └── type_mapping.test       # SQL-based tests
├── docs/
│   └── type_mapping_reference.md   # Complete type mapping documentation
├── examples/
│   └── type_conversion_demo.cpp    # Usage examples
└── README.md                       # This file
```

## Type Mapping Reference

### Direct Mappings (No Data Loss)
| DuckDB | Arrow | Snowflake |
|--------|-------|-----------|
| TINYINT | int8 | NUMBER(3,0) |
| INTEGER | int32 | NUMBER(10,0) |
| FLOAT | float32 | FLOAT |
| VARCHAR | utf8 | VARCHAR |
| BOOLEAN | boolean | BOOLEAN |
| DATE | date32 | DATE |

### Mappings with Constraints
| DuckDB | Arrow | Snowflake | Constraint |
|--------|-------|-----------|------------|
| HUGEINT | decimal128(38,0) | NUMBER(38,0) | Precision may be lost |
| DECIMAL(p,s) | decimal128(p,s) | NUMBER(p,s) | Max precision 38 |
| UTINYINT | uint8 | NUMBER(3,0) | Needs CHECK constraint ≥ 0 |

### Complex Type Mappings
| DuckDB | Arrow | Snowflake | Strategy |
|--------|-------|-----------|----------|
| LIST | list(element_type) | ARRAY(element_type) | Recursive conversion |
| STRUCT | struct(fields) | OBJECT | Field-by-field conversion |
| MAP | map(key,value) | MAP | Iceberg tables only |
| UNION | union(types) | VARIANT | Discriminator handling |

## Building

### Prerequisites
- DuckDB development headers
- CMake 3.16+
- C++17 compiler
- Apache Arrow (for full Arrow integration)

### Build Instructions

```bash
mkdir build && cd build
cmake ..
make
```

### Running Tests

```bash
# Unit tests
make test

# SQL tests
duckdb test/sql/type_mapping.test
```

## API Reference

### Primary Conversion Functions

```cpp
// DuckDB → Arrow
static ConversionResult<std::shared_ptr<arrow::DataType>> 
ConvertDuckDBToArrow(const LogicalType& duckdb_type);

// Arrow → Snowflake
static ConversionResult<std::string> 
ConvertArrowToSnowflake(const arrow::DataType& arrow_type);

// DuckDB → Snowflake (direct)
static ConversionResult<std::string> 
ConvertDuckDBToSnowflake(const LogicalType& duckdb_type);

// Snowflake → DuckDB
static ConversionResult<LogicalType> 
ConvertSnowflakeToDuckDB(const std::string& snowflake_type);
```

### Utility Functions

```cpp
// Get comprehensive mapping information
static ConversionResult<TypeMappingInfo> 
GetTypeMappingInfo(const LogicalType& duckdb_type);

// Check type compatibility
static ConversionResult<std::string> 
CheckTypeCompatibility(const LogicalType& source_type, 
                      const LogicalType& target_type);

// Handle decimal precision adjustments
static DecimalAdjustment 
AdjustDecimalForSnowflake(uint8_t precision, uint8_t scale);
```

## Error Handling

All conversion functions return a `ConversionResult<T>` structure:

```cpp
template<typename T>
struct ConversionResult {
    T result;
    bool success;
    std::string error_message;
};
```

Example error handling:

```cpp
auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(type);
if (!result.success) {
    std::cerr << "Conversion failed: " << result.error_message << std::endl;
    // Handle error appropriately
}
```

## Performance Considerations

- **Caching**: Type conversions are cached for repeated operations
- **Memory Management**: Uses shared_ptr for Arrow types and move semantics for results
- **Batch Processing**: Supports bulk type conversion operations
- **Thread Safety**: All functions are stateless and thread-safe

## Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add comprehensive tests
5. Update documentation
6. Submit a pull request

### Testing Guidelines
- Add unit tests for new type conversions
- Include SQL tests for integration scenarios
- Test error conditions and edge cases
- Validate performance impact

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Related Projects

- [DuckDB](https://github.com/duckdb/duckdb) - In-process SQL OLAP database
- [Apache Arrow](https://arrow.apache.org/) - Columnar in-memory data format
- [Snowflake](https://www.snowflake.com/) - Cloud data platform

## Support

For questions and support:
- Create an issue on GitHub
- Check the [documentation](docs/)
- Review the [examples](examples/) 