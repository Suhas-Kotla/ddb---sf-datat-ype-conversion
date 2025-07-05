# DuckDB-Snowflake Type Mapping Reference

## Overview
This document provides comprehensive mapping between DuckDB types, Arrow intermediate types, and Snowflake target types for the DuckDB-Snowflake extension.

## Conversion Pipeline
```
DuckDB Type → Arrow Type → Snowflake Type
```

## Direct Mappings (No Data Loss)

| DuckDB Type | Arrow Type | Snowflake Type | Notes |
|-------------|------------|----------------|-------|
| TINYINT | int8 | NUMBER(3,0) | ✅ Direct mapping |
| SMALLINT | int16 | NUMBER(5,0) | ✅ Direct mapping |
| INTEGER | int32 | NUMBER(10,0) | ✅ Direct mapping |
| BIGINT | int64 | NUMBER(19,0) | ✅ Direct mapping |
| FLOAT | float32 | FLOAT | ✅ Direct mapping |
| DOUBLE | float64 | DOUBLE | ✅ Direct mapping |
| VARCHAR | utf8 | VARCHAR | ✅ Direct mapping |
| BOOLEAN | boolean | BOOLEAN | ✅ Direct mapping |
| DATE | date32 | DATE | ✅ Direct mapping |
| TIME | time64[us] | TIME | ✅ Direct mapping |
| TIMESTAMP | timestamp[us] | TIMESTAMP_NTZ | ✅ Direct mapping |

## Mappings with Constraints

| DuckDB Type | Arrow Type | Snowflake Type | Constraint/Warning |
|-------------|------------|----------------|-------------------|
| HUGEINT | decimal128(38,0) | NUMBER(38,0) | ⚠️ Precision may be lost |
| DECIMAL(p,s) | decimal128(p,s) | NUMBER(p,s) | ⚠️ Max precision 38 in Snowflake |
| UTINYINT | uint8 | NUMBER(3,0) | ⚠️ Needs CHECK constraint ≥ 0 |
| USMALLINT | uint16 | NUMBER(5,0) | ⚠️ Needs CHECK constraint ≥ 0 |
| UINTEGER | uint32 | NUMBER(10,0) | ⚠️ Needs CHECK constraint ≥ 0 |
| UBIGINT | uint64 | NUMBER(20,0) | ⚠️ Needs CHECK constraint ≥ 0 |

## Complex Type Mappings

| DuckDB Type | Arrow Type | Snowflake Type | Conversion Strategy |
|-------------|------------|----------------|-------------------|
| LIST | list(element_type) | ARRAY(element_type) | 🔄 Recursive element conversion |
| ARRAY | fixed_size_list(element_type, size) | ARRAY(element_type) | 🔄 Size information lost |
| STRUCT | struct(fields) | OBJECT | 🔄 Field-by-field conversion |
| MAP | map(key_type, value_type) | MAP | 🔄 Iceberg tables only |
| UNION | union(types) | VARIANT | 🔄 Discriminator handling required |

## Special Cases

| DuckDB Type | Arrow Representation | Snowflake Equivalent | Notes |
|-------------|---------------------|---------------------|-------|
| UUID | utf8 | VARCHAR | String representation |
| JSON | utf8 | VARIANT | Parse/stringify required |
| BIT | utf8 | VARCHAR | String representation |
| INTERVAL | duration[us] | VARCHAR | No native Snowflake equivalent |

## Precision Handling Rules

### Decimal Precision Adjustment
1. **Source precision ≤ 38**: Direct mapping
2. **Source precision > 38**: 
   - Reduce precision to 38
   - Reduce scale proportionally
   - Issue warning about data loss

### Example Adjustments
- `DECIMAL(45,5)` → `NUMBER(38,0)` (scale reduced to fit)
- `DECIMAL(50,10)` → `NUMBER(38,3)` (both precision and scale reduced)

## Error Conditions

### Unsupported Conversions
- Complex nested UNION types
- Arrow Dictionary types
- Arrow View types (StringView, BinaryView)
- DuckDB VARINT (unlimited precision)

### Conversion Failures
All type conversions return a `ConversionResult<T>` structure that includes:
- Success/failure status
- Result value (if successful)
- Detailed error message (if failed)

## Usage Examples

### Basic Type Conversion
```cpp
auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(LogicalType::INTEGER);
// Returns: "NUMBER(10,0)"
```

### Decimal with Precision Handling
```cpp
auto decimal_type = LogicalType::DECIMAL(18, 3);
auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(decimal_type);
// Returns: "NUMBER(18,3)"
```

### Complex Type Conversion
```cpp
auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(list_type);
// Returns: "ARRAY(VARCHAR)"
```

### Error Handling
```cpp
auto result = SnowflakeTypeConverter::ConvertDuckDBToSnowflake(unsupported_type);
if (!result.success) {
    std::cout << "Conversion failed: " << result.error_message << std::endl;
}
```

## Implementation Notes

### Performance Considerations
- Type conversion is cached for repeated operations
- Arrow intermediate format optimizes memory usage
- Batch processing supported for large datasets

### Memory Management
- Arrow DataType objects are shared_ptr for automatic cleanup
- Conversion results use move semantics for efficiency
- Error messages are copied to avoid dangling references

### Thread Safety
- All conversion functions are stateless and thread-safe
- Lookup tables are read-only after initialization
- No global state or mutable data structures

## Testing Strategy

### Unit Tests
- Individual type conversion functions
- Error handling and edge cases
- Precision adjustment logic
- Complex type handling

### Integration Tests
- End-to-end conversion pipeline
- Round-trip conversions (DuckDB → Snowflake → DuckDB)
- Performance benchmarks
- Memory usage validation

### SQL Tests
- Type conversion in SQL queries
- Schema creation and validation
- Data insertion and retrieval
- Error message verification

## Future Enhancements

### Planned Features
- Support for Arrow Dictionary types
- Enhanced UNION type handling
- Custom type mapping configuration
- Performance optimization for bulk operations

### Potential Improvements
- Lazy evaluation for complex types
- Parallel processing for large datasets
- Caching strategies for repeated conversions
- Integration with DuckDB's type system extensions 