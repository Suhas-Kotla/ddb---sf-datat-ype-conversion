# DuckDB-Snowflake Type Mapping Reference

## Overview
This document describes the type conversion mappings used by the DuckDB-Snowflake extension.

## Conversion Pipeline
DuckDB Type → Arrow Type → Snowflake Type

## Direct Mappings

| DuckDB Type | Arrow Type | Snowflake Type | Notes |
|-------------|------------|----------------|-------|
| TINYINT     | Int8       | NUMBER(3,0)    | Direct mapping |
| INTEGER     | Int32      | NUMBER(10,0)   | Direct mapping |
| BIGINT      | Int64      | NUMBER(19,0)   | Direct mapping |
| FLOAT       | Float32    | FLOAT          | Direct mapping |
| DOUBLE      | Float64    | DOUBLE         | Direct mapping |
| VARCHAR     | Utf8       | VARCHAR        | Direct mapping |
| BOOLEAN     | Boolean    | BOOLEAN        | Direct mapping |

## Complex Mappings

### Decimal Types
- DuckDB DECIMAL(p,s) → Arrow Decimal128(p,s) → Snowflake NUMBER(p,s)
- Precision limited to 38 digits in Snowflake
- Automatic precision reduction with warnings

### Nested Types
- DuckDB LIST → Arrow List → Snowflake ARRAY
- DuckDB STRUCT → Arrow Struct → Snowflake OBJECT
- DuckDB MAP → Arrow Map → Snowflake MAP (Iceberg only)

## Unsupported Conversions
- DuckDB unsigned integers → Snowflake (no native unsigned support)
- Arrow Dictionary types → DuckDB/Snowflake
- Complex UNION types require special handling

## Implementation Notes

### Precision Handling
When converting decimal types with precision > 38, the extension will:
1. Reduce scale first (if possible)
2. Reduce precision to 38
3. Log warnings about data truncation

### Error Handling
All type conversions return a `ConversionResult<T>` structure that includes:
- Success/failure status
- Result value (if successful)
- Detailed error message (if failed)

### Performance Considerations
- Type conversion is cached for repeated operations
- Arrow intermediate format optimizes memory usage
- Batch processing supported for large datasets 