# Test configuration for DuckDB-Snowflake extension

# Find Catch2 testing framework
find_package(Catch2 3 REQUIRED)

# Add test executable
add_executable(test_type_converter test/cpp/test_type_converter.cpp)

# Link with DuckDB and extension
target_link_libraries(test_type_converter 
    PRIVATE 
    Catch2::Catch2
    duckdb
    snowflake
)

# Include directories
target_include_directories(test_type_converter 
    PRIVATE 
    src/include
)

# Add test
catch_discover_tests(test_type_converter) 