# Test configuration for DuckDB-Snowflake extension

# Find Arrow library
find_library(ARROW_LIBRARY
    NAMES arrow libarrow
    PATHS /opt/homebrew/lib /usr/local/lib /usr/lib
    DOC "Arrow library"
)

if(NOT ARROW_LIBRARY)
    message(FATAL_ERROR "Arrow library not found")
endif()

# Add test executable
add_executable(test_type_converter cpp/test_type_converter.cpp)

# Link with DuckDB and extension
target_link_libraries(test_type_converter 
    PRIVATE 
    snowflake
    ${DUCKDB_LIBRARY}
    ${ARROW_LIBRARY}
)

# Include directories
target_include_directories(test_type_converter 
    PRIVATE 
    ${CMAKE_SOURCE_DIR}/src/include
    ${DUCKDB_INCLUDE_DIR}
)

target_compile_features(test_type_converter PRIVATE cxx_std_17) 