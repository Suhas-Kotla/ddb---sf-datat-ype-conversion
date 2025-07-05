cmake_minimum_required(VERSION 3.15)

# Project configuration
project(duckdb-snowflake-extension)

# Extension configuration
set(EXTENSION_NAME snowflake)
set(EXTENSION_SOURCES 
    src/snowflake_extension.cpp
    src/type_converter.cpp
)

# Create static library
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# Set up include directories
target_include_directories(${EXTENSION_NAME} PRIVATE src/include)

# Find DuckDB headers manually
find_path(DUCKDB_INCLUDE_DIR duckdb.hpp
    PATHS /opt/homebrew/include /usr/local/include /usr/include
    DOC "DuckDB include directory"
)

if(DUCKDB_INCLUDE_DIR)
    target_include_directories(${EXTENSION_NAME} PRIVATE ${DUCKDB_INCLUDE_DIR})
else()
    message(FATAL_ERROR "DuckDB headers not found")
endif()

# Find DuckDB library manually
find_library(DUCKDB_LIBRARY
    NAMES duckdb libduckdb
    PATHS /opt/homebrew/lib /usr/local/lib /usr/lib
    DOC "DuckDB library"
)

if(DUCKDB_LIBRARY)
    target_link_libraries(${EXTENSION_NAME} ${DUCKDB_LIBRARY})
else()
    message(FATAL_ERROR "DuckDB library not found")
endif()

# Compiler flags for C++17
target_compile_features(${EXTENSION_NAME} PRIVATE cxx_std_17)

# Testing
enable_testing()
add_subdirectory(test) 