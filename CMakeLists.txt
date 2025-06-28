cmake_minimum_required(VERSION 3.15)

# Extension configuration
set(EXTENSION_NAME snowflake)
set(EXTENSION_SOURCES 
    src/snowflake_extension.cpp
    src/type_converter.cpp
    src/adbc_connector.cpp
)

# Build DuckDB extension
build_static_extension(${EXTENSION_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${EXTENSION_NAME} "" ${EXTENSION_SOURCES})

# Set up include directories
target_include_directories(${EXTENSION_NAME} PRIVATE src/include)
target_include_directories(${EXTENSION_NAME}_loadable_extension PRIVATE src/include)

# ADBC dependencies
find_package(PkgConfig REQUIRED)
pkg_check_modules(ADBC REQUIRED adbc-driver-manager)

# Link ADBC libraries
target_link_libraries(${EXTENSION_NAME} ${ADBC_LIBRARIES})
target_link_libraries(${EXTENSION_NAME}_loadable_extension ${ADBC_LIBRARIES})

# Include ADBC headers
target_include_directories(${EXTENSION_NAME} PRIVATE ${ADBC_INCLUDE_DIRS})
target_include_directories(${EXTENSION_NAME}_loadable_extension PRIVATE ${ADBC_INCLUDE_DIRS})

# Compiler flags for C++17
target_compile_features(${EXTENSION_NAME} PRIVATE cxx_std_17)
target_compile_features(${EXTENSION_NAME}_loadable_extension PRIVATE cxx_std_17)

# Testing
enable_testing()
add_subdirectory(test) 