cmake_minimum_required(VERSION 3.14)
project(ingestion VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Library target
add_library(ingestion_lib
    src/hello.cpp
)
target_include_directories(ingestion_lib 
    PUBLIC 
        ${CMAKE_SOURCE_DIR}/include
)

# Main executable
add_executable(ingestion_app
    src/main.cpp
)
target_link_libraries(ingestion_app PRIVATE ingestion_lib)

# Enable testing
enable_testing()

# Add GoogleTest
add_subdirectory(third_party/googletest)

# Tests executable
add_executable(ingestion_tests
    tests/hello_test.cpp
)
target_link_libraries(ingestion_tests
    PRIVATE
        ingestion_lib
        gtest
        gtest_main
)

# Register tests
add_test(NAME unit_tests COMMAND ingestion_tests)

# Examples
add_subdirectory(examples)
