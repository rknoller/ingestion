cmake_minimum_required(VERSION 3.14)
project(ingestion VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable debug symbols
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

# Library target
add_library(ingestion_lib
    src/hello.cpp
    src/opinion.cpp
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

# GoogleTest via FetchContent instead of submodule
include(FetchContent)
set(GTEST_VERSION 1.14.0)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v${GTEST_VERSION}.zip
    URL_HASH SHA256=94a9f9a23474131a681a4bf63eecf97f5673d433b89adef4234dc688f71d7b47
)
set(BUILD_GMOCK OFF CACHE BOOL "Disable gmock" FORCE)
set(INSTALL_GTEST OFF CACHE BOOL "Disable install" FORCE)
FetchContent_MakeAvailable(googletest)

# Tests executable
add_executable(ingestion_tests
    tests/hello_test.cpp
    tests/opinion_test.cpp
)
target_link_libraries(ingestion_tests
    PRIVATE
        ingestion_lib
        GTest::gtest
        GTest::gtest_main
)

# Register tests
add_test(NAME unit_tests COMMAND ingestion_tests)

# Examples
add_subdirectory(examples)
