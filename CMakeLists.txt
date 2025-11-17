cmake_minimum_required(VERSION 3.14)
project(ingestion VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable debug symbols
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

# Find PostgreSQL library
find_package(PostgreSQL REQUIRED)
find_library(PQXX_LIB pqxx REQUIRED)

# Library target
add_library(ingestion_lib
    src/opinion.cpp
    src/opinion_db.cpp
)
target_include_directories(ingestion_lib 
    PUBLIC 
        ${CMAKE_SOURCE_DIR}/include
        ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(ingestion_lib
    PUBLIC
        ${PQXX_LIB}
        ${PostgreSQL_LIBRARIES}
)

# Cluster library target
add_library(cluster_lib
    src/opinion_cluster.cpp
    src/opinion_cluster_db.cpp
)
target_include_directories(cluster_lib 
    PUBLIC 
        ${CMAKE_SOURCE_DIR}/include
        ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(cluster_lib
    PUBLIC
        ${PQXX_LIB}
        ${PostgreSQL_LIBRARIES}
)

# Panel library target
add_library(panel_lib
    src/opinion_cluster_panel.cpp
    src/opinion_cluster_panel_db.cpp
)
target_include_directories(panel_lib 
    PUBLIC 
        ${CMAKE_SOURCE_DIR}/include
        ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(panel_lib
    PUBLIC
        ${PQXX_LIB}
        ${PostgreSQL_LIBRARIES}
)

# Main executable (opinions)
add_executable(ingestion_app
    src/main.cpp
)
target_link_libraries(ingestion_app PRIVATE ingestion_lib)

# Cluster ingestion executable
add_executable(cluster_ingestion_app
    src/cluster_main.cpp
)
target_link_libraries(cluster_ingestion_app PRIVATE cluster_lib)

# Panel ingestion executable
add_executable(panel_ingestion_app
    src/panel_main.cpp
)
target_link_libraries(panel_ingestion_app PRIVATE panel_lib)

# Joined By library target
add_library(joined_by_lib
    src/opinion_joined_by.cpp
    src/opinion_joined_by_db.cpp
)
target_include_directories(joined_by_lib 
    PUBLIC 
        ${CMAKE_SOURCE_DIR}/include
        ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(joined_by_lib
    PUBLIC
        ${PQXX_LIB}
        ${PostgreSQL_LIBRARIES}
)

# Joined By ingestion executable
add_executable(joined_by_ingestion_app
    src/joined_by_main.cpp
)
target_link_libraries(joined_by_ingestion_app PRIVATE joined_by_lib)

# Enable testing
enable_testing()

# Tests executable (custom lightweight test harness - no external frameworks)
add_executable(ingestion_tests
    tests/opinion_test.cpp
)
target_link_libraries(ingestion_tests PRIVATE ingestion_lib)

# Register tests
add_test(NAME unit_tests COMMAND ingestion_tests)

# Examples
add_subdirectory(examples)
