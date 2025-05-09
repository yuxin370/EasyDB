add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(type)
add_subdirectory(storage)
add_subdirectory(buffer)
add_subdirectory(record)
add_subdirectory(planner)
add_subdirectory(execution)
add_subdirectory(concurrency)
add_subdirectory(transaction)
add_subdirectory(recovery)
add_subdirectory(system)
add_subdirectory(parser)
add_subdirectory(analyze)

add_library(easydb STATIC ${ALL_OBJECT_FILES})

set(EASYDB_LIBS
        easydb_buffer
        easydb_catalog
        easydb_common
        easydb_concurrency
        easydb_execution
        easydb_recovery
        easydb_record
        easydb_type
        # easydb_container_disk_hash
        easydb_storage_disk
        easydb_storage_index
        easydb_storage_page
        easydb_storage_table
        easydb_system
        easydb_planner
        easydb_transaction
        easydb_parser
        easydb_analyze
        )

find_package(Threads REQUIRED)
find_package(nlohmann_json REQUIRED)

set(EASYDB_THIRDPARTY_LIBS
        easydb_murmur3
        # duckdb_pg_query
        # fmt
        # libfort::fort
        Threads::Threads
        )

target_link_libraries(
        easydb
        ${EASYDB_LIBS}
        ${EASYDB_THIRDPARTY_LIBS})

target_include_directories(
        easydb PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

add_executable(easydb_server easydb.cpp)
target_link_libraries(easydb_server easydb)

