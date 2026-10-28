# Database tests are explicit: enabling them must find real PostgreSQL tools.
option(ARENA_ENABLE_POSTGRES_TESTS "Run isolated PostgreSQL storage migration tests" OFF)
set(ARENA_POSTGRES_BIN "$ENV{PG_BIN}" CACHE PATH "Directory containing PostgreSQL server and client tools")
set(ARENA_POSTGRES_SHARE "$ENV{PG_SHARE}" CACHE PATH "Optional relocated PostgreSQL share directory")
set(ARENA_POSTGRES_TEST_PYTHON "" CACHE FILEPATH "Optional Python interpreter with storage requirements installed")

if(ARENA_ENABLE_POSTGRES_TESTS)
    if(NOT UNIX)
        message(FATAL_ERROR "PostgreSQL storage tests require Linux/WSL")
    endif()
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    if(NOT ARENA_POSTGRES_BIN)
        find_program(ARENA_PG_CONFIG pg_config REQUIRED)
        execute_process(COMMAND "${ARENA_PG_CONFIG}" --bindir
            OUTPUT_VARIABLE ARENA_POSTGRES_BIN OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY)
    endif()
    foreach(pg_tool initdb pg_ctl postgres psql pg_dump)
        if(NOT EXISTS "${ARENA_POSTGRES_BIN}/${pg_tool}")
            message(FATAL_ERROR "Missing ${pg_tool}; set ARENA_POSTGRES_BIN to a complete PostgreSQL installation")
        endif()
    endforeach()
    set(pg_test_arguments --repository-root "${PROJECT_SOURCE_DIR}"
        --pg-bin "${ARENA_POSTGRES_BIN}" --cycles 20
        --report "${CMAKE_CURRENT_BINARY_DIR}/storage-migration-report.json")
    if(ARENA_POSTGRES_SHARE)
        list(APPEND pg_test_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
    endif()
    add_test(NAME PostgresStorageMigrations
        COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/StorageMigrationTest.py"
            ${pg_test_arguments})
    set_tests_properties(PostgresStorageMigrations PROPERTIES
        TIMEOUT 180 RUN_SERIAL TRUE LABELS "integration;storage;postgresql;migration")

    set(asset_test_python "${Python3_EXECUTABLE}")
    if(ARENA_POSTGRES_TEST_PYTHON)
        get_filename_component(asset_test_python "${ARENA_POSTGRES_TEST_PYTHON}" ABSOLUTE
            BASE_DIR "${PROJECT_SOURCE_DIR}")
    endif()
    execute_process(
        COMMAND "${asset_test_python}" -c
            "import psycopg; import psycopg.pq; import psycopg_pool; assert psycopg.__version__ == '3.2.13'; assert psycopg_pool.__version__ == '3.2.8'; print(psycopg.__version__, psycopg_pool.__version__, psycopg.pq.version(), psycopg.pq.__impl__)"
        RESULT_VARIABLE asset_driver_status OUTPUT_VARIABLE asset_driver_version
        ERROR_VARIABLE asset_driver_error OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT asset_driver_status STREQUAL "0")
        message(FATAL_ERROR
            "Storage transaction tests require psycopg==3.2.13 and psycopg_pool==3.2.8. Install services/storage/requirements.txt "
            "and set ARENA_POSTGRES_TEST_PYTHON to that interpreter. ${asset_driver_error}")
    endif()
    message(STATUS "Asset transfer PostgreSQL driver: ${asset_driver_version}")
    set(asset_test_arguments --repository-root "${PROJECT_SOURCE_DIR}"
        --pg-bin "${ARENA_POSTGRES_BIN}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/asset-transfer-report.json")
    if(ARENA_POSTGRES_SHARE)
        list(APPEND asset_test_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
    endif()
    add_test(NAME AssetTransferConcurrency
        COMMAND "${asset_test_python}" "${CMAKE_CURRENT_SOURCE_DIR}/AssetTransferTest.py"
            ${asset_test_arguments})
    set_tests_properties(AssetTransferConcurrency PROPERTIES
        TIMEOUT 300 RUN_SERIAL TRUE LABELS "integration;storage;postgresql;assets;concurrency")

    set(repository_test_arguments --repository-root "${PROJECT_SOURCE_DIR}"
        --pg-bin "${ARENA_POSTGRES_BIN}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/repository-boundary-report.json")
    if(ARENA_POSTGRES_SHARE)
        list(APPEND repository_test_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
    endif()
    add_test(NAME RepositoryTransactionBoundary
        COMMAND "${asset_test_python}" "${CMAKE_CURRENT_SOURCE_DIR}/RepositoryBoundaryTest.py"
            ${repository_test_arguments})
    set_tests_properties(RepositoryTransactionBoundary PROPERTIES
        TIMEOUT 300 RUN_SERIAL TRUE LABELS "integration;storage;postgresql;repository;pool")
endif()