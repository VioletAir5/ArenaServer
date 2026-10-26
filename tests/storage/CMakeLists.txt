# Database tests are explicit: enabling them must find real PostgreSQL tools.
option(ARENA_ENABLE_POSTGRES_TESTS "Run isolated PostgreSQL storage migration tests" OFF)
set(ARENA_POSTGRES_BIN "$ENV{PG_BIN}" CACHE PATH "Directory containing PostgreSQL server and client tools")
set(ARENA_POSTGRES_SHARE "$ENV{PG_SHARE}" CACHE PATH "Optional relocated PostgreSQL share directory")

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
endif()