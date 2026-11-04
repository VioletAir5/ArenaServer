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
    set(postgres_clock_command)
    if(TARGET arena_test_clock)
        list(APPEND postgres_clock_command "${Python3_EXECUTABLE}"
            "${PROJECT_SOURCE_DIR}/tests/shared/clock/RunWithClock.py"
            --clock-shim "$<TARGET_FILE:arena_test_clock>" --)
    endif()
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
        COMMAND ${postgres_clock_command} "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/StorageMigrationTest.py"
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
            "import psycopg; import psycopg.pq; import psycopg_pool; assert psycopg.__version__ == '3.2.13'; assert psycopg_pool.__version__ == '3.2.8'; assert psycopg.pq.version() >= 170000; print(psycopg.__version__, psycopg_pool.__version__, psycopg.pq.version(), psycopg.pq.__impl__)"
        RESULT_VARIABLE asset_driver_status OUTPUT_VARIABLE asset_driver_version
        ERROR_VARIABLE asset_driver_error OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT asset_driver_status STREQUAL "0")
        message(FATAL_ERROR
            "Storage transaction tests require psycopg==3.2.13, psycopg_pool==3.2.8 and libpq 17+. Install services/storage/requirements.txt "
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
        COMMAND ${postgres_clock_command} "${asset_test_python}" "${CMAKE_CURRENT_SOURCE_DIR}/AssetTransferTest.py"
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
        COMMAND ${postgres_clock_command} "${asset_test_python}" "${CMAKE_CURRENT_SOURCE_DIR}/RepositoryBoundaryTest.py"
            ${repository_test_arguments})
    set_tests_properties(RepositoryTransactionBoundary PROPERTIES
        TIMEOUT 300 RUN_SERIAL TRUE LABELS "integration;storage;postgresql;repository;pool")
    set(settlement_test_arguments --repository-root "${PROJECT_SOURCE_DIR}"
        --pg-bin "${ARENA_POSTGRES_BIN}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/settlement-outbox-report.json")
    if(ARENA_POSTGRES_SHARE)
        list(APPEND settlement_test_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
    endif()
    add_test(NAME SettlementOutboxAcceptance
        COMMAND ${postgres_clock_command} "${asset_test_python}" "${CMAKE_CURRENT_SOURCE_DIR}/SettlementOutboxTest.py"
            ${settlement_test_arguments})
    set_tests_properties(SettlementOutboxAcceptance PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE
        LABELS "integration;storage;postgresql;settlement;outbox")
    if(ARENA_ENABLE_RPC_TESTS)
        set(storage_rpc_arguments --repository-root "${PROJECT_SOURCE_DIR}"
            --pg-bin "${ARENA_POSTGRES_BIN}"
            --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
            --native-probe "$<TARGET_FILE:arena_storage_probe>"
            --report "${CMAKE_CURRENT_BINARY_DIR}/storage-rpc-report.json")
        if(ARENA_POSTGRES_SHARE)
            list(APPEND storage_rpc_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
        endif()
        set(storage_rpc_clock_command)
        if(TARGET arena_test_clock)
            list(APPEND storage_rpc_clock_command
                "${asset_test_python}" "${PROJECT_SOURCE_DIR}/tests/shared/clock/RunWithClock.py"
                --clock-shim "$<TARGET_FILE:arena_test_clock>"
                --sanitizer-binary "$<TARGET_FILE:arena_storage_probe>" --)
        endif()
        add_test(NAME StorageRpcAcceptance COMMAND ${storage_rpc_clock_command}
            "${asset_test_python}" "${CMAKE_CURRENT_SOURCE_DIR}/StorageRpcTest.py" ${storage_rpc_arguments})
        set_tests_properties(StorageRpcAcceptance PROPERTIES TIMEOUT 90 RUN_SERIAL TRUE
            LABELS "integration;storage;postgresql;rpc")
        set(deadline_test_arguments --repository-root "${PROJECT_SOURCE_DIR}"
            --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
            --pg-bin "${ARENA_POSTGRES_BIN}"
            --report "${CMAKE_CURRENT_BINARY_DIR}/deadline-cancellation-report.json")
        if(ARENA_POSTGRES_SHARE)
            list(APPEND deadline_test_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
        endif()
        add_test(NAME PostgresDeadlineCancellation COMMAND ${postgres_clock_command} "${asset_test_python}"
            "${CMAKE_CURRENT_SOURCE_DIR}/DeadlineCancellationTest.py" ${deadline_test_arguments})
        set_tests_properties(PostgresDeadlineCancellation PROPERTIES TIMEOUT 90 RUN_SERIAL TRUE
            LABELS "integration;storage;postgresql;rpc;deadline;cancellation")
        set(persistent_gateway_arguments --repository-root "${PROJECT_SOURCE_DIR}"
            --pg-bin "${ARENA_POSTGRES_BIN}"
            --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
            --gateway "$<TARGET_FILE:ArenaServer>"
            --auth "$<TARGET_FILE:ArenaAuth>" --lobby "$<TARGET_FILE:ArenaLobby>" --room "$<TARGET_FILE:ArenaRoom>"
            --lobby-descriptor "${PROJECT_BINARY_DIR}/services/lobby/generated/lobby-private.desc"
            --replay-export "$<TARGET_FILE:arena_replay_export>" --replay "$<TARGET_FILE:arena_replay>"
            --report "${CMAKE_CURRENT_BINARY_DIR}/persistent-gateway-report.json")
        if(ARENA_POSTGRES_SHARE)
            list(APPEND persistent_gateway_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
        endif()
        set(persistent_gateway_clock_command)
        if(TARGET arena_test_clock)
            list(APPEND persistent_gateway_clock_command "${asset_test_python}"
                "${PROJECT_SOURCE_DIR}/tests/shared/clock/RunWithClock.py"
                --clock-shim "$<TARGET_FILE:arena_test_clock>"
                --sanitizer-binary "$<TARGET_FILE:ArenaServer>" --)
        endif()
        add_test(NAME PersistentGatewayJourney COMMAND ${persistent_gateway_clock_command} "${asset_test_python}"
            "${CMAKE_CURRENT_SOURCE_DIR}/PersistentGatewayTest.py" ${persistent_gateway_arguments})
        set_tests_properties(PersistentGatewayJourney PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE
            LABELS "integration;storage;postgresql;gateway;journey;recovery")
    endif()
endif()
option(ARENA_ENABLE_REDIS_TESTS "Run isolated Redis cache acceptance" OFF)
set(ARENA_REDIS_SERVER "" CACHE FILEPATH "redis-server used only for private test fixtures")
if(ARENA_ENABLE_REDIS_TESTS)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    if(NOT ARENA_REDIS_SERVER)
        find_program(ARENA_REDIS_SERVER_EXECUTABLE redis-server REQUIRED)
        set(ARENA_REDIS_SERVER "${ARENA_REDIS_SERVER_EXECUTABLE}")
    endif()
    if(NOT EXISTS "${ARENA_REDIS_SERVER}")
        message(FATAL_ERROR "ARENA_REDIS_SERVER must name an existing redis-server")
    endif()
    set(redis_python "${Python3_EXECUTABLE}")
    if(ARENA_POSTGRES_TEST_PYTHON)
        set(redis_python "${ARENA_POSTGRES_TEST_PYTHON}")
    endif()
    execute_process(COMMAND "${redis_python}" -c
        "import redis, async_timeout; assert redis.__version__ == '5.2.1'; assert async_timeout.__version__ == '5.0.1'"
        RESULT_VARIABLE redis_driver_status ERROR_VARIABLE redis_driver_error)
    if(NOT redis_driver_status STREQUAL "0")
        message(FATAL_ERROR "Install services/storage/requirements.txt and set ARENA_POSTGRES_TEST_PYTHON. ${redis_driver_error}")
    endif()
    add_test(NAME RedisCacheAcceptance COMMAND "${redis_python}"
        "${CMAKE_CURRENT_SOURCE_DIR}/RedisCacheTest.py"
        --repository-root "${PROJECT_SOURCE_DIR}" --redis-server "${ARENA_REDIS_SERVER}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/redis-cache-report.json")
    set_tests_properties(RedisCacheAcceptance PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE
        LABELS "integration;storage;redis;cache")
endif()
