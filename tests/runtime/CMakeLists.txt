add_executable(arena_runtime_config_probe ConfigSnapshotProbe.cpp)
target_link_libraries(arena_runtime_config_probe PRIVATE arena_shared)
target_compile_options(arena_runtime_config_probe PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_runtime_config_probe PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)

if(ARENA_ENABLE_RPC_TESTS)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    set(config_test_python "${Python3_EXECUTABLE}")
    if(ARENA_RUNTIME_TEST_PYTHON)
        set(config_test_python "${ARENA_RUNTIME_TEST_PYTHON}")
    endif()
    set(config_python_arguments)
    if(ARENA_ENABLE_POSTGRES_TESTS)
        list(APPEND config_python_arguments --storage-lifecycle)
        if(ARENA_POSTGRES_TEST_PYTHON)
            set(config_test_python "${ARENA_POSTGRES_TEST_PYTHON}")
        endif()
    endif()
    add_test(NAME RuntimeConfigPython COMMAND "${config_test_python}"
        "${PROJECT_SOURCE_DIR}/tests/shared/runtime/ConfigTest.py"
        --repository-root "${PROJECT_SOURCE_DIR}"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --report "${CMAKE_CURRENT_BINARY_DIR}/python-config-report.json" ${config_python_arguments})
    set_tests_properties(RuntimeConfigPython PROPERTIES TIMEOUT 45
        LABELS "integration;runtime;configuration")
    add_test(NAME RuntimeConfigParity COMMAND "${config_test_python}"
        "${PROJECT_SOURCE_DIR}/tests/shared/runtime/ConfigParityTest.py"
        --repository-root "${PROJECT_SOURCE_DIR}"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --probe "$<TARGET_FILE:arena_runtime_config_probe>"
        --report "${CMAKE_CURRENT_BINARY_DIR}/config-parity-report.json")
    set_tests_properties(RuntimeConfigParity PROPERTIES TIMEOUT 45
        LABELS "integration;runtime;configuration;contract")
    if(ARENA_ENABLE_POSTGRES_TESTS)
        set(discovery_pg_bin "${ARENA_POSTGRES_BIN}")
        if(NOT discovery_pg_bin)
            find_program(ARENA_RUNTIME_PG_CONFIG pg_config REQUIRED)
            execute_process(COMMAND "${ARENA_RUNTIME_PG_CONFIG}" --bindir
                OUTPUT_VARIABLE discovery_pg_bin OUTPUT_STRIP_TRAILING_WHITESPACE
                COMMAND_ERROR_IS_FATAL ANY)
        endif()
        set(discovery_arguments --repository-root "${PROJECT_SOURCE_DIR}"
            --pg-bin "${discovery_pg_bin}"
            --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
            --gateway "$<TARGET_FILE:ArenaServer>"
            --auth "$<TARGET_FILE:ArenaAuth>" --lobby "$<TARGET_FILE:ArenaLobby>" --room "$<TARGET_FILE:ArenaRoom>"
            --lobby-descriptor "${PROJECT_BINARY_DIR}/services/lobby/generated/lobby-private.desc"
            --replay-export "$<TARGET_FILE:arena_replay_export>" --replay "$<TARGET_FILE:arena_replay>"
            --report "${CMAKE_CURRENT_BINARY_DIR}/config-discovery-report.json")
        if(ARENA_POSTGRES_SHARE)
            list(APPEND discovery_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
        endif()
        set(discovery_clock_command)
        if(TARGET arena_test_clock)
            list(APPEND discovery_clock_command "${config_test_python}"
                "${PROJECT_SOURCE_DIR}/tests/shared/clock/RunWithClock.py"
                --clock-shim "$<TARGET_FILE:arena_test_clock>"
                --sanitizer-binary "$<TARGET_FILE:ArenaServer>" --)
        endif()
        add_test(NAME RuntimeConfigDiscovery COMMAND ${discovery_clock_command}
            "${config_test_python}" "${CMAKE_CURRENT_SOURCE_DIR}/ConfigDiscoveryIntegrationTest.py"
            ${discovery_arguments})
        set_tests_properties(RuntimeConfigDiscovery PROPERTIES TIMEOUT 240 RUN_SERIAL TRUE
            LABELS "integration;runtime;configuration;discovery;postgresql;gateway")
    endif()
endif()