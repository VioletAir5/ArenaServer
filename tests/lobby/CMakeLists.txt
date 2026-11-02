add_executable(arena_lobby_state_tests DurableStateStoreTest.cpp)
target_link_libraries(arena_lobby_state_tests PRIVATE arena_lobby_state GTest::gtest_main)
target_compile_options(arena_lobby_state_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_lobby_state_tests PRIVATE -Wl,--wrap=fdatasync
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
include(GoogleTest)
gtest_discover_tests(arena_lobby_state_tests)

add_executable(arena_verified_session_tests VerifiedSessionTest.cpp)
target_link_libraries(arena_verified_session_tests PRIVATE arena_lobby_core GTest::gtest_main)
target_compile_options(arena_verified_session_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_verified_session_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_verified_session_tests)

add_executable(arena_lobby_auth_tests AuthClientTest.cpp)
target_link_libraries(arena_lobby_auth_tests PRIVATE arena_lobby_auth GTest::gtest_main)
target_compile_options(arena_lobby_auth_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_lobby_auth_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_lobby_auth_tests)

target_sources(arena_verified_session_tests PRIVATE OnlineRegistryRecoveryTest.cpp)

add_executable(arena_room_service_recovery_tests RoomServiceRecoveryTest.cpp)
target_link_libraries(arena_room_service_recovery_tests PRIVATE arena_lobby_core GTest::gtest_main)
target_compile_options(arena_room_service_recovery_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_room_service_recovery_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_room_service_recovery_tests)

add_executable(arena_lobby_codec_tests AuthorityCodecTest.cpp)
target_link_libraries(arena_lobby_codec_tests PRIVATE arena_lobby_codec GTest::gtest_main)
target_compile_options(arena_lobby_codec_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_lobby_codec_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_lobby_codec_tests)

add_executable(arena_lobby_storage_tests StorageClientTest.cpp)
target_link_libraries(arena_lobby_storage_tests PRIVATE arena_lobby_storage GTest::gtest_main)
target_compile_options(arena_lobby_storage_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_lobby_storage_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_lobby_storage_tests)

if(ARENA_ENABLE_POSTGRES_TESTS)
    set(lobby_process_python "${Python3_EXECUTABLE}")
    if(ARENA_POSTGRES_TEST_PYTHON)
        set(lobby_process_python "${ARENA_POSTGRES_TEST_PYTHON}")
    endif()
    set(lobby_process_pg_bin "${ARENA_POSTGRES_BIN}")
    if(NOT lobby_process_pg_bin)
        find_program(ARENA_LOBBY_PG_CONFIG pg_config REQUIRED)
        execute_process(COMMAND "${ARENA_LOBBY_PG_CONFIG}" --bindir
            OUTPUT_VARIABLE lobby_process_pg_bin OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY)
    endif()
    set(lobby_process_arguments --repository-root "${PROJECT_SOURCE_DIR}"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --auth "$<TARGET_FILE:ArenaAuth>" --lobby "$<TARGET_FILE:ArenaLobby>"
        --gateway "$<TARGET_FILE:ArenaServer>"
        --replay "$<TARGET_FILE:arena_replay>" --replay-export "$<TARGET_FILE:arena_replay_export>" --pg-bin "${lobby_process_pg_bin}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/lobby-process-report.json")
    if(ARENA_POSTGRES_SHARE)
        list(APPEND lobby_process_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
    endif()
    set(lobby_process_clock)
    if(TARGET arena_test_clock)
        list(APPEND lobby_process_clock "${lobby_process_python}"
            "${PROJECT_SOURCE_DIR}/tests/shared/clock/RunWithClock.py"
            --clock-shim "$<TARGET_FILE:arena_test_clock>"
            --sanitizer-binary "$<TARGET_FILE:ArenaLobby>" --)
        add_dependencies(ArenaLobby arena_test_clock)
    endif()
    add_test(NAME LobbyProcessAcceptance COMMAND ${lobby_process_clock}
        "${lobby_process_python}" "${CMAKE_CURRENT_SOURCE_DIR}/LobbyProcessIntegrationTest.py"
        ${lobby_process_arguments})
    set_tests_properties(LobbyProcessAcceptance PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE
        LABELS "integration;lobby;rpc;postgresql;process;recovery")
endif()

add_executable(arena_lobby_authority_tests LobbyAuthorityTest.cpp)
target_link_libraries(arena_lobby_authority_tests PRIVATE arena_lobby_authority GTest::gtest_main)
target_compile_options(arena_lobby_authority_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_lobby_authority_tests PRIVATE -Wl,--wrap=fdatasync
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_lobby_authority_tests)

add_executable(arena_lobby_service_tests LobbyServiceTest.cpp)
target_link_libraries(arena_lobby_service_tests PRIVATE arena_lobby_service GTest::gtest_main)
target_compile_options(arena_lobby_service_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_lobby_service_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_lobby_service_tests)

add_executable(arena_lobby_replay_tests ReplayArchiveTest.cpp)
target_link_libraries(arena_lobby_replay_tests PRIVATE arena_lobby_replay GTest::gtest_main)
target_compile_options(arena_lobby_replay_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_lobby_replay_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_lobby_replay_tests)

if(UNIX)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    add_test(NAME ServiceSupervisorAcceptance COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/ServiceSupervisorTest.py" --repo-root "${PROJECT_SOURCE_DIR}")
    set_tests_properties(ServiceSupervisorAcceptance PROPERTIES TIMEOUT 90 RUN_SERIAL TRUE
        LABELS "integration;runtime;supervisor;lifecycle")
endif()

if(ARENA_ENABLE_POSTGRES_TESTS)
    set(lobby_restart_arguments --repository-root "${PROJECT_SOURCE_DIR}"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --auth "$<TARGET_FILE:ArenaAuth>" --lobby "$<TARGET_FILE:ArenaLobby>"
        --gateway "$<TARGET_FILE:ArenaServer>" --pg-bin "${lobby_process_pg_bin}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/service-restart-report.json")
    set(supervised_process_arguments --repository-root "${PROJECT_SOURCE_DIR}"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --auth "$<TARGET_FILE:ArenaAuth>" --lobby "$<TARGET_FILE:ArenaLobby>"
        --gateway "$<TARGET_FILE:ArenaServer>" --pg-bin "${lobby_process_pg_bin}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/supervised-process-report.json")
    if(ARENA_POSTGRES_SHARE)
        list(APPEND lobby_restart_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
        list(APPEND supervised_process_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
    endif()
    add_test(NAME ServiceRestartAcceptance COMMAND ${lobby_process_clock}
        "${lobby_process_python}" "${CMAKE_CURRENT_SOURCE_DIR}/ServiceRestartAcceptanceTest.py"
        ${lobby_restart_arguments})
    set_tests_properties(ServiceRestartAcceptance PROPERTIES TIMEOUT 600 RUN_SERIAL TRUE
        LABELS "integration;process;recovery;postgresql;restart")
    # Exercise the production supervisor's explicit environment isolation under
    # the same host clock as its actual service children.
    add_test(NAME SupervisedProcessAcceptance COMMAND "${lobby_process_python}"
        "${CMAKE_CURRENT_SOURCE_DIR}/SupervisedProcessIntegrationTest.py" ${supervised_process_arguments})
    set_tests_properties(SupervisedProcessAcceptance PROPERTIES TIMEOUT 120 RUN_SERIAL TRUE
        LABELS "integration;process;supervisor;postgresql;recovery")
endif()

if(ARENA_ENABLE_POSTGRES_TESTS)
    set(service_boundary_arguments --repository-root "${PROJECT_SOURCE_DIR}"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --auth "$<TARGET_FILE:ArenaAuth>" --lobby "$<TARGET_FILE:ArenaLobby>"
        --gateway "$<TARGET_FILE:ArenaServer>" --pg-bin "${lobby_process_pg_bin}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/service-boundary-report.json")
    if(ARENA_POSTGRES_SHARE)
        list(APPEND service_boundary_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
    endif()
    add_test(NAME ServiceBoundaryProcessAcceptance COMMAND ${lobby_process_clock}
        "${lobby_process_python}" "${CMAKE_CURRENT_SOURCE_DIR}/ServiceBoundaryProcessTest.py"
        ${service_boundary_arguments})
    set_tests_properties(ServiceBoundaryProcessAcceptance PROPERTIES TIMEOUT 120 RUN_SERIAL TRUE
        LABELS "integration;process;contract;rpc;postgresql;deadline")
endif()
