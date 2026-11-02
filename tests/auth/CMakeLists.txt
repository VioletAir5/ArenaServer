add_executable(arena_auth_tests AuthSessionTest.cpp PasswordAdaptersTest.cpp
    OnlineRegistryTest.cpp OnlineRegistryMaintenanceTest.cpp OnlineProtocolTest.cpp)
target_link_libraries(arena_auth_tests PRIVATE arena_gateway arena_gateway_test_support GTest::gtest_main)
target_compile_options(arena_auth_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_auth_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
include(GoogleTest)
gtest_discover_tests(arena_auth_tests)

# Real transport owners exercise the same authentication interface.
find_package(Python3 REQUIRED COMPONENTS Interpreter)
add_dependencies(arena_auth_tests arena_tcp_server_test_host
    arena_asio_tcp_server_test_host arena_websocket_test_host)
foreach(backend epoll asio)
    if(backend STREQUAL "epoll")
        set(auth_host arena_tcp_server_test_host)
    else()
        set(auth_host arena_asio_tcp_server_test_host)
    endif()
    foreach(workers 0 4)
        add_test(NAME AccountLoginTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/AccountLoginIntegrationTest.py"
                "$<TARGET_FILE:${auth_host}>" tcp ${workers})
        set_tests_properties(AccountLoginTransport_${backend}_${workers} PROPERTIES
            TIMEOUT 40 RUN_SERIAL TRUE LABELS "integration;network;authentication")
        add_test(NAME OnlinePresenceTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/OnlinePresenceIntegrationTest.py"
                "$<TARGET_FILE:${auth_host}>" tcp ${workers})
        set_tests_properties(OnlinePresenceTransport_${backend}_${workers} PROPERTIES
            TIMEOUT 35 RUN_SERIAL TRUE LABELS "integration;network;authentication;presence")
    endforeach()
endforeach()
add_test(NAME AccountLoginTransport_ws
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/AccountLoginIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
set_tests_properties(AccountLoginTransport_ws PROPERTIES
    TIMEOUT 40 RUN_SERIAL TRUE LABELS "integration;network;authentication;websocket")
add_test(NAME OnlinePresenceTransport_ws
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/OnlinePresenceIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
set_tests_properties(OnlinePresenceTransport_ws PROPERTIES
    TIMEOUT 35 RUN_SERIAL TRUE LABELS "integration;network;authentication;presence;websocket")

add_executable(arena_auth_service_tests AuthServiceTest.cpp)
target_link_libraries(arena_auth_service_tests PRIVATE arena_auth_runtime GTest::gtest_main)
target_compile_options(arena_auth_service_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_auth_service_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_auth_service_tests)

if(ARENA_ENABLE_POSTGRES_TESTS)
    set(auth_process_python "${Python3_EXECUTABLE}")
    if(ARENA_POSTGRES_TEST_PYTHON)
        set(auth_process_python "${ARENA_POSTGRES_TEST_PYTHON}")
    endif()
    set(auth_process_pg_bin "${ARENA_POSTGRES_BIN}")
    if(NOT auth_process_pg_bin)
        find_program(ARENA_AUTH_PG_CONFIG pg_config REQUIRED)
        execute_process(COMMAND "${ARENA_AUTH_PG_CONFIG}" --bindir
            OUTPUT_VARIABLE auth_process_pg_bin OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY)
    endif()
    set(auth_process_arguments --repository-root "${PROJECT_SOURCE_DIR}"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --auth "$<TARGET_FILE:ArenaAuth>" --pg-bin "${auth_process_pg_bin}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/auth-process-report.json")
    if(ARENA_POSTGRES_SHARE)
        list(APPEND auth_process_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
    endif()
    set(auth_process_clock)
    if(TARGET arena_test_clock)
        list(APPEND auth_process_clock "${auth_process_python}"
            "${PROJECT_SOURCE_DIR}/tests/shared/clock/RunWithClock.py"
            --clock-shim "$<TARGET_FILE:arena_test_clock>"
            --sanitizer-binary "$<TARGET_FILE:ArenaAuth>" --)
    endif()
    add_test(NAME AuthProcessAcceptance COMMAND ${auth_process_clock}
        "${auth_process_python}" "${CMAKE_CURRENT_SOURCE_DIR}/AuthProcessIntegrationTest.py"
        ${auth_process_arguments})
    set_tests_properties(AuthProcessAcceptance PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE
        LABELS "integration;authentication;rpc;postgresql;process")
    add_dependencies(ArenaAuth arena_test_clock)
endif()
