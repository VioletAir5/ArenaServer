arena_collect_tests()

find_package(Python3 REQUIRED COMPONENTS Interpreter)

add_executable(arena_tcp_server_test_host TcpServerTestHost.cpp)
target_link_libraries(arena_tcp_server_test_host PRIVATE arena_gateway)
target_compile_options(arena_tcp_server_test_host PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)
# Avoid intermittent ASan startup failures with PIE binaries under WSL.
target_link_options(arena_tcp_server_test_host PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
    -no-pie
)
add_dependencies(arena_tests arena_tcp_server_test_host)

add_test(
    NAME TcpServerThousandConnectionsNoFdLeak
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        churn
)
add_test(
    NAME TcpServerRejectsOverLimit
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        limit
)
add_test(
    NAME TcpServerDisconnectsSlowClient
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        slow
)
add_test(
    NAME TcpServerClosesIdleConnection
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        idle
)
add_test(
    NAME TcpServerHandlesPeerReset
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        rst
)
add_test(
    NAME TcpServerGracefulStopDeadline
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        stop
)
add_test(
    NAME TcpServerGracefulStopCompletesEarly
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        stop_fast
)
add_test(
    NAME TcpServerPreservesRandomByteStream
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        stream
)
add_test(
    NAME TcpServerRecoversFromFdExhaustion
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        fd_limit
)
add_test(
    NAME TcpServerStopsDuringFdExhaustion
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
        "$<TARGET_FILE:arena_tcp_server_test_host>"
        fd_limit_stop
)
set_tests_properties(
    TcpServerThousandConnectionsNoFdLeak TcpServerRejectsOverLimit
    TcpServerDisconnectsSlowClient TcpServerPreservesRandomByteStream
    TcpServerClosesIdleConnection TcpServerHandlesPeerReset
    TcpServerGracefulStopDeadline
    TcpServerGracefulStopCompletesEarly TcpServerRecoversFromFdExhaustion
    TcpServerStopsDuringFdExhaustion
    PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE LABELS "integration;network"
)

# Deterministic worker-construction failure without exhausting sanitizer FDs.
# Only arena_tests wraps this OS call; production/performance binaries do not.
target_link_options(arena_tests PRIVATE -Wl,--wrap=timerfd_create)
