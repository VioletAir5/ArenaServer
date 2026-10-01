arena_collect_tests()

find_package(Python3 REQUIRED COMPONENTS Interpreter)

add_executable(arena_tcp_server_test_host TcpServerTestHost.cpp)
target_link_libraries(arena_tcp_server_test_host PRIVATE arena_gateway)
target_compile_options(arena_tcp_server_test_host PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -fsanitize=undefined
)
# Avoid intermittent ASan startup failures with PIE binaries under WSL.
target_link_options(arena_tcp_server_test_host PRIVATE -fsanitize=undefined -no-pie)
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
set_tests_properties(
    TcpServerThousandConnectionsNoFdLeak TcpServerRejectsOverLimit
    PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE LABELS "integration;network"
)
