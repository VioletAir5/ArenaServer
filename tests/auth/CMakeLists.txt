add_executable(arena_auth_tests AuthSessionTest.cpp
    OnlineRegistryTest.cpp OnlineRegistryMaintenanceTest.cpp OnlineProtocolTest.cpp)
target_link_libraries(arena_auth_tests PRIVATE arena_gateway GTest::gtest_main)
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
