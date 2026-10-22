add_executable(arena_match_tests MatchQueueTest.cpp MatchmakingServiceTest.cpp)
target_link_libraries(arena_match_tests PRIVATE arena_gateway GTest::gtest_main)
target_compile_options(arena_match_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_match_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
include(GoogleTest)
gtest_discover_tests(arena_match_tests)

find_package(Python3 REQUIRED COMPONENTS Interpreter)
add_dependencies(arena_match_tests arena_tcp_server_test_host
    arena_asio_tcp_server_test_host arena_websocket_test_host)
foreach(backend epoll asio)
    if(backend STREQUAL "epoll")
        set(match_host arena_tcp_server_test_host)
    else()
        set(match_host arena_asio_tcp_server_test_host)
    endif()
    foreach(workers 0 4)
        add_test(NAME MatchmakingTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/MatchmakingIntegrationTest.py"
                "$<TARGET_FILE:${match_host}>" tcp ${workers})
        set_tests_properties(MatchmakingTransport_${backend}_${workers} PROPERTIES
            TIMEOUT 40 RUN_SERIAL TRUE LABELS "integration;network;matchmaking")
    endforeach()
endforeach()
add_test(NAME MatchmakingTransport_ws
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/MatchmakingIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
set_tests_properties(MatchmakingTransport_ws PROPERTIES
    TIMEOUT 40 RUN_SERIAL TRUE LABELS "integration;network;matchmaking;websocket")
