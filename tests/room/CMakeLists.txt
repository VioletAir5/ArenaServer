add_executable(arena_room_tests RoomActorTest.cpp RoomServiceTest.cpp)
target_link_libraries(arena_room_tests PRIVATE arena_gateway arena_room GTest::gtest_main)
target_compile_options(arena_room_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_room_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
include(GoogleTest)
gtest_discover_tests(arena_room_tests)

find_package(Python3 REQUIRED COMPONENTS Interpreter)
add_dependencies(arena_room_tests arena_tcp_server_test_host
    arena_asio_tcp_server_test_host arena_websocket_test_host)
foreach(backend epoll asio)
    if(backend STREQUAL "epoll")
        set(room_host arena_tcp_server_test_host)
    else()
        set(room_host arena_asio_tcp_server_test_host)
    endif()
    foreach(workers 0 4)
        add_test(NAME RoomTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/RoomIntegrationTest.py"
                "$<TARGET_FILE:${room_host}>" tcp ${workers})
        set_tests_properties(RoomTransport_${backend}_${workers} PROPERTIES
            TIMEOUT 40 RUN_SERIAL TRUE LABELS "integration;network;room")
    endforeach()
endforeach()
add_test(NAME RoomTransport_ws
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/RoomIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
set_tests_properties(RoomTransport_ws PROPERTIES
    TIMEOUT 40 RUN_SERIAL TRUE LABELS "integration;network;room;websocket")
