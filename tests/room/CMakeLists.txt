add_executable(arena_room_tests RoomActorTest.cpp RoomServiceTest.cpp
    ReplayFileTest.cpp ReplayAcceptanceTest.cpp ReplayRecorderTest.cpp)
target_link_libraries(arena_room_tests PRIVATE arena_gateway arena_gateway_test_support arena_room GTest::gtest_main)
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

add_dependencies(arena_room_tests arena_replay)
add_test(NAME ReplayToolAcceptance
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/ReplayToolTest.py"
        "$<TARGET_FILE:arena_room_tests>" "$<TARGET_FILE:arena_replay>")
set_tests_properties(ReplayToolAcceptance PROPERTIES
    TIMEOUT 180 RUN_SERIAL TRUE LABELS "integration;replay;determinism")

# The production handshake now requires actual independent business processes.
if(ARENA_ENABLE_POSTGRES_TESTS)
    set(journey_python "${Python3_EXECUTABLE}")
    if(ARENA_POSTGRES_TEST_PYTHON)
        set(journey_python "${ARENA_POSTGRES_TEST_PYTHON}")
    endif()
    set(journey_pg_bin "${ARENA_POSTGRES_BIN}")
    if(NOT journey_pg_bin)
        find_program(ARENA_JOURNEY_PG_CONFIG pg_config REQUIRED)
        execute_process(COMMAND "${ARENA_JOURNEY_PG_CONFIG}" --bindir
            OUTPUT_VARIABLE journey_pg_bin OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY)
    endif()
    set(journey_arguments "$<TARGET_FILE:ArenaServer>" "$<TARGET_FILE:arena_replay>"
        --auth "$<TARGET_FILE:ArenaAuth>" --lobby "$<TARGET_FILE:ArenaLobby>"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --replay-export "$<TARGET_FILE:arena_replay_export>" --pg-bin "${journey_pg_bin}"
        --report-dir "${CMAKE_CURRENT_BINARY_DIR}/full-journey-report")
    if(ARENA_POSTGRES_SHARE)
        list(APPEND journey_arguments --pg-share "${ARENA_POSTGRES_SHARE}")
    endif()
    set(journey_clock)
    if(TARGET arena_test_clock)
        list(APPEND journey_clock "${journey_python}"
            "${PROJECT_SOURCE_DIR}/tests/shared/clock/RunWithClock.py"
            --clock-shim "$<TARGET_FILE:arena_test_clock>"
            --sanitizer-binary "$<TARGET_FILE:ArenaServer>" --)
        add_dependencies(arena_room_tests arena_test_clock)
    endif()
    add_dependencies(arena_room_tests ArenaServer ArenaAuth ArenaLobby arena_replay_export)
    add_test(NAME FullJourneyProductionGateway
        COMMAND ${journey_clock} "${journey_python}"
            "${CMAKE_CURRENT_SOURCE_DIR}/FullJourneyIntegrationTest.py" ${journey_arguments})
    set_tests_properties(FullJourneyProductionGateway PROPERTIES
        TIMEOUT 120 RUN_SERIAL TRUE LABELS "integration;network;journey;postgresql;process")
endif()
add_executable(arena_room_recovery_tests RoomRecoveryTest.cpp)
target_link_libraries(arena_room_recovery_tests PRIVATE arena_room GTest::gtest_main)
target_compile_options(arena_room_recovery_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_room_recovery_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_room_recovery_tests)
