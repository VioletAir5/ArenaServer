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

# Repeat the networking acceptance on four actual worker Reactors.
foreach(scenario churn limit slow stream idle rst stop stop_fast)
    add_test(
        NAME TcpServerWorkers_${scenario}
        COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
            "$<TARGET_FILE:arena_tcp_server_test_host>"
            ${scenario}
            4
    )
    set_tests_properties(TcpServerWorkers_${scenario} PROPERTIES
        TIMEOUT 60 RUN_SERIAL TRUE LABELS "integration;network;workers"
    )
endforeach()

add_executable(arena_asio_tcp_server_test_host AsioTcpServerTestHost.cpp)
target_link_libraries(arena_asio_tcp_server_test_host PRIVATE arena_gateway)
target_compile_options(arena_asio_tcp_server_test_host PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_asio_tcp_server_test_host PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
add_dependencies(arena_tests arena_asio_tcp_server_test_host)
foreach(workers 0 4)
    foreach(scenario churn limit slow stream idle rst stop stop_fast fd_limit fd_limit_stop)
        add_test(NAME AsioTcpServer_${scenario}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/TcpServerIntegrationTest.py"
                "$<TARGET_FILE:arena_asio_tcp_server_test_host>" ${scenario} ${workers})
        set_tests_properties(AsioTcpServer_${scenario}_${workers} PROPERTIES
            TIMEOUT 60 RUN_SERIAL TRUE LABELS "integration;network;asio")
    endforeach()
endforeach()

# The same protocol acceptance suite crosses each real transport/worker setup.
foreach(backend epoll asio)
    if(backend STREQUAL "epoll")
        set(packet_host arena_tcp_server_test_host)
    else()
        set(packet_host arena_asio_tcp_server_test_host)
    endif()
    foreach(workers 0 4)
        add_test(NAME PacketCodecTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/PacketCodecIntegrationTest.py"
                "$<TARGET_FILE:${packet_host}>" ${workers})
        set_tests_properties(PacketCodecTransport_${backend}_${workers} PROPERTIES
            TIMEOUT 60 RUN_SERIAL TRUE LABELS "integration;network;protocol")
    endforeach()
endforeach()

add_executable(arena_websocket_test_host WebSocketTestHost.cpp)
target_link_libraries(arena_websocket_test_host PRIVATE arena_gateway)
target_compile_options(arena_websocket_test_host PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)
target_link_options(arena_websocket_test_host PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie
)
add_dependencies(arena_tests arena_websocket_test_host)
add_test(NAME WebSocketTransport_HandshakeLimits
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/WebSocketIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>"
)
set_tests_properties(WebSocketTransport_HandshakeLimits PROPERTIES
    LABELS "integration;network;websocket"
    TIMEOUT 30
    RUN_SERIAL TRUE
)

# Admission metadata crosses the real owner handoff in each backend.
foreach(backend epoll asio)
    if(backend STREQUAL "epoll")
        set(context_host arena_tcp_server_test_host)
    else()
        set(context_host arena_asio_tcp_server_test_host)
    endif()
    foreach(workers 0 4)
        add_test(NAME ConnectionContextTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/ConnectionContextIntegrationTest.py"
                "$<TARGET_FILE:${context_host}>" tcp ${workers})
        set_tests_properties(ConnectionContextTransport_${backend}_${workers}
            PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;context")
    endforeach()
endforeach()
add_test(NAME ConnectionContextTransport_ws
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/ConnectionContextIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
set_tests_properties(ConnectionContextTransport_ws PROPERTIES
    TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;context;websocket")

# One profile drives all transport owners; packet fragments/coalescing are shared.
foreach(backend epoll asio)
    if(backend STREQUAL "epoll")
        set(feature_host arena_tcp_server_test_host)
    else()
        set(feature_host arena_asio_tcp_server_test_host)
    endif()
    foreach(workers 0 4)
        add_test(NAME PacketPolicyTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/PacketPolicyIntegrationTest.py"
                "$<TARGET_FILE:${feature_host}>" tcp ${workers})
        set_tests_properties(PacketPolicyTransport_${backend}_${workers}
            PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;features")
    endforeach()
endforeach()
add_test(NAME PacketPolicyTransport_ws
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/PacketPolicyIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
set_tests_properties(PacketPolicyTransport_ws PROPERTIES
    TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;features;websocket")

# Authentication and exact reason counts cross all actual transport owners.
foreach(backend epoll asio)
    if(backend STREQUAL "epoll")
        set(security_host arena_tcp_server_test_host)
    else()
        set(security_host arena_asio_tcp_server_test_host)
    endif()
    foreach(workers 0 4)
        add_test(NAME SecurityPolicyTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/SecurityPolicyIntegrationTest.py"
                "$<TARGET_FILE:${security_host}>" tcp ${workers})
        set_tests_properties(SecurityPolicyTransport_${backend}_${workers}
            PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;security")
    endforeach()
endforeach()
add_test(NAME SecurityPolicyTransport_ws
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/SecurityPolicyIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
set_tests_properties(SecurityPolicyTransport_ws PROPERTIES
    TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;security;websocket")

add_test(NAME MessageDispatchTransport_Default
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/MessageDispatchIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>")
set_tests_properties(MessageDispatchTransport_Default PROPERTIES
    TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;rpc;websocket")

foreach(backend epoll asio)
    if(backend STREQUAL "epoll")
        set(rpc_host arena_tcp_server_test_host)
    else()
        set(rpc_host arena_asio_tcp_server_test_host)
    endif()
    foreach(workers 0 4)
        add_test(NAME MessageDispatchTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/MessageDispatchIntegrationTest.py"
                "$<TARGET_FILE:${rpc_host}>" tcp ${workers})
        set_tests_properties(MessageDispatchTransport_${backend}_${workers}
            PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;rpc")
        add_test(NAME MessageDrainTransport_${backend}_${workers}
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/MessageDispatchIntegrationTest.py"
                "$<TARGET_FILE:${rpc_host}>" tcp ${workers} drain)
        set_tests_properties(MessageDrainTransport_${backend}_${workers}
            PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;rpc;drain")
    endforeach()
endforeach()
add_test(NAME MessageDispatchTransport_ws
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/MessageDispatchIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>" ws 0)
set_tests_properties(MessageDispatchTransport_ws PROPERTIES
    TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;rpc;websocket")
add_test(NAME MessageDrainTransport_ws
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/MessageDispatchIntegrationTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>" ws 0 drain)
set_tests_properties(MessageDrainTransport_ws PROPERTIES
    TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;rpc;drain;websocket")

# A short framing regression; the 30-minute chaos run stays opt-in.
add_test(NAME ChaosRun_WebSocketFraming
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/ChaosRunFramingTest.py"
        "$<TARGET_FILE:arena_websocket_test_host>")
set_tests_properties(ChaosRun_WebSocketFraming PROPERTIES
    TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;chaos;websocket")

# Exercise actual application assembly, signals and shared listener budgets.
add_dependencies(arena_tests ArenaServer)
foreach(scenario epoll asio invalid shared_ws authenticated_shutdown)
    add_test(NAME ApplicationLifecycle_${scenario}
        COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/ApplicationLifecycleIntegrationTest.py"
            "$<TARGET_FILE:ArenaServer>" ${scenario})
    set_tests_properties(ApplicationLifecycle_${scenario} PROPERTIES
        TIMEOUT 30 RUN_SERIAL TRUE LABELS "integration;network;application")
endforeach()
