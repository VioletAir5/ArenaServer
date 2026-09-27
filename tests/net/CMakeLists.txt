# tests/net/CMakeLists.txt

target_sources(arena_tests PRIVATE
    SessionManagerTest.cpp
    PacketBufferTest.cpp
    SocketTest.cpp
    ChannelTest.cpp
    EpollPollerTest.cpp
)


target_sources(arena_perf_tests PRIVATE
    EpollPollerPerformanceTest.cpp
)
