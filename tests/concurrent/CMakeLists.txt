# tests/net/CMakeLists.txt

target_sources(arena_tests PRIVATE
    BlockingQueueTest.cpp
    ThreadPoolTest.cpp
)

target_sources(arena_perf_tests PRIVATE
    BlockingQueuePerformanceTest.cpp
    ThreadPoolPerformanceTest.cpp
)