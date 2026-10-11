# tests/CMakeLists.txt

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；61行。

[对应分析](../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cmake
add_executable(arena_tests)
add_executable(arena_perf_tests)

target_link_libraries(arena_tests PRIVATE
    arena_gateway
    arena_shared
    GTest::gtest_main
)

target_link_libraries(arena_perf_tests PRIVATE
    arena_gateway
    arena_shared
    GTest::gtest_main
)

target_compile_options(arena_tests PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)

target_compile_options(arena_perf_tests PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)

target_link_options(arena_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
    -no-pie
)

target_link_options(arena_perf_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
    -no-pie
)

function(arena_collect_tests)
    file(GLOB test_sources CONFIGURE_DEPENDS "*Test.cpp")
    file(GLOB performance_sources CONFIGURE_DEPENDS "*PerformanceTest.cpp")

    if(performance_sources)
        list(REMOVE_ITEM test_sources ${performance_sources})
        target_sources(arena_perf_tests PRIVATE ${performance_sources})
    endif()

    if(test_sources)
        target_sources(arena_tests PRIVATE ${test_sources})
    endif()
endfunction()

add_subdirectory(shared/os)
add_subdirectory(shared/concurrent)
add_subdirectory(shared/protocol)
add_subdirectory(gateway/net)

include(GoogleTest)
gtest_discover_tests(arena_tests)
gtest_discover_tests(arena_perf_tests)
~~~
