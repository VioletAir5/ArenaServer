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
add_subdirectory(shared/clock)
add_subdirectory(gateway/net)
add_subdirectory(domain)
add_subdirectory(auth)
add_subdirectory(match)
add_subdirectory(room)
add_subdirectory(storage)
add_subdirectory(architecture)
add_subdirectory(shared/rpc)
add_subdirectory(shared/runtime)
add_subdirectory(runtime)
add_dependencies(arena_runtime_config_tests arena_runtime_config_probe)
if(TARGET arena_storage_probe AND TARGET arena_test_clock)
    add_dependencies(arena_storage_probe arena_test_clock)
endif()

include(GoogleTest)
gtest_discover_tests(arena_tests)
gtest_discover_tests(arena_perf_tests)
