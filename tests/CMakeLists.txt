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
    -fsanitize=undefined
)

target_compile_options(arena_perf_tests PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -fsanitize=undefined
)

target_link_options(arena_tests PRIVATE
    -fsanitize=undefined
)

target_link_options(arena_perf_tests PRIVATE
    -fsanitize=undefined
)

add_subdirectory(shared/os)
add_subdirectory(shared/concurrent)
add_subdirectory(gateway/net)

include(GoogleTest)
gtest_discover_tests(arena_tests)
gtest_discover_tests(arena_perf_tests)
