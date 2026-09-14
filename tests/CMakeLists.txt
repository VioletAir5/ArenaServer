# tests/CMakeLists.txt

add_executable(arena_tests)
add_executable(arena_perf_tests)

target_link_libraries(arena_tests PRIVATE
    arena_core
    GTest::gtest_main
)

target_compile_options(arena_tests PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -fsanitize=undefined
)
target_link_libraries(arena_perf_tests PRIVATE
    arena_core
    GTest::gtest_main
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

add_subdirectory(os)
add_subdirectory(net)
add_subdirectory(concurrent)

include(GoogleTest)
gtest_discover_tests(arena_tests)
gtest_discover_tests(arena_perf_tests)