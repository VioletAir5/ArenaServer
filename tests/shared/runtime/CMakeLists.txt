add_executable(arena_runtime_config_tests RuntimeConfigTest.cpp RuntimeControlTest.cpp)
target_link_libraries(arena_runtime_config_tests PRIVATE arena_shared GTest::gtest_main)
target_compile_options(arena_runtime_config_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_runtime_config_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
include(GoogleTest)
gtest_discover_tests(arena_runtime_config_tests)