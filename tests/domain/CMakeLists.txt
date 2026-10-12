find_package(Threads REQUIRED)
add_executable(arena_domain_tests LifecycleTest.cpp)
target_link_libraries(arena_domain_tests PRIVATE
    arena_domain
    GTest::gtest_main
    Threads::Threads
)
target_compile_options(arena_domain_tests PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)
target_link_options(arena_domain_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
    -no-pie
)
include(GoogleTest)
gtest_discover_tests(arena_domain_tests)
