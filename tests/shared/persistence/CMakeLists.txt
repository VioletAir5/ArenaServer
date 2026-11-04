add_executable(arena_atomic_journal_tests AtomicJournalTest.cpp)
target_link_libraries(arena_atomic_journal_tests PRIVATE arena_shared GTest::gtest_main)
target_compile_options(arena_atomic_journal_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_atomic_journal_tests PRIVATE
    -Wl,--wrap=fdatasync -Wl,--wrap=renameat -Wl,--wrap=fsync
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
include(GoogleTest)
gtest_discover_tests(arena_atomic_journal_tests)
