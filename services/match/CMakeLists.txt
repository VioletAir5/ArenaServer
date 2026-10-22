# Matchmaking owns business policy and depends only on domain identities/lifecycles.
add_library(arena_match src/MatchQueue.cpp)
target_include_directories(arena_match PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_match PUBLIC arena_domain)
target_compile_options(arena_match PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
