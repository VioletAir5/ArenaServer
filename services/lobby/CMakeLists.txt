add_library(arena_lobby_state src/DurableStateStore.cpp)
target_include_directories(arena_lobby_state PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_lobby_state PUBLIC arena_shared)
target_compile_options(arena_lobby_state PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_library(arena_lobby_auth src/AuthClient.cpp)
target_include_directories(arena_lobby_auth PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_lobby_auth PUBLIC arena_shared)
target_compile_options(arena_lobby_auth PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
