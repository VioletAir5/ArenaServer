# Test-only local business adapters and the retired Gateway persistence owner.
add_library(arena_gateway_test_support
    src/LoginHandler.cpp
    src/MatchHandler.cpp
    src/RoomHandler.cpp
    src/OnlineRegistryMaintenance.cpp
    src/PersistentStorage.cpp)
target_include_directories(arena_gateway_test_support PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_gateway_test_support PUBLIC
    arena_gateway
    arena_lobby_core
    arena_auth
    arena_match
    arena_room)
target_compile_options(arena_gateway_test_support PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
