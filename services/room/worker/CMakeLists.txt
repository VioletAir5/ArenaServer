find_package(OpenSSL 3.0 REQUIRED COMPONENTS Crypto)

# Structural wire conversion intentionally has no simulation-kernel linkage.
add_library(arena_room_wire src/RoomWire.cpp)
target_include_directories(arena_room_wire PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(arena_room_wire PUBLIC arena_domain arena_shared OpenSSL::Crypto)
target_compile_options(arena_room_wire PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_library(arena_room_worker src/RoomWorker.cpp)
target_link_libraries(arena_room_worker PUBLIC arena_room_wire arena_room_runtime)
target_compile_options(arena_room_worker PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_executable(ArenaRoom app/main.cpp)
target_link_libraries(ArenaRoom PRIVATE arena_room_worker)
target_compile_options(ArenaRoom PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(ArenaRoom PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)