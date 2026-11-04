find_package(Threads REQUIRED)

add_library(arena_room
    src/RoomActor.cpp
    src/ReplayFile.cpp
    src/ReplayRecorder.cpp
)
target_include_directories(arena_room PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_room PUBLIC arena_domain Threads::Threads)
target_compile_options(arena_room PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)

add_executable(arena_replay app/ReplayMain.cpp)
target_link_libraries(arena_replay PRIVATE arena_room)
target_compile_options(arena_replay PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)
target_link_options(arena_replay PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)

# Shared immutable query policy contains no simulation implementation.
add_library(arena_room_query src/RoomQuery.cpp)

target_include_directories(arena_room_query PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_room_query PUBLIC arena_domain)
target_compile_options(arena_room_query PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

# Pure worker kernel; no network, registry, journal or clock dependency.
add_library(arena_room_runtime src/RoomRuntime.cpp)
target_link_libraries(arena_room_runtime PUBLIC arena_room arena_room_query)
target_compile_options(arena_room_runtime PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
add_subdirectory(worker)
