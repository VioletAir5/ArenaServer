add_library(arena_room src/RoomActor.cpp)
target_include_directories(arena_room PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_room PUBLIC arena_domain)
target_compile_options(arena_room PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)
