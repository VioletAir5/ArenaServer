add_library(arena_gateway)

target_include_directories(arena_gateway PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(arena_gateway PUBLIC
    arena_shared
)

target_compile_options(arena_gateway PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -fsanitize=undefined
)

add_subdirectory(src/net)
add_subdirectory(app)
