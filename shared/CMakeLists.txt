add_library(arena_shared)

target_include_directories(arena_shared PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(arena_shared PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -fsanitize=undefined
)

add_subdirectory(src/os)
add_subdirectory(src/concurrent)
