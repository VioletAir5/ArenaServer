add_library(arena_core)

target_include_directories(arena_core PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_compile_options(arena_core PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -fsanitize=undefined
)

add_subdirectory(os)
add_subdirectory(net)