find_package(Boost 1.74 REQUIRED CONFIG)
find_package(Threads REQUIRED)

add_library(arena_gateway)

target_include_directories(arena_gateway PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(arena_gateway PUBLIC
    arena_shared
    Boost::boost
    Threads::Threads
)

target_compile_options(arena_gateway PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)

add_subdirectory(src/net)
add_subdirectory(app)

# Boost.System is header-only; no compiled boost_system dependency.
target_compile_definitions(arena_gateway PUBLIC BOOST_ERROR_CODE_HEADER_ONLY)
