# Business lifecycle definitions, not an executable service or shared infrastructure.
find_package(Threads REQUIRED)
add_library(arena_domain INTERFACE)
target_include_directories(arena_domain INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(arena_domain INTERFACE arena_shared Threads::Threads)
