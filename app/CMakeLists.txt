add_executable(ArenaServer
    main.cpp
)

target_link_libraries(ArenaServer PRIVATE
    arena_core
)

target_compile_options(ArenaServer PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)