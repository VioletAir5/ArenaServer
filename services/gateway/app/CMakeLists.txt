add_executable(ArenaServer
    main.cpp
)

target_link_libraries(ArenaServer PRIVATE
    arena_gateway
)

target_compile_options(ArenaServer PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -fsanitize=undefined
)

target_link_options(ArenaServer PRIVATE
    -fsanitize=undefined
)
