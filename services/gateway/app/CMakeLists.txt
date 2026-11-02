find_package(OpenSSL REQUIRED)

# Builds the gateway application executable.
add_executable(ArenaServer
    main.cpp
)

target_link_libraries(ArenaServer PRIVATE
    arena_gateway
    OpenSSL::Crypto
)

target_compile_options(ArenaServer PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)

target_link_options(ArenaServer PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)
