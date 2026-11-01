find_package(OpenSSL 3.0 REQUIRED COMPONENTS Crypto)
find_package(Boost 1.74 REQUIRED CONFIG)
add_library(arena_auth src/JwtTokens.cpp src/LoginAuthenticator.cpp src/PasswordDigest.cpp src/FilePasswordAccounts.cpp)
target_include_directories(arena_auth PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_auth PUBLIC arena_domain OpenSSL::Crypto Boost::boost)
target_compile_options(arena_auth PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_library(arena_auth_runtime src/AuthService.cpp)
target_link_libraries(arena_auth_runtime PUBLIC arena_auth arena_shared)
target_compile_options(arena_auth_runtime PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
add_executable(ArenaAuth app/main.cpp)
target_link_libraries(ArenaAuth PRIVATE arena_auth_runtime)
target_compile_options(ArenaAuth PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(ArenaAuth PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
