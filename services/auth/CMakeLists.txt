find_package(OpenSSL 3.0 REQUIRED COMPONENTS Crypto)
find_package(Boost 1.74 REQUIRED CONFIG)
add_library(arena_auth src/JwtTokens.cpp src/LoginAuthenticator.cpp src/PasswordDigest.cpp src/FilePasswordAccounts.cpp)
target_include_directories(arena_auth PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_auth PUBLIC arena_domain OpenSSL::Crypto Boost::boost)
target_compile_options(arena_auth PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
