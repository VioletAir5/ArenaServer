cmake_minimum_required(VERSION 3.23)

project(ArenaServer)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

# ----------------------
# Core library
# ----------------------

add_library(arena_core
    src/os/FileDescriptor.cpp
    src/net/Socket.cpp
    src/net/Session.cpp
    src/net/SessionManager.cpp
)

target_include_directories(arena_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(arena_core PRIVATE
    -Wall
    -Wextra
    -Wpedantic

    # 当前环境禁用 ASan，只保留 UBSan
    -fsanitize=undefined
)

# ----------------------
# Main executable
# ----------------------

add_executable(ArenaServer
    app/main.cpp
)

target_link_libraries(ArenaServer PRIVATE
    arena_core
)

target_compile_options(ArenaServer PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

# ----------------------
# Tests
# ----------------------

enable_testing()

add_executable(
    arena_tests

    tests/os/FileDescriptorTest.cpp
    tests/net/SessionManagerTest.cpp
)

target_link_libraries(
    arena_tests
    PRIVATE
    arena_core
    GTest::gtest_main
)

target_compile_options(arena_tests PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -fsanitize=undefined
)

target_link_options(arena_tests PRIVATE
    -fsanitize=undefined
)

include(GoogleTest)

gtest_discover_tests(arena_tests)