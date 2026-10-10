# Configures the ArenaServer build and its implemented service targets.
cmake_minimum_required(VERSION 3.23)

project(ArenaServer)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Keep undefined-behavior checks by default. The TSan preset runs
# separately because UBSan vptr probes race inside the installed runtime.
option(ARENA_ENABLE_UBSAN "Enable undefined-behavior sanitizer" ON)

include(FetchContent)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

enable_testing()

add_subdirectory(shared)
add_subdirectory(services/gateway)
add_subdirectory(tests)
