# services/gateway/CMakeLists.txt

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；30行。

[对应分析](../../../06-build-and-entry.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cmake
# Defines the implemented gateway service library and its dependencies.
find_package(Boost 1.74 REQUIRED CONFIG)
find_package(Threads REQUIRED)
find_package(spdlog REQUIRED CONFIG)

add_library(arena_gateway)

target_include_directories(arena_gateway PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(arena_gateway PUBLIC
    arena_shared
    Boost::boost
    Threads::Threads
    spdlog::spdlog
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
~~~
