# services/gateway/app/CMakeLists.txt

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；19行。

[对应分析](../../../../06-build-and-entry.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cmake
# Builds the gateway application executable.
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
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)

target_link_options(ArenaServer PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)
~~~
