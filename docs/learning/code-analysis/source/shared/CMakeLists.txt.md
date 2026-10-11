# shared/CMakeLists.txt

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；44行。

[对应分析](../../06-build-and-entry.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cmake
# Defines infrastructure shared by implemented services.
add_library(arena_shared)

target_include_directories(arena_shared PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(arena_shared PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>
)

find_package(Protobuf REQUIRED)

set(ARENA_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(ARENA_PROTO_OUT "${CMAKE_CURRENT_BINARY_DIR}/generated")
add_custom_command(
    OUTPUT "${ARENA_PROTO_OUT}/envelope.pb.cc"
           "${ARENA_PROTO_OUT}/envelope.pb.h"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${ARENA_PROTO_OUT}"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --cpp_out ${ARENA_PROTO_OUT}
            --proto_path ${ARENA_PROTO_DIR}
            "${ARENA_PROTO_DIR}/envelope.proto"
    DEPENDS "${ARENA_PROTO_DIR}/envelope.proto"
    VERBATIM
)

add_custom_target(arena_envelope_proto DEPENDS
    "${ARENA_PROTO_OUT}/envelope.pb.cc"
    "${ARENA_PROTO_OUT}/envelope.pb.h"
)
add_dependencies(arena_shared arena_envelope_proto)

target_sources(arena_shared PRIVATE
    "${ARENA_PROTO_OUT}/envelope.pb.cc"
)
target_include_directories(arena_shared PUBLIC "${ARENA_PROTO_OUT}")
target_link_libraries(arena_shared PUBLIC protobuf::libprotobuf)
add_subdirectory(src/os)
add_subdirectory(src/concurrent)
add_subdirectory(src/protocol)
~~~
