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
target_sources(arena_shared PRIVATE src/persistence/AtomicJournal.cpp)

add_subdirectory(src/os)
add_subdirectory(src/concurrent)
add_subdirectory(src/protocol)

# Emit a descriptor set for Python without checking generated bindings into Git.
add_custom_command(
    OUTPUT "${ARENA_PROTO_OUT}/rpc.pb.cc" "${ARENA_PROTO_OUT}/rpc.pb.h"
           "${ARENA_PROTO_OUT}/rpc.desc"
           "${ARENA_PROTO_OUT}/storage.pb.cc" "${ARENA_PROTO_OUT}/storage.pb.h"
           "${ARENA_PROTO_OUT}/runtime_config.pb.cc" "${ARENA_PROTO_OUT}/runtime_config.pb.h"
           "${ARENA_PROTO_OUT}/auth.pb.cc" "${ARENA_PROTO_OUT}/auth.pb.h"
           "${ARENA_PROTO_OUT}/lobby.pb.cc" "${ARENA_PROTO_OUT}/lobby.pb.h"
           "${ARENA_PROTO_OUT}/room.pb.cc" "${ARENA_PROTO_OUT}/room.pb.h"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${ARENA_PROTO_OUT}"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE} --experimental_allow_proto3_optional --cpp_out "${ARENA_PROTO_OUT}"
            --include_imports --descriptor_set_out "${ARENA_PROTO_OUT}/rpc.desc"
            --proto_path "${ARENA_PROTO_DIR}" "${ARENA_PROTO_DIR}/rpc.proto"
            "${ARENA_PROTO_DIR}/storage.proto" "${ARENA_PROTO_DIR}/runtime_config.proto"
            "${ARENA_PROTO_DIR}/auth.proto" "${ARENA_PROTO_DIR}/lobby.proto"
            "${ARENA_PROTO_DIR}/room.proto"
    DEPENDS "${ARENA_PROTO_DIR}/rpc.proto" "${ARENA_PROTO_DIR}/storage.proto"
            "${ARENA_PROTO_DIR}/runtime_config.proto" "${ARENA_PROTO_DIR}/auth.proto"
            "${ARENA_PROTO_DIR}/lobby.proto" "${ARENA_PROTO_DIR}/envelope.proto"
            "${ARENA_PROTO_DIR}/room.proto"
    VERBATIM)
add_custom_target(arena_rpc_proto DEPENDS "${ARENA_PROTO_OUT}/rpc.pb.cc"
    "${ARENA_PROTO_OUT}/rpc.pb.h" "${ARENA_PROTO_OUT}/rpc.desc")
add_dependencies(arena_shared arena_rpc_proto)
target_sources(arena_shared PRIVATE "${ARENA_PROTO_OUT}/rpc.pb.cc"
    "${ARENA_PROTO_OUT}/storage.pb.cc" "${ARENA_PROTO_OUT}/runtime_config.pb.cc"
    "${ARENA_PROTO_OUT}/auth.pb.cc" "${ARENA_PROTO_OUT}/lobby.pb.cc"
    "${ARENA_PROTO_OUT}/room.pb.cc"
    src/runtime/RuntimeConfig.cpp src/runtime/RuntimeControl.cpp
    src/runtime/OwnerScheduler.cpp)
find_package(Boost 1.74 REQUIRED CONFIG)
find_package(Threads REQUIRED)
target_sources(arena_shared PRIVATE src/rpc/Client.cpp src/rpc/Server.cpp)
target_link_libraries(arena_shared PUBLIC Boost::boost Threads::Threads)
target_compile_definitions(arena_shared PUBLIC BOOST_ERROR_CODE_HEADER_ONLY)