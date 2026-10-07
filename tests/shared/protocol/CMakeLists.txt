set(LEGACY_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/compat")
set(LEGACY_PROTO_OUT "${CMAKE_CURRENT_BINARY_DIR}/generated")
add_custom_command(
    OUTPUT "${LEGACY_PROTO_OUT}/legacy_envelope.pb.cc"
           "${LEGACY_PROTO_OUT}/legacy_envelope.pb.h"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${LEGACY_PROTO_OUT}"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --cpp_out ${LEGACY_PROTO_OUT}
            --proto_path ${LEGACY_PROTO_DIR}
            "${LEGACY_PROTO_DIR}/legacy_envelope.proto"
    DEPENDS "${LEGACY_PROTO_DIR}/legacy_envelope.proto"
    VERBATIM
)
add_custom_target(arena_legacy_proto DEPENDS
    "${LEGACY_PROTO_OUT}/legacy_envelope.pb.cc"
    "${LEGACY_PROTO_OUT}/legacy_envelope.pb.h"
)
add_dependencies(arena_tests arena_legacy_proto)
set_source_files_properties("${LEGACY_PROTO_OUT}/legacy_envelope.pb.cc"
    PROPERTIES GENERATED TRUE)
target_sources(arena_tests PRIVATE
    "${LEGACY_PROTO_OUT}/legacy_envelope.pb.cc"
)
target_include_directories(arena_tests PRIVATE "${LEGACY_PROTO_OUT}")
arena_collect_tests()
