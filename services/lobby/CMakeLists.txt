add_library(arena_lobby_contract INTERFACE)
target_include_directories(arena_lobby_contract INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/api/include)
target_link_libraries(arena_lobby_contract INTERFACE arena_domain)

add_library(arena_lobby_state src/DurableStateStore.cpp)
target_include_directories(arena_lobby_state PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_lobby_state PUBLIC arena_shared)
target_compile_options(arena_lobby_state PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_library(arena_lobby_auth src/AuthClient.cpp)
target_include_directories(arena_lobby_auth PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_lobby_auth PUBLIC arena_shared)
target_compile_options(arena_lobby_auth PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

find_package(spdlog REQUIRED CONFIG)
add_library(arena_lobby_core
    src/OnlineRegistry.cpp
    src/RoomService.cpp
    src/MatchmakingService.cpp)
target_include_directories(arena_lobby_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_lobby_core PUBLIC
    arena_lobby_contract arena_shared arena_auth arena_match arena_room spdlog::spdlog)
target_compile_options(arena_lobby_core PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

target_sources(arena_lobby_core PRIVATE src/Protocol.cpp)

find_package(Protobuf REQUIRED)
set(lobby_proto_out "${CMAKE_CURRENT_BINARY_DIR}/generated")
add_custom_command(
    OUTPUT "${lobby_proto_out}/lobby_state.pb.cc" "${lobby_proto_out}/lobby_state.pb.h"
           "${lobby_proto_out}/authority_meta.pb.cc" "${lobby_proto_out}/authority_meta.pb.h"
           "${lobby_proto_out}/lobby-private.desc"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${lobby_proto_out}"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE} --cpp_out "${lobby_proto_out}"
        --descriptor_set_out "${lobby_proto_out}/lobby-private.desc"
        --proto_path "${CMAKE_CURRENT_SOURCE_DIR}/proto"
        "${CMAKE_CURRENT_SOURCE_DIR}/proto/lobby_state.proto"
        "${CMAKE_CURRENT_SOURCE_DIR}/proto/authority_meta.proto"
    DEPENDS proto/lobby_state.proto proto/authority_meta.proto VERBATIM)
add_library(arena_lobby_codec src/AuthorityCodec.cpp
    "${lobby_proto_out}/lobby_state.pb.cc" "${lobby_proto_out}/authority_meta.pb.cc")
target_include_directories(arena_lobby_codec PUBLIC "${lobby_proto_out}")
target_link_libraries(arena_lobby_codec PUBLIC arena_lobby_core protobuf::libprotobuf)
target_compile_options(arena_lobby_codec PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_library(arena_lobby_storage src/StorageClient.cpp)
target_include_directories(arena_lobby_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(arena_lobby_storage PUBLIC arena_shared)
target_compile_options(arena_lobby_storage PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_library(arena_lobby_replay src/ReplayArchive.cpp)
target_link_libraries(arena_lobby_replay PUBLIC arena_lobby_codec arena_lobby_state)
target_compile_options(arena_lobby_replay PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_library(arena_lobby_authority src/LobbyAuthority.cpp)
target_link_libraries(arena_lobby_authority PUBLIC arena_lobby_replay)
target_compile_options(arena_lobby_authority PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_library(arena_lobby_service src/LobbyService.cpp)
target_link_libraries(arena_lobby_service PUBLIC arena_lobby_authority arena_lobby_auth arena_lobby_storage)
target_compile_options(arena_lobby_service PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_executable(ArenaLobby app/main.cpp)
target_link_libraries(ArenaLobby PRIVATE arena_lobby_service)
target_compile_options(ArenaLobby PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(ArenaLobby PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)

add_executable(arena_replay_export tools/ReplayExport.cpp)
target_link_libraries(arena_replay_export PRIVATE arena_lobby_replay)
target_compile_options(arena_replay_export PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_replay_export PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)

find_package(OpenSSL REQUIRED)
add_library(arena_lobby_migration src/RoomDirectory.cpp src/RoomMigration.cpp)

target_link_libraries(arena_lobby_migration PUBLIC arena_lobby_codec arena_lobby_state
    PRIVATE OpenSSL::Crypto)

target_compile_options(arena_lobby_migration PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)