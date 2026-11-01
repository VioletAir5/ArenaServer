add_executable(arena_lobby_state_tests DurableStateStoreTest.cpp)
target_link_libraries(arena_lobby_state_tests PRIVATE arena_lobby_state GTest::gtest_main)
target_compile_options(arena_lobby_state_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_lobby_state_tests PRIVATE -Wl,--wrap=fdatasync
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
include(GoogleTest)
gtest_discover_tests(arena_lobby_state_tests)

add_executable(arena_verified_session_tests VerifiedSessionTest.cpp)
target_link_libraries(arena_verified_session_tests PRIVATE arena_gateway GTest::gtest_main)
target_compile_options(arena_verified_session_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_verified_session_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_verified_session_tests)

add_executable(arena_lobby_auth_tests AuthClientTest.cpp)
target_link_libraries(arena_lobby_auth_tests PRIVATE arena_lobby_auth GTest::gtest_main)
target_compile_options(arena_lobby_auth_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_lobby_auth_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_lobby_auth_tests)

target_sources(arena_verified_session_tests PRIVATE OnlineRegistryRecoveryTest.cpp)

add_executable(arena_room_service_recovery_tests RoomServiceRecoveryTest.cpp)
target_link_libraries(arena_room_service_recovery_tests PRIVATE arena_gateway GTest::gtest_main)
target_compile_options(arena_room_service_recovery_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_room_service_recovery_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_room_service_recovery_tests)
