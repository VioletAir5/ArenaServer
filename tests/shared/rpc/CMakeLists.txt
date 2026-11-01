add_executable(arena_rpc_client_tests RpcClientTest.cpp)
target_link_libraries(arena_rpc_client_tests PRIVATE arena_shared GTest::gtest_main)
target_compile_options(arena_rpc_client_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_rpc_client_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
include(GoogleTest)
gtest_discover_tests(arena_rpc_client_tests)
option(ARENA_ENABLE_RPC_TESTS "Run internal RPC integration tests" OFF)
set(ARENA_RUNTIME_TEST_PYTHON "" CACHE FILEPATH "Python with shared/runtime requirements installed")
if(ARENA_ENABLE_RPC_TESTS)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    set(rpc_python "${Python3_EXECUTABLE}")
    if(ARENA_RUNTIME_TEST_PYTHON)
        set(rpc_python "${ARENA_RUNTIME_TEST_PYTHON}")
    endif()
    execute_process(COMMAND "${rpc_python}" -c
        "import google.protobuf; assert google.protobuf.__version__ == '5.29.5'"
        RESULT_VARIABLE rpc_driver_status ERROR_VARIABLE rpc_driver_error)
    if(NOT rpc_driver_status STREQUAL "0")
        message(FATAL_ERROR "Install shared/runtime/requirements.txt and set ARENA_RUNTIME_TEST_PYTHON. ${rpc_driver_error}")
    endif()
    add_test(NAME InternalRpcBoundary COMMAND "${rpc_python}"
        "${CMAKE_CURRENT_SOURCE_DIR}/RpcTest.py"
        --repository-root "${PROJECT_SOURCE_DIR}"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --report "${CMAKE_CURRENT_BINARY_DIR}/rpc-report.json")
    set_tests_properties(InternalRpcBoundary PROPERTIES TIMEOUT 60 LABELS "integration;rpc")
    add_test(NAME ServiceRpcMetadata COMMAND "${rpc_python}"
        "${CMAKE_CURRENT_SOURCE_DIR}/ServiceMetadataTest.py"
        --repository-root "${PROJECT_SOURCE_DIR}"
        --descriptor "${PROJECT_BINARY_DIR}/shared/generated/rpc.desc"
        --report "${CMAKE_CURRENT_BINARY_DIR}/service-metadata-report.json")
    set_tests_properties(ServiceRpcMetadata PROPERTIES TIMEOUT 45 LABELS "integration;rpc;contract")
endif()

add_executable(arena_storage_probe StorageProbe.cpp)
target_link_libraries(arena_storage_probe PRIVATE arena_shared)
target_link_options(arena_storage_probe PRIVATE $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)

add_executable(arena_rpc_server_tests RpcServerTest.cpp)
target_link_libraries(arena_rpc_server_tests PRIVATE arena_shared GTest::gtest_main)
target_compile_options(arena_rpc_server_tests PRIVATE -Wall -Wextra -Wpedantic
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined>)
target_link_options(arena_rpc_server_tests PRIVATE
    $<$<BOOL:${ARENA_ENABLE_UBSAN}>:-fsanitize=undefined> -no-pie)
gtest_discover_tests(arena_rpc_server_tests)
