# Source boundary checks run without database or network dependencies.
find_package(Python3 REQUIRED COMPONENTS Interpreter)

add_test(NAME ArchitectureBoundaryLint
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/tools/architecture/boundary_lint.py"
        --root "${PROJECT_SOURCE_DIR}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/boundary-lint.json"
        --dot "${CMAKE_CURRENT_BINARY_DIR}/dependencies.dot")
add_test(NAME ArchitectureBoundaryContract
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/BoundaryContractTest.py"
        --repository-root "${PROJECT_SOURCE_DIR}"
        --report "${CMAKE_CURRENT_BINARY_DIR}/boundary-contract-report.json")
set_tests_properties(ArchitectureBoundaryLint ArchitectureBoundaryContract PROPERTIES
    TIMEOUT 60 LABELS "architecture;contract")
