# This fixture is loaded only into explicitly selected Linux test processes.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_package(Threads REQUIRED)
    add_library(arena_test_clock SHARED ClockShim.c)
    target_compile_features(arena_test_clock PRIVATE c_std_11)
    target_compile_options(arena_test_clock PRIVATE -Wall -Wextra -Wpedantic)
    target_link_libraries(arena_test_clock PRIVATE Threads::Threads)
endif()