file(GLOB sources CONFIGURE_DEPENDS "*.cpp")
target_sources(arena_shared PRIVATE ${sources})
