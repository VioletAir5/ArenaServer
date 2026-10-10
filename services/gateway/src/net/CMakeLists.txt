# Adds the gateway networking implementation to the service library.
file(GLOB sources CONFIGURE_DEPENDS "*.cpp")
target_sources(arena_gateway PRIVATE ${sources})
