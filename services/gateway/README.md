# Gateway service

This is the current executable service. Its existing networking code lives under include/net and src/net; app/main.cpp is the current entry point. Future protocol and gateway-routing code belongs in this service.

CMake builds arena_gateway as a library and ArenaServer as the executable. The low-level FileDescriptor, BlockingQueue, and ThreadPool live in shared.
