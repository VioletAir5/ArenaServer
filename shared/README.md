# Shared infrastructure

Only code reused across service boundaries belongs here. Current modules are os (FileDescriptor) and concurrent (BlockingQueue and ThreadPool).

Service-specific networking, session state, protocol routing, and game logic belong in their owning service directories.
