# Shared infrastructure

Only code reused across service boundaries belongs here. Current modules are os (FileDescriptor), concurrent (BlockingQueue and ThreadPool), and protocol (bounded Buffer and a reusable length-prefix wire codec). The gateway has not yet adopted the protocol codec for its TCP echo path.

Service-specific networking, session state, protocol routing, and game logic belong in their owning service directories.
