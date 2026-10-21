# Shared infrastructure

Only code reusable across service boundaries belongs here. Current modules are os (FileDescriptor), concurrent (BlockingQueue and ThreadPool), and protocol (bounded Buffer and a reusable length-prefix wire codec). Gateway FrameProtocol uses that codec for packet and Arena request profiles over TCP and WebSocket; raw echo operates directly on bytes.

Service-specific networking, session state, protocol routing, and game logic belong in their owning service directories.
