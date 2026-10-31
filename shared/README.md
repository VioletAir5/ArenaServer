# Shared infrastructure

Only code reusable across service boundaries belongs here. Current modules are os (FileDescriptor), concurrent (BlockingQueue and ThreadPool), protocol (bounded Buffer and a reusable length-prefix wire codec), and internal RPC (bounded C++/Python calls). Gateway FrameProtocol uses that codec for packet and Arena request profiles over TCP and WebSocket; raw echo operates directly on bytes.

Service-specific networking, session state, protocol routing, and game logic belong in their owning service directories.

## Internal RPC

`rpc::Client` and `shared.runtime.rpc` exchange Protobuf requests over loopback
TCP using a four-byte network-order length and a 256 KiB frame limit. A stream
carries one request/reply. Internal services require `ARENA_INTERNAL_TOKEN`.
Requests include correlation, deadline and optional idempotency identities;
children clip their budget to their parent. No automatic retries are performed.
Timeout or transport loss after sending does not prove a mutation rolled back.

The C++ client uses an external executor and finite admission. Accepted calls
complete once on a strand; rejected calls return immediately without callback
queueing. Keep the executor running through stop and completion. Python handlers
run on one asyncio loop and must propagate cancellation; synchronous dependencies
need a bounded worker with explicit connection ownership.

CMake emits C++ bindings and `rpc.desc` in `build/shared/generated`. Python loads
the descriptor using pinned `runtime/requirements.txt`. Enable Python acceptance
with `ARENA_ENABLE_RPC_TESTS` and `ARENA_RUNTIME_TEST_PYTHON`. Native tests build as
`arena_rpc_client_tests`; Python CTest is `InternalRpcBoundary`.