# Gateway service

This is the current executable service. Its existing networking code lives under include/net and src/net; app/main.cpp is the current entry point. Future protocol and gateway-routing code belongs in this service.

CMake builds arena_gateway as a library and ArenaServer as the executable. The low-level FileDescriptor, BlockingQueue, and ThreadPool live in shared.
## TCP and WebSocket pre-login handshake

`ArenaServer asio handshake [ws-origin]` listens on TCP `127.0.0.1:8888` and WebSocket `127.0.0.1:8889/ws`. The default allowed Origin is `http://localhost:3000`; a different or missing Origin receives HTTP 403. Existing `raw` and `packet` modes retain their behavior. The `handshake` mode is available with the Asio backend.

Both transports carry the same binary PacketCodec frame (magic, length, type, request ID, version, payload). A pre-login request uses type 1 and a protobuf Envelope containing Ping; the server returns type 2 with Envelope Pong and the same nonce. This verifies transport and protocol readiness before login. Authentication and LoginRequest handling are not implemented yet.

WebSocket requires binary messages, limits each message to 256 KiB plus the 8-byte PacketCodec prefix, and caps queued outbound bytes at 1 MiB per connection. Invalid application packets close with code 1008; text messages close with 1003; oversized messages close with 1009. Local and peer close codes are written to the server log. Output overflow closes the connection, with a five-second fallback if the peer does not finish the close handshake.

## Packet handler chain

PacketCodec delivers complete frames to PacketPipeline. Each connection gets a
fresh chain of PacketHandler objects. Override handle(PacketContext&): a passing
filter or an unmatched router returns PacketHandler::handle(context) to forward;
a successful response returns PacketHandleResult::Handled; a denied request
returns PacketHandleResult::Reject. The chain tail rejects an unhandled frame,
and makePacketHandler closes the connection on rejection or an exception.

EnvelopeDecodeHandler, PingHandler (PreLoginHandshake.h), and EchoHandler
(PacketEcho.h) are public nodes. The default pre-login chain is assembled as:

```cpp
auto decoder = std::make_unique<EnvelopeDecodeHandler>();
decoder->setNext(std::make_unique<PingHandler>());
auto callback = makePacketHandler(
    PacketPipeline(std::move(decoder)), kMaxPacketSize);
```

Use setNext(std::unique_ptr<PacketHandler>) to assemble new policy and routing
nodes before installing the callback. Nodes own their successors, and the
connection callback owns the chain. Links and node state are used on the
connection owner; callback copies retain the same decoder and chain, so create
a fresh callback for each connection through ConnectionHandlerFactory.

IP policies need trusted peer metadata supplied at connection admission;
IConnection currently exposes no peer address. A login deadline also needs an
owner timer so an inactive client can expire without sending another packet.
IP rate limits, whitelists, authentication, and login deadlines are future
policies; this change supplies their handler interface.
