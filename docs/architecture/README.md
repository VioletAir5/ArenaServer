# ArenaServer architecture

This directory describes the **target** architecture for a room-based multiplayer game server. The repository currently implements only part of the network foundation. Dashed boxes in the diagrams indicate planned elements; solid green boxes indicate code that was present when the model was written. The model is a design proposal, not a claim that the full service platform is deployed.

## Diagrams

The source of truth is [workspace.dsl](workspace.dsl). Rendered SVG and PNG files are in [rendered](rendered).

| View | Purpose |
| --- | --- |
| [01 System context](rendered/structurizr-01-System-Context.svg) | Players, operators, and data stores |
| [02 Containers](rendered/structurizr-02-Containers.svg) | Gateway, Auth, Lobby, Match, Room, and Storage |
| [03 Gateway components](rendered/structurizr-03-Gateway-Components.svg) | Connection admission, Reactor I/O, framing, backpressure, and routing |
| [04 Auth components](rendered/structurizr-04-Auth-Components.svg) | Login, tokens, and permission checks |
| [05 Lobby components](rendered/structurizr-05-Lobby-Components.svg) | Presence, friends, parties, and chat |
| [06 Match components](rendered/structurizr-06-Match-Components.svg) | Queueing, rules, capacity-aware allocation, and assignment |
| [07 Room components](rendered/structurizr-07-Room-Components.svg) | Authoritative state, input validation, ticks, snapshots, and reconnect |
| [08 Storage components](rendered/structurizr-08-Storage-Components.svg) | Cache, durable writes, and retry policy |
| [09 Login request](rendered/structurizr-09-Login-Request.svg) | Request path from client to account data |
| [10 Match to room](rendered/structurizr-10-Match-To-Room.svg) | Match request, room allocation, and result persistence |
| [11 Reconnect](rendered/structurizr-11-Reconnect.svg) | Identity check and room rejoin |
| [12 Target deployment](rendered/structurizr-12-Target-Deployment.svg) | Logical scaling pools, not a fixed machine count |

## Repository mapping

- services/gateway is the only implemented service. It owns the current network headers, sources, and executable entry point.
- services/auth, services/lobby, services/match, services/room, and services/storage are reserved for the planned C4 containers; each has a scope README.
- shared contains only infrastructure intended for reuse across service boundaries: os and concurrent.
- tests/gateway and tests/shared mirror those owners.
- Each service can become a separate CMake executable or library without requiring a separate repository.
## Design rules

- Gateway owns sockets and session-to-connection mapping. It performs bounded framing and routing; Room owns game state and game rules.
- Each connection has bounded queued output. When a nonblocking write cannot complete, bytes remain queued until write readiness; a documented high-watermark policy must prevent unbounded memory growth.
- Each active room has one authoritative owner. Player commands are validated and ordered before a fixed simulation tick applies them.
- Room snapshots serve client synchronization. Persistence of checkpoints and final results follows a separate policy; not every tick is assumed to be a database write.
- Reconnect must prove player identity and find the previous room assignment before restoring membership and sending a current snapshot.
- Redis holds expiring or recoverable state. MySQL holds durable account and result records. Exact schemas and consistency guarantees remain to be designed.

## Decisions still needed

1. Client protocol: TCP framing, version negotiation, size limits, and whether UDP or WebSocket is required.
2. Internal communication: transport, message contracts, timeouts, retries, idempotency, and service discovery.
3. Capacity limits: per-IP and global connections, outbound queue watermarks, per-room limits, tick rate, and overload response.
4. Recovery: ownership transfer after a Room process failure, checkpoint cadence, reconnect window, and acceptable data loss.
5. Operations: health checks, metrics, structured logs, tracing, rollout, and load-test targets.
6. C++ standard: the original architecture brief names C++17, while the current CMake configuration selects C++23.

## Regeneration

Validate and export with Structurizr CLI, then render the exported PlantUML with PlantUML:

    structurizr validate -workspace workspace.dsl
    structurizr export -workspace workspace.dsl -format plantuml -output rendered
    plantuml -tsvg rendered/structurizr-*.puml
    plantuml -tpng rendered/structurizr-*.puml

