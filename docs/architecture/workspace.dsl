workspace "ArenaServer" "Target C4 architecture for a room-based multiplayer game server; implementation is in progress." {
    !identifiers hierarchical

    model {
        player = person "Game Client" "PC, mobile, or console game client."
        admin = person "Admin / Operations" "Configures services and monitors health, metrics, and logs."
        redis = softwareSystem "Redis" "Target cache for presence, tickets, and recoverable state." "External,Planned"
        mysql = softwareSystem "MySQL" "Target durable store for accounts and game results." "External,Planned"

        arena = softwareSystem "ArenaServer Game Platform" "Target architecture for a room-based multiplayer game server." {
            gateway = container "Gateway Service" "Owns client connections, protocol framing, session routing, and connection safety." "C++17, Linux epoll, Reactor" "InProgress" {
                tcpServer = component "TcpServer" "Accepts nonblocking client connections." "net" "Planned"
                admission = component "AdmissionController" "Enforces connection and request limits before admitting clients." "gateway" "Planned"
                loopPool = component "EventLoopThreadPool" "Assigns connections to one I/O loop per thread." "net" "Planned"
                eventLoop = component "EventLoop" "Drives I/O and callbacks on its owning thread." "services/gateway/src/net" "Implemented"
                poller = component "EpollPoller" "Registers channels and waits for epoll events." "services/gateway/src/net" "Implemented"
                channel = component "Channel" "Wraps fd event interests and callbacks." "services/gateway/src/net" "Implemented"
                connection = component "TcpConnection" "Owns socket state, read/write lifecycle, and per-connection buffers." "net" "Planned"
                buffer = component "PacketBuffer" "Buffers partial and coalesced TCP packets." "services/gateway/src/net" "Implemented"
                outbound = component "OutboundQueue" "Retains partial writes with bounded per-connection bytes and watermarks." "net" "Planned"
                codec = component "PacketCodec" "Validates magic, version, length, messageId, and payload bounds." "protocol" "Planned"
                sessions = component "SessionManager" "Maps authenticated session IDs to live connections." "services/gateway/src/net" "Implemented"
                liveness = component "ConnectionSupervisor" "Applies heartbeat, idle timeout, and disconnect cleanup policy." "gateway" "Planned"
                dispatcher = component "MessageDispatcher" "Dispatches decoded messages by messageId." "gateway" "Planned"
                router = component "Router" "Forwards authenticated messages to the owning game service." "gateway" "Planned"

                tcpServer -> admission "Checks connection limits"
                tcpServer -> loopPool "Assigns accepted connections"
                loopPool -> eventLoop "Runs one loop per thread"
                eventLoop -> poller "Polls for I/O events"
                poller -> channel "Returns ready channels"
                channel -> connection "Invokes callbacks"
                connection -> buffer "Buffers incoming bytes"
                connection -> outbound "Queues partial writes"
                outbound -> channel "Enables write readiness until drained"
                connection -> codec "Decodes and encodes bounded packets"
                connection -> sessions "Registers and removes sessions"
                liveness -> sessions "Expires idle sessions"
                codec -> dispatcher "Passes validated messages"
                dispatcher -> router "Selects destination by messageId"
            }

            auth = container "Auth Service" "Verifies identity, issues tokens, and authorizes operations." "C++" "Planned" {
                login = component "LoginAuthenticator" "Validates credentials against account records." "auth" "Planned"
                token = component "TokenService" "Issues and validates expiring identity tokens." "auth" "Planned"
                permission = component "PermissionVerifier" "Checks player permissions for protected operations." "auth" "Planned"

                login -> token "Issues a token after successful login"
                permission -> token "Validates token claims"
            }

            lobby = container "Lobby Service" "Manages online state, friends, parties, and chat." "C++" "Planned" {
                presence = component "PresenceManager" "Tracks online state and session transitions." "lobby" "Planned"
                friends = component "FriendManager" "Manages friend relationships." "lobby" "Planned"
                party = component "PartyManager" "Creates parties and manages membership." "lobby" "Planned"
                chat = component "ChatHandler" "Delivers lobby and party messages with abuse limits." "lobby" "Planned"

                friends -> presence "Queries online state"
                party -> presence "Queries member state"
                chat -> party "Resolves party recipients"
            }

            match = container "Match Service" "Queues players, applies match rules, and assigns rooms." "C++" "Planned" {
                queue = component "MatchQueue" "Queues players or parties and supports cancellation and timeout." "match" "Planned"
                rules = component "MatchRuleEngine" "Selects compatible players according to matching rules." "match" "Planned"
                allocator = component "RoomAllocator" "Selects a Room Server with capacity and requests room creation." "match" "Planned"
                assignment = component "MatchAssignment" "Records player-to-room assignment for routing and reconnect." "match" "Planned"

                queue -> rules "Supplies waiting players"
                rules -> allocator "Submits matched players"
                allocator -> assignment "Records assigned room"
            }

            room = container "Room Server" "Owns authoritative room state, input handling, fixed ticks, and combat." "C++" "Planned" {
                lifecycle = component "RoomLifecycle" "Creates, starts, and closes rooms; one owner per room." "room" "Planned"
                playerState = component "PlayerStateManager" "Tracks authoritative in-room player state." "room" "Planned"
                input = component "InputValidator" "Validates and orders player commands before simulation." "room" "Planned"
                tick = component "TickScheduler" "Advances room simulation on fixed ticks." "room" "Planned"
                combat = component "CombatEngine" "Applies deterministic game rules to room state." "room" "Planned"
                snapshot = component "SnapshotPublisher" "Produces versioned state snapshots and final results." "room" "Planned"
                reconnect = component "ReconnectCoordinator" "Reattaches a verified player and sends a fresh snapshot." "room" "Planned"

                lifecycle -> playerState "Owns membership and state"
                lifecycle -> tick "Starts and stops ticks"
                input -> playerState "Checks player and command state"
                tick -> input "Consumes validated commands"
                tick -> combat "Advances combat"
                combat -> playerState "Updates authoritative state"
                tick -> snapshot "Publishes versioned state"
                reconnect -> playerState "Restores player membership"
                reconnect -> snapshot "Sends current state"
            }

            storage = container "Storage Service" "Provides bounded cache access and durable persistence for account and game data." "C++" "Planned" {
                access = component "DataAccessFacade" "Provides data contracts to Auth, Lobby, and Room." "storage" "Planned"
                cache = component "RedisAdapter" "Reads and writes expiring cached state." "storage" "Planned"
                persistence = component "PersistenceCoordinator" "Applies idempotent writes and retry policy for durable data." "storage" "Planned"
                database = component "MySQLAdapter" "Reads and writes durable account and game records." "storage" "Planned"

                access -> cache "Uses cache"
                access -> persistence "Submits durable writes"
                persistence -> database "Persists records"
                cache -> redis "Caches expiring state" "Planned"
                database -> mysql "Persists durable records" "Planned"
            }
        }

        player -> arena.gateway "Connects and sends game messages" "TCP; UDP/WebSocket optional later"
        admin -> arena "Configures and monitors" "Operations interfaces TBD"
        arena.gateway -> arena.auth "Validates login and reconnect identity" "Internal request/response TBD"
        arena.gateway -> arena.lobby "Routes lobby messages" "Internal messaging TBD"
        arena.gateway -> arena.match "Routes matchmaking requests" "Internal messaging TBD"
        arena.gateway -> arena.room "Routes room commands and state updates" "Internal messaging TBD"
        arena.match -> arena.room "Requests room allocation" "Internal request/response TBD"
        arena.auth -> arena.storage "Reads account data" "Internal request/response TBD"
        arena.lobby -> arena.storage "Reads and writes social state" "Internal request/response TBD"
        arena.room -> arena.storage "Loads checkpoints and persists final results" "Internal messaging TBD"

        deploymentEnvironment "Target" {
            deploymentNode "Gateway pool" "Horizontally scalable edge processes." "Linux" {
                instances "2..N"
                containerInstance arena.gateway
            }
            deploymentNode "Auth pool" "Authentication processes." "Linux" {
                containerInstance arena.auth
            }
            deploymentNode "Lobby pool" "Lobby processes." "Linux" {
                containerInstance arena.lobby
            }
            deploymentNode "Match pool" "Matchmaking processes." "Linux" {
                containerInstance arena.match
            }
            deploymentNode "Room pool" "Room processes; each active room has one owner." "Linux" {
                instances "1..N"
                containerInstance arena.room
            }
            deploymentNode "Storage pool" "Data access processes." "Linux" {
                containerInstance arena.storage
            }
            deploymentNode "Data services" "Managed or self-hosted data services; topology TBD." {
                softwareSystemInstance redis
                softwareSystemInstance mysql
            }
        }
    }

    views {
        systemContext arena "01-System-Context" "Target ArenaServer and its users and data stores" {
            include player admin arena redis mysql
            autoLayout lr
        }

        container arena "02-Containers" "Target service responsibilities and dependencies" {
            include player arena.gateway arena.auth arena.lobby arena.match arena.room arena.storage redis mysql
            autoLayout tb
        }

        component arena.gateway "03-Gateway-Components" "Connection lifecycle, flow control, protocol, and routing" {
            include *
            autoLayout tb
        }

        component arena.auth "04-Auth-Components" "Identity and authorization" {
            include *
            autoLayout lr
        }

        component arena.lobby "05-Lobby-Components" "Presence, social state, and chat" {
            include *
            autoLayout lr
        }

        component arena.match "06-Match-Components" "Queueing, matching, and room assignment" {
            include *
            autoLayout lr
        }

        component arena.room "07-Room-Components" "Authoritative room simulation and reconnect" {
            include *
            autoLayout tb
        }

        component arena.storage "08-Storage-Components" "Cache and durable persistence" {
            include *
            autoLayout lr
        }

        dynamic arena "09-Login-Request" "Target login request path" {
            player -> arena.gateway "Sends login request"
            arena.gateway -> arena.auth "Requests identity verification"
            arena.auth -> arena.storage "Loads account record"
            autoLayout lr
        }

        dynamic arena "10-Match-To-Room" "Target match and room creation path" {
            player -> arena.gateway "Requests matchmaking"
            arena.gateway -> arena.match "Enqueues authenticated player"
            arena.match -> arena.room "Allocates an owned room"
            arena.gateway -> arena.room "Routes validated player commands"
            arena.room -> arena.storage "Persists final result"
            autoLayout lr
        }

        dynamic arena "11-Reconnect" "Target reconnect request path" {
            player -> arena.gateway "Reconnects with an identity token"
            arena.gateway -> arena.auth "Validates reconnect identity"
            arena.gateway -> arena.room "Routes player to the assigned room"
            arena.room -> arena.storage "Loads a checkpoint if recovery is needed"
            autoLayout lr
        }

        deployment * "Target" "12-Target-Deployment" "Logical scaling pools; exact topology and capacity remain design decisions" {
            include *
            autoLayout tb
        }

        styles {
            element "Person" {
                shape Person
                background #08427B
                color #FFFFFF
            }
            element "Software System" {
                background #1168BD
                color #FFFFFF
            }
            element "Container" {
                background #438DD5
                color #FFFFFF
            }
            element "Component" {
                background #85BBF0
                color #172B4D
            }
            element "External" {
                background #747D8C
                color #FFFFFF
            }
            element "InProgress" {
                background #167D73
                color #FFFFFF
            }
            element "Implemented" {
                background #167D73
                color #FFFFFF
            }
            element "Planned" {
                border dashed
                opacity 65
            }
        }
    }
}


