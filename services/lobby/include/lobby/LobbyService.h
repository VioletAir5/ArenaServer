/** @file
 * @brief Runs remote identity and persistence effects around durable
 * Lobby candidates.
 */
#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>

#include <boost/asio/any_io_executor.hpp>

#include "lobby.pb.h"
#include "lobby/AuthClient.h"
#include "lobby/LobbyAuthority.h"
#include "lobby/RoomClient.h"
#include "lobby/StorageClient.h"

namespace service::lobby
{
/** @brief Owns bounded asynchronous orchestration; Authority alone
 * owns business state. The external executor must pump through
 * stopped before destroying the owner. Cancellation can report
 * promptly while an admitted WAL write still drains; only the
 * physical completion releases its admission slot.
 */
class LobbyService final
{
public:
    struct Config
    {
        /// Complete durable business policy and journal bindings.
        LobbyAuthority::Config authority;
        /// Independent role-bound remote dependencies.
        AuthClient::Config auth;
        StorageClient::Config storage;
        /// Exact Room nodes; private worker journals remain separate.
        RoomClient::Config rooms;
        /// Business jobs, including cancelled physical operations
        /// awaiting cleanup.
        std::size_t maxJobs{128};
        /// Gateway heartbeat lease, separate from token expiry and
        /// reconnect grace.
        std::chrono::milliseconds transportLease{1000};
        /// Retry receipt lifetime, never renewed by same-connection
        /// replay.
        std::chrono::milliseconds receiptRetention{60000};
        /// Frozen finite external-settlement policy; recovery
        /// preserves each budget.
        std::uint32_t settlementMaxAttempts{8};
        std::chrono::milliseconds settlementMaxElapsed{60000};
        std::chrono::milliseconds retryBase{100}, retryMax{5000};
    };
    template <class Reply>
    using Result = std::expected<Reply, arena::rpc::Status>;
    template <class Reply>
    using Completion = std::function<void(Result<Reply>)>;
    using Cancellation = std::function<void()>;
    using Admission = std::expected<Cancellation, arena::rpc::Status>;
    struct Stats
    {
        bool initialized{false}, ready{false}, failed{false},
            stopping{false}, stopped{false};
        /// Includes fixed bounded background loans as well as
        /// business jobs.
        std::size_t active{0}, authInflight{0}, storageInflight{0},
            roomInflight{0}, coordinatorPending{0};
        /// Last committed gauges; observations never prune or mutate
        /// authority.
        std::size_t onlineActive{0}, onlineRetained{0},
            roomIndexes{0}, transports{0}, receipts{0};
        std::size_t queued{0}, roomActors{0};
        std::uint64_t matchedPairs{0}, roomTicks{0},
            settlementsCommitted{0}, settlementsBlocked{0};
        std::uint64_t committedRevision{0};
        LobbyAuthority::Stats authority;
    };
    /** @brief Starts asynchronous recovery and real dependency
     * probes. */
    LobbyService(boost::asio::any_io_executor, Config);
    ~LobbyService() noexcept;
    LobbyService(const LobbyService&) = delete;
    LobbyService& operator=(const LobbyService&) = delete;
    /** @brief Executes one unchanged public request under a trusted
     * physical identity. */
    Admission execute(
        arena::lobby::ExecuteRequest,
        arena::internal_rpc::Context,
        Completion<arena::lobby::ExecuteReply>
    );
    /** @brief Detaches only the exact retained physical and authority
     * generation. */
    Admission detach(
        arena::lobby::DetachRequest,
        arena::internal_rpc::Context,
        Completion<arena::lobby::DetachReply>
    );
    /** @brief Refreshes current leases and returns unknown or stale
     * physical IDs. */
    Admission heartbeat(
        arena::lobby::HeartbeatRequest,
        arena::internal_rpc::Context,
        Completion<arena::lobby::HeartbeatReply>
    );
    /** @brief Starts an operations-authorized handoff and returns
     * durable Prepare. */
    Admission migrate(
        arena::lobby::MigrationRequest,
        arena::internal_rpc::Context,
        Completion<arena::lobby::MigrationReply>
    );
    /** @brief Reads immutable migration facts without changing room
     * state. */
    Result<arena::lobby::MigrationReply>
        migrationStatus(arena::lobby::MigrationStatusRequest) const;
    /** @brief Fences admission and drains actual child/WAL work
     * without blocking. */
    void stop() noexcept;
    /** @brief Observes published readiness and physical work from any
     * thread. */
    Stats stats() const noexcept;

private:
    struct State;
    /// Self-retained until every accepted physical operation has
    /// completed.
    std::shared_ptr<State> m_state;
};
} // namespace service::lobby