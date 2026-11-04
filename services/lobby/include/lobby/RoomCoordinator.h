/** @file
 * @brief Coordinates worker proposals and owner handoffs through one
 * Lobby WAL.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>

#include <boost/asio/any_io_executor.hpp>

#include "lobby/LobbyAuthority.h"
#include "lobby/RoomClient.h"

namespace service::lobby
{
/** @brief Schedules isolated Room RPC work; only Authority may
 * publish it. The caller keeps Authority alive until stats().stopped.
 * The external executor pumps throughout stop, including already
 * admitted RPC and WAL completions.
 */
class RoomCoordinator final
{
public:
    using Clock = LobbyAuthority::Clock;
    using Cancellation = std::function<void()>;
    using MigrationCompletion = std::function<
        void(std::expected<RoomHandoff, arena::rpc::Status>)>;
    struct Config
    {
        /// Exact worker discovery and Lobby-only credentials, without
        /// fallback.
        RoomClient::Config client;
        /// Creation policy; no simulation or runtime callback is
        /// stored here.
        service::room::RuntimePolicy policy;
        /// Control lane limit; Data permits twice this many physical
        /// loans so a draining old generation cannot block its
        /// successor.
        std::size_t maxInflight{128};
        /// Maximum independent room changes composed into one
        /// authority write.
        std::size_t maxBatch{128};
        /// Bounded background scheduling interval.
        std::chrono::milliseconds poll{10};
        /// Qualified per-node worker-instance probe interval.
        std::chrono::milliseconds probe{200};
        /// Same-boot clock used by authority and durable migration
        /// deadlines.
        std::function<Clock::time_point()> now{Clock::now};
        /// Unix seconds, matching Authority's captured authentication
        /// clock.
        std::function<std::int64_t()> wallNow;
    };
    struct Stats
    {
        /// At least one qualified worker and no passive legacy rooms
        /// remain.
        bool ready{false};
        /// Admission is permanently fenced.
        bool stopping{false};
        /// Timer, worker transport, owner posts and WAL callbacks all
        /// drained.
        bool stopped{false};
        /// Actual accepted transport work retained by RoomClient.
        std::size_t rpcInflight{0};
        /// Room jobs retained through candidate publication or
        /// definitive reject.
        std::size_t inflight{0};
        /// Pending physical Authority write/completion owned by this
        /// scheduler.
        std::size_t walInflight{0};
        /// Qualified current process instances, never inferred from
        /// endpoints.
        std::size_t healthyWorkers{0};
        /// Admitted migration callers awaiting one bounded result.
        std::size_t migrations{0};
        /// Successfully committed worker cutoffs.
        std::uint64_t applied{0};
        /// Successfully committed room creations, including legacy
        /// imports.
        std::uint64_t created{0};
        /// Completed epoch handoffs.
        std::uint64_t migrated{0};
        /// Disposal intents waiting for qualified worker admission or
        /// retry.
        std::size_t cacheHintPending{0};
        /// Physical disposal RPC loans, bounded independently of room
        /// data.
        std::size_t cacheHintInflight{0};
        /// Ownership/lifecycle changes that rebuilt cache disposal
        /// intents.
        std::uint64_t cacheReconciliations{0};
        /// Stale or unavailable isolated proposals discarded without
        /// publication.
        std::uint64_t rejected{0};
    };
    /** @brief Starts bounded scheduling while Authority completes
     * recovery. */
    RoomCoordinator(
        boost::asio::any_io_executor, LobbyAuthority&, Config
    );
    ~RoomCoordinator() noexcept;
    RoomCoordinator(const RoomCoordinator&) = delete;
    RoomCoordinator& operator=(const RoomCoordinator&) = delete;
    /** @brief Coalesces an early scheduling pass after accepted
     * durable ingress. */
    void wake() noexcept;
    /** @brief Starts one exact owner-epoch migration with a fixed
     * eight-second handoff budget, reserving compensation time within
     * the ten-second recovery goal.
     * Duplicate identifiers return the same durable handoff; changed
     * tuples fail. Completion reports the first durable Prepare or
     * the matching saved handoff; background progress continues
     * independently of caller cancellation.
     */
    std::expected<Cancellation, arena::rpc::Status> startMigration(
        RoomIdentity,
        std::uint64_t ownerEpoch,
        std::uint64_t migrationId,
        std::string targetNode,
        Clock::time_point deadline,
        MigrationCompletion
    );
    /** @brief Returns passive accounting from any thread. */
    Stats stats() const;
    /** @brief Fences new work and drains physical RPC/WAL
     * completions. */
    void stop() noexcept;

private:
    struct State;
    /// Physical jobs retain their scheduler independently of the
    /// public wrapper.
    std::shared_ptr<State> m_state;
};
} // namespace service::lobby