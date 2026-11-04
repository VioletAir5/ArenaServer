/** @file
 * @brief Publishes Lobby transactions only after their complete
 * journal batch is synced.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio/any_io_executor.hpp>

#include "authority_meta.pb.h"
#include "envelope.pb.h"
#include "lobby/AuthorityCodec.h"
#include "lobby/RoomMigration.h"
#include "persistence/AtomicJournal.h"
#include "room/RoomRuntime.h"

namespace service::lobby
{
using arena::persistence::AtomicJournal;
/** @brief Serializes isolated business candidates and one physical
 * WAL write. The supplied executor must keep pumping until
 * stats().stopped. Mutable models exist only inside one owner
 * callback and never enter publication.
 */
class LobbyAuthority final
{
    struct State;

public:
    using Clock = arena::runtime::OwnerClock;
    using Reply = std::expected<
        arena::protocol::Envelope,
        arena::protocol::ErrorCode>;
    using Capture = std::function<void(Reply)>;
    using Cancellation = std::function<void()>;

    struct Config
    {
        /// Exclusive local journal; opening/recovery precedes
        /// readiness.
        AtomicJournal::Config journal;
        /// Linux boot identity fencing unmapped steady-clock
        /// deadlines.
        std::string bootId;
        OnlineRegistryConfig registry;
        service::match::QueueConfig queue;
        RoomServiceConfig rooms;
        RoomDirectoryLimits directoryLimits;
        /// Control jobs, including undelivered completions.
        std::size_t maxPending{128};
        /// Tick jobs, including physical writes and completion
        /// delivery.
        std::size_t maxRoomPending{256};
        /// Retry receipts retained in the same bounded metadata
        /// image.
        std::size_t maxReceipts{4096};
        bool autoTicks{true};
        /// Domain clocks sampled once per candidate, never inside
        /// models.
        std::function<Clock::time_point()> now{Clock::now};
        std::function<std::int64_t()> wallNow;
        /// Post-commit nonblocking revocation notification;
        /// exceptions are isolated.
        std::function<void(SessionBinding)> onRevoked;
    };
    struct Snapshot
    {
        std::uint64_t revision{0};
        CoreCheckpoint core;
        arena::lobby_meta::Metadata meta;
        std::shared_ptr<const RoomDirectory> directory;
        std::map<
            RoomIdentity,
            std::shared_ptr<const service::room::RuntimeImage>>
            roomImages;
    };
    struct WorkingState
    {
    private:
        struct Auxiliary;
        friend struct State;
        explicit WorkingState(
            std::shared_ptr<Auxiliary>,
            std::shared_ptr<const Snapshot>
        );
        /// Owns the frozen clocks, metadata and disposable effect
        /// collector.
        std::shared_ptr<Auxiliary> m_aux;

    public:
        /// Fresh owner-local models; retaining these outside Mutation
        /// is forbidden.
        std::shared_ptr<OnlineRegistry> online;
        std::unique_ptr<service::match::MatchQueue> queue;
        /// Committed routing and worker values; never a local
        /// simulation.
        const std::shared_ptr<const Snapshot> base;
        /// Composed once with registry/queue/metadata in the same WAL
        /// CAS.
        std::vector<RoomDirectoryCommand> roomChanges;
        /// Passive remaining v1 values until their real worker Stage
        /// succeeds.
        RoomCheckpoint legacyRooms;
        arena::lobby_meta::Metadata& meta;
        const Clock::time_point now;
        const std::int64_t wallNow;
        ~WorkingState();
        WorkingState(const WorkingState&) = delete;
        WorkingState& operator=(const WorkingState&) = delete;
        /** @brief Returns a clock that retains only frozen auxiliary
         * values. */
        std::function<std::int64_t()> leaseClock() const;
        /** @brief Returns callbacks collecting candidate-only
         * revocation effects. */
        OnlineRegistry::KickFactory kickFactory() const;
        /** @brief Retains candidate metadata for bounded
         * owner-mailbox callbacks. */
        std::shared_ptr<arena::lobby_meta::Metadata>
        metadataHandle() const;
    };
    /// Mutations perform bounded domain work only: no RPC, disk,
    /// external effects, or escaping references to WorkingState.
    /// Capture must occur once before freeze.
    using Mutation = std::function<void(WorkingState&, Capture)>;
    enum class Error
    {
        Cancelled,
        Capacity,
        Unavailable,
        OutcomeUnknown,
        InvalidState
    };
    struct CommitReply
    {
        Reply reply;
        std::shared_ptr<const Snapshot> committed;
    };
    using Completion = std::function<
        void(std::expected<CommitReply, Error>)>;
    struct Stats
    {
        bool initialized{false}, ready{false}, failed{false},
            stopped{false};
        bool writeInFlight{false};
        std::size_t pending{0}, roomPending{0};
        std::uint64_t committed{0}, rejected{0}, cancelled{0},
            callbackFailures{0};
        AtomicJournal::Stats journal;
    };

    /** @brief Queues startup recovery; listeners must wait for
     * initialized/ready. */
    LobbyAuthority(boost::asio::any_io_executor, Config);
    ~LobbyAuthority() noexcept;
    LobbyAuthority(const LobbyAuthority&) = delete;
    LobbyAuthority& operator=(const LobbyAuthority&) = delete;
    /** @brief Returns detached immutable committed data from any
     * thread. */
    std::shared_ptr<const Snapshot> snapshot() const noexcept;
    /** @brief Admits one bounded transaction; synchronous rejection
     * never calls back. Cancellation/deadline before WAL admission
     * excludes the mutation. After admission, publication and exactly
     * one physical completion still occur.
     */
    std::expected<Cancellation, Error> submit(
        Mutation,
        Clock::time_point deadline,
        Completion,
        bool atTick = false
    );
    /** @brief Coalesces one explicit logical cutoff, including in
     * manual tick mode. */
    void requestTick() noexcept;
    /** @brief Closes admission and drains physical I/O without
     * blocking its caller. */
    void stop() noexcept;
    Stats stats() const noexcept;

private:
    /// Owner posts and real journal completions retain shutdown
    /// state.
    std::shared_ptr<State> m_state;
};
} // namespace service::lobby
