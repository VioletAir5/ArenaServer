/** @file
 * @brief Forwards Gateway commands and bounded presence heartbeats to
 * Lobby.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "lobby.pb.h"
#include "lobby/SessionBinding.h"
#include "rpc/Client.h"

namespace arena::runtime
{
class ConfigStore;
}

namespace service::gateway
{
/** @brief Owns finite asynchronous RPC and passive transport
 * projections. No executor threads are created. Health probes use a
 * third one-call slot. Admission, tracking and stop are thread-safe;
 * keep the supplied executor pumping until stats().stopped before
 * teardown.
 */
class LobbyClient final
{
public:
    struct Config
    {
        /// Explicit loopback endpoint used only without runtime
        /// discovery.
        boost::asio::ip::tcp::endpoint endpoint{
            boost::asio::ip::address_v4::loopback(), 7102
        };
        /// Gateway-to-Lobby credential, never logged or replaced with
        /// a default.
        std::string serviceToken;
        /// Complete immutable discovery snapshots; missing Lobby
        /// never falls back.
        std::shared_ptr<arena::runtime::ConfigStore> runtime;
        /// Fresh process identity encoded as exactly 32 lowercase
        /// hexadecimal digits.
        std::string gatewayInstance;
        /// Maximum concurrent business execute/detach calls.
        std::size_t maxInflight{64};
        /// Maximum retained physical connection projections.
        std::size_t maxConnections{65536};
        /// One propagated RPC budget in the inclusive range [1ms,
        /// 1000ms].
        std::chrono::milliseconds timeout{1000};
    };
    struct Stats
    {
        /// Tracked entries, including revoked entries awaiting owner
        /// cleanup.
        std::size_t tracked{0};
        /// Actual admitted business and independent heartbeat
        /// callbacks.
        std::size_t businessInflight{0}, heartbeatInflight{0};
        /// Actual read-only health calls and their cleanup callbacks.
        std::size_t probeInflight{0};
        /// Latest qualified health.ready result for the current
        /// discovery version.
        bool dependencyReady{false};
        /// Owner posts and timer handlers still requiring executor
        /// progress.
        std::size_t ownerPosts{0};
        /// Cumulative valid heartbeats and transport revocations.
        std::uint64_t heartbeats{0}, revoked{0};
        /// New work is rejected after stop fences admission.
        bool accepting{false};
        /// Every physical call and timer/post has completed.
        bool stopped{false};
    };
    using Context = arena::internal_rpc::Context;
    using Call = arena::internal_rpc::Call;
    using Status = arena::rpc::Status;
    using ExecuteResult = std::
        expected<arena::lobby::ExecuteReply, Status>;
    using ExecuteCompletion = std::function<void(ExecuteResult)>;
    using DetachResult = std::
        expected<arena::lobby::DetachReply, Status>;
    using DetachCompletion = std::function<void(DetachResult)>;
    using HeartbeatResult = std::
        expected<arena::lobby::HeartbeatReply, Status>;
    using HeartbeatCompletion = std::function<void(HeartbeatResult)>;

    /** @brief Validates configuration and starts a 250ms control
     * timer. */
    LobbyClient(boost::asio::any_io_executor, Config);
    ~LobbyClient() noexcept;
    LobbyClient(const LobbyClient&) = delete;
    LobbyClient& operator=(const LobbyClient&) = delete;
    /** @brief Admits one exact typed command with no automatic retry.
     */
    std::expected<Call, Status> execute(
        arena::lobby::ExecuteRequest, Context, ExecuteCompletion
    );
    /** @brief Admits a conditional detach for one exact transport
     * generation. */
    std::expected<Call, Status> detach(
        arena::lobby::DetachRequest, Context, DetachCompletion
    );
    /** @brief Uses a separate one-call slot and validates revoked IDs
     * against its batch. */
    std::expected<Call, Status> heartbeat(
        arena::lobby::HeartbeatRequest, Context, HeartbeatCompletion
    );
    /** @brief Installs a passive projection without accessing an
     * owner session. */
    std::expected<std::shared_ptr<std::atomic<bool>>, Status> track(
        service::lobby::SessionBinding,
        std::int64_t expiresAtUnix,
        std::function<void()> closeMailbox
    );
    /** @brief Removes one exact projection and attempts its detach
     * once. */
    void untrack(
        service::lobby::SessionBinding, bool sendDetach = true
    ) noexcept;
    /** @brief Revokes one exact projection and posts its weak owner
     * close handle. */
    void invalidate(service::lobby::SessionBinding) noexcept;
    /** @brief Closes admission and cancels all physical calls without
     * waiting. */
    void stop() noexcept;
    /** @brief Reads bounded lifetime accounting from any thread. */
    Stats stats() const noexcept;

private:
    struct State;
    /// Callbacks retain state through real physical RPC completion.
    std::shared_ptr<State> m_state;
};
} // namespace service::gateway
