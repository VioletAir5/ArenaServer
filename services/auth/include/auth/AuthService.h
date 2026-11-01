/** @file
 * @brief Owns bounded identity verification and signing in the Auth
 * process.
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
#include <boost/asio/ip/tcp.hpp>

#include "auth.pb.h"
#include "auth/JwtTokens.h"
#include "rpc/Client.h"
#include "runtime/RuntimeConfig.h"

namespace service::auth
{
/** @brief Verifies credentials and signs candidates without owning
 * Lobby state.
 *
 * Admission, cancellation, stop() and stats() are thread-safe.
 * Accepted completions run on one strand after actual lookup/KDF
 * cleanup. The RPC server owns the prompt deadline response; a
 * running KDF retains its slot until OpenSSL returns. Keep the
 * executor pumping until stats().stopped, then destroy this object
 * outside its callbacks to join the idle workers.
 */
class AuthService final
{
public:
    struct Config
    {
        /// Signing profile retained exclusively by this Auth process.
        JwtConfig jwt;
        /// Fixed loopback route used only without a runtime store.
        boost::asio::ip::tcp::endpoint storageEndpoint;
        /// Explicit Auth-to-Storage credential, distinct from Lobby
        /// ingress.
        std::string storageToken;
        /// Optional immutable discovery source; no ready node means
        /// no fallback.
        std::shared_ptr<arena::runtime::ConfigStore> runtime;
        /// Fixed number of actual password/signing workers.
        std::size_t maxWorkers{4};
        /// Maximum waiting worker tasks; admission never waits for
        /// capacity.
        std::size_t maxQueued{8};
        /// Maximum retained business jobs, including Storage wait and
        /// cleanup.
        std::size_t maxInflight{32};
        /// Child Storage budget additionally clipped by the caller's
        /// deadline.
        std::chrono::milliseconds rpcTimeout{1000};
    };
    struct Stats
    {
        /// Actual business jobs retained through physical completion.
        std::size_t active{0};
        /// Tasks waiting for an actual worker.
        std::size_t queued{0};
        /// Workers currently executing a task.
        std::size_t busy{0};
        /// Worker threads that have not exited.
        std::size_t workersAlive{0};
        /// Total accepted business calls.
        std::uint64_t accepted{0};
        /// Immediate bounded-admission rejections.
        std::uint64_t rejected{0};
        /// Business jobs physically completed, including cancelled
        /// work.
        std::uint64_t completed{0};
        /// Current ready Storage route has a recent authenticated
        /// probe reply.
        bool storageReady{false};
        /// New business admission is closed.
        bool stopping{false};
        /// Jobs, workers, probes and outbound calls have all retired.
        bool stopped{false};
    };
    using Completion = std::function<void(arena::rpc::Response)>;
    using Cancellation = std::function<void()>;

    /** @brief Starts finite workers and asynchronous Storage health
     * observation. */
    AuthService(boost::asio::any_io_executor, Config);
    ~AuthService() noexcept;
    AuthService(const AuthService&) = delete;
    AuthService& operator=(const AuthService&) = delete;

    /** @brief Admits one candidate without installing any
     * authoritative session.
     * @return Cancellation handle, or a synchronous rejection with no
     * callback.
     */
    std::expected<Cancellation, arena::rpc::Status> authenticate(
        arena::auth::AuthenticateRequest,
        arena::internal_rpc::Context,
        Completion
    );
    /** @brief Samples bounded work and current dependency readiness.
     */
    Stats stats() const;
    /** @brief Cancels admission and work; keep pumping until stopped
     * is true. */
    void stop() noexcept;

private:
    struct State;
    /// Callback state is weakly observed by queued work and outbound
    /// replies.
    std::shared_ptr<State> m_state;
};
} // namespace service::auth
