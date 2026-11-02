/** @file
 * @brief Forwards typed identity candidates to the independent Auth
 * process.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <expected>
#include <functional>
#include <memory>
#include <string>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "auth.pb.h"
#include "rpc/Client.h"
#include "runtime/RuntimeConfig.h"

namespace service::lobby
{
/** @brief Owns bounded Auth RPC and validates its typed candidate
 * contract.
 *
 * This adapter performs no password verification, signing or
 * authority mutation. A successful JWT candidate still requires
 * Lobby's exact prior-claims/revision check before installation.
 * Accepted calls complete once on the RPC strand; immediate
 * rejections never invoke completion. authenticate(), stop() and
 * inflight() are thread-safe. stop() closes admission, but the
 * external executor must keep pumping until inflight() is zero. The
 * caller owns returned session_token storage and must erase it when
 * no longer needed.
 */
class AuthClient final
{
public:
    struct Config
    {
        /// Fixed loopback route used only when runtime discovery is
        /// absent.
        boost::asio::ip::tcp::endpoint endpoint;
        /// Explicit Lobby-to-Auth role credential; it has no default
        /// value.
        std::string token;
        /// Optional immutable discovery source, acquired once per
        /// admission.
        std::shared_ptr<arena::runtime::ConfigStore> runtime;
        /// Finite transport admission limit, including pending
        /// completion.
        std::size_t maxInflight{128};
        /// Child budget clipped to the original caller's monotonic
        /// deadline.
        std::chrono::milliseconds timeout{1000};
    };
    using Result = std::
        expected<arena::auth::AuthenticateReply, arena::rpc::Status>;
    using Completion = std::function<void(Result)>;
    using ProbeResult = std::expected<void, arena::rpc::Status>;
    using ProbeCompletion = std::function<void(ProbeResult)>;

    /** @brief Validates configuration without connecting or resolving
     * DNS. */
    AuthClient(boost::asio::any_io_executor, Config);
    ~AuthClient() noexcept;
    AuthClient(const AuthClient&) = delete;
    AuthClient& operator=(const AuthClient&) = delete;

    /** @brief Captures one ready Auth endpoint and forwards an owning
     * request.
     * @return Existing weak cancellation handle, or immediate public
     * failure.
     */
    std::expected<arena::internal_rpc::Call, arena::rpc::Status>
        authenticate(
            arena::auth::AuthenticateRequest,
            arena::internal_rpc::Context,
            Completion
        );
    /** @brief Probes the same authenticated ready route without
     * credentials in a payload. */
    std::expected<arena::internal_rpc::Call, arena::rpc::Status>
        probe(arena::internal_rpc::Context, ProbeCompletion);
    /** @brief Fences admission and cancels accepted transports
     * without waiting. */
    void stop();
    /** @brief Counts actual accepted transports before completion
     * dispatch. */
    std::size_t inflight() const;

private:
    /** @brief Captures one route and installs common caller metadata.
     */
    std::expected<arena::internal_rpc::Call, arena::rpc::Status>
        exchange(
            arena::rpc::Request,
            arena::internal_rpc::Context,
            arena::internal_rpc::Client::Completion
        );
    /// Read-only route/credential policy; its secret is erased on
    /// destruction.
    Config m_config;
    /// Owns deadline, cancellation and physical socket lifetime.
    arena::internal_rpc::Client m_client;
};
} // namespace service::lobby
