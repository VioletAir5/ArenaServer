/** @file
 * @brief Gives Lobby typed, bounded access to Storage without
 * persistence ownership.
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

#include "rpc/Client.h"
#include "runtime/RuntimeConfig.h"
#include "storage.pb.h"

namespace service::lobby
{
/** @brief Validates Storage replies while retaining no SQL, retries
 * or authority. Accepted calls complete once on the shared RPC
 * strand. Immediate rejection invokes no completion. Public
 * operations are thread-safe; the caller keeps pumping the executor
 * after stop until inflight is zero. Cancellation or an unknown
 * outcome never proves that a remote mutation rolled back.
 */
class StorageClient final
{
public:
    struct Config
    {
        /// Fixed loopback route, used only when discovery is absent.
        boost::asio::ip::tcp::endpoint endpoint;
        /// Explicit Lobby-to-Storage credential with no default or
        /// fallback.
        std::string token;
        /// One immutable route snapshot is acquired for each
        /// admission.
        std::shared_ptr<arena::runtime::ConfigStore> runtime;
        /// Finite transports, including pending completion dispatch.
        std::size_t maxInflight{128};
        /// Child timeout clipped to its inherited parent deadline.
        std::chrono::milliseconds timeout{1000};
    };
    template <class Reply>
    using Result = std::expected<Reply, arena::rpc::Status>;
    template <class Reply>
    using Completion = std::function<void(Result<Reply>)>;
    using Admission = std::
        expected<arena::internal_rpc::Call, arena::rpc::Status>;

    /** @brief Validates bindings without connecting, resolving or
     * creating threads. */
    StorageClient(boost::asio::any_io_executor, Config);
    ~StorageClient() noexcept;
    StorageClient(const StorageClient&) = delete;
    StorageClient& operator=(const StorageClient&) = delete;

    /** @brief Fetches an exact active player and canonical rating. */
    Admission player(
        arena::storage::PlayerGetRequest,
        arena::internal_rpc::Context,
        Completion<arena::storage::PlayerGetReply>
    );
    /** @brief Reserves one non-retried inclusive signed-63-bit ID
     * range. */
    Admission allocateIds(
        arena::storage::AllocateIdsRequest,
        arena::internal_rpc::Context,
        Completion<arena::storage::AllocateIdsReply>
    );
    /** @brief Submits immutable terminal facts under the caller's
     * stable key. Zero requested timestamps permit Storage-assigned
     * receipt times. Replay flags never replace exact immutable-fact
     * and receipt identity checks.
     */
    Admission saveResult(
        arena::storage::SaveResultRequest,
        arena::internal_rpc::Context,
        std::string stableIdempotencyKey,
        Completion<arena::storage::SaveResultReply>
    );
    /** @brief Fetches one exact managed match, including valid
     * intermediate states. */
    Admission match(
        arena::storage::MatchGetRequest,
        arena::internal_rpc::Context,
        Completion<arena::storage::MatchGetReply>
    );
    /** @brief Fetches an exact committed receipt without inferring
     * match completion. */
    Admission getSettlement(
        arena::storage::SettlementGetRequest,
        arena::internal_rpc::Context,
        Completion<arena::storage::SettlementGetReply>
    );
    /** @brief Requires an authenticated successful readiness response
     * with no payload. */
    Admission probe(arena::internal_rpc::Context, Completion<void>);
    /** @brief Closes admission and cancels actual transports without
     * waiting. */
    void stop();
    /** @brief Counts physical admitted operations before their
     * completion dispatch. */
    std::size_t inflight() const;

private:
    /** @brief Captures a route and fills shared metadata without
     * changing the method. */
    Admission exchange(
        arena::rpc::Request,
        arena::internal_rpc::Context,
        arena::internal_rpc::Client::Completion
    );
    /// Immutable route and role policy; erase its token on
    /// destruction.
    Config m_config;
    /// Owns physical sockets, finite admission, deadlines and
    /// cancellation.
    arena::internal_rpc::Client m_client;
};
} // namespace service::lobby