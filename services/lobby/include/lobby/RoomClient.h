/** @file
 * @brief Captures exact Room node routes and validates typed worker
 * RPC replies.
 */
#pragma once
#include <chrono>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/any_io_executor.hpp>

#include "room.pb.h"
#include "rpc/Client.h"
#include "runtime/RuntimeConfig.h"

namespace service::lobby
{
/** @brief Owns bounded worker transport, never a room kernel or
 * authority. Every admission captures one exact node from a coherent
 * discovery snapshot. An unavailable owner never falls back to
 * another worker. Accepted completions run once on the native client
 * strand; the executor pumps through stop/inflight.
 */
class RoomClient final
{
public:
    struct Node
    {
        std::string id; ///< Stable configured discovery identity.
        boost::asio::ip::tcp::endpoint
            endpoint; ///< Exact loopback route.
    };
    struct Config
    {
        std::vector<Node>
            nodes; ///< Explicit routes when discovery is absent.
        std::string token; ///< Lobby-only Room credential, erased on
                           ///< destruction.
        /// Immutable discovery source acquired for each admission,
        /// when configured.
        std::shared_ptr<arena::runtime::ConfigStore> runtime;
        /// Bound on accepted transports, including queued completion
        /// delivery.
        std::size_t maxInflight{256};
        /// Per-call child budget clipped to the inherited parent
        /// deadline.
        std::chrono::milliseconds timeout{1000};
    };
    template <class T>
    using Result = std::expected<T, arena::rpc::Status>;
    template <class T>
    using Completion = std::function<void(Result<T>)>;
    using Admission = std::
        expected<arena::internal_rpc::Call, arena::rpc::Status>;
    /** @brief Validates route policy and constructs a bounded native
     * client.
     * @param[in] executor Executor kept alive through all accepted
     * completions.
     * @param[in] config Exact routes, role credential and finite
     * transport bounds.
     */
    RoomClient(boost::asio::any_io_executor executor, Config config);
    /** @brief Requests transport stop and erases the retained role
     * credential. */
    ~RoomClient() noexcept;
    RoomClient(const RoomClient&) = delete;
    RoomClient& operator=(const RoomClient&) = delete;
    /** @brief Returns ready configured node names, not an
     * owner-selection fallback. */
    std::vector<std::string> nodes() const;
    /** @brief Probes one exact node, including operator-unready
     * records. */
    Admission probe(
        std::string nodeId,
        arena::internal_rpc::Context,
        Completion<arena::room::HealthReply>
    );
    /** @brief Computes an initial image on the exact worker without
     * creating a private cache record. */
    Admission create(
        arena::room::CreateRequest,
        arena::internal_rpc::Context,
        Completion<arena::room::CreateReply>
    );
    /** @brief Validates and durably stages an inactive image with its
     * exact transfer identity. */
    Admission stage(
        arena::room::StageRequest,
        arena::internal_rpc::Context,
        Completion<arena::room::StageReceipt>
    );
    /** @brief Installs the image already acknowledged by Lobby; it
     * cannot publish canonical state. */
    Admission activate(
        arena::room::ActivateRequest,
        arena::internal_rpc::Context,
        Completion<arena::room::CacheReceipt>
    );
    /** @brief Computes an isolated ordered-prefix proposal without
     * advancing canonical authority. */
    Admission execute(
        arena::room::ExecuteRequest,
        arena::internal_rpc::Context,
        Completion<arena::room::ExecuteReply>
    );
    /** @brief Fences the exact cached owner epoch while preserving
     * newer staged transfers. */
    Admission freeze(
        arena::room::HintRequest,
        arena::internal_rpc::Context,
        Completion<arena::room::CacheReceipt>
    );
    /** @brief Releases the exact cached image while retaining its
     * epoch fence. */
    Admission retire(
        arena::room::HintRequest,
        arena::internal_rpc::Context,
        Completion<arena::room::CacheReceipt>
    );
    /** @brief Fences admission and cancels transports without waiting
     * for delivery. */
    void stop();
    /** @brief Counts real accepted transports until their completion
     * dispatch ends. */
    std::size_t inflight() const;
    /** @brief Checks canonical node:32-lowercase-hex process
     * identity. */
    static std::optional<std::string>
    nodeOf(std::string_view workerId);

private:
    /** @brief Resolves only the requested node and its required
     * readiness policy. */
    Result<boost::asio::ip::tcp::endpoint>
    endpoint(std::string_view, bool ready) const;
    /** @brief Captures a route and installs role-bound inherited RPC
     * metadata. */
    Admission exchange(
        std::string nodeId,
        bool requireReady,
        arena::rpc::Request,
        arena::internal_rpc::Context,
        arena::internal_rpc::Client::Completion
    );
    /** @brief Encodes one request and verifies the complete typed
     * reply identity. */
    template <class Request, class Reply, class Verify>
    Admission typed(
        arena::rpc::Request,
        Request,
        arena::internal_rpc::Context,
        Verify,
        Completion<Reply>
    );
    /** @brief Shares exact cache-receipt validation for freeze and
     * retirement. */
    Admission hint(
        arena::rpc::Request,
        arena::room::HintRequest,
        arena::internal_rpc::Context,
        Completion<arena::room::CacheReceipt>
    );
    /// Immutable routes and role policy; the credential is erased at
    /// destruction.
    Config m_config;
    /// Physical socket, cancellation, deadline and admission owner.
    arena::internal_rpc::Client m_client;
};
} // namespace service::lobby
