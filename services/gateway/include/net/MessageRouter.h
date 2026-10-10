/** @file
 * @brief Routes decoded requests to registered business handlers.
 */
#pragma once

#include <chrono>
#include <expected>
#include <functional>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include <spdlog/spdlog.h>

#include "net/PacketPipeline.h"

namespace service::gateway
{
using MsgId = arena::protocol::MsgId;
using ErrorCode = arena::protocol::ErrorCode;
/** @brief Owns the data and context needed to handle one request. */
struct RequestMessage
{
    /// Connection that sent this request.
    std::uint64_t connectionId;
    /// Correlation identifier supplied by the client.
    std::uint64_t requestId;
    /// Application message identifier used for route lookup.
    std::uint32_t msgId;
    /// Decoded protobuf request.
    arena::protocol::Envelope envelope;
    /// Immutable admission metadata for this connection.
    ConnectionContext::Ptr context;
};
using MessageReply = std::
    expected<arena::protocol::Envelope, ErrorCode>;
// Complete on the connection owner. The request owns its protobuf
// data; retain its shared pointer when an asynchronous handler needs
// it.
using RequestCompletion = std::function<void(MessageReply)>;
using MessageHandler = std::function<
    void(std::shared_ptr<const RequestMessage>, RequestCompletion)>;
/** @brief Sets admission and timeout policy for a registered route. */
struct MessageRouteOptions
{
    /// Whether the connection must be authenticated first.
    bool requiresAuthentication{true};
    /// Maximum time allowed for handler completion.
    std::chrono::milliseconds timeout{std::chrono::seconds(5)};
};
/** @brief Carries an application error code through handler failures. */
class BusinessError final : public std::runtime_error
{
public:
    explicit BusinessError(ErrorCode code);
    ErrorCode code() const noexcept
    {
        return m_code;
    }

private:
    /// Stores the code.
    ErrorCode m_code;
};
/** @brief Represents message registry. */
class MessageRegistry final
{
public:
    MessageRegistry() = default;
    MessageRegistry(const MessageRegistry&) = delete;
    MessageRegistry& operator=(const MessageRegistry&) = delete;
    void registerHandler(
        MsgId, MessageHandler, MessageRouteOptions = {}
    );
    void seal() noexcept
    {
        m_sealed = true;
    }

private:
    friend class MessageRouter;
    /** @brief Represents route. */
    struct Route
    {
        MessageHandler handler;
        MessageRouteOptions options;
    };
    /// Whether route registration has ended.
    bool m_sealed{false};
    /// Stores the routes.
    std::unordered_map<std::uint32_t, Route> m_routes;
};
/** @brief Routes decoded requests to registered business handlers. */
class MessageRouter final : public PacketHandler
{
public:
    static constexpr std::size_t kMaxPendingRequests = 64;
    MessageRouter(
        ConnectionContext::Ptr,
        std::shared_ptr<const MessageRegistry>,
        std::shared_ptr<spdlog::logger> logger = {}
    );
    /** @brief Releases owned resources. */
    ~MessageRouter() override;
    PacketHandleResult handle(PacketContext&) override;

private:
    struct State;
    /// Shared request and timer state retained by callbacks.
    std::shared_ptr<State> m_state;
};
} // namespace service::gateway
