/** @file
 * @brief Defines owning business requests and the immutable message
 * route table.
 */
#pragma once

#include <chrono>
#include <expected>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <unordered_map>

#include "net/ConnectionContext.h"

#include "envelope.pb.h"

namespace service::gateway
{
using MsgId = arena::protocol::MsgId;
using ErrorCode = arena::protocol::ErrorCode;

/** @brief Owns request data that can outlive the incoming read
 * buffer. */
struct RequestMessage
{
    /// Stable identity of the originating connection.
    std::uint64_t connectionId;
    /// Logical correlation ID, including the legacy wire fallback.
    std::uint64_t requestId;
    /// Application route selected from explicit or inferred message
    /// ID.
    std::uint32_t msgId;
    /// Owning protobuf data, safe to retain past the read callback.
    arena::protocol::Envelope envelope;
    // Metadata is immutable; session mutation still requires the
    // connection owner.
    ConnectionContext::Ptr context;
};

using MessageReply = std::
    expected<arena::protocol::Envelope, ErrorCode>;
// Completion may run on any thread. The dispatcher marshals it to the
// owner.
using RequestCompletion = std::function<void(MessageReply)>;
using MessageHandler = std::function<
    void(std::shared_ptr<const RequestMessage>, RequestCompletion)>;

struct MessageRouteOptions
{
    /// Requires authenticated connection state before handler
    /// invocation.
    bool requiresAuthentication{true};
    /// Maximum duration from request admission to owner-thread
    /// completion.
    std::chrono::milliseconds timeout{std::chrono::seconds(5)};
};

ErrorCode normalizeErrorCode(ErrorCode) noexcept;
std::string_view errorMessage(ErrorCode) noexcept;

class BusinessError final : public std::runtime_error
{
public:
    explicit BusinessError(ErrorCode code);
    ErrorCode code() const noexcept
    {
        return m_code;
    }

private:
    /// Normalized public error code without internal exception
    /// details.
    ErrorCode m_code;
};

/** @brief Publishes a sealed route table shared by connection
 * dispatchers. */
class MessageRegistry final
{
public:
    struct Route
    {
        /// Owner-invoked business handler; completion may return from
        /// any thread.
        MessageHandler handler;
        /// Authentication and deadline contract for the route.
        MessageRouteOptions options;
    };

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
    bool sealed() const noexcept
    {
        return m_sealed;
    }
    const Route* find(std::uint32_t id) const;

private:
    /// Registration ends before publication to worker threads.
    bool m_sealed{false};
    /// Immutable lookup table once sealed.
    std::unordered_map<std::uint32_t, Route> m_routes;
};
} // namespace service::gateway
