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
struct RequestMessage
{
    std::uint64_t connectionId;
    std::uint64_t requestId;
    std::uint32_t msgId;
    arena::protocol::Envelope envelope;
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
struct MessageRouteOptions
{
    bool requiresAuthentication{true};
    std::chrono::milliseconds timeout{std::chrono::seconds(5)};
};
class BusinessError final : public std::runtime_error
{
public:
    explicit BusinessError(ErrorCode code);
    ErrorCode code() const noexcept
    {
        return m_code;
    }

private:
    ErrorCode m_code;
};
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
    struct Route
    {
        MessageHandler handler;
        MessageRouteOptions options;
    };
    bool m_sealed{false};
    std::unordered_map<std::uint32_t, Route> m_routes;
};
class MessageRouter final : public PacketHandler
{
public:
    static constexpr std::size_t kMaxPendingRequests = 64;
    MessageRouter(
        ConnectionContext::Ptr,
        std::shared_ptr<const MessageRegistry>,
        std::shared_ptr<spdlog::logger> logger = {}
    );
    ~MessageRouter() override;
    PacketHandleResult handle(PacketContext&) override;

private:
    struct State;
    std::shared_ptr<State> m_state;
};
} // namespace service::gateway
