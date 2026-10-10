#include "net/MessageRouter.h"

#include <span>
#include <thread>
#include <utility>

#include "net/ConnectionFeature.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
namespace
{
std::uint32_t payloadMsgId(const arena::protocol::Envelope& value)
{
    switch (value.payload_case())
    {
    case arena::protocol::Envelope::kLoginRequest:
        return arena::protocol::MSG_LOGIN_REQUEST;
    case arena::protocol::Envelope::kLoginResponse:
        return arena::protocol::MSG_LOGIN_RESPONSE;
    case arena::protocol::Envelope::kPing:
        return arena::protocol::MSG_PING;
    case arena::protocol::Envelope::kPong:
        return arena::protocol::MSG_PONG;
    case arena::protocol::Envelope::kError:
        return arena::protocol::MSG_ERROR;
    default:
        return arena::protocol::MSG_UNSPECIFIED;
    }
}
ErrorCode normalizedError(ErrorCode code)
{
    return code != arena::protocol::ERR_OK &&
                   arena::protocol::ErrorCode_IsValid(
                       static_cast<int>(code)
                   )
               ? code
               : arena::protocol::ERR_INTERNAL;
}
std::string_view errorMessage(ErrorCode code)
{
    switch (code)
    {
    case arena::protocol::ERR_UNKNOWN_MESSAGE:
        return "unknown message";
    case arena::protocol::ERR_INVALID_REQUEST:
        return "invalid request";
    case arena::protocol::ERR_UNAUTHENTICATED:
        return "authentication required";
    case arena::protocol::ERR_TIMEOUT:
        return "request timed out";
    case arena::protocol::ERR_TOO_MANY_REQUESTS:
        return "too many requests";
    case arena::protocol::ERR_DUPLICATE_REQUEST:
        return "duplicate request id";
    case arena::protocol::ERR_BUSINESS_FAILURE:
        return "business failure";
    case arena::protocol::ERR_UNAVAILABLE:
        return "service unavailable";
    default:
        return "internal error";
    }
}
} // namespace
BusinessError::BusinessError(ErrorCode code)
    : std::runtime_error(
          std::string(errorMessage(normalizedError(code)))
      )
    , m_code(normalizedError(code))
{
}
void MessageRegistry::registerHandler(
    MsgId id, MessageHandler handler, MessageRouteOptions options
)
{
    if (m_sealed)
        throw std::logic_error("message registry is sealed");
    if (id <= arena::protocol::MSG_UNSPECIFIED ||
        id == arena::protocol::MSG_LOGIN_RESPONSE ||
        id == arena::protocol::MSG_PONG ||
        id == arena::protocol::MSG_ERROR || !handler ||
        options.timeout <= std::chrono::milliseconds::zero() ||
        options.timeout > std::chrono::hours(1))
        throw std::invalid_argument("invalid message route");
    if (!m_routes
             .emplace(
                 static_cast<std::uint32_t>(id),
                 Route{std::move(handler), options}
             )
             .second)
        throw std::invalid_argument("duplicate message route");
}
struct MessageRouter::State : std::enable_shared_from_this<State>
{
    struct Pending
    {
        std::shared_ptr<const RequestMessage> request;
        std::uint32_t wireRequestId;
        bool explicitMsgId;
        std::uint64_t generation;
        ConnectionClock::time_point deadline{
            ConnectionClock::time_point::max()};
        ConnectionTimer timer;
    };
    ConnectionContext::Ptr context;
    std::shared_ptr<const MessageRegistry> registry;
    std::shared_ptr<spdlog::logger> logger;
    const std::thread::id owner{std::this_thread::get_id()};
    IConnection* connection{};
    std::unordered_map<std::uint64_t, Pending> pending;
    std::uint64_t generation{0};

    State(
        ConnectionContext::Ptr context,
        std::shared_ptr<const MessageRegistry> registry,
        std::shared_ptr<spdlog::logger> logger
    )
        : context(std::move(context))
        , registry(std::move(registry))
        , logger(std::move(logger))
    {
    }
    void
    log(std::string_view event,
        const RequestMessage& request,
        ErrorCode code,
        std::string_view outcome = "reply") noexcept
    {
        try
        {
            logger->info(
                "event={} connection_id={} request_id={} msg_id={} "
                "code={} outcome={}",
                event,
                request.connectionId,
                request.requestId,
                request.msgId,
                static_cast<int>(code),
                outcome
            );
        }
        catch (...)
        {
        }
    }
    ~State()
    {
        for (const auto& [id, entry] : pending)
        {
            (void)id;
            log("request_end",
                *entry.request,
                arena::protocol::ERR_OK,
                "cancelled");
        }
    }
    void respond(const Pending& entry, MessageReply result)
    {
        if (!connection->connected())
        {
            log("request_end",
                *entry.request,
                arena::protocol::ERR_OK,
                "cancelled");
            return;
        }
        ErrorCode code = arena::protocol::ERR_OK;
        arena::protocol::Envelope reply;
        if (result)
        {
            reply = std::move(*result);
            const auto responseId = payloadMsgId(reply);
            const bool correctReply =
                entry.request->msgId == arena::protocol::MSG_PING
                    ? responseId == arena::protocol::MSG_PONG
                : entry.request->msgId ==
                        arena::protocol::MSG_LOGIN_REQUEST
                    ? responseId ==
                          arena::protocol::MSG_LOGIN_RESPONSE
                    : responseId ==
                              arena::protocol::MSG_LOGIN_RESPONSE ||
                          responseId == arena::protocol::MSG_PONG;
            if (!correctReply)
                code = arena::protocol::ERR_INTERNAL;
        }
        else
            code = normalizedError(result.error());
        const auto setError = [&]
        {
            reply.Clear();
            reply.mutable_error()->set_code(code);
            reply.mutable_error()->set_message(
                std::string(errorMessage(code))
            );
        };
        if (code != arena::protocol::ERR_OK)
            setError();
        reply.set_request_id(entry.request->requestId);
        reply.set_msg_id(
            entry.explicitMsgId ? payloadMsgId(reply) : 0
        );
        auto payload = protocol::EnvelopeCodec::encode(reply);
        if (!payload)
        {
            code = arena::protocol::ERR_INTERNAL;
            setError();
            reply.set_request_id(entry.request->requestId);
            reply.set_msg_id(
                entry.explicitMsgId ? arena::protocol::MSG_ERROR : 0
            );
            payload = protocol::EnvelopeCodec::encode(reply);
        }
        const auto
            frame = payload
                        ? protocol::PacketCodec::encode(
                              protocol::PacketHeader::kEchoResponse,
                              entry.wireRequestId,
                              std::as_bytes(std::span(*payload))
                          )
                        : std::nullopt;
        if (!frame)
        {
            connection->close();
            log("request_end",
                *entry.request,
                arena::protocol::ERR_INTERNAL,
                "encode_failure");
            return;
        }
        if (code != arena::protocol::ERR_OK)
            context->rejections->record(
                code == arena::protocol::ERR_UNAUTHENTICATED
                    ? RejectionReason::Unauthenticated
                    : RejectionReason::HandlerRejected
            );
        connection->send(std::as_bytes(
            std::span(frame->peek(), frame->readableBytes())
        ));
        log("request_end", *entry.request, code);
    }
    void finish(
        std::uint64_t id, std::uint64_t ticket, MessageReply result
    )
    {
        auto found = pending.find(id);
        if (found == pending.end() ||
            found->second.generation != ticket)
            return;
        auto entry = std::move(found->second);
        pending.erase(found);
        entry.timer.reset();
        if (ConnectionClock::now() >= entry.deadline)
            result = std::unexpected(arena::protocol::ERR_TIMEOUT);
        respond(entry, std::move(result));
    }
};
MessageRouter::MessageRouter(
    ConnectionContext::Ptr context,
    std::shared_ptr<const MessageRegistry> registry,
    std::shared_ptr<spdlog::logger> logger
)
{
    if (!context || !registry || !registry->m_sealed)
        throw std::invalid_argument(
            "router requires context and sealed registry"
        );
    m_state = std::make_shared<State>(
        std::move(context),
        std::move(registry),
        logger ? std::move(logger) : spdlog::default_logger()
    );
}
MessageRouter::~MessageRouter() = default;
PacketHandleResult MessageRouter::handle(PacketContext& packet)
{
    if (std::this_thread::get_id() != m_state->owner)
        throw std::logic_error(
            "message router requires connection owner"
        );
    if (!packet.envelope ||
        packet.connection.id() != m_state->context->connectionId)
        return PacketHandleResult::Reject;
    if (m_state->connection &&
        m_state->connection != &packet.connection)
        throw std::logic_error(
            "message router belongs to one connection"
        );
    m_state->connection = &packet.connection;
    const auto& envelope = *packet.envelope;
    auto request = std::make_shared<const RequestMessage>(
        RequestMessage{
            packet.connection.id(),
            envelope.request_id() ? envelope.request_id()
                                  : packet.header.m_requestId,
            envelope.msg_id() ? envelope.msg_id()
                              : payloadMsgId(envelope),
            envelope,
            m_state->context}
    );
    State::Pending entry{
        request,
        packet.header.m_requestId,
        envelope.msg_id() != 0,
        ++m_state->generation,
        ConnectionClock::time_point::max(),
        ConnectionTimer{}};
    if (auto found = m_state->pending.find(request->requestId);
        found != m_state->pending.end())
    {
        m_state->finish(
            request->requestId,
            found->second.generation,
            std::unexpected(arena::protocol::ERR_DUPLICATE_REQUEST)
        );
        return PacketHandleResult::Handled;
    }
    m_state->log("request_start", *request, arena::protocol::ERR_OK);
    const auto route = m_state->registry->m_routes.find(request->msgId
    );
    const auto reject = [&](ErrorCode code)
    {
        m_state->respond(entry, std::unexpected(code));
        return PacketHandleResult::Handled;
    };
    if (!request->requestId ||
        packet.header.m_type != protocol::PacketHeader::kEchoRequest)
        return reject(arena::protocol::ERR_INVALID_REQUEST);
    if (route == m_state->registry->m_routes.end())
        return reject(arena::protocol::ERR_UNKNOWN_MESSAGE);
    if ((request->msgId == arena::protocol::MSG_PING &&
         !envelope.has_ping()) ||
        (request->msgId == arena::protocol::MSG_LOGIN_REQUEST &&
         !envelope.has_login_request()))
        return reject(arena::protocol::ERR_INVALID_REQUEST);
    const auto login = m_state->context->session->loginState();
    if (login == ConnectionSession::LoginState::Expired ||
        (route->second.options.requiresAuthentication &&
         login != ConnectionSession::LoginState::Authenticated))
        return reject(arena::protocol::ERR_UNAUTHENTICATED);
    if (m_state->pending.size() >= kMaxPendingRequests)
        return reject(arena::protocol::ERR_TOO_MANY_REQUESTS);
    auto* control = dynamic_cast<IConnectionControl*>(
        &packet.connection
    );
    if (!control)
        return reject(arena::protocol::ERR_INTERNAL);
    entry.deadline = ConnectionClock::now() +
                     route->second.options.timeout;
    const auto id = request->requestId;
    const auto ticket = entry.generation;
    m_state->pending.emplace(id, std::move(entry));
    const RequestCompletion complete =
        [weak = std::weak_ptr<State>(m_state),
         owner = m_state->owner,
         id,
         ticket](MessageReply result)
    {
        if (std::this_thread::get_id() != owner)
            throw std::logic_error(
                "request completion requires connection owner"
            );
        if (auto state = weak.lock())
            state->finish(id, ticket, std::move(result));
    };
    try
    {
        auto timer = control->scheduleAt(
            m_state->pending.at(id).deadline,
            [weak = std::weak_ptr<State>(m_state),
             id,
             ticket](IConnectionControl&)
            {
                if (auto state = weak.lock())
                    state->finish(
                        id,
                        ticket,
                        std::unexpected(arena::protocol::ERR_TIMEOUT)
                    );
            }
        );
        m_state->pending.at(id).timer = std::move(timer);
        m_state->log(
            "request_handler",
            *request,
            arena::protocol::ERR_OK
        );
        route->second.handler(std::move(request), complete);
    }
    catch (const BusinessError& error)
    {
        complete(std::unexpected(error.code()));
    }
    catch (...)
    {
        complete(std::unexpected(arena::protocol::ERR_INTERNAL));
    }
    return PacketHandleResult::Handled;
}
} // namespace service::gateway
