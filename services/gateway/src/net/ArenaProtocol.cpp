/** @file
 * @brief Implements Arena protobuf wire conversion outside request
 * dispatch.
 */
#include "net/ArenaProtocol.h"

#include "net/PacketBuffer.h"
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
    case arena::protocol::Envelope::kSessionStateRequest:
        return arena::protocol::MSG_SESSION_STATE_REQUEST;
    case arena::protocol::Envelope::kSessionStateResponse:
        return arena::protocol::MSG_SESSION_STATE_RESPONSE;
    case arena::protocol::Envelope::kRoomJoinRequest:
        return arena::protocol::MSG_ROOM_JOIN_REQUEST;
    case arena::protocol::Envelope::kRoomJoinResponse:
        return arena::protocol::MSG_ROOM_JOIN_RESPONSE;
    case arena::protocol::Envelope::kRoomLeaveRequest:
        return arena::protocol::MSG_ROOM_LEAVE_REQUEST;
    case arena::protocol::Envelope::kRoomLeaveResponse:
        return arena::protocol::MSG_ROOM_LEAVE_RESPONSE;
    case arena::protocol::Envelope::kMatchJoinRequest:
        return arena::protocol::MSG_MATCH_JOIN_REQUEST;
    case arena::protocol::Envelope::kMatchJoinResponse:
        return arena::protocol::MSG_MATCH_JOIN_RESPONSE;
    case arena::protocol::Envelope::kMatchCancelRequest:
        return arena::protocol::MSG_MATCH_CANCEL_REQUEST;
    case arena::protocol::Envelope::kMatchCancelResponse:
        return arena::protocol::MSG_MATCH_CANCEL_RESPONSE;
    case arena::protocol::Envelope::kMatchStatusRequest:
        return arena::protocol::MSG_MATCH_STATUS_REQUEST;
    case arena::protocol::Envelope::kMatchStatusResponse:
        return arena::protocol::MSG_MATCH_STATUS_RESPONSE;
    case arena::protocol::Envelope::kRoomInputRequest:
        return arena::protocol::MSG_ROOM_INPUT_REQUEST;
    case arena::protocol::Envelope::kRoomInputResponse:
        return arena::protocol::MSG_ROOM_INPUT_RESPONSE;
    case arena::protocol::Envelope::kRoomSnapshotRequest:
        return arena::protocol::MSG_ROOM_SNAPSHOT_REQUEST;
    case arena::protocol::Envelope::kRoomSnapshotResponse:
        return arena::protocol::MSG_ROOM_SNAPSHOT_RESPONSE;
    case arena::protocol::Envelope::kRoomReadyRequest:
        return arena::protocol::MSG_ROOM_READY_REQUEST;
    case arena::protocol::Envelope::kRoomReadyResponse:
        return arena::protocol::MSG_ROOM_READY_RESPONSE;
    case arena::protocol::Envelope::kRoomStatusRequest:
        return arena::protocol::MSG_ROOM_STATUS_REQUEST;
    case arena::protocol::Envelope::kRoomStatusResponse:
        return arena::protocol::MSG_ROOM_STATUS_RESPONSE;
    case arena::protocol::Envelope::kSettlementRequest:
        return arena::protocol::MSG_SETTLEMENT_REQUEST;
    case arena::protocol::Envelope::kSettlementResponse:
        return arena::protocol::MSG_SETTLEMENT_RESPONSE;
    default:
        return arena::protocol::MSG_UNSPECIFIED;
    }
}

/** @brief Checks known request payloads without hiding unknown
 * message errors. */
bool requestPayloadMatches(
    std::uint32_t id, const arena::protocol::Envelope& envelope
)
{
    switch (id)
    {
    case arena::protocol::MSG_PING:
        return envelope.has_ping();
    case arena::protocol::MSG_LOGIN_REQUEST:
        return envelope.has_login_request();
    case arena::protocol::MSG_SESSION_STATE_REQUEST:
        return envelope.has_session_state_request();
    case arena::protocol::MSG_ROOM_JOIN_REQUEST:
        return envelope.has_room_join_request();
    case arena::protocol::MSG_ROOM_LEAVE_REQUEST:
        return envelope.has_room_leave_request();
    case arena::protocol::MSG_MATCH_JOIN_REQUEST:
        return envelope.has_match_join_request();
    case arena::protocol::MSG_MATCH_CANCEL_REQUEST:
        return envelope.has_match_cancel_request();
    case arena::protocol::MSG_MATCH_STATUS_REQUEST:
        return envelope.has_match_status_request();
    case arena::protocol::MSG_ROOM_INPUT_REQUEST:
        return envelope.has_room_input_request();
    case arena::protocol::MSG_ROOM_SNAPSHOT_REQUEST:
        return envelope.has_room_snapshot_request();
    case arena::protocol::MSG_ROOM_READY_REQUEST:
        return envelope.has_room_ready_request();
    case arena::protocol::MSG_ROOM_STATUS_REQUEST:
        return envelope.has_room_status_request();
    case arena::protocol::MSG_SETTLEMENT_REQUEST:
        return envelope.has_settlement_request();
    default:
        return true;
    }
}

/** @brief Preserves typed response direction for each built-in
 * application route. */
bool responseMatches(std::uint32_t request, std::uint32_t response)
{
    switch (request)
    {
    case arena::protocol::MSG_PING:
        return response == arena::protocol::MSG_PONG;
    case arena::protocol::MSG_LOGIN_REQUEST:
        return response == arena::protocol::MSG_LOGIN_RESPONSE;
    case arena::protocol::MSG_SESSION_STATE_REQUEST:
        return response ==
               arena::protocol::MSG_SESSION_STATE_RESPONSE;
    case arena::protocol::MSG_ROOM_JOIN_REQUEST:
        return response == arena::protocol::MSG_ROOM_JOIN_RESPONSE;
    case arena::protocol::MSG_ROOM_LEAVE_REQUEST:
        return response == arena::protocol::MSG_ROOM_LEAVE_RESPONSE;
    case arena::protocol::MSG_MATCH_JOIN_REQUEST:
        return response == arena::protocol::MSG_MATCH_JOIN_RESPONSE;
    case arena::protocol::MSG_MATCH_CANCEL_REQUEST:
        return response == arena::protocol::MSG_MATCH_CANCEL_RESPONSE;
    case arena::protocol::MSG_MATCH_STATUS_REQUEST:
        return response == arena::protocol::MSG_MATCH_STATUS_RESPONSE;
    case arena::protocol::MSG_ROOM_INPUT_REQUEST:
        return response == arena::protocol::MSG_ROOM_INPUT_RESPONSE;
    case arena::protocol::MSG_ROOM_SNAPSHOT_REQUEST:
        return response ==
               arena::protocol::MSG_ROOM_SNAPSHOT_RESPONSE;
    case arena::protocol::MSG_ROOM_READY_REQUEST:
        return response == arena::protocol::MSG_ROOM_READY_RESPONSE;
    case arena::protocol::MSG_ROOM_STATUS_REQUEST:
        return response == arena::protocol::MSG_ROOM_STATUS_RESPONSE;
    case arena::protocol::MSG_SETTLEMENT_REQUEST:
        return response == arena::protocol::MSG_SETTLEMENT_RESPONSE;
    default:
        // Custom routes retain the existing supported reply contract.
        return response == arena::protocol::MSG_LOGIN_RESPONSE ||
               response == arena::protocol::MSG_PONG;
    }
}

ErrorCode sendReply(
    ConnectionChannel& channel,
    const ReplyMetadata& metadata,
    MessageReply result
)
{
    ErrorCode code = arena::protocol::ERR_OK;
    arena::protocol::Envelope reply;
    if (result)
    {
        reply = std::move(*result);
        const auto responseId = payloadMsgId(reply);
        const bool valid = responseMatches(
            metadata.request->msgId,
            responseId
        );
        if (!valid)
            code = arena::protocol::ERR_INTERNAL;
    }
    else
        code = normalizeErrorCode(result.error());
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
    reply.set_request_id(metadata.request->requestId);
    reply.set_msg_id(
        metadata.explicitMsgId ? payloadMsgId(reply) : 0
    );
    auto payload = protocol::EnvelopeCodec::encode(reply);
    if (!payload ||
        payload->size() >
            kMaxPacketSize - protocol::PacketHeader::kMetadataSize)
    {
        code = arena::protocol::ERR_INTERNAL;
        setError();
        reply.set_request_id(metadata.request->requestId);
        reply.set_msg_id(
            metadata.explicitMsgId ? arena::protocol::MSG_ERROR : 0
        );
        payload = protocol::EnvelopeCodec::encode(reply);
    }
    const auto
        frame = payload ? protocol::PacketCodec::encode(
                              protocol::PacketHeader::kEchoResponse,
                              metadata.wireRequestId,
                              std::as_bytes(std::span(*payload))
                          )
                        : std::nullopt;
    if (!frame)
    {
        channel.close();
        return arena::protocol::ERR_INTERNAL;
    }
    if (code != arena::protocol::ERR_OK)
        metadata.request->context->rejections->record(
            code == arena::protocol::ERR_UNAUTHENTICATED
                ? RejectionReason::Unauthenticated
                : RejectionReason::HandlerRejected
        );
    channel.send(
        std::as_bytes(
            std::span(frame->peek(), frame->readableBytes())
        )
    );
    return code;
}
} // namespace

ArenaProtocol::ArenaProtocol(
    ConnectionContext::Ptr context,
    std::shared_ptr<const MessageRegistry> registry,
    PacketPolicyFactory policies,
    std::shared_ptr<spdlog::logger> logger
)
    : m_context(std::move(context))
    , m_dispatcher(
          m_context, std::move(registry), sendReply, std::move(logger)
      )
{
    PacketPolicyChain chain;
    if (policies)
        policies(chain, *m_context);
    m_frames = makeFrameProtocolHandler(
        [this](FrameContext& frame) { return onFrame(frame); },
        kMaxPacketSize,
        m_context->rejections,
        std::move(chain)
    );
}

void ArenaProtocol::start(ConnectionChannel& channel)
{
    m_context->session->setCloseRequest(
        [post = channel.poster()]
        { post([](ConnectionChannel& owner) { owner.close(); }); }
    );
}

void ArenaProtocol::receive(
    ConnectionChannel& channel, std::span<const std::byte> bytes
)
{
    m_frames(channel, bytes);
}

void ArenaProtocol::stop() noexcept
{
    m_dispatcher.stop();
    m_context->session->disconnect();
}

void ArenaProtocol::drain(std::function<void()> drained)
{
    m_dispatcher.drain(std::move(drained));
}

FrameResult ArenaProtocol::onFrame(FrameContext& frame)
{
    arena::protocol::Envelope envelope;
    if (!protocol::EnvelopeCodec::decode(
            std::as_bytes(std::span(frame.payload)),
            envelope
        ))
    {
        frame.rejectionReason = RejectionReason::InvalidEnvelope;
        return FrameResult::Reject;
    }
    const auto id = envelope.msg_id() ? envelope.msg_id()
                                      : payloadMsgId(envelope);
    const bool validPayload = requestPayloadMatches(id, envelope);
    const bool explicitId = envelope.msg_id() != 0;
    const auto requestId = envelope.request_id()
                               ? envelope.request_id()
                               : frame.header.m_requestId;
    auto request = std::make_shared<const RequestMessage>(
        RequestMessage{
            frame.connection.id(),
            requestId,
            id,
            std::move(envelope),
            m_context
        }
    );
    m_dispatcher.dispatch(
        frame.connection,
        ReplyMetadata{
            std::move(request),
            frame.header.m_requestId,
            explicitId
        },
        frame.header.m_type == protocol::PacketHeader::kEchoRequest &&
            validPayload
    );
    return FrameResult::Handled;
}

ConnectionHandler makeArenaProtocolHandler(
    ConnectionContext::Ptr context,
    std::shared_ptr<const MessageRegistry> registry,
    PacketPolicyFactory policies,
    std::shared_ptr<spdlog::logger> logger
)
{
    auto protocol = std::make_shared<ArenaProtocol>(
        std::move(context),
        std::move(registry),
        std::move(policies),
        std::move(logger)
    );
    ConnectionHandler handler;
    handler.onMessage = [protocol](
                            ConnectionChannel& channel,
                            std::span<const std::byte> bytes
                        ) { protocol->receive(channel, bytes); };
    handler.onStarted = [protocol](ConnectionChannel& channel)
    { protocol->start(channel); };
    handler.onClosed = [protocol] { protocol->stop(); };
    handler.onDraining = [protocol](std::function<void()> drained)
    { protocol->drain(std::move(drained)); };
    return handler;
}
} // namespace service::gateway
