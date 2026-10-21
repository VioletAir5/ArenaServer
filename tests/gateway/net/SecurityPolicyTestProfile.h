#pragma once

#include <atomic>
#include <iostream>

#include "net/FrameProtocol.h"
#include "net/PacketBuffer.h"
#include "net/PacketRatePolicy.h"
#include "net/PacketTypePolicy.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
// Synthetic credentials and business payloads never enter production
// routes.
inline std::atomic<unsigned> securityTestRoutes{0};

inline FrameResult sendSecurityTestEnvelope(
    FrameContext& frame, const arena::protocol::Envelope& envelope
)
{
    const auto payload = protocol::EnvelopeCodec::encode(envelope);
    if (!payload)
        return FrameResult::Reject;
    const auto output = protocol::PacketCodec::encode(
        protocol::PacketHeader::kEchoResponse,
        frame.header.m_requestId,
        std::as_bytes(std::span(*payload))
    );
    if (!output)
        return FrameResult::Reject;
    frame.connection.send(
        std::as_bytes(
            std::span(output->peek(), output->readableBytes())
        )
    );
    return frame.connection.connected() ? FrameResult::Handled
                                        : FrameResult::Reject;
}

inline ConnectionHandlerFactory makeSecurityTestFactory()
{
    return [](ConnectionContext::Ptr context)
    {
        PacketPolicyChain policies;
        policies.add<PacketRatePolicy>(2.0, 0.1);
        policies.add<PacketTypePolicy>(std::vector<std::uint16_t>{1});
        ConnectionHandler result = makeFrameProtocolHandler(
            [context](FrameContext& frame)
            {
                arena::protocol::Envelope envelope;
                if (!protocol::EnvelopeCodec::decode(
                        std::as_bytes(std::span(frame.payload)),
                        envelope
                    ))
                {
                    frame.rejectionReason = RejectionReason::
                        InvalidEnvelope;
                    return FrameResult::Reject;
                }
                const auto rejectAuthentication = [&]
                {
                    frame.rejectionReason = RejectionReason::
                        Unauthenticated;
                    return FrameResult::Reject;
                };
                const auto state = context->session->loginState();
                if (state == ConnectionSession::LoginState::Expired ||
                    (state ==
                         ConnectionSession::LoginState::Pending &&
                     !envelope.has_ping() &&
                     !envelope.has_login_request()))
                    return rejectAuthentication();
                if (envelope.has_ping())
                {
                    arena::protocol::Envelope reply;
                    reply.set_request_id(envelope.request_id());
                    reply.mutable_pong()->set_nonce(
                        envelope.ping().nonce()
                    );
                    return sendSecurityTestEnvelope(frame, reply);
                }
                if (envelope.has_login_request())
                {
                    const auto& login = envelope.login_request();
                    if (login.account() != "test-user" ||
                        login.credential() != "test-only")
                        return rejectAuthentication();
                    if (!context->session->markAuthenticated())
                        return FrameResult::Reject;
                    arena::protocol::Envelope reply;
                    reply.mutable_login_response()->set_accepted(
                        true
                    );
                    return sendSecurityTestEnvelope(frame, reply);
                }
                if (envelope.has_error())
                {
                    securityTestRoutes.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                    return sendSecurityTestEnvelope(frame, envelope);
                }
                return FrameResult::Reject;
            },
            kMaxPacketSize,
            context->rejections,
            std::move(policies)
        );
        result.onClosed = [context]
        { context->session->disconnect(); };
        return result;
    };
}

inline void printSecurityTestCounters()
{
    const auto counts = defaultRejectionCounters()->snapshot();
    std::cout << "REJECTION_COUNTS";
    for (std::size_t i = 0; i < counts.size(); ++i)
        std::cout << ' '
                  << RejectionCounters::name(
                         static_cast<RejectionReason>(i)
                     )
                  << '=' << counts[i];
    std::cout << " routed=" << securityTestRoutes.load() << std::endl;
}
} // namespace service::gateway
