#pragma once

#include <atomic>
#include <iostream>
#include <memory>

#include "net/BackpressureFeature.h"
#include "net/PacketBuffer.h"
#include "net/PacketPipeline.h"
#include "net/PacketWhitelistFeature.h"
#include "net/PreLoginHandshake.h"
#include "net/TokenBucketFeature.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
// Test-only authentication and router; never linked into the
// application.
inline std::atomic<unsigned> securityTestRoutes{0};
inline PacketHandleResult sendSecurityTestEnvelope(
    PacketContext& context, const arena::protocol::Envelope& envelope
)
{
    const auto payload = protocol::EnvelopeCodec::encode(envelope);
    if (!payload)
        return PacketHandleResult::Reject;
    const auto frame = protocol::PacketCodec::encode(
        protocol::PacketHeader::kEchoResponse,
        context.header.m_requestId,
        std::as_bytes(std::span(*payload))
    );
    if (!frame)
        return PacketHandleResult::Reject;
    context.connection.send(
        std::as_bytes(
            std::span(frame->peek(), frame->readableBytes())
        )
    );
    return context.connection.connected()
               ? PacketHandleResult::Handled
               : PacketHandleResult::Reject;
}
class SecurityTestLoginHandler final : public PacketHandler
{
public:
    explicit SecurityTestLoginHandler(
        std::shared_ptr<ConnectionSession> session
    )
        : m_session(std::move(session))
    {
    }
    PacketHandleResult handle(PacketContext& context) override
    {
        if (!context.envelope->has_login_request())
            return PacketHandler::handle(context);
        const auto& login = context.envelope->login_request();
        if (login.account() != "test-user" ||
            login.credential() != "test-only")
        {
            context
                .rejectionReason = RejectionReason::Unauthenticated;
            return PacketHandleResult::Reject;
        }
        if (!m_session->markAuthenticated())
            return PacketHandleResult::Reject;
        arena::protocol::Envelope response;
        response.mutable_login_response()->set_accepted(true);
        return sendSecurityTestEnvelope(context, response);
    }

private:
    std::shared_ptr<ConnectionSession> m_session;
};
class SecurityTestBusinessHandler final : public PacketHandler
{
public:
    PacketHandleResult handle(PacketContext& context) override
    {
        if (!context.envelope->has_error())
            return PacketHandler::handle(context);
        securityTestRoutes.fetch_add(1, std::memory_order_relaxed);
        return sendSecurityTestEnvelope(context, *context.envelope);
    }
};
inline ConnectionHandlerFactory makeSecurityTestFactory()
{
    return [](ConnectionContext::Ptr context)
    {
        auto decoder = std::make_unique<EnvelopeDecodeHandler>();
        auto gate = std::make_unique<AuthenticationGateHandler>(
            context->session
        );
        auto ping = std::make_unique<PingHandler>();
        auto login = std::make_unique<SecurityTestLoginHandler>(
            context->session
        );
        login->setNext(
            std::make_unique<SecurityTestBusinessHandler>()
        );
        ping->setNext(std::move(login));
        gate->setNext(std::move(ping));
        decoder->setNext(std::move(gate));
        return makePacketHandler(
            PacketPipeline(std::move(decoder)),
            kMaxPacketSize,
            context->rejections
        );
    };
}
inline ConnectionFeatureFactory makeSecurityTestFeatures()
{
    return [](ConnectionPipeline& pipeline, const ConnectionContext&)
    {
        pipeline.add<BackpressureFeature>();
        pipeline.add<TokenBucketFeature>(2.0, 0.1);
        pipeline.add<PacketWhitelistFeature>(
            std::vector<std::uint16_t>{1}
        );
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