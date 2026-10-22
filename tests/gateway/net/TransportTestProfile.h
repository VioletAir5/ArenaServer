#pragma once

#include <stdexcept>
#include <string_view>

#include "AuthTestProfile.h"
#include "ChaosTestProfile.h"
#include "ConnectionContextTestHandler.h"
#include "MessageDispatchTestProfile.h"
#include "PacketPolicyTestProfile.h"
#include "SecurityPolicyTestProfile.h"

namespace service::gateway
{
// Every real transport runs the same protocol profiles.
inline ConnectionHandlerFactory transportTestProfile(
    std::string_view mode,
    std::shared_ptr<OnlineRegistry>* onlineOut = nullptr,
    std::shared_ptr<MatchmakingService>* matcherOut = nullptr
)
{
    if (onlineOut)
        onlineOut->reset();
    if (matcherOut)
        matcherOut->reset();
    if (mode == "context")
        return makeContextTestFactory();
    if (mode == "auth" || mode == "presence" || mode == "match")
    {
        OnlineRegistryConfig config;
        if (mode != "auth")
            config.grace = std::chrono::milliseconds(1000);
        auto registry = std::make_shared<OnlineRegistry>(
            std::move(config)
        );
        if (onlineOut)
            *onlineOut = registry;
        std::shared_ptr<MatchmakingService> matcher;
        if (mode == "match")
        {
            service::match::QueueConfig queue;
            queue.initialWindow = 50;
            queue.expansionStep = 100;
            queue.expansionInterval = std::chrono::milliseconds(100);
            queue.maximumWindow = 300;
            queue.timeout = std::chrono::milliseconds(1000);
            queue.retention = std::chrono::milliseconds(1000);
            matcher = std::make_shared<MatchmakingService>(
                registry,
                [](arena::domain::PlayerId player)
                    -> std::optional<std::uint32_t>
                {
                    if (player.value() == 42)
                        return 1000;
                    if (player.value() == 43)
                        return 1200;
                    if (player.value() == 44)
                        return 3000;
                    return {};
                },
                queue,
                std::chrono::milliseconds(20)
            );
            if (matcherOut)
                *matcherOut = matcher;
        }
        return makeAuthTestFactory(
            std::move(registry),
            mode != "auth",
            std::move(matcher)
        );
    }
    if (mode == "rpc")
        return makeRpcTestFactory();
    if (mode == "security")
        return makeSecurityTestFactory();
    if (mode == "features")
        return makePacketEchoTestFactory(
            makePacketPolicyTestProfile()
        );
    if (mode == "chaos_packet")
        return makePacketEchoTestFactory(makeChaosTestPolicies());
    if (mode == "packet")
        return makePacketEchoTestFactory();
    if (mode == "chaos" || mode == "handshake")
        return [policies = mode == "chaos" ? makeChaosTestPolicies()
                                           : PacketPolicyFactory{}](
                   ConnectionContext::Ptr context
               )
        {
            return makePreLoginHandshakeHandler(
                std::move(context),
                {},
                policies
            );
        };
    throw std::invalid_argument("unknown transport test profile");
}
} // namespace service::gateway
