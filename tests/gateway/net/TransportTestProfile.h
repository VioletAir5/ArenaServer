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
    std::shared_ptr<OnlineRegistry>* onlineOut = nullptr
)
{
    if (onlineOut)
        onlineOut->reset();
    if (mode == "context")
        return makeContextTestFactory();
    if (mode == "auth" || mode == "presence")
    {
        OnlineRegistryConfig config;
        if (mode == "presence")
            config.grace = std::chrono::milliseconds(1000);
        auto registry = std::make_shared<OnlineRegistry>(
            std::move(config)
        );
        if (onlineOut)
            *onlineOut = registry;
        return makeAuthTestFactory(
            std::move(registry),
            mode == "presence"
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
