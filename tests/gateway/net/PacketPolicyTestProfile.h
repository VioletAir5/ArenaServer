#pragma once

#include <iostream>

#include "net/PacketEcho.h"
#include "net/PacketRatePolicy.h"
#include "net/PacketTypePolicy.h"

namespace service::gateway
{
inline PacketPolicyFactory makePacketPolicyTestProfile()
{
    return [](PacketPolicyChain& policies, const ConnectionContext&)
    {
        policies.add<PacketTypePolicy>(std::vector<std::uint16_t>{1});
        policies.add<PacketRatePolicy>(2.0, 0.1);
    };
}

inline ConnectionHandlerFactory
makePacketEchoTestFactory(PacketPolicyFactory policies = {})
{
    return [policies = std::move(policies)](
               ConnectionContext::Ptr context
           )
    {
        PacketPolicyChain chain;
        if (policies)
            policies(chain, *context);
        return makePacketEchoHandler(
            kMaxPacketSize,
            context->rejections,
            std::move(chain)
        );
    };
}

inline void printPacketPolicyTestCounters()
{
    const auto counters = defaultRejectionCounters();
    std::cout << "POLICY_COUNTS packet_rate_exceeded="
              << counters->count(RejectionReason::PacketRateExceeded)
              << " packet_type_not_allowed="
              << counters->count(
                     RejectionReason::PacketTypeNotAllowed
                 )
              << std::endl;
}
} // namespace service::gateway
