#pragma once
#include <iostream>

#include "net/PacketRatePolicy.h"
#include "net/PacketTypePolicy.h"
#include "net/PreLoginHandshake.h"
namespace service::gateway
{
// Production handshake packet policies, also used for test-only echo.
inline PacketPolicyFactory makeChaosTestPolicies()
{
    return [](PacketPolicyChain& p, const ConnectionContext&)
    {
        p.add<PacketRatePolicy>(200.0, 100.0);
        p.add<PacketTypePolicy>(std::vector<std::uint16_t>{1});
    };
}
inline void printChaosTestCounters()
{
    const auto counts = defaultRejectionCounters()->snapshot();
    for (std::size_t i = 0; i < counts.size(); ++i)
        std::cout << "CHAOS_REJECTIONS reason="
                  << RejectionCounters::name(
                         static_cast<RejectionReason>(i)
                     )
                  << " count=" << counts[i] << '\n';
}
} // namespace service::gateway
