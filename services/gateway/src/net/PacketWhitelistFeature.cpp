#include "net/PacketWhitelistFeature.h"

#include <algorithm>
#include <iostream>
namespace service::gateway
{
PacketWhitelistFeature::PacketWhitelistFeature(
    std::vector<std::uint16_t> allowed
)
    : m_allowed(std::move(allowed))
{
    std::sort(m_allowed.begin(), m_allowed.end());
    m_allowed.erase(
        std::unique(m_allowed.begin(), m_allowed.end()),
        m_allowed.end()
    );
}
FeatureResult PacketWhitelistFeature::onPacket(
    IConnectionControl& control, PacketView packet
)
{
    if (std::binary_search(
            m_allowed.begin(),
            m_allowed.end(),
            packet.type
        ))
        return FeatureResult::Continue;
    control.context().rejections->record(RejectionReason::PacketTypeNotAllowed);
    std::cerr << "connection packet type rejected id="
              << control.context().connectionId
              << " type=" << packet.type << '\n';
    return FeatureResult::Close;
}
} // namespace service::gateway
