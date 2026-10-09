#pragma once
#include "net/ConnectionFeature.h"
namespace service::gateway
{
// Immutable outer PacketHeader::type allowlist, before payload
// decoding. Empty list denies every packet; admission/peer IP policy
// is separate.
class PacketWhitelistFeature final : public IConnectionFeature
{
public:
    explicit PacketWhitelistFeature(std::vector<std::uint16_t> allowed
    );
    FeatureResult onPacket(IConnectionControl&, PacketView) override;

private:
    std::vector<std::uint16_t> m_allowed;
};
} // namespace service::gateway
