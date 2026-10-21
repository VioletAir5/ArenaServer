/** @file
 * @brief Rejects packet types outside the configured allowlist.
 */
#pragma once
#include "net/ConnectionFeature.h"
namespace service::gateway
{
// Immutable outer PacketHeader::type allowlist, before payload
// decoding. Empty list denies every packet; admission/peer IP policy
// is separate.
/** @brief Rejects packet types outside the configured allowlist. */
class PacketWhitelistFeature final : public IConnectionFeature
{
public:
    explicit PacketWhitelistFeature(
        std::vector<std::uint16_t> allowed
    );
    FeatureResult onPacket(IConnectionControl&, PacketView) override;

private:
    /// Stores the allowed.
    std::vector<std::uint16_t> m_allowed;
};
} // namespace service::gateway
