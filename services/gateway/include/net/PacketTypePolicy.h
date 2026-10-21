/** @file
 * @brief Restricts outer packet types before payload decoding.
 */
#pragma once

#include "net/PacketPolicy.h"

namespace service::gateway
{
/** @brief Denies frame types absent from a configured immutable
 * allowlist. */
class PacketTypePolicy final : public PacketPolicy
{
public:
    explicit PacketTypePolicy(std::vector<std::uint16_t> allowed);
    PacketDecision
    inspect(const ConnectionContext&, PacketView) override;

private:
    /// Sorted unique accepted outer frame types; empty denies all
    /// frames.
    std::vector<std::uint16_t> m_allowed;
};
} // namespace service::gateway
