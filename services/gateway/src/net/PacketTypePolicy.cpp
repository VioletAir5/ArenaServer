/** @file
 * @brief Implements the immutable outer packet allowlist.
 */
#include "net/PacketTypePolicy.h"

#include <algorithm>

namespace service::gateway
{
PacketTypePolicy::PacketTypePolicy(std::vector<std::uint16_t> allowed)
    : m_allowed(std::move(allowed))
{
    std::sort(m_allowed.begin(), m_allowed.end());
    m_allowed.erase(
        std::unique(m_allowed.begin(), m_allowed.end()),
        m_allowed.end()
    );
}

PacketDecision PacketTypePolicy::inspect(
    const ConnectionContext& context, PacketView packet
)
{
    if (std::binary_search(
            m_allowed.begin(),
            m_allowed.end(),
            packet.type
        ))
        return PacketDecision::Continue;
    context.rejections->record(RejectionReason::PacketTypeNotAllowed);
    return PacketDecision::Close;
}
} // namespace service::gateway
