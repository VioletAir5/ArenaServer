/** @file
 * @brief Implements immutable room-directory observations and image
 * identity.
 */
#include "lobby/RoomDirectory.h"

#include <stdexcept>

#include "room/RoomWire.h"

namespace service::lobby
{
std::uint64_t RoomDirectory::revision() const noexcept
{
    return m_revision;
}
std::size_t RoomDirectory::size() const noexcept
{
    return m_rooms.size();
}
const RoomRouteRecord*
RoomDirectory::find(RoomIdentity id) const noexcept
{
    const auto found = m_rooms.find(id);
    return found == m_rooms.end() ? nullptr : found->second.get();
}
std::vector<RoomIdentity> RoomDirectory::ids() const
{
    std::vector<RoomIdentity> out;
    out.reserve(m_rooms.size());
    for (const auto& [id, room] : m_rooms)
        out.push_back(id);
    return out;
}
std::string
roomImageFingerprint(RoomIdentity id, const RoomWorkerImage& image)
{
    const auto proof = service::room::wire::imageFingerprint(
        {id.room,
         id.incarnation,
         image.tick,
         image.checksum,
         image.cursor,
         image.logicalTick},
        image.checkpoint
    );
    if (!proof)
        throw std::runtime_error(
            "room image fingerprint unavailable"
        );
    return *proof;
}
} // namespace service::lobby