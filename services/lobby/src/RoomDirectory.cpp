/** @file
 * @brief Implements immutable room-directory observations and image
 * identity.
 */
#include "lobby/RoomDirectory.h"

#include <stdexcept>

#include <openssl/sha.h>

namespace service::lobby
{
namespace
{
void appendNumber(std::string& out, std::uint64_t value)
{
    for (int shift = 56; shift >= 0; shift -= 8)
        out.push_back(static_cast<char>((value >> shift) & 255));
}
} // namespace
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
std::string
roomImageFingerprint(RoomIdentity id, const RoomWorkerImage& image)
{
    // Fixed-width network-order metadata precedes the opaque payload.
    std::string canonical = "arena-room-image/v1";
    appendNumber(canonical, id.room);
    appendNumber(canonical, id.incarnation);
    appendNumber(canonical, image.tick);
    appendNumber(canonical, image.checksum);
    appendNumber(canonical, image.cursor);
    canonical += image.checkpoint;
    std::string digest(SHA256_DIGEST_LENGTH, '\0');
    if (!SHA256(
            reinterpret_cast<const unsigned char*>(canonical.data()),
            canonical.size(),
            reinterpret_cast<unsigned char*>(digest.data())
        ))
        throw std::runtime_error(
            "room image fingerprint unavailable"
        );
    return digest;
}
} // namespace service::lobby