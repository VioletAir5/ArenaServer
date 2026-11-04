/** @file
 * @brief Defines bounded pure Room value codecs and one shared image
 * proof.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>

#include <google/protobuf/message.h>

#include "room.pb.h"
#include "room/RoomRuntime.h"

namespace service::room::wire
{
/// Maximum canonical RuntimeImage bytes; no rolling replay history is
/// encoded.
inline constexpr std::size_t kMaxImageBytes = 128 * 1024;
/// Reserve outer RPC metadata within its unchanged 256 KiB frame
/// limit.
inline constexpr std::size_t kMaxPayloadBytes = 256 * 1024 - 512;
/// One finite command batch followed by exactly one final cutoff.
inline constexpr std::size_t kMaxOperations = 257;
/// Maximum complete roster represented by a transferable image.
inline constexpr std::size_t kMaxMembers = 64;

/** @brief Reports structural encoding failures without evaluating a
 * kernel. */
enum class CodecError
{
    Invalid,
    Capacity,
    Version,
    NonCanonical,
    Unavailable
};

/** @brief Binds opaque bytes to the complete Directory image
 * identity. */
struct ImageIdentity
{
    /// Stable room identity.
    std::uint64_t roomId{0};
    /// Immutable actor incarnation.
    std::uint64_t incarnation{0};
    /// Physical actor tick, which can stop during terminal retention.
    std::uint64_t physicalTick{0};
    /// Full unsigned actor checksum.
    std::uint64_t checksum{0};
    /// Last ordered Directory operation included in the image.
    std::uint64_t cursor{0};
    /// Runtime logical cutoff, including frozen/retained cutoffs.
    std::uint64_t logicalTick{0};
    bool operator==(const ImageIdentity&) const = default;
};

/** @brief Hashes exact ASCII arena-room-image/v2 (19 bytes, no NUL),
 * six BE64 fields in ImageIdentity order, then the opaque bytes.
 * @return Exactly 64 lowercase hexadecimal SHA-256 characters.
 * @note This function never validates RoomRuntime or constructs
 * RoomActor.
 */
std::expected<std::string, CodecError>
imageFingerprint(const ImageIdentity&, std::string_view opaque);

/** @brief Encodes complete owned values without clocks, IDs, I/O or
 * actors. */
std::expected<std::string, CodecError>
encodeImage(const RuntimeImage&);
/** @brief Checks wire structure only; workers separately validate the
 * kernel. */
std::expected<RuntimeImage, CodecError> decodeImage(std::string_view);
/** @brief Encodes one ordered command, retaining its authorization
 * fence. */
std::expected<std::string, CodecError>
encodeCommand(const RuntimeCommand&);
/** @brief Decodes one bounded typed command without applying it. */
std::expected<RuntimeCommand, CodecError>
    decodeCommand(std::string_view);
/** @brief Encodes a final tick context; commands must be empty. */
std::expected<std::string, CodecError>
encodeCutoff(const RuntimeCutoff&);
/** @brief Returns the final tick context with an empty command
 * vector. */
std::expected<RuntimeCutoff, CodecError>
    decodeCutoff(std::string_view);
/** @brief Encodes canonical ordered attempts and their recorded
 * checksum. */
std::expected<std::string, CodecError> encodeTick(const TickRecord&);
/** @brief Decodes a tick record without evaluating simulation state.
 */
std::expected<TickRecord, CodecError> decodeTick(std::string_view);
/** @brief Encodes only an optional bounded legacy history proof. */
std::expected<std::string, CodecError>
encodeReplayLog(const ReplayLog&);
/** @brief Decodes an optional bounded history without replaying it.
 */
std::expected<ReplayLog, CodecError>
    decodeReplayLog(std::string_view);

/** @brief Maps creation values without constructing a room kernel. */
std::expected<arena::room::Policy, CodecError>
packPolicy(const RuntimePolicy&);
/** @brief Reads finite creation policy scalars without kernel
 * validation. */
std::expected<RuntimePolicy, CodecError>
unpackPolicy(const arena::room::Policy&);
/** @brief Encodes the fixed two-member matching allocation. */
std::expected<arena::room::Allocation, CodecError>
packAllocation(const RuntimeAllocation&);
/** @brief Requires both fixed allocation members and their strong
 * IDs. */
std::expected<RuntimeAllocation, CodecError>
unpackAllocation(const arena::room::Allocation&);

/** @brief Adds canonical metadata and the shared v2 proof to an
 * image. */
std::expected<arena::room::Image, CodecError>
packImage(const RuntimeImage&, std::uint64_t cursor);
/** @brief Checks metadata/proof correspondence without kernel
 * evaluation. */
std::expected<RuntimeImage, CodecError>
unpackImage(const arena::room::Image&);

/** @brief Strictly parses one bounded canonical protobuf, preserving
 * output on error and rejecting unknown fields/enums at every nesting
 * level. */
std::expected<void, CodecError> parseMessage(
    std::string_view,
    google::protobuf::Message&,
    std::size_t maximum = kMaxPayloadBytes
);
/** @brief Serializes only known canonical fields within the supplied
 * bound. */
std::expected<std::string, CodecError> serializeMessage(
    const google::protobuf::Message&,
    std::size_t maximum = kMaxPayloadBytes
);
} // namespace service::room::wire