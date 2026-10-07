#pragma once

#include <cstddef>
#include <optional>
#include <span>
#include <string>

#include "envelope.pb.h"

namespace service::protocol
{
// Bounds apply to the serialized protobuf payload inside PacketCodec's
// existing frame. A failed decode leaves the output message unchanged.
class EnvelopeCodec
{
public:
    static constexpr std::size_t kMaxBytes = 256 * 1024;
    static constexpr int kMaxDepth = 16;

    static bool decode(
        std::span<const std::byte> bytes,
        arena::protocol::Envelope& output
    );
    static std::optional<std::string> encode(
        const arena::protocol::Envelope& envelope
    );
};
} // namespace service::protocol