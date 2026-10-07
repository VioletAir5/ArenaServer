#pragma once

#include <cstddef>
#include <functional>
#include <optional>
#include <string_view>
#include <vector>

#include "envelope.pb.h"
#include "net/ConnectionHandler.h"
#include "protocol/PacketHeader.hpp"

namespace service::gateway
{
// A context exists for one complete frame. payload refers to PacketCodec's
// input and must not be retained after the callback returns.
struct PacketContext
{
    IConnection& connection;
    const protocol::PacketHeader& header;
    std::string_view payload;
    std::optional<arena::protocol::Envelope> envelope{};
};

enum class PacketStageResult
{
    Continue, // Run the next stage.
    Handled,  // Stop this chain; accept the current frame.
    Reject    // Stop this chain and close the connection.
};

// Configured once per connection, run serially by that connection's owner.
// Framing stays in PacketCodec; stages only see complete frames.
class PacketPipeline
{
public:
    using Stage = std::function<PacketStageResult(PacketContext&)>;

    void addStage(Stage stage);
    PacketStageResult process(PacketContext& context) const;

    // Opt-in stage for protobuf payloads. Later stages can read
    // context.envelope; malformed or over-limit payloads are rejected.
    static PacketStageResult decodeEnvelope(PacketContext& context);

private:
    std::vector<Stage> m_stages;
};

// Creates a per-connection decoder and attaches the ordered pipeline to
// complete-frame callbacks. Reject closes the connection.
ConnectionMessageCallback makePacketHandler(
    PacketPipeline pipeline, std::size_t maxBodySize
);
} // namespace service::gateway