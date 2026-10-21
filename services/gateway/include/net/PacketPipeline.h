/** @file
 * @brief Runs packet handlers in order until one stops processing.
 */
#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string_view>

#include "net/ConnectionHandler.h"
#include "protocol/PacketHeader.hpp"

#include "envelope.pb.h"

namespace service::gateway
{
// A context exists for one complete frame. header and payload refer
// to PacketCodec's input and must not be retained after the callback
// returns.
/** @brief Represents packet context. */
struct PacketContext
{
    IConnection& connection;
    const protocol::PacketHeader& header;
    std::string_view payload;
    std::optional<arena::protocol::Envelope> envelope{};
    RejectionReason rejectionReason{RejectionReason::HandlerRejected};
};

/** @brief Controls whether the packet handler chain continues. */
enum class PacketHandleResult
{
    Handled, // Accept this frame and stop the chain.
    Reject   // Stop the chain and close the connection.
};

// Each connection owns a fresh chain. Configure links before
// installing it; handle() and node state belong to that connection's
// execution context.
/** @brief Represents packet handler. */
class PacketHandler
{
public:
    virtual ~PacketHandler() = default;

    /** @brief Sets next. */
    void setNext(std::unique_ptr<PacketHandler> next);

    // A filter that passes, or a router that does not match, calls
    // this base implementation to forward. An unhandled frame is
    // rejected at the tail.
    virtual PacketHandleResult handle(PacketContext& context);

private:
    /// Stores the next.
    std::unique_ptr<PacketHandler> m_next;
};

// Opt-in protobuf decoding. Downstream handlers can read
// context.envelope.
/** @brief Represents envelope decode handler. */
class EnvelopeDecodeHandler final : public PacketHandler
{
public:
    PacketHandleResult handle(PacketContext& context) override;
};

// Place after EnvelopeDecodeHandler and before login/business
// routing. Pending sessions permit only request-type LoginRequest and
// Ping. Only the actual authentication handler may mark the session
// authenticated.
/** @brief Represents authentication gate handler. */
class AuthenticationGateHandler final : public PacketHandler
{
public:
    explicit AuthenticationGateHandler(
        std::shared_ptr<ConnectionSession>
    );
    PacketHandleResult handle(PacketContext&) override;

private:
    /// Stores the session.
    std::shared_ptr<ConnectionSession> m_session;
};

// Owns the chain head. Framing stays in PacketCodec; nodes see
// complete frames.
/** @brief Runs packet handlers in order until one stops processing.
 */
class PacketPipeline
{
public:
    /** @brief Constructs the packet pipeline. */
    explicit PacketPipeline(std::unique_ptr<PacketHandler> first);
    PacketHandleResult process(PacketContext& context);

private:
    /// Stores the first.
    std::unique_ptr<PacketHandler> m_first;
};

// Creates a per-connection decoder and attaches the chain to complete
// frames. Reject (including an unhandled frame) and callback
// exceptions close it.
ConnectionMessageCallback makePacketHandler(
    PacketPipeline pipeline,
    std::size_t maxBodySize,
    RejectionCounters::Ptr counters = defaultRejectionCounters()
);
} // namespace service::gateway
