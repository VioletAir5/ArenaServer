/** @file
 * @brief Owns Arena framing, packet policies and protobuf
 * request/reply conversion.
 */
#pragma once

#include "net/FrameProtocol.h"
#include "net/RequestDispatcher.h"

namespace service::gateway
{
/** @brief Translates bytes and owning requests for a single
 * connection. */
class ArenaProtocol final
{
public:
    ArenaProtocol(
        ConnectionContext::Ptr,
        std::shared_ptr<const MessageRegistry>,
        PacketPolicyFactory = {},
        std::shared_ptr<spdlog::logger> logger = {}
    );
    /** @brief Installs the durable owner endpoint used by session
     * replacement. */
    void start(ConnectionChannel&);
    /** @brief Decodes borrowed input and dispatches each admitted
     * request. */
    void receive(ConnectionChannel&, std::span<const std::byte>);
    /** @brief Stops request admission and waits for accepted work to
     * finish. */
    void drain(std::function<void()> drained);
    /** @brief Cancels requests and detaches the connection's session
     * binding. */
    void stop() noexcept;

private:
    FrameResult onFrame(FrameContext&);
    /// Connection metadata and owner-thread authentication state.
    ConnectionContext::Ptr m_context;
    /// Bounded requests and their owner-thread deadlines.
    RequestDispatcher m_dispatcher;
    /// Decoder and complete-frame policies private to this protocol.
    ConnectionMessageCallback m_frames;
};

/** @brief Builds protocol lifecycle hooks and fresh policies for one
 * connection. */
ConnectionHandler makeArenaProtocolHandler(
    ConnectionContext::Ptr,
    std::shared_ptr<const MessageRegistry>,
    PacketPolicyFactory = {},
    std::shared_ptr<spdlog::logger> logger = {}
);
} // namespace service::gateway
