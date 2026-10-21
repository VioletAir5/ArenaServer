/** @file
 * @brief Defines the socket transport operations used by connection
 * hosts.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace service::gateway
{
// Operations and final release belong to the owning execution
// context, except immutable id() and documented concrete-method
// exceptions. AsioConnect::send and TcpConnection::send accept
// concurrent callers. Ptr shares lifetime, not general cross-thread
// access.
/** @brief Defines the socket transport operations used by connection
 * hosts. */
class ITransport
{
public:
    using Ptr = std::shared_ptr<ITransport>;

    /** @brief Releases the transport after owner-thread I/O teardown.
     */
    virtual ~ITransport() noexcept = default;

    // Stable, nonzero identity for this connection's lifetime.
    virtual std::uint64_t id() const noexcept = 0;

    // Sends immediately or copies queued bytes before returning. The
    // caller may release the input afterwards. Ignored after shutdown
    // starts or close; an output-limit violation closes the
    // connection.
    virtual void send(std::span<const std::byte> data) = 0;

    // Stop accepting sends and drain queued output. TCP then shuts
    // down its write half and reads until peer EOF; WebSocket starts
    // a normal Close handshake. Server deadlines bound retirement.
    virtual void shutdown() = 0;
    // Logical close and notification, without flushing pending bytes.
    // Owner unregisters I/O before final release. Callback code must
    // not synchronously destroy this connection. Idempotent.
    virtual void close() = 0;

    // True until logical close, including draining and half-closed
    // states. Does not imply that new sends are accepted.
    virtual bool connected() const noexcept = 0;

    /** @brief Pauses inbound reads. */
    virtual void pauseRead() = 0;
    // Resume delivery of already available bytes as well as future
    // data.
    virtual void resumeRead() = 0;

    // Output bytes whose successful write has not yet been observed.
    // Async implementations include in-flight bytes until completion;
    // excludes bytes already acknowledged by a successful write
    // result.
    virtual std::size_t pendingBytes() const noexcept = 0;
};
} // namespace service::gateway
