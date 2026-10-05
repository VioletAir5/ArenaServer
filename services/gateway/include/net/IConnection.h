#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace service::gateway
{
// Operations other than id(), and final release, belong to the owning
// execution context. Implementations do not promise cross-thread access
// or automatically marshal calls. Ptr only shares lifetime, not access.
class IConnection
{
public:
    using Ptr = std::shared_ptr<IConnection>;

    virtual ~IConnection() noexcept = default;

    // Stable, nonzero identity for this connection's lifetime.
    virtual std::uint64_t id() const noexcept = 0;

    // Sends immediately or copies queued bytes before returning. The
    // caller may release the input afterwards. Ignored after shutdown
    // starts or close; an output-limit violation closes the connection.
    virtual void send(std::span<const std::byte> data) = 0;

    // Stop accepting sends, drain queued bytes, then shut down the write
    // half. Continue receiving until peer EOF or an error/timeout.
    virtual void shutdown() = 0;
    // Logical close and notification, without flushing pending bytes.
    // Owner unregisters I/O before final release. Callback code must not
    // synchronously destroy this connection. Idempotent.
    virtual void close() = 0;

    // True until logical close, including draining and half-closed
    // states. Does not imply that new sends are accepted.
    virtual bool connected() const noexcept = 0;

    virtual void pauseRead() = 0;
    // Resume delivery of already available bytes as well as future data.
    virtual void resumeRead() = 0;

    // User-space output bytes awaiting a successful write. Excludes
    // bytes already handed to the kernel.
    virtual std::size_t pendingBytes() const noexcept = 0;
};
} // namespace service::gateway
