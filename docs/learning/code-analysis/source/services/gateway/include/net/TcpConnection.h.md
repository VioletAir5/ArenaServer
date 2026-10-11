# services/gateway/include/net/TcpConnection.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；179行。

[对应分析](../../../../../04-transports.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Owns a nonblocking TCP socket and its buffered I/O.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

#include "net/Channel.h"
#include "net/EventLoop.h"
#include "net/IConnection.h"
#include "net/PacketBuffer.h"
#include "net/Socket.h"

namespace service::gateway
{
/** @brief Tracks TCP half-close and shutdown progress. */
enum struct TcpConnectionState
{
    Connected,
    PeerHalfClosed,
    WriteDraining,
    WriteDrainingPeerClosed,
    WriteHalfClosed,
    Disconnected
};

/** @brief Owns a nonblocking TCP socket and its buffered I/O. */
class TcpConnection final
    : public IConnection,
      public std::enable_shared_from_this<TcpConnection>
{
public:
    using CloseCallback = std::function<void(TcpConnection*)>;
    using MessageCallback = std::function<
        void(TcpConnection&, std::string_view)>;
    using OutputCallback = std::function<void(std::size_t)>;

    static constexpr std::size_t kHardOutputLimit = kMaxBufferSize;

    // Construct/register on the owning loop. Transfer the Socket to
    // that thread before construction; connection state and teardown
    // stay there.
    TcpConnection(EventLoop*, Socket);
    // Final release belongs to owner, after removing its Channel.
    ~TcpConnection() noexcept override;

    /** @brief Copy construction is disabled. */
    TcpConnection(const TcpConnection&) = delete;
    /** @brief Move construction is disabled. */
    TcpConnection(TcpConnection&&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;
    TcpConnection& operator=(TcpConnection&&) = delete;

    // send() copies bytes from any thread. Foreign callers must hold
    // shared ownership while calling; final release and other
    // operations stay on the owning EventLoop thread. id() is
    // immutable and cross-thread safe. Process-wide identity,
    // distinct from server-local admission tickets.
    std::uint64_t id() const noexcept override;
    /** @brief Queues bytes for transmission. */
    void send(std::span<const std::byte> data) override;
    void shutdown() override;
    void close() override;
    /** @brief Reports whether the connection is still active. */
    bool connected() const noexcept override;
    /** @brief Pauses inbound reads. */
    void pauseRead() override;
    /** @brief Resumes inbound reads. */
    void resumeRead() override;
    /** @brief Returns the number of bytes awaiting transmission. */
    std::size_t pendingBytes() const noexcept override;

    // Compatibility overload for existing byte-pointer callers.
    void send(const void* data, std::size_t length);

    // Event dispatch on the owner thread.
    void handleRead();
    /** @brief Flushes queued socket output. */
    void handleWrite();
    /** @brief Handles peer closure and connection teardown. */
    void handleClose();
    /** @brief Handles a socket or channel error. */
    void handleError();

    /** @brief Sets close callback. */
    void setCloseCallback(CloseCallback);
    // The message view is valid only for the duration of the
    // callback.
    void setMessageCallback(MessageCallback);
    // Owner reports pending output after queue/write changes.
    void setOutputCallback(OutputCallback);

    // Owner EventLoop thread only. Flush queued bytes, then send FIN;
    // keep reading until the peer also closes its write half.
    void shutdownWrite();
    /** @brief Disables socket read interest on the owner thread. */
    void pauseReading();
    /** @brief Enables socket read interest on the owner thread. */
    void resumeReading();

    /** @brief Returns the socket channel owned by this connection. */
    Channel& channel() noexcept;
    /** @brief Returns the underlying file descriptor. */
    int fd() const noexcept;
    // Read only on the owning EventLoop thread.
    std::size_t pendingOutputBytes() const noexcept;
    std::chrono::steady_clock::time_point
    lastActivity() const noexcept;
    /** @brief Reports whether connection teardown has begun. */
    bool closing() const noexcept;

private:
    /** @brief Transfers cross-thread sends to the owner buffer. */
    void drainSendQueue();
    /** @brief Writes or buffers bytes on the owning loop. */
    void sendInLoop(std::span<const std::byte> data);
    /** @brief Notifies output changed. */
    void notifyOutputChanged();
    void finishShutdownWrite();
    /** @brief Handles a specific socket I/O error. */
    void handleIoError(int error);

    /// Event loop that owns this object and its callbacks.
    EventLoop* m_loop;
    /// Process-wide identifier for this object.
    const std::uint64_t m_id;

    /// Stores the close callback.
    CloseCallback m_closeCallback;
    /// Stores the message callback.
    MessageCallback m_messageCallback;
    /// Stores the output callback.
    OutputCallback m_outputCallback;

    /// Owned TCP socket.
    Socket m_socket;
    /// Owner-thread registration and callbacks for the socket.
    Channel m_channel;

    /// Bytes received but not yet consumed.
    PacketBuffer m_inputBuffer;
    /// Bytes awaiting a socket write.
    PacketBuffer m_outputBuffer;

    // Foreign callers only touch this ingress queue. The owner moves
    // bytes into m_outputBuffer and is the sole socket and Channel
    // mutator.
    mutable std::mutex m_sendMutex;
    /// Bytes waiting to be transferred to the owner thread.
    std::deque<std::vector<std::byte>> m_sendQueue;
    /// Number of bytes accepted but not yet fully written.
    std::size_t m_pendingBytes{0};
    /// Whether new cross-thread sends are accepted.
    bool m_acceptingSends{true};
    /// Whether a send-queue drain is scheduled.
    bool m_sendDrainPosted{false};
    /// Whether the owner is draining queued sends.
    bool m_drainingSends{false};

    /// Time of the most recent connection activity.
    std::chrono::steady_clock::time_point m_lastActivity{
        std::chrono::steady_clock::now()};

    /// Current lifecycle state.
    TcpConnectionState m_state{TcpConnectionState::Connected};
    /// Whether socket read interest is paused.
    bool m_readPaused{false};
    /// Whether a read callback is in progress.
    bool m_reading{false};
};
} // namespace service::gateway
~~~
