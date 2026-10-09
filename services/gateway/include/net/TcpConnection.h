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
enum struct TcpConnectionState
{
    Connected,
    PeerHalfClosed,
    WriteDraining,
    WriteDrainingPeerClosed,
    WriteHalfClosed,
    Disconnected
};

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

    TcpConnection(const TcpConnection&) = delete;
    TcpConnection(TcpConnection&&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;
    TcpConnection& operator=(TcpConnection&&) = delete;

    // send() copies bytes from any thread. Foreign callers must hold
    // shared ownership while calling; final release and other
    // operations stay on the owning EventLoop thread. id() is
    // immutable and cross-thread safe. Process-wide identity,
    // distinct from server-local admission tickets.
    std::uint64_t id() const noexcept override;
    void send(std::span<const std::byte> data) override;
    void shutdown() override;
    void close() override;
    bool connected() const noexcept override;
    void pauseRead() override;
    void resumeRead() override;
    std::size_t pendingBytes() const noexcept override;

    // Compatibility overload for existing byte-pointer callers.
    void send(const void* data, std::size_t length);

    // Event dispatch on the owner thread.
    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

    void setCloseCallback(CloseCallback);
    // The message view is valid only for the duration of the
    // callback.
    void setMessageCallback(MessageCallback);
    // Owner reports pending output after queue/write changes.
    void setOutputCallback(OutputCallback);

    // Owner EventLoop thread only. Flush queued bytes, then send FIN;
    // keep reading until the peer also closes its write half.
    void shutdownWrite();
    void pauseReading();
    void resumeReading();

    Channel& channel() noexcept;
    int fd() const noexcept;
    // Read only on the owning EventLoop thread.
    std::size_t pendingOutputBytes() const noexcept;
    std::chrono::steady_clock::time_point
    lastActivity() const noexcept;
    bool closing() const noexcept;

private:
    void drainSendQueue();
    void sendInLoop(std::span<const std::byte> data);
    void notifyOutputChanged();
    void finishShutdownWrite();
    void handleIoError(int error);

    EventLoop* m_loop;
    const std::uint64_t m_id;

    CloseCallback m_closeCallback;
    MessageCallback m_messageCallback;
    OutputCallback m_outputCallback;

    Socket m_socket;
    Channel m_channel;

    PacketBuffer m_inputBuffer;
    PacketBuffer m_outputBuffer;

    // Foreign callers only touch this ingress queue. The owner moves
    // bytes into m_outputBuffer and is the sole socket and Channel
    // mutator.
    mutable std::mutex m_sendMutex;
    std::deque<std::vector<std::byte>> m_sendQueue;
    std::size_t m_pendingBytes{0};
    bool m_acceptingSends{true};
    bool m_sendDrainPosted{false};
    bool m_drainingSends{false};

    std::chrono::steady_clock::time_point m_lastActivity{
        std::chrono::steady_clock::now()};

    TcpConnectionState m_state{TcpConnectionState::Connected};
    bool m_readPaused{false};
    bool m_reading{false};
};
} // namespace service::gateway
