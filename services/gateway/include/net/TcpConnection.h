#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <string_view>

#include "net/Channel.h"
#include "net/EventLoop.h"
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
class TcpConnection
{
public:
    using CloseCallback = std::function<void(TcpConnection*)>;
    using MessageCallback = std::function<
        void(TcpConnection&, std::string_view)>;
    using WatermarkCallback = std::function<
        void(TcpConnection&, std::size_t)>;

    static constexpr std::size_t kLowWaterMark = 256 * 1024;
    static constexpr std::size_t kHighWaterMark = 512 * 1024;
    static constexpr std::size_t kHardOutputLimit = kMaxBufferSize;
    static constexpr std::chrono::seconds kSlowClientTimeout{5};

    // Construct/register on the owning loop. Transfer the Socket to
    // that thread before construction; connection state and teardown
    // stay there.
    TcpConnection(EventLoop*, Socket);
    // Final release belongs to owner, after removing its Channel.
    ~TcpConnection() noexcept;

    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

    void setCloseCallback(CloseCallback);
    // The message view is valid only for the duration of the
    // callback.
    void setMessageCallback(MessageCallback);
    // Called synchronously on threshold crossing in the EventLoop
    // thread. Register before sending; callbacks must not destroy
    // this connection.
    void setHighWaterMarkCallback(WatermarkCallback);
    void setLowWaterMarkCallback(WatermarkCallback);

    void send(const void* data, size_t len);
    // Owner EventLoop thread only. Flush queued bytes, then send FIN;
    // keep reading until the peer also closes its write half.
    void shutdownWrite();
    void pauseReading();
    void resumeReading();
    void closeIfSlow(std::chrono::steady_clock::time_point now);

    Channel& channel() noexcept;
    int fd() const noexcept;
    // Read only on the owning EventLoop thread.
    std::size_t pendingOutputBytes() const noexcept;
    std::chrono::steady_clock::time_point
    lastActivity() const noexcept;
    bool closing() const noexcept;

private:
    void notifyHighWaterIfNeeded();
    void notifyLowWaterIfNeeded();
    void finishShutdownWrite();
    void handleIoError(int error);

    EventLoop* m_loop;

    CloseCallback m_closeCallback;
    MessageCallback m_messageCallback;
    WatermarkCallback m_highWaterMarkCallback;
    WatermarkCallback m_lowWaterMarkCallback;

    Socket m_socket;
    Channel m_channel;

    PacketBuffer m_inputBuffer;
    PacketBuffer m_outputBuffer;

    std::chrono::steady_clock::time_point m_highWaterSince{};
    std::chrono::steady_clock::time_point m_lastActivity{
        std::chrono::steady_clock::now()};

    TcpConnectionState m_state{TcpConnectionState::Connected};
    bool m_aboveHighWater{false};
    bool m_readPaused{false};
};
} // namespace service::gateway
