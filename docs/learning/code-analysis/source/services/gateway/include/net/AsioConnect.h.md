# services/gateway/include/net/AsioConnect.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；184行。

[对应分析](../../../../../04-transports.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Owns one Boost.Asio TCP connection and its asynchronous I/O.
 */
#pragma once

#include <array>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include "net/IConnection.h"

namespace service::gateway
{
// Run the context on exactly one owner thread. Construct, start,
// mutate (except send), and finally release on that thread. send()
// copies and queues bytes from any thread; foreign callers need
// shared ownership. id() is also cross-thread safe. start() follows
// callback setup and requires shared ownership.
/** @brief Owns one Boost.Asio TCP connection and its asynchronous I/O. */
class AsioConnect final
    : public IConnection,
      public std::enable_shared_from_this<AsioConnect>
{
public:
    using MessageCallback = std::function<
        void(IConnection&, std::span<const std::byte>)>;
    using OutputCallback = std::function<void(std::size_t)>;
    using CloseCallback = std::function<void(AsioConnect*)>;

    static constexpr std::size_t kHardOutputLimit = 1024 * 1024;

    AsioConnect(
        boost::asio::io_context&,
        boost::asio::ip::tcp::socket,
        std::chrono::milliseconds idleTimeout =
            std::chrono::seconds(60)
    );
    /** @brief Releases owned resources. */
    ~AsioConnect() noexcept override;
    /** @brief Copy construction is disabled. */
    AsioConnect(const AsioConnect&) = delete;
    AsioConnect& operator=(const AsioConnect&) = delete;

    /** @brief Returns the process-wide connection identifier. */
    std::uint64_t id() const noexcept override;
    /** @brief Queues bytes for transmission. */
    void send(std::span<const std::byte>) override;
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

    /** @brief Sets message callback. */
    void setMessageCallback(MessageCallback);
    /** @brief Sets output callback. */
    void setOutputCallback(OutputCallback);
    /** @brief Sets close callback. */
    void setCloseCallback(CloseCallback);
    void start();
    /** @brief Returns the underlying file descriptor. */
    int fd() const noexcept;
    auto executor() const
    {
        return m_strand;
    }

private:
    using Clock = std::chrono::steady_clock;
    using ReadBuffer = std::array<std::byte, 8192>;
    using WriteBuffer = std::vector<std::byte>;

    // TCP half-close lifecycle; start() is an independent activation
    // step.
    /** @brief Tracks the TCP half-close lifecycle. */
    enum class State
    {
        Connected,
        PeerHalfClosed,
        WriteDraining,
        WriteDrainingPeerClosed,
        WriteHalfClosed,
        Closed
    };

    // A completed read may wait here while delivery is paused.
    /** @brief Tracks an asynchronous read through delivery. */
    enum class ReadState
    {
        Idle,
        InFlight,
        Ready,
        Delivering
    };

    void requireOwner() const;
    /** @brief Transfers cross-thread sends to the owner buffer. */
    void drainSendQueue();
    /** @brief Closes the connection exactly once. */
    void closeOnce();
    /** @brief Reads next. */
    void readNext();
    void deliverRead();
    /** @brief Writes next. */
    void writeNext();
    void finishWrite();
    /** @brief Notifies output changed. */
    void notifyOutputChanged();
    void armIdleTimer();
    void ioError(const boost::system::error_code&);

    /// Thread that owns mutable connection state.
    const std::thread::id m_owner;
    /// Process-wide identifier for this object.
    const std::uint64_t m_id;
    /// Serializes callbacks for this connection.
    boost::asio::strand<boost::asio::io_context::executor_type>
        m_strand;
    /// Owned TCP socket.
    boost::asio::ip::tcp::socket m_socket;
    /// Stores the idle timer.
    boost::asio::steady_timer m_idleTimer;
    /// Stores the idle timeout.
    const std::chrono::milliseconds m_idleTimeout;
    /// Time of the most recent connection activity.
    Clock::time_point m_lastActivity{Clock::now()};

    /// Stores the message callback.
    MessageCallback m_messageCallback;
    /// Stores the output callback.
    OutputCallback m_outputCallback;
    /// Stores the close callback.
    CloseCallback m_closeCallback;

    /// Buffer kept alive while an asynchronous read is in flight.
    std::shared_ptr<ReadBuffer> m_input{
        std::make_shared<ReadBuffer>()};
    // Concurrent callers enter here; the owner drains into m_output.
    mutable std::mutex m_sendMutex;
    /// Bytes waiting to be transferred to the owner thread.
    std::deque<std::shared_ptr<WriteBuffer>> m_sendQueue;
    /// Whether new cross-thread sends are accepted.
    bool m_acceptingSends{true};
    /// Whether a send-queue drain is scheduled.
    bool m_sendDrainPosted{false};
    /// Number of bytes accepted but not yet fully written.
    std::size_t m_pendingBytes{0};

    // Only the owner thread mutates the active output queue.
    std::deque<std::shared_ptr<WriteBuffer>> m_output;
    /// Stores the read bytes.
    std::size_t m_readBytes{0};
    /// Stores the read error.
    boost::system::error_code m_readError;
    /// Stores the front offset.
    std::size_t m_frontOffset{0};

    /// Current lifecycle state.
    State m_state{State::Connected};
    /// Stores the started.
    bool m_started{false};
    /// Stores the read state.
    ReadState m_readState{ReadState::Idle};
    /// Whether socket read interest is paused.
    bool m_readPaused{false};
    /// Stores the writing.
    bool m_writing{false};
};
} // namespace service::gateway
~~~
