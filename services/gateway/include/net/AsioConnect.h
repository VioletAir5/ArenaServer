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
    ~AsioConnect() noexcept override;
    AsioConnect(const AsioConnect&) = delete;
    AsioConnect& operator=(const AsioConnect&) = delete;

    std::uint64_t id() const noexcept override;
    void send(std::span<const std::byte>) override;
    void shutdown() override;
    void close() override;
    bool connected() const noexcept override;
    void pauseRead() override;
    void resumeRead() override;
    std::size_t pendingBytes() const noexcept override;

    void setMessageCallback(MessageCallback);
    void setOutputCallback(OutputCallback);
    void setCloseCallback(CloseCallback);
    void start();
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
    enum class ReadState
    {
        Idle,
        InFlight,
        Ready,
        Delivering
    };

    void requireOwner() const;
    void drainSendQueue();
    void closeOnce();
    void readNext();
    void deliverRead();
    void writeNext();
    void finishWrite();
    void notifyOutputChanged();
    void armIdleTimer();
    void ioError(const boost::system::error_code&);

    const std::thread::id m_owner;
    const std::uint64_t m_id;
    boost::asio::strand<boost::asio::io_context::executor_type>
        m_strand;
    boost::asio::ip::tcp::socket m_socket;
    boost::asio::steady_timer m_idleTimer;
    const std::chrono::milliseconds m_idleTimeout;
    Clock::time_point m_lastActivity{Clock::now()};

    MessageCallback m_messageCallback;
    OutputCallback m_outputCallback;
    CloseCallback m_closeCallback;

    std::shared_ptr<ReadBuffer> m_input{
        std::make_shared<ReadBuffer>()};
    // Concurrent callers enter here; the owner drains into m_output.
    mutable std::mutex m_sendMutex;
    std::deque<std::shared_ptr<WriteBuffer>> m_sendQueue;
    bool m_acceptingSends{true};
    bool m_sendDrainPosted{false};
    std::size_t m_pendingBytes{0};

    // Only the owner thread mutates the active output queue.
    std::deque<std::shared_ptr<WriteBuffer>> m_output;
    std::size_t m_readBytes{0};
    boost::system::error_code m_readError;
    std::size_t m_frontOffset{0};

    State m_state{State::Connected};
    bool m_started{false};
    ReadState m_readState{ReadState::Idle};
    bool m_readPaused{false};
    bool m_writing{false};
};
} // namespace service::gateway
