#pragma once

#include <array>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>

#include "net/IConnection.h"

namespace service::gateway
{
// Run the context on exactly one owner thread. Construct, start,
// mutate, and finally release on that thread; id() is the sole
// cross-thread query. start() requires shared ownership and must
// follow callback registration.
class AsioConnect final
    : public IConnection,
      public std::enable_shared_from_this<AsioConnect>
{
public:
    using MessageCallback = std::function<
        void(IConnection&, std::span<const std::byte>)>;
    using WatermarkCallback = std::function<
        void(IConnection&, std::size_t)>;
    using CloseCallback = std::function<void(AsioConnect*)>;

    static constexpr std::size_t kLowWaterMark = 256 * 1024;
    static constexpr std::size_t kHighWaterMark = 512 * 1024;
    static constexpr std::size_t kHardOutputLimit = 1024 * 1024;
    static constexpr std::chrono::seconds kSlowClientTimeout{5};

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
    void setHighWaterMarkCallback(WatermarkCallback);
    void setLowWaterMarkCallback(WatermarkCallback);
    void setCloseCallback(CloseCallback);
    void start();
    int fd() const noexcept;

private:
    using Clock = std::chrono::steady_clock;
    using ReadBuffer = std::array<std::byte, 8192>;
    using WriteBuffer = std::vector<std::byte>;

    void requireOwner() const;
    void readNext();
    void deliverRead();
    void writeNext();
    void finishWrite();
    void updateWatermark();
    void armIdleTimer();
    void ioError(const boost::system::error_code&);

    const std::thread::id m_owner;
    const std::uint64_t m_id;
    boost::asio::ip::tcp::socket m_socket;
    boost::asio::steady_timer m_idleTimer;
    boost::asio::steady_timer m_slowTimer;
    const std::chrono::milliseconds m_idleTimeout;
    Clock::time_point m_lastActivity{Clock::now()};
    Clock::time_point m_highWaterSince{};

    MessageCallback m_messageCallback;
    WatermarkCallback m_highWaterCallback;
    WatermarkCallback m_lowWaterCallback;
    CloseCallback m_closeCallback;

    std::shared_ptr<ReadBuffer> m_input{
        std::make_shared<ReadBuffer>()};
    std::deque<std::shared_ptr<WriteBuffer>> m_output;
    std::size_t m_readBytes{0};
    boost::system::error_code m_readError;
    std::size_t m_frontOffset{0};
    std::size_t m_pendingBytes{0};

    bool m_started{false};
    bool m_closed{false};
    bool m_readPaused{false};
    bool m_reading{false};
    bool m_readReady{false};
    bool m_delivering{false};
    bool m_writing{false};
    bool m_shutdownRequested{false};
    bool m_writeHalfClosed{false};
    bool m_peerHalfClosed{false};
    bool m_aboveHighWater{false};
};
} // namespace service::gateway
