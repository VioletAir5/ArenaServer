#include "net/AsioConnect.h"

#include <fcntl.h>

#include <algorithm>
#include <cerrno>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "ConnectionId.h"

namespace service::gateway
{
AsioConnect::AsioConnect(
    boost::asio::io_context& context,
    boost::asio::ip::tcp::socket socket,
    std::chrono::milliseconds idleTimeout
)
    : m_owner(std::this_thread::get_id())
    , m_id(detail::nextConnectionId())
    , m_socket(std::move(socket))
    , m_idleTimer(context)
    , m_slowTimer(context)
    , m_idleTimeout(idleTimeout)
{
    if (&m_socket.get_executor().context() != &context ||
        !m_socket.is_open() ||
        idleTimeout <= std::chrono::milliseconds::zero())
    {
        throw std::invalid_argument(
            "AsioConnect requires an open owner socket and positive "
            "idle timeout"
        );
    }
    m_socket.non_blocking(true);
    const int flags = ::fcntl(fd(), F_GETFD);
    if (flags < 0 || ::fcntl(fd(), F_SETFD, flags | FD_CLOEXEC) < 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "FD_CLOEXEC"
        );
    }
}

AsioConnect::~AsioConnect() noexcept
{
    if (std::this_thread::get_id() != m_owner)
    {
        std::terminate();
    }
}

std::uint64_t AsioConnect::id() const noexcept
{
    return m_id;
}

void AsioConnect::send(std::span<const std::byte> bytes)
{
    requireOwner();
    if ((m_state != State::Connected &&
         m_state != State::PeerHalfClosed) || bytes.empty())
    {
        return;
    }
    auto keepAlive = weak_from_this().lock();
    if (bytes.size() > kHardOutputLimit - m_pendingBytes)
    {
        std::cerr << "AsioConnect output hard limit reached\n";
        close();
        return;
    }
    // Each operation captures its own immutable buffer. Appending or
    // closing the queue cannot invalidate memory still used by a
    // cancelled operation. Coalesce small sends into a tail not used
    // by the active write. This bounds per-chunk metadata as well as
    // payload memory for tiny sends.
    if (!m_output.empty() && (!m_writing || m_output.size() > 1) &&
        m_output.back()->size() + bytes.size() <= 64 * 1024)
    {
        auto& tail = *m_output.back();
        tail.insert(tail.end(), bytes.begin(), bytes.end());
    }
    else
    {
        auto buffer = std::make_shared<WriteBuffer>();
        buffer->reserve(std::max<std::size_t>(8192, bytes.size()));
        buffer->assign(bytes.begin(), bytes.end());
        m_output.push_back(std::move(buffer));
    }
    m_pendingBytes += bytes.size();
    updateWatermark();
    writeNext();
}

void AsioConnect::shutdown()
{
    requireOwner();
    if (m_state != State::Connected &&
        m_state != State::PeerHalfClosed)
    {
        return;
    }
    m_state = m_state == State::PeerHalfClosed
                  ? State::WriteDrainingPeerClosed
                  : State::WriteDraining;
    finishWrite();
}

void AsioConnect::close()
{
    requireOwner();
    if (m_state == State::Closed)
    {
        return;
    }
    auto keepAlive = weak_from_this().lock();
    m_state = State::Closed;
    boost::system::error_code ignored;
    m_socket.close(ignored);
    m_idleTimer.cancel(ignored);
    m_slowTimer.cancel(ignored);
    m_output.clear();
    m_frontOffset = m_pendingBytes = 0;
    m_readState = ReadState::Idle;
    // Completion handlers retain buffers, never the connection.
    // Notify once; the server defers map erasure until this dispatch
    // has returned.
    if (m_closeCallback)
    {
        auto callback = m_closeCallback;
        try
        {
            callback(this);
        }
        catch (const std::exception& e)
        {
            std::cerr << "AsioConnect close callback: " << e.what()
                      << '\n';
        }
        catch (...)
        {
            std::cerr << "AsioConnect close callback failed\n";
        }
    }
}

bool AsioConnect::connected() const noexcept
{
    return m_state != State::Closed;
}

void AsioConnect::pauseRead()
{
    requireOwner();
    m_readPaused = true;
}

void AsioConnect::resumeRead()
{
    requireOwner();
    auto keepAlive = weak_from_this().lock();
    m_readPaused = false;
    if (m_readState == ReadState::Ready)
    {
        deliverRead();
    }
    readNext();
}

std::size_t AsioConnect::pendingBytes() const noexcept
{
    return m_pendingBytes;
}

void AsioConnect::setMessageCallback(MessageCallback callback)
{
    requireOwner();
    m_messageCallback = std::move(callback);
}
void AsioConnect::setHighWaterMarkCallback(WatermarkCallback callback)
{
    requireOwner();
    m_highWaterCallback = std::move(callback);
}
void AsioConnect::setLowWaterMarkCallback(WatermarkCallback callback)
{
    requireOwner();
    m_lowWaterCallback = std::move(callback);
}
void AsioConnect::setCloseCallback(CloseCallback callback)
{
    requireOwner();
    m_closeCallback = std::move(callback);
}

void AsioConnect::start()
{
    requireOwner();
    if (m_started || m_state == State::Closed)
    {
        return;
    }
    if (weak_from_this().expired())
    {
        throw std::logic_error(
            "AsioConnect::start requires shared ownership"
        );
    }
    m_started = true;
    m_lastActivity = Clock::now();
    armIdleTimer();
    readNext();
    writeNext();
}

int AsioConnect::fd() const noexcept
{
    // Boost 1.74 native_handle() is not const.
    return const_cast<boost::asio::ip::tcp::socket&>(m_socket)
        .native_handle();
}

void AsioConnect::requireOwner() const
{
    if (std::this_thread::get_id() != m_owner)
    {
        throw std::logic_error(
            "AsioConnect requires owner io_context thread"
        );
    }
}

void AsioConnect::readNext()
{
    if (!m_started ||
        (m_state != State::Connected &&
         m_state != State::WriteDraining &&
         m_state != State::WriteHalfClosed) ||
        m_readPaused || m_readState != ReadState::Idle)
    {
        return;
    }
    m_readState = ReadState::InFlight;
    auto buffer = m_input;
    m_socket.async_read_some(
        boost::asio::buffer(*buffer),
        [weak = weak_from_this(),
         buffer](boost::system::error_code error, std::size_t bytes)
        {
            auto self = weak.lock();
            if (!self || self->m_state == State::Closed)
            {
                return;
            }
            self->m_readBytes = bytes;
            self->m_readError = error;
            self->m_readState = ReadState::Ready;
            if (bytes != 0)
            {
                self->m_lastActivity = Clock::now();
            }
            if (!self->m_readPaused)
            {
                self->deliverRead();
            }
        }
    );
}

void AsioConnect::deliverRead()
{
    if (m_state == State::Closed ||
        m_readState != ReadState::Ready || m_readPaused)
    {
        return;
    }
    const auto bytes = m_readBytes;
    const auto error = m_readError;
    m_readState = ReadState::Delivering;
    if (bytes != 0 && m_messageCallback)
    {
        auto callback = m_messageCallback;
        try
        {
            callback(
                *this,
                std::span<const std::byte>(m_input->data(), bytes)
            );
        }
        catch (const std::exception& e)
        {
            std::cerr << "AsioConnect message callback: " << e.what()
                      << '\n';
            close();
        }
        catch (...)
        {
            close();
        }
    }
    m_readState = ReadState::Idle;
    if (m_state == State::Closed)
    {
        return;
    }
    if (error == boost::asio::error::eof)
    {
        if (m_state == State::WriteHalfClosed)
        {
            close();
            return;
        }
        m_state = m_state == State::WriteDraining
                      ? State::WriteDrainingPeerClosed
                      : State::PeerHalfClosed;
        finishWrite();
    }
    else if (error)
    {
        ioError(error);
    }
    else
    {
        readNext();
    }
}

void AsioConnect::writeNext()
{
    if (!m_started || m_state == State::Closed ||
        m_writing || m_output.empty())
    {
        return;
    }
    m_writing = true;
    auto buffer = m_output.front();
    const auto offset = m_frontOffset;
    m_socket.async_write_some(
        boost::asio::buffer(
            buffer->data() + offset,
            buffer->size() - offset
        ),
        [weak = weak_from_this(),
         buffer](boost::system::error_code error, std::size_t bytes)
        {
            auto self = weak.lock();
            if (!self || self->m_state == State::Closed)
            {
                return;
            }
            self->m_writing = false;
            self->m_frontOffset += bytes;
            self->m_pendingBytes -= bytes;
            if (bytes != 0)
            {
                self->m_lastActivity = Clock::now();
            }
            if (self->m_frontOffset == buffer->size())
            {
                self->m_output.pop_front();
                self->m_frontOffset = 0;
            }
            if (error)
            {
                self->ioError(error);
                return;
            }
            self->updateWatermark();
            self->writeNext();
            self->finishWrite();
        }
    );
}

void AsioConnect::finishWrite()
{
    if (m_state == State::Closed || m_pendingBytes != 0)
    {
        return;
    }
    if (m_state == State::WriteDraining ||
        m_state == State::WriteDrainingPeerClosed)
    {
        boost::system::error_code error;
        m_socket.shutdown(
            boost::asio::ip::tcp::socket::shutdown_send,
            error
        );
        if (error && error != boost::asio::error::not_connected)
        {
            ioError(error);
            return;
        }
        if (m_state == State::WriteDrainingPeerClosed)
        {
            close();
            return;
        }
        m_state = State::WriteHalfClosed;
    }
    if (m_state == State::PeerHalfClosed)
    {
        close();
    }
}

void AsioConnect::updateWatermark()
{
    if (m_state == State::Closed)
    {
        return;
    }
    WatermarkCallback callback;
    if (!m_aboveHighWater && m_pendingBytes >= kHighWaterMark)
    {
        m_aboveHighWater = true;
        m_highWaterSince = Clock::now();
        const auto phase = m_highWaterSince;
        m_slowTimer.expires_at(phase + kSlowClientTimeout);
        m_slowTimer.async_wait(
            [weak = weak_from_this(), phase](auto error)
            {
                auto self = weak.lock();
                if (!error && self && self->m_state != State::Closed &&
                    self->m_aboveHighWater &&
                    self->m_highWaterSince == phase)
                {
                    std::cerr << "AsioConnect slow client timeout\n";
                    self->close();
                }
            }
        );
        callback = m_highWaterCallback;
    }
    else if (m_aboveHighWater && m_pendingBytes <= kLowWaterMark)
    {
        m_aboveHighWater = false;
        boost::system::error_code ignored;
        m_slowTimer.cancel(ignored);
        callback = m_lowWaterCallback;
    }
    if (callback)
    {
        try
        {
            callback(*this, m_pendingBytes);
        }
        catch (const std::exception& e)
        {
            std::cerr << "AsioConnect watermark callback: "
                      << e.what() << '\n';
            close();
        }
        catch (...)
        {
            close();
        }
    }
}

void AsioConnect::armIdleTimer()
{
    m_idleTimer.expires_at(m_lastActivity + m_idleTimeout);
    m_idleTimer.async_wait(
        [weak = weak_from_this()](auto error)
        {
            auto self = weak.lock();
            if (error || !self || self->m_state == State::Closed)
            {
                return;
            }
            if (Clock::now() - self->m_lastActivity >=
                self->m_idleTimeout)
            {
                std::cerr << "AsioConnect idle timeout\n";
                self->close();
            }
            else
            {
                self->armIdleTimer();
            }
        }
    );
}

void AsioConnect::ioError(const boost::system::error_code& error)
{
    if (error == boost::asio::error::connection_reset)
    {
        std::cerr << "AsioConnect peer reset\n";
    }
    else if (error != boost::asio::error::operation_aborted)
    {
        std::cerr << "AsioConnect I/O error: " << error.message()
                  << '\n';
    }
    close();
}
} // namespace service::gateway
