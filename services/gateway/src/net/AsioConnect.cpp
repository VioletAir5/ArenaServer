/** @file
 * @brief Owns one Boost.Asio TCP connection and its asynchronous I/O.
 */
#include "net/AsioConnect.h"

#include <fcntl.h>

#include <algorithm>
#include <cerrno>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/post.hpp>

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
    , m_strand(boost::asio::make_strand(context))
    , m_socket(std::move(socket))
    , m_idleTimer(context)
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
    if (bytes.empty())
    {
        return;
    }
    const bool owner = std::this_thread::get_id() == m_owner;
    if (!owner && weak_from_this().expired())
    {
        throw std::logic_error(
            "AsioConnect foreign send requires shared ownership"
        );
    }
    // Owner-side output callbacks may close and release their caller.
    auto keepAlive = owner ? weak_from_this().lock()
                           : std::shared_ptr<AsioConnect>{};
    bool overflow = false;
    bool scheduleDrain = false;
    {
        std::lock_guard lock(m_sendMutex);
        if (!m_acceptingSends)
        {
            return;
        }
        if (bytes.size() > kHardOutputLimit - m_pendingBytes)
        {
            m_acceptingSends = false;
            overflow = true;
        }
        else
        {
            // Only the ingress queue is shared. Its tail is never
            // used by an active async write, so small calls can
            // coalesce.
            if (!m_sendQueue.empty() &&
                m_sendQueue.back()->size() + bytes.size() <=
                    64 * 1024)
            {
                auto& tail = *m_sendQueue.back();
                tail.insert(tail.end(), bytes.begin(), bytes.end());
            }
            else
            {
                auto buffer = std::make_shared<WriteBuffer>();
                buffer->reserve(
                    std::max<std::size_t>(8192, bytes.size())
                );
                buffer->assign(bytes.begin(), bytes.end());
                m_sendQueue.push_back(std::move(buffer));
            }
            m_pendingBytes += bytes.size();
            if (!owner && !m_sendDrainPosted)
            {
                m_sendDrainPosted = true;
                scheduleDrain = true;
            }
        }
    }
    if (overflow)
    {
        std::cerr << "AsioConnect output hard limit reached\n";
        if (owner)
        {
            closeOnce();
        }
        else
        {
            boost::asio::post(
                m_strand,
                [weak = weak_from_this()]
                {
                    if (auto self = weak.lock())
                    {
                        self->closeOnce();
                    }
                }
            );
        }
        return;
    }
    if (owner)
    {
        drainSendQueue();
    }
    else if (scheduleDrain)
    {
        boost::asio::post(
            m_strand,
            [weak = weak_from_this()]
            {
                if (auto self = weak.lock())
                {
                    self->drainSendQueue();
                }
            }
        );
    }
}

void AsioConnect::drainSendQueue()
{
    requireOwner();
    std::deque<std::shared_ptr<WriteBuffer>> incoming;
    {
        std::lock_guard lock(m_sendMutex);
        incoming.swap(m_sendQueue);
        m_sendDrainPosted = false;
    }
    if (m_state == State::Closed || incoming.empty())
    {
        return;
    }
    for (auto& buffer : incoming)
    {
        // The active front buffer must not move while Asio uses it.
        if (!m_output.empty() &&
            (!m_writing || m_output.size() > 1) &&
            m_output.back()->size() + buffer->size() <= 64 * 1024)
        {
            auto& tail = *m_output.back();
            tail.insert(tail.end(), buffer->begin(), buffer->end());
        }
        else
        {
            m_output.push_back(std::move(buffer));
        }
    }
    notifyOutputChanged();
    writeNext();
}

void AsioConnect::shutdown()
{
    requireOwner();
    auto keepAlive = weak_from_this().lock();
    if (m_state != State::Connected &&
        m_state != State::PeerHalfClosed)
    {
        return;
    }
    {
        std::lock_guard lock(m_sendMutex);
        m_acceptingSends = false;
    }
    m_state = m_state == State::PeerHalfClosed
                  ? State::WriteDrainingPeerClosed
                  : State::WriteDraining;
    drainSendQueue();
    finishWrite();
}

void AsioConnect::close()
{
    requireOwner();
    closeOnce();
}

void AsioConnect::closeOnce()
{
    requireOwner();
    if (m_state == State::Closed)
    {
        return;
    }
    auto keepAlive = weak_from_this().lock();
    m_state = State::Closed;
    {
        std::lock_guard lock(m_sendMutex);
        m_acceptingSends = false;
        m_sendQueue.clear();
        m_sendDrainPosted = false;
        m_pendingBytes = 0;
    }
    boost::system::error_code ignored;
    m_socket.close(ignored);
    m_idleTimer.cancel(ignored);
    m_output.clear();
    m_frontOffset = 0;
    m_readState = ReadState::Idle;
    // In-flight handlers retain their buffers, never the connection.
    // Notify once; the server defers map erasure until dispatch
    // returns.
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
    std::lock_guard lock(m_sendMutex);
    return m_pendingBytes;
}

void AsioConnect::setMessageCallback(MessageCallback callback)
{
    requireOwner();
    m_messageCallback = std::move(callback);
}
void AsioConnect::setOutputCallback(OutputCallback callback)
{
    requireOwner();
    m_outputCallback = std::move(callback);
}
void AsioConnect::setCloseCallback(CloseCallback callback)
{
    requireOwner();
    m_closeCallback = std::move(callback);
}

void AsioConnect::start()
{
    requireOwner();
    auto keepAlive = weak_from_this().lock();
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
    drainSendQueue();
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
        boost::asio::bind_executor(
            m_strand,
            [weak = weak_from_this(), buffer](
                boost::system::error_code error,
                std::size_t bytes
            )
            {
                auto self = weak.lock();
                if (!self)
                {
                    return;
                }
                if (error == boost::asio::error::operation_aborted)
                {
                    self->closeOnce();
                    return;
                }
                if (self->m_state == State::Closed)
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
        )
    );
}

void AsioConnect::deliverRead()
{
    if (m_state == State::Closed || m_readState != ReadState::Ready ||
        m_readPaused)
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
    if (!m_started || m_state == State::Closed || m_writing ||
        m_output.empty())
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
        boost::asio::bind_executor(
            m_strand,
            [weak = weak_from_this(), buffer](
                boost::system::error_code error,
                std::size_t bytes
            )
            {
                auto self = weak.lock();
                if (!self)
                {
                    return;
                }
                if (error == boost::asio::error::operation_aborted)
                {
                    self->closeOnce();
                    return;
                }
                if (self->m_state == State::Closed)
                {
                    return;
                }
                self->m_writing = false;
                self->m_frontOffset += bytes;
                {
                    std::lock_guard lock(self->m_sendMutex);
                    self->m_pendingBytes -= bytes;
                }
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
                self->notifyOutputChanged();
                self->writeNext();
                self->finishWrite();
            }
        )
    );
}

void AsioConnect::finishWrite()
{
    if (m_state == State::Closed || pendingBytes() != 0)
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

void AsioConnect::notifyOutputChanged()
{
    if (m_outputCallback && connected())
    {
        auto callback = m_outputCallback;
        try
        {
            callback(pendingBytes());
        }
        catch (...)
        {
            closeOnce();
        }
    }
}

void AsioConnect::armIdleTimer()
{
    m_idleTimer.expires_at(m_lastActivity + m_idleTimeout);
    m_idleTimer.async_wait(boost::asio::bind_executor(
        m_strand,
        [weak = weak_from_this()](auto error)
        {
            auto self = weak.lock();
            if (error == boost::asio::error::operation_aborted)
            {
                return;
            }
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
    ));
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
    closeOnce();
}
} // namespace service::gateway
