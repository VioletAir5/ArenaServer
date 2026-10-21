/** @file
 * @brief Serves WebSocket connections over the gateway transport.
 */
#include "net/WebSocketServer.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#include "net/ConnectionHost.h"
#include "net/ConnectionSchedulers.h"
#include "net/ITransport.h"

#include "ConnectionId.h"

namespace service::gateway
{
namespace
{
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using Tcp = asio::ip::tcp;
using Error = boost::system::error_code;

constexpr std::size_t kMaxMessageBytes = 256 * 1024 + 8;
constexpr std::size_t kMaxQueuedBytes = 1024 * 1024;
constexpr auto kCloseTimeout = std::chrono::seconds(5);

class WebSocketConnection final
    : public ITransport,
      public std::enable_shared_from_this<WebSocketConnection>
{
    enum class State
    {
        ReadingHttp,
        Upgrading,
        Open,
        Draining,
        ClosePendingWrite,
        CloseHandshake,
        Closed
    };

public:
    WebSocketConnection(
        asio::io_context& context,
        Tcp::socket socket,
        std::uint64_t id,
        std::string origin,
        std::function<void(std::uint64_t)> removed
    )
        : m_id(id)
        , m_strand(asio::make_strand(context))
        , m_ws(std::move(socket))
        , m_handshakeTimer(context)
        , m_closeTimer(context)
        , m_httpBuffer(8192)
        , m_input(kMaxMessageBytes)
        , m_allowedOrigin(std::move(origin))
        , m_removed(std::move(removed))
    {
        m_request.header_limit(8192);
        m_request.body_limit(0);
        m_ws.read_message_max(kMaxMessageBytes);
        m_ws.binary(true);
    }

    std::uint64_t id() const noexcept override
    {
        return m_id;
    }
    bool connected() const noexcept override
    {
        const auto state = m_state.load();
        return state == State::Open || state == State::Draining;
    }
    std::size_t pendingBytes() const noexcept override
    {
        std::lock_guard lock(m_sendMutex);
        return m_pendingBytes;
    }

    void send(std::span<const std::byte> bytes) override
    {
        if (bytes.empty())
            return;
        bool post = false;
        {
            std::lock_guard lock(m_sendMutex);
            if (!m_acceptingSends)
                return;
            if (bytes.size() > kMaxQueuedBytes - m_pendingBytes)
            {
                m_acceptingSends = false;
                m_outputOverflow = true;
            }
            else
            {
                auto copy = std::make_shared<std::string>(
                    reinterpret_cast<const char*>(bytes.data()),
                    bytes.size()
                );
                m_pendingBytes += copy->size();
                m_output.push_back(std::move(copy));
            }
            if (!m_sendDrainPosted)
            {
                m_sendDrainPosted = true;
                post = true;
            }
        }
        if (!post)
            return;
        auto self = shared_from_this();
        asio::post(
            m_strand,
            [self]
            {
                bool overflow;
                {
                    std::lock_guard lock(self->m_sendMutex);
                    self->m_sendDrainPosted = false;
                    overflow = self->m_outputOverflow;
                }
                const auto state = self->m_state.load();
                if (state != State::Open && state != State::Draining)
                    return;
                if (overflow)
                {
                    std::cerr
                        << "WebSocket output limit id=" << self->m_id
                        << " pending=" << self->pendingBytes()
                        << '\n';
                    self->startClose(
                        websocket::close_code::policy_error,
                        "output limit"
                    );
                }
                else
                {
                    self->notifyOutputChanged();
                    self->writeNext();
                }
            }
        );
    }
    void shutdown() override
    {
        const auto state = m_state.load();
        if (state == State::Draining ||
            state == State::ClosePendingWrite ||
            state == State::CloseHandshake || state == State::Closed)
            return;
        if (state != State::Open)
        {
            abort();
            return;
        }
        m_state = State::Draining;
        m_closeCode = websocket::close_code::normal;
        m_closeReason = "shutdown";
        {
            std::lock_guard lock(m_sendMutex);
            m_acceptingSends = false;
        }
        // Discard only input waiting behind read backpressure.
        // Accepted output, including sends whose drain callback is
        // still queued, remains in order until its write completion.
        if (m_readReady)
        {
            m_readReady = false;
            m_input.consume(m_input.size());
        }
        armCloseTimeout();
        writeNext();
    }
    void close() override
    {
        startClose(
            websocket::close_code::policy_error,
            "invalid packet"
        );
    }
    void pauseRead() override
    {
        m_readPaused = true;
    }
    void resumeRead() override
    {
        m_readPaused = false;
        if (m_readReady)
            deliverRead();
        readNext();
    }

    void setCallbacks(
        std::function<void(ITransport&, std::span<const std::byte>)>
            handler,
        std::function<void()> opened,
        std::function<void(std::size_t)> output,
        std::function<void()> closed
    )
    {
        m_handler = std::move(handler);
        m_opened = std::move(opened);
        m_outputChanged = std::move(output);
        m_closed = std::move(closed);
    }
    auto executor() const
    {
        return m_strand;
    }
    void start()
    {
        auto self = shared_from_this();
        m_handshakeTimer.expires_after(std::chrono::seconds(5));
        m_handshakeTimer.async_wait(
            asio::bind_executor(
                m_strand,
                [self](Error error)
                {
                    if (!error &&
                        self->m_state.load() != State::Open &&
                        self->m_state.load() != State::Closed)
                    {
                        std::cerr << "WebSocket handshake timeout id="
                                  << self->m_id << '\n';
                        self->abort();
                    }
                }
            )
        );
        http::async_read(
            m_ws.next_layer(),
            m_httpBuffer,
            m_request,
            asio::bind_executor(
                m_strand,
                [self](Error error, std::size_t)
                { self->onRequest(error); }
            )
        );
    }

    void abort() noexcept
    {
        if (m_state.exchange(State::Closed) == State::Closed)
            return;
        {
            std::lock_guard lock(m_sendMutex);
            m_acceptingSends = false;
            m_output.clear();
            m_pendingBytes = 0;
        }
        Error ignored;
        m_handshakeTimer.cancel(ignored);
        m_closeTimer.cancel(ignored);
        m_ws.next_layer().cancel(ignored);
        m_ws.next_layer().close(ignored);
        if (m_closed)
            m_closed();
        m_removed(m_id);
    }

private:
    void onRequest(Error error)
    {
        if (m_state.load() != State::ReadingHttp)
            return;
        if (error)
        {
            abort();
            return;
        }
        const auto& request = m_request.get();
        if (!websocket::is_upgrade(request) ||
            request.target() != "/ws" ||
            request[http::field::origin] != m_allowedOrigin)
        {
            const auto status = websocket::is_upgrade(request)
                                    ? http::status::forbidden
                                    : http::status::bad_request;
            auto response = std::make_shared<
                http::response<http::string_body>>(
                status,
                request.version()
            );
            response->set(http::field::server, "ArenaServer");
            response->set(http::field::content_type, "text/plain");
            response->body() = "WebSocket upgrade rejected";
            response->prepare_payload();
            auto self = shared_from_this();
            http::async_write(
                m_ws.next_layer(),
                *response,
                asio::bind_executor(
                    m_strand,
                    [self, response](Error, std::size_t)
                    { self->abort(); }
                )
            );
            return;
        }
        auto self = shared_from_this();
        m_state = State::Upgrading;
        m_ws.async_accept(
            request,
            asio::bind_executor(
                m_strand,
                [self](Error acceptError)
                {
                    if (self->m_state.load() != State::Upgrading)
                        return;
                    if (acceptError)
                    {
                        self->abort();
                        return;
                    }
                    self->m_state = State::Open;
                    Error ignored;
                    self->m_handshakeTimer.cancel(ignored);
                    if (self->m_opened)
                        self->m_opened();
                    self->readNext();
                }
            )
        );
    }

    void readNext()
    {
        const auto state = m_state.load();
        if ((state != State::Open && state != State::Draining) ||
            (state == State::Open && m_readPaused) || m_reading ||
            m_readReady || m_delivering)
            return;
        m_reading = true;
        auto self = shared_from_this();
        m_ws.async_read(
            m_input,
            asio::bind_executor(
                m_strand,
                [self](Error error, std::size_t)
                { self->onRead(error); }
            )
        );
    }

    void onRead(Error error)
    {
        m_reading = false;
        if (m_state.load() == State::Closed)
            return;
        if (error == websocket::error::closed)
        {
            std::cerr << "WebSocket peer close id=" << m_id
                      << " code="
                      << static_cast<unsigned>(m_ws.reason().code)
                      << '\n';
            abort();
            return;
        }
        if (error == websocket::error::message_too_big)
        {
            std::cerr << "WebSocket message too big id=" << m_id
                      << " code=1009\n";
            abort();
            return;
        }
        if (error)
        {
            abort();
            return;
        }
        if (m_state.load() != State::Open)
        {
            // Reads continue only to process WebSocket control/close
            // frames; graceful drain admits no further application
            // data.
            m_input.consume(m_input.size());
            readNext();
            return;
        }
        if (!m_ws.got_binary())
        {
            startClose(
                websocket::close_code::unknown_data,
                "binary required"
            );
            return;
        }
        m_readReady = true;
        if (!m_readPaused)
            deliverRead();
    }

    void deliverRead()
    {
        if (m_state.load() != State::Open || !m_readReady ||
            m_readPaused || m_delivering)
            return;
        m_readReady = false;
        m_delivering = true;
        try
        {
            const auto* data = asio::buffer_cast<const std::byte*>(
                m_input.data()
            );
            m_handler(*this, std::span(data, m_input.size()));
        }
        catch (const std::exception& exception)
        {
            std::cerr << "WebSocket handler id=" << m_id << ": "
                      << exception.what() << '\n';
            startClose(
                websocket::close_code::internal_error,
                "handler error"
            );
        }
        catch (...)
        {
            startClose(
                websocket::close_code::internal_error,
                "handler error"
            );
        }
        m_input.consume(m_input.size());
        m_delivering = false;
        readNext();
    }

    void writeNext()
    {
        const auto state = m_state.load();
        if ((state != State::Open && state != State::Draining &&
             state != State::ClosePendingWrite) ||
            m_writing)
            return;
        if (state == State::ClosePendingWrite)
        {
            performClose();
            return;
        }
        std::shared_ptr<std::string> front;
        {
            std::lock_guard lock(m_sendMutex);
            if (!m_output.empty())
                front = m_output.front();
        }
        if (!front)
        {
            if (state == State::Draining)
            {
                m_state = State::ClosePendingWrite;
                performClose();
            }
            return;
        }
        m_writing = true;
        auto self = shared_from_this();
        m_ws.async_write(
            asio::buffer(*front),
            asio::bind_executor(
                m_strand,
                [self, front](Error error, std::size_t)
                { self->onWrite(error); }
            )
        );
    }

    void onWrite(Error error)
    {
        m_writing = false;
        if (m_state.load() == State::Closed)
            return;
        if (error)
        {
            abort();
            return;
        }
        {
            std::lock_guard lock(m_sendMutex);
            if (!m_output.empty())
            {
                m_pendingBytes -= m_output.front()->size();
                m_output.pop_front();
            }
        }
        notifyOutputChanged();
        writeNext();
    }

    void startClose(websocket::close_code code, std::string reason)
    {
        const auto state = m_state.load();
        if (state == State::Closed ||
            state == State::ClosePendingWrite ||
            state == State::CloseHandshake)
            return;
        if (state != State::Open && state != State::Draining)
        {
            abort();
            return;
        }
        m_state = State::ClosePendingWrite;
        m_closeCode = code;
        m_closeReason = std::move(reason);
        {
            std::lock_guard lock(m_sendMutex);
            m_acceptingSends = false;
            // Preserve an in-flight write until its completion
            // callback.
            const auto keep = m_writing ? 1u : 0u;
            while (m_output.size() > keep)
            {
                m_pendingBytes -= m_output.back()->size();
                m_output.pop_back();
            }
        }
        armCloseTimeout();
        if (!m_writing)
            performClose();
    }

    void armCloseTimeout()
    {
        auto self = shared_from_this();
        m_closeTimer.expires_after(kCloseTimeout);
        m_closeTimer.async_wait(
            asio::bind_executor(
                m_strand,
                [self](Error error)
                {
                    if (!error)
                        self->abort();
                }
            )
        );
    }

    void performClose()
    {
        if (m_state.load() != State::ClosePendingWrite)
            return;
        m_state = State::CloseHandshake;
        websocket::close_reason reason{m_closeCode};
        reason.reason = m_closeReason;
        std::cerr << "WebSocket local close id=" << m_id
                  << " code=" << static_cast<unsigned>(m_closeCode)
                  << " reason=" << m_closeReason << '\n';
        auto self = shared_from_this();
        m_ws.async_close(
            reason,
            asio::bind_executor(
                m_strand,
                [self](Error) { self->abort(); }
            )
        );
    }

    void notifyOutputChanged()
    {
        if (m_outputChanged)
            m_outputChanged(pendingBytes());
    }
    const std::uint64_t m_id;
    asio::strand<asio::io_context::executor_type> m_strand;
    websocket::stream<Tcp::socket> m_ws;
    asio::steady_timer m_handshakeTimer;
    asio::steady_timer m_closeTimer;
    beast::flat_buffer m_httpBuffer;
    beast::flat_buffer m_input;
    http::request_parser<http::empty_body> m_request;
    const std::string m_allowedOrigin;
    std::function<void(ITransport&, std::span<const std::byte>)>
        m_handler;
    std::function<void()> m_opened, m_closed;
    std::function<void(std::size_t)> m_outputChanged;
    std::function<void(std::uint64_t)> m_removed;
    mutable std::mutex m_sendMutex;
    std::deque<std::shared_ptr<std::string>> m_output;
    std::size_t m_pendingBytes{0};
    bool m_acceptingSends{true};
    bool m_sendDrainPosted{false}, m_outputOverflow{false};
    // The connection owner changes the lifecycle; connected() may
    // read it from another thread. Read/write activity is orthogonal
    // to this state.
    std::atomic<State> m_state{State::ReadingHttp};
    bool m_reading{false};
    bool m_readReady{false}, m_delivering{false};
    bool m_readPaused{false};
    bool m_writing{false};
    websocket::close_code m_closeCode{websocket::close_code::normal};
    std::string m_closeReason;
};
} // namespace

struct WebSocketServer::Impl : std::enable_shared_from_this<Impl>
{
    asio::io_context& context;
    Tcp::acceptor acceptor;
    asio::steady_timer stopTimer;
    const std::string allowedOrigin;
    const ConnectionHandlerFactory factory;
    const ConnectionFeatureFactory features;
    const std::size_t capacity;
    const std::shared_ptr<AdmissionController> admission;
    struct Entry
    {
        // The transport must close before its reservation is
        // returned.
        std::shared_ptr<AdmissionController::Lease> admission;
        std::shared_ptr<ConnectionHost> host;
        WebSocketConnection* transport;
        WebSocketConnection* operator->() const noexcept
        {
            return transport;
        }
    };
    std::unordered_map<std::uint64_t, Entry> connections;
    bool stopping{false};

    Impl(
        asio::io_context& io,
        std::string origin,
        ConnectionHandlerFactory handlerFactory,
        std::size_t max,
        ConnectionFeatureFactory featureFactory,
        std::shared_ptr<AdmissionController> admissionController
    )
        : context(io)
        , acceptor(io)
        , stopTimer(io)
        , allowedOrigin(std::move(origin))
        , factory(std::move(handlerFactory))
        , features(std::move(featureFactory))
        , capacity(max)
        , admission(
              admissionController
                  ? std::move(admissionController)
                  : std::make_shared<AdmissionController>(
                        AdmissionController::Limits{max, max}
                    )
          )
    {
        if (allowedOrigin.empty() || !factory || capacity == 0)
            throw std::invalid_argument(
                "WebSocketServer configuration"
            );
    }

    void start(const InetAddress& address)
    {
        acceptor.open(Tcp::v4());
        acceptor.set_option(Tcp::acceptor::reuse_address(true));
        acceptor.bind(
            Tcp::endpoint(
                asio::ip::make_address_v4(address.ip()),
                address.port()
            )
        );
        acceptor.listen();
        acceptNext();
    }

    void acceptNext()
    {
        if (stopping)
            return;
        auto self = shared_from_this();
        acceptor.async_accept(
            [self](Error error, Tcp::socket socket)
            {
                if (!error && !self->stopping)
                {
                    const auto
                        acceptedAt = ConnectionContext::Clock::now();
                    if (self->connections.size() >= self->capacity)
                    {
                        self->admission->recordLocalRejection();
                        Error ignored;
                        socket.close(ignored);
                    }
                    else
                    {
                        std::uint64_t id = 0;
                        std::shared_ptr<AdmissionController::Lease>
                            admission;
                        try
                        {
                            id = detail::nextConnectionId();
                            const auto
                                endpoint = socket.remote_endpoint();
                            const InetAddress peer(
                                endpoint.address()
                                    .to_v4()
                                    .to_string(),
                                endpoint.port()
                            );
                            admission = self->admission->tryAcquire(
                                peer
                            );
                            if (!admission)
                            {
                                Error ignored;
                                socket.close(ignored);
                                self->acceptNext();
                                return;
                            }
                            auto contextInfo = std::make_shared<
                                ConnectionContext>(
                                id,
                                peer,
                                acceptedAt
                            );
                            auto session = std::make_shared<
                                WebSocketConnection>(
                                self->context,
                                std::move(socket),
                                id,
                                self->allowedOrigin,
                                [weak = self->weak_from_this()](
                                    std::uint64_t key
                                )
                                {
                                    if (auto owner = weak.lock())
                                        owner->removeConnection(key);
                                }
                            );
                            auto host = makeConnectionHost(
                                session,
                                contextInfo,
                                connectionScheduler(
                                    session->executor()
                                ),
                                self->factory,
                                self->features
                            );
                            const std::weak_ptr<ConnectionHost>
                                weakHost = host;
                            session->setCallbacks(
                                [weakHost](ITransport&, auto bytes)
                                {
                                    if (auto r = weakHost.lock())
                                        r->received(bytes);
                                },
                                [weakHost]
                                {
                                    if (auto r = weakHost.lock())
                                        r->start();
                                },
                                [weakHost](auto bytes)
                                {
                                    if (auto r = weakHost.lock())
                                        r->outputChanged(bytes);
                                },
                                [weakHost]
                                {
                                    if (auto r = weakHost.lock())
                                        r->transportClosed();
                                }
                            );
                            self->connections.emplace(
                                id,
                                Entry{
                                    std::move(admission),
                                    host,
                                    session.get()
                                }
                            );
                            session->start();
                        }
                        catch (...)
                        {
                            const auto found = self->connections.find(
                                id
                            );
                            if (found != self->connections.end())
                            {
                                // Keep the host alive while abort
                                // removes its registry entry through
                                // the callback.
                                const auto host = found->second.host;
                                found->second->abort();
                                self->connections.erase(id);
                            }
                            Error ignored;
                            socket.close(ignored);
                            try
                            {
                                throw;
                            }
                            catch (const std::exception& error)
                            {
                                std::cerr << "WebSocket setup: "
                                          << error.what() << '\n';
                            }
                            catch (...)
                            {
                                std::cerr << "WebSocket setup: "
                                             "unknown exception\n";
                            }
                        }
                    }
                }
                if (!self->stopping)
                    self->acceptNext();
            }
        );
    }

    void removeConnection(std::uint64_t id)
    {
        connections.erase(id);
        if (stopping && connections.empty())
        {
            Error ignored;
            stopTimer.cancel(ignored);
        }
    }

    void abortAll() noexcept
    {
        stopping = true;
        Error ignored;
        acceptor.close(ignored);
        stopTimer.cancel(ignored);
        auto pending = std::move(connections);
        connections.clear();
        for (auto& [id, session] : pending)
        {
            (void)id;
            session->abort();
        }
    }

    void stop(std::chrono::milliseconds grace) noexcept
    {
        if (grace <= std::chrono::milliseconds::zero())
        {
            abortAll();
            return;
        }
        if (stopping)
            return;
        stopping = true;
        Error ignored;
        acceptor.close(ignored);
        if (connections.empty())
            return;
        try
        {
            stopTimer.expires_after(grace);
            stopTimer.async_wait(
                [weak = weak_from_this()](Error error)
                {
                    if (!error)
                        if (auto self = weak.lock())
                            self->abortAll();
                }
            );
            // Shutdown may synchronously abort an incomplete upgrade
            // and erase its registry entry. Snapshot hosts before
            // callbacks.
            std::vector<std::shared_ptr<ConnectionHost>> active;
            active.reserve(connections.size());
            for (const auto& [id, connection] : connections)
            {
                (void)id;
                active.push_back(connection.host);
            }
            for (const auto& host : active)
                host->shutdown();
        }
        catch (...)
        {
            abortAll();
        }
    }
};

WebSocketServer::WebSocketServer(
    asio::io_context& context,
    const InetAddress& address,
    std::string allowedOrigin,
    ConnectionHandlerFactory handlerFactory,
    std::size_t maxConnections,
    ConnectionFeatureFactory featureFactory,
    std::shared_ptr<AdmissionController> admission
)
    : m_impl(
          std::make_shared<Impl>(
              context,
              std::move(allowedOrigin),
              std::move(handlerFactory),
              maxConnections,
              std::move(featureFactory),
              std::move(admission)
          )
      )
{
    m_impl->start(address);
}
WebSocketServer::~WebSocketServer() noexcept
{
    m_impl->abortAll();
}
void WebSocketServer::stop(std::chrono::milliseconds grace) noexcept
{
    m_impl->stop(grace);
}
std::size_t WebSocketServer::connectionCount() const noexcept
{
    return m_impl->connections.size();
}
} // namespace service::gateway
