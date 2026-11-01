/** @file
 * @brief Implements independent health I/O and one coalescing
 * configuration loader.
 */
#include "runtime/RuntimeControl.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <expected>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http.hpp>

namespace arena::runtime
{
namespace
{
using Tcp = boost::asio::ip::tcp;
namespace http = boost::beast::http;
using namespace std::chrono_literals;

/** @brief Restricts identities so health serialization cannot contain
 * escapes. */
bool publicName(const std::string& name, std::size_t maximum)
{
    return !name.empty() && name.size() <= maximum &&
           name.front() >= 'a' && name.front() <= 'z' &&
           std::all_of(
               name.begin(),
               name.end(),
               [](unsigned char c)
               {
                   return (c >= 'a' && c <= 'z') ||
                          (c >= '0' && c <= '9') || c == '-' ||
                          c == '_';
               }
           );
}

/** @brief Returns only stable public categories, never parser
 * diagnostics. */
const char* errorName(ConfigError error)
{
    switch (error)
    {
    case ConfigError::Invalid:
        return "invalid";
    case ConfigError::File:
        return "file";
    case ConfigError::Changed:
        return "changed";
    case ConfigError::Stale:
        return "stale";
    case ConfigError::Busy:
        return "busy";
    }
    return "invalid";
}
} // namespace

struct RuntimeControl::State : std::enable_shared_from_this<State>
{
    struct Session;

    explicit State(Config value)
        : config(std::move(value))
        , work(boost::asio::make_work_guard(io))
        , acceptor(io)
        , acceptRetry(io)
    {
        if (!publicName(config.service, 32) ||
            (!config.nodeId.empty() &&
             !publicName(config.nodeId, 64)) ||
            (!config.configFile.empty() && !config.store))
            throw std::invalid_argument(
                "invalid runtime control configuration"
            );
        acceptor.open(Tcp::v4());
        acceptor.bind(
            {boost::asio::ip::address_v4::loopback(),
             config.healthPort}
        );
        acceptor.listen(16);
        boundPort = acceptor.local_endpoint().port();
    }

    /** @brief Uses explicit own-node intent when present; discovery
     * need not list this process. */
    bool configuredReady(
        const ConfigSnapshot::Ptr& snapshot
    ) const noexcept
    {
        if (!snapshot)
            return true;
        if (!snapshot->desiredReady())
            return false;
        const auto own = std::find_if(
            snapshot->nodes().begin(),
            snapshot->nodes().end(),
            [&](const Node& node)
            {
                return node.service == config.service &&
                       node.id == config.nodeId;
            }
        );
        return own == snapshot->nodes().end() || own->ready;
    }

    /** @brief Observes one snapshot; providers never perform
     * dependency I/O. */
    bool readiness(
        const ConfigSnapshot::Ptr& snapshot, bool terminal
    ) const noexcept
    {
        if (terminal || !configuredReady(snapshot))
            return false;
        try
        {
            return !config.dependencyReady ||
                   config.dependencyReady();
        }
        catch (...)
        {
            return false;
        }
    }

    /** @brief Builds bounded health data from one retained
     * configuration version. */
    std::string healthBody(
        const ConfigSnapshot::Ptr& snapshot, bool ready, bool terminal
    ) const
    {
        std::size_t inflight = 0;
        try
        {
            if (config.inflight)
                inflight = config.inflight();
        }
        catch (...)
        {
        }
        const auto error = lastError.load();
        return "{\"service\":\"" + config.service +
               "\",\"node_id\":\"" + config.nodeId +
               "\",\"alive\":true,\"ready\":" +
               (ready ? "true" : "false") + ",\"config_version\":\"" +
               std::to_string(snapshot ? snapshot->version() : 0) +
               "\",\"desired_ready\":" +
               (!snapshot || snapshot->desiredReady() ? "true"
                                                      : "false") +
               ",\"configured_ready\":" +
               (configuredReady(snapshot) ? "true" : "false") +
               ",\"draining\":" + (terminal ? "true" : "false") +
               ",\"inflight\":" + std::to_string(inflight) +
               ",\"control_connections\":" +
               std::to_string(peerCount.load()) +
               ",\"reload\":{\"applied\":" +
               std::to_string(applied.load()) +
               ",\"rejected\":" + std::to_string(rejected.load()) +
               ",\"coalesced\":" + std::to_string(coalesced.load()) +
               ",\"active\":" + std::to_string(active.load()) +
               ",\"peak_active\":" +
               std::to_string(peakActive.load()) +
               ",\"last_error\":" +
               (error ? "\"" +
                            std::string(errorName(
                                static_cast<ConfigError>(error - 1)
                            )) +
                            "\""
                      : "null") +
               "}}";
    }

    /** @brief Accepts finite peers; rejected sockets close without
     * another queue. */
    void accept();
    /** @brief Runs on the control owner; only one loader request may
     * be pending. */
    void queueLoad();
    /** @brief Publishes after the reader returns, unless shutdown
     * already won. */
    void
    loaded(std::expected<ConfigSnapshot::Ptr, ConfigError> result);
    /** @brief Blocks only the dedicated reader thread, never a
     * reactor. */
    void readLoop();
    /** @brief Cancels accept and every session, then lets completions
     * drain. */
    void closeIo();

    /// Immutable startup ownership and callback contracts.
    const Config config;
    /// Dedicated owner of health sessions and publication completion.
    boost::asio::io_context io;
    /// Keeps the owner alive while the file reader can still post
    /// completion.
    boost::asio::executor_work_guard<
        boost::asio::io_context::executor_type>
        work;
    /// Loopback listener, touched only before start or by its I/O
    /// owner.
    Tcp::acceptor acceptor;
    /// Failed accept retries back off instead of spinning on resource
    /// exhaustion.
    boost::asio::steady_timer acceptRetry;
    /// Actual startup binding, immutable after construction.
    std::uint16_t boundPort{0};
    /// Active sessions owned only by the control reactor.
    std::unordered_set<std::shared_ptr<Session>> sessions;
    /// Reactor and file reader are joined by RuntimeControl::stop.
    std::thread owner, reader;
    /// Prevents new posts from racing after the owner has exited.
    std::mutex ingress;
    /// One owner dispatch is enough for any number of pending reload
    /// signals.
    bool posted{false};
    /// Terminal state is visible before business or control shutdown
    /// starts.
    std::atomic<bool> stopping{false}, drain{false}, closed{false};
    /// Publication state is confined to the control owner.
    bool loading{false}, again{false};
    /// One reader mailbox, guarded independently of the publication
    /// lock.
    std::mutex readerMutex;
    std::condition_variable readerWake;
    /// A bool bounds the reader mailbox to one job.
    bool readPending{false}, readerExit{false};
    /// Observational counters are available to other owners without
    /// I/O waits.
    std::atomic<std::uint64_t> applied{0}, rejected{0}, coalesced{0},
        rejectedPeers{0};
    std::atomic<std::size_t> active{0}, peakActive{0}, peerCount{0};
    /// Zero means none; otherwise ConfigError plus one.
    std::atomic<int> lastError{0};
};

struct RuntimeControl::State::Session
    : std::enable_shared_from_this<Session>
{
    Session(Tcp::socket socket, const std::shared_ptr<State>& state)
        : stream(std::move(socket))
        , owner(state)
        , buffer(8192)
    {
        parser.header_limit(4096);
        parser.body_limit(0);
    }

    /** @brief Bounds header read and closes malformed or slow peers.
     */
    void start()
    {
        stream.expires_after(250ms);
        http::async_read_header(
            stream,
            buffer,
            parser,
            [self = shared_from_this()](
                boost::system::error_code error,
                std::size_t
            )
            {
                if (error)
                    self->finish();
                else
                    self->respond();
            }
        );
    }

    /** @brief Serves one read-only request with a finite write
     * deadline. */
    void respond()
    {
        const auto state = owner.lock();
        if (!state || finished)
        {
            finish();
            return;
        }
        const auto snapshot = state->config.store
                                  ? state->config.store->snapshot()
                                  : nullptr;
        const bool terminal = state->drain.load() ||
                              state->stopping.load();
        const bool ready = state->readiness(snapshot, terminal);
        const auto& request = parser.get();
        const auto target = request.target();
        auto status = http::status::ok;
        if (request.find(http::field::transfer_encoding) !=
                request.end() ||
            parser.content_length().value_or(0) != 0)
            status = http::status::bad_request;
        else if (request.method() != http::verb::get)
            status = http::status::method_not_allowed;
        else if (
            target != "/health/alive" && target != "/health/ready" &&
            target != "/health/config"
        )
            status = http::status::not_found;
        else if (target == "/health/ready" && !ready)
            status = http::status::service_unavailable;
        response.emplace(status, 11);
        response->set(http::field::content_type, "application/json");
        response->keep_alive(false);
        response
            ->body() = state->healthBody(snapshot, ready, terminal);
        response->prepare_payload();
        stream.expires_after(250ms);
        http::async_write(
            stream,
            *response,
            [self = shared_from_this()](
                boost::system::error_code,
                std::size_t
            ) { self->finish(); }
        );
    }

    /** @brief Reclaims a peer once; cancellation completions may
     * arrive later. */
    void finish()
    {
        if (finished)
            return;
        finished = true;
        boost::system::error_code ignored;
        stream.socket().cancel(ignored);
        stream.socket().close(ignored);
        if (const auto state = owner.lock())
        {
            state->sessions.erase(shared_from_this());
            --state->peerCount;
        }
    }

    /// Stream and timer operations belong to the control reactor.
    boost::beast::tcp_stream stream;
    /// Session callbacks cannot retain a stopped control state.
    std::weak_ptr<State> owner;
    /// Input allocation and parser limits are independent finite
    /// bounds.
    boost::beast::flat_buffer buffer;
    http::request_parser<http::empty_body> parser;
    /// The response remains owned until the asynchronous write
    /// completes.
    std::optional<http::response<http::string_body>> response;
    /// Owner-thread fence for cancellation and completion races.
    bool finished{false};
};

void RuntimeControl::State::accept()
{
    acceptor.async_accept(
        [self = shared_from_this()](
            boost::system::error_code error,
            Tcp::socket socket
        )
        {
            if (!error)
            {
                if (self->stopping.load() ||
                    self->sessions.size() >= 16)
                {
                    ++self->rejectedPeers;
                    boost::system::error_code ignored;
                    socket.close(ignored);
                }
                else
                {
                    auto session = std::make_shared<Session>(
                        std::move(socket),
                        self
                    );
                    self->sessions.insert(session);
                    ++self->peerCount;
                    session->start();
                }
            }
            if (!self->stopping.load() && self->acceptor.is_open())
            {
                if (error)
                {
                    self->acceptRetry.expires_after(50ms);
                    self->acceptRetry.async_wait(
                        [self](boost::system::error_code waitError)
                        {
                            if (!waitError && !self->stopping.load())
                                self->accept();
                        }
                    );
                }
                else
                    self->accept();
            }
        }
    );
}

void RuntimeControl::State::queueLoad()
{
    if (stopping.load())
        return;
    if (loading)
    {
        again = true;
        ++coalesced;
        return;
    }
    loading = true;
    {
        std::lock_guard lock(readerMutex);
        readPending = true;
    }
    readerWake.notify_one();
}

void RuntimeControl::State::loaded(
    std::expected<ConfigSnapshot::Ptr, ConfigError> result
)
{
    // A notification already executing may finish while stop begins.
    // stop joins this owner, so no publication or observer survives
    // its return.
    if (!stopping.load())
    {
        if (result)
        {
            const auto published = config.store->publish(
                *result,
                config.notify
            );
            if (published)
                ++applied;
            else
            {
                ++rejected;
                lastError.store(
                    static_cast<int>(published.error()) + 1
                );
            }
        }
        else
        {
            ++rejected;
            lastError.store(static_cast<int>(result.error()) + 1);
        }
    }
    loading = false;
    if (again && !stopping.load())
    {
        again = false;
        queueLoad();
    }
}

void RuntimeControl::State::readLoop()
{
    while (true)
    {
        {
            std::unique_lock lock(readerMutex);
            readerWake.wait(
                lock,
                [&] { return readerExit || readPending; }
            );
            if (readerExit)
                return;
            readPending = false;
        }
        active.store(1);
        peakActive.store(1);
        auto result = std::expected<ConfigSnapshot::Ptr, ConfigError>(
            std::unexpected(ConfigError::File)
        );
        try
        {
            result = ConfigSnapshot::read(config.configFile);
        }
        catch (...)
        {
            result = std::unexpected(ConfigError::File);
        }
        active.store(0);
        boost::asio::post(
            io,
            [self = shared_from_this(),
             result = std::move(result)]() mutable
            { self->loaded(std::move(result)); }
        );
    }
}

void RuntimeControl::State::closeIo()
{
    boost::system::error_code ignored;
    acceptRetry.cancel(ignored);
    acceptor.cancel(ignored);
    acceptor.close(ignored);
    const std::vector<std::shared_ptr<Session>> peers(
        sessions.begin(),
        sessions.end()
    );
    for (const auto& session : peers)
        session->finish();
    work.reset();
}

RuntimeControl::RuntimeControl(Config config)
    : m_state(std::make_shared<State>(std::move(config)))
{
    try
    {
        m_state->accept();
        m_state->owner = std::thread([state = m_state]
                                     { state->io.run(); });
        m_state->reader = std::thread([state = m_state]
                                      { state->readLoop(); });
    }
    catch (...)
    {
        stop();
        throw;
    }
}

RuntimeControl::~RuntimeControl() noexcept
{
    try
    {
        stop();
    }
    catch (...)
    {
        std::terminate();
    }
}

std::uint16_t RuntimeControl::port() const noexcept
{
    return m_state->boundPort;
}

bool RuntimeControl::ready() const noexcept
{
    const auto snapshot = m_state->config.store
                              ? m_state->config.store->snapshot()
                              : nullptr;
    return m_state->readiness(
        snapshot,
        m_state->drain.load() || m_state->stopping.load()
    );
}

bool RuntimeControl::configuredReady() const noexcept
{
    const auto snapshot = m_state->config.store
                              ? m_state->config.store->snapshot()
                              : nullptr;
    return m_state->configuredReady(snapshot);
}

void RuntimeControl::setDraining() noexcept
{
    m_state->drain.store(true);
}
bool RuntimeControl::draining() const noexcept
{
    return m_state->drain.load();
}

bool RuntimeControl::requestReload()
{
    const auto state = m_state;
    std::lock_guard gate(state->ingress);
    if (state->stopping.load())
        return false;
    if (state->config.configFile.empty() || !state->config.store)
    {
        ++state->rejected;
        state->lastError.store(
            static_cast<int>(ConfigError::File) + 1
        );
        return false;
    }
    if (state->posted)
    {
        ++state->coalesced;
        return false;
    }
    state->posted = true;
    boost::asio::post(
        state->io,
        [state]
        {
            {
                std::lock_guard gate(state->ingress);
                state->posted = false;
            }
            state->queueLoad();
        }
    );
    return true;
}

RuntimeControl::Metrics RuntimeControl::metrics() const noexcept
{
    const auto& state = *m_state;
    const auto error = state.lastError.load();
    return Metrics{
        state.applied.load(),
        state.rejected.load(),
        state.coalesced.load(),
        state.active.load(),
        state.peakActive.load(),
        state.peerCount.load(),
        state.rejectedPeers.load(),
        error ? std::optional<ConfigError>(
                    static_cast<ConfigError>(error - 1)
                )
              : std::nullopt,
        state.drain.load(),
        state.closed.load()
    };
}

void RuntimeControl::stop()
{
    const auto state = m_state;
    if (state->closed.load())
        return;
    if (state->io.get_executor().running_in_this_thread())
        throw std::logic_error(
            "runtime control stop requires an outside owner"
        );
    std::lock_guard stopped(m_stopMutex);
    if (state->closed.load())
        return;
    {
        std::lock_guard gate(state->ingress);
        state->stopping.store(true);
        state->drain.store(true);
    }
    {
        std::lock_guard lock(state->readerMutex);
        state->readerExit = true;
        state->readPending = false;
    }
    state->readerWake.notify_one();
    if (state->reader.joinable())
        state->reader.join();
    if (state->owner.joinable())
    {
        boost::asio::post(state->io, [state] { state->closeIo(); });
        state->owner.join();
    }
    else
    {
        state->closeIo();
        state->io.run();
    }
    state->closed.store(true);
}
} // namespace arena::runtime