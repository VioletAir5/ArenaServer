/** @file
 * @brief Serializes RPC listener, socket and actual handler ownership
 * on a strand.
 */
#include "rpc/Server.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>

#include "security/SecureErase.h"

namespace arena::internal_rpc
{
namespace
{
using arena::security::eraseSecret;
using Clock = std::chrono::steady_clock;
using Tcp = boost::asio::ip::tcp;
using Strand = boost::asio::strand<boost::asio::any_io_executor>;
using namespace std::chrono_literals;

bool identity(std::string_view text)
{
    if (text.empty() || text.size() > 32 || text.front() < 'a' ||
        text.front() > 'z')
        return false;
    return std::all_of(
        text.begin(),
        text.end(),
        [](char c)
        {
            return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                   c == '_' || c == '-';
        }
    );
}

bool correlation(std::string_view text)
{
    return !text.empty() && text.size() <= 64 &&
           std::all_of(
               text.begin(),
               text.end(),
               [](char c)
               {
                   return (c >= 'a' && c <= 'z') ||
                          (c >= 'A' && c <= 'Z') ||
                          (c >= '0' && c <= '9') || c == '_' ||
                          c == '-';
               }
           );
}

bool method(std::string_view text)
{
    return !text.empty() && text.size() <= 64 &&
           text.front() >= 'a' && text.front() <= 'z' &&
           std::all_of(
               text.begin(),
               text.end(),
               [](char c)
               {
                   return (c >= 'a' && c <= 'z') ||
                          (c >= '0' && c <= '9') || c == '_' ||
                          c == '.';
               }
           );
}

bool secretEqual(std::string_view left, std::string_view right)
{
    unsigned difference = static_cast<unsigned>(
        left.size() ^ right.size()
    );
    for (std::size_t index = 0; index != 256; ++index)
    {
        const auto a = index < left.size()
                           ? static_cast<unsigned char>(left[index])
                           : 0;
        const auto b = index < right.size()
                           ? static_cast<unsigned char>(right[index])
                           : 0;
        difference |= a ^ b;
    }
    return difference == 0;
}

/** @brief Scrubs owned payloads even when queued or duplicate
 * completions are discarded. */
struct OwnedResponse final
{
    explicit OwnedResponse(arena::rpc::Response response)
        : value(std::move(response))
    {
    }
    OwnedResponse(OwnedResponse&&) = default;
    OwnedResponse(const OwnedResponse&) = delete;
    ~OwnedResponse()
    {
        eraseSecret(*value.mutable_payload());
    }
    /// The only protobuf payload owned by this move-only handoff.
    arena::rpc::Response value;
};
} // namespace

namespace detail
{
struct ServerPeer;
struct ServerJob;

/** @brief Retires the owner's strong reference at the first real
 * completion. */
struct CompletionGate
{
    std::atomic<bool> claimed{false};
    std::mutex mutex;
    std::shared_ptr<ServerState> owner;
    std::uint64_t id{0};
    void submit(arena::rpc::Response);
};

struct ServerState : std::enable_shared_from_this<ServerState>
{
    ServerState(
        boost::asio::any_io_executor target,
        Server::Config value,
        Server::Handlers routes
    )
        : executor(boost::asio::make_strand(std::move(target)))
        , config(std::move(value))
        , handlers(std::move(routes))
        , listener(executor)
        , acceptTimer(executor)
        , stopTimer(executor)
    {
        if (!identity(config.service) ||
            !config.endpoint.address().is_loopback() ||
            config.callerTokens.empty() ||
            config.callerTokens.size() > 32 || config.maxPeers < 1 ||
            config.maxPeers > 4096 || config.maxActive < 1 ||
            config.maxActive > 4096 || config.maxFrame < 128 ||
            config.maxFrame > Client::maxFrame ||
            config.maxBudget < 1ms || config.maxBudget > 30s ||
            config.readTimeout < 1ms || config.readTimeout > 1s ||
            config.writeTimeout < 1ms || config.writeTimeout > 1s ||
            handlers.empty() || handlers.size() > 128)
            throw std::invalid_argument(
                "invalid bounded RPC server configuration"
            );
        for (const auto& [caller, token] : config.callerTokens)
            if (!identity(caller) || token.size() < 32 ||
                token.size() > 256)
                throw std::invalid_argument(
                    "invalid RPC peer credential"
                );
        for (const auto& [name, handler] : handlers)
            if (!method(name) || !handler)
                throw std::invalid_argument(
                    "invalid RPC handler registration"
                );
        listener.open(config.endpoint.protocol());
        listener.set_option(Tcp::acceptor::reuse_address(true));
        listener.bind(config.endpoint);
        listener.listen(128);
        boundPort = listener.local_endpoint().port();
    }

    ~ServerState()
    {
        for (auto& [caller, token] : config.callerTokens)
            eraseSecret(token);
    }

    void accept();
    void
    admit(const std::shared_ptr<ServerPeer>&, arena::rpc::Request);
    void complete(std::uint64_t, arena::rpc::Response);
    void beginStop(std::chrono::milliseconds);
    void forceStop();
    void checkStopped()
    {
        if (stopRequested.load() && !listener.is_open() &&
            peers.empty() && jobs.empty())
        {
            boost::system::error_code ignored;
            stopTimer.cancel(ignored);
            stopped.store(true);
        }
    }

    Strand executor;
    Server::Config config;
    const Server::Handlers handlers;
    Tcp::acceptor listener;
    boost::asio::steady_timer acceptTimer, stopTimer;
    std::uint16_t boundPort{0};
    std::uint64_t sequence{0};
    std::map<std::uint64_t, std::shared_ptr<ServerPeer>> peers;
    std::map<std::uint64_t, std::shared_ptr<ServerJob>> jobs;
    std::atomic<std::size_t> peerCount{0}, activeCount{0};
    std::atomic<std::uint64_t> accepted{0}, rejected{0}, completed{0},
        cancelled{0};
    std::atomic<bool> stopRequested{false}, stopped{false};
};

struct ServerPeer : std::enable_shared_from_this<ServerPeer>
{
    ServerPeer(
        const std::shared_ptr<ServerState>& state,
        std::uint64_t identity,
        Tcp::socket connection
    )
        : owner(state)
        , id(identity)
        , socket(std::move(connection))
        , timer(state->executor)
    {
    }

    ~ServerPeer()
    {
        eraseSecret(input);
        eraseSecret(output);
    }

    void read()
    {
        const auto state = owner.lock();
        if (!state)
            return close();
        timer.expires_after(state->config.readTimeout);
        timer.async_wait(
            [self = shared_from_this()](auto error)
            {
                if (!error)
                    self->close();
            }
        );
        boost::asio::async_read(
            socket,
            boost::asio::buffer(header),
            [self = shared_from_this()](auto error, std::size_t)
            {
                const auto state = self->owner.lock();
                if (error || self->closed || !state)
                    return self->close();
                std::uint32_t length = 0;
                for (const auto byte : self->header)
                    length = (length << 8) | byte;
                if (!length || length > state->config.maxFrame)
                {
                    ++state->rejected;
                    return self->close();
                }
                self->input.resize(length);
                boost::asio::async_read(
                    self->socket,
                    boost::asio::buffer(self->input),
                    [self](auto error, std::size_t)
                    {
                        const auto state = self->owner.lock();
                        if (error || self->closed || !state)
                            return self->close();
                        boost::system::error_code ignored;
                        self->timer.cancel(ignored);
                        arena::rpc::Request request;
                        const bool parsed = request.ParseFromString(
                            self->input
                        );
                        eraseSecret(self->input);
                        if (!parsed)
                        {
                            eraseSecret(
                                *request.mutable_service_token()
                            );
                            eraseSecret(*request.mutable_payload());
                            ++state->rejected;
                            return self->close();
                        }
                        state->admit(self, std::move(request));
                    }
                );
            }
        );
    }

    void monitor()
    {
        socket.async_read_some(
            boost::asio::buffer(extra),
            [self = shared_from_this()](auto, std::size_t)
            {
                // One request owns the connection. EOF and pipelining
                // both retire it.
                self->close();
            }
        );
    }

    void reply(arena::rpc::Response result)
    {
        OwnedResponse owned(std::move(result));
        auto& response = owned.value;
        if (closed || replied)
            return;
        const auto state = owner.lock();
        if (!state)
            return close();
        replied = true;
        response.set_request_id(requestId);
        response.set_correlation_id(correlationId);
        response.set_protocol_version(protocolVersion);
        if (!arena::rpc::Status_IsValid(response.status()) ||
            response.ByteSizeLong() > state->config.maxFrame)
        {
            response.set_status(arena::rpc::INTERNAL);
            response.set_retryable(false);
            eraseSecret(*response.mutable_payload());
        }
        if (response.status() != arena::rpc::OK)
            eraseSecret(*response.mutable_payload());
        const auto size = static_cast<std::uint32_t>(
            response.ByteSizeLong()
        );
        output.resize(4 + size);
        for (unsigned index = 0; index != 4; ++index)
            output[index] = static_cast<char>(
                size >> (24 - 8 * index)
            );
        if (!response.SerializeToArray(
                output.data() + 4,
                static_cast<int>(size)
            ))
            return close();
        eraseSecret(*response.mutable_payload());
        timer.expires_after(state->config.writeTimeout);
        timer.async_wait(
            [self = shared_from_this()](auto error)
            {
                if (!error)
                    self->close();
            }
        );
        boost::asio::async_write(
            socket,
            boost::asio::buffer(output),
            [self = shared_from_this()](auto, std::size_t)
            { self->close(); }
        );
    }

    void failure(arena::rpc::Status status)
    {
        arena::rpc::Response response;
        response.set_status(status);
        reply(std::move(response));
    }

    void close();

    std::weak_ptr<ServerState> owner;
    const std::uint64_t id;
    Tcp::socket socket;
    boost::asio::steady_timer timer;
    std::array<unsigned char, 4> header{};
    std::array<char, 1> extra{};
    std::string input, output, correlationId;
    std::uint64_t requestId{0}, jobId{0};
    bool closed{false}, replied{false};
};

struct ServerJob : std::enable_shared_from_this<ServerJob>
{
    ServerJob(
        const std::shared_ptr<ServerState>& state,
        const std::shared_ptr<ServerPeer>& connection,
        std::shared_ptr<CompletionGate> completion
    )
        : owner(state)
        , peer(connection)
        , gate(std::move(completion))
        , deadline(state->executor)
    {
    }

    void requestCancel()
    {
        if (cancelNeeded || gate->claimed.load())
            return;
        cancelNeeded = true;
        if (const auto state = owner.lock())
            ++state->cancelled;
        invokeCancel();
    }
    void invokeCancel()
    {
        if (!returned || cancelSent || !cancelNeeded ||
            gate->claimed.load())
            return;
        cancelSent = true;
        auto action = std::move(cancel);
        if (action)
            try
            {
                action();
            }
            catch (...)
            {
            }
    }

    std::weak_ptr<ServerState> owner;
    std::weak_ptr<ServerPeer> peer;
    std::shared_ptr<CompletionGate> gate;
    boost::asio::steady_timer deadline;
    Server::Cancellation cancel;
    bool returned{false}, cancelNeeded{false}, cancelSent{false};
};

void ServerPeer::close()
{
    if (closed)
        return;
    closed = true;
    boost::system::error_code ignored;
    timer.cancel(ignored);
    socket.cancel(ignored);
    socket.close(ignored);
    if (const auto state = owner.lock())
    {
        if (const auto job = state->jobs.find(jobId);
            job != state->jobs.end())
            job->second->requestCancel();
        state->peers.erase(id);
        --state->peerCount;
        state->checkStopped();
    }
}

void CompletionGate::submit(arena::rpc::Response response)
{
    OwnedResponse owned(std::move(response));
    if (claimed.exchange(true))
        return;
    std::shared_ptr<ServerState> state;
    {
        std::lock_guard lock(mutex);
        state = std::move(owner);
    }
    if (state)
        boost::asio::post(
            state->executor,
            [state, id = id, owned = std::move(owned)]() mutable
            { state->complete(id, std::move(owned.value)); }
        );
}

void ServerState::accept()
{
    if (stopRequested.load())
        return;
    listener.async_accept(
        [self = shared_from_this()](auto error, Tcp::socket socket)
        {
            if (!error)
            {
                // Linux doubles this SO_SNDBUF request; it remains a
                // fixed per-peer kernel bound, separate from the
                // protobuf frame.
                boost::system::error_code optionError;
                socket.set_option(
                    boost::asio::socket_base::send_buffer_size(
                        16 * 1024
                    ),
                    optionError
                );
                if (self->stopRequested.load() || optionError ||
                    self->peers.size() >= self->config.maxPeers)
                {
                    ++self->rejected;
                    boost::system::error_code ignored;
                    socket.close(ignored);
                }
                else
                {
                    const auto id = ++self->sequence;
                    auto peer = std::make_shared<ServerPeer>(
                        self,
                        id,
                        std::move(socket)
                    );
                    self->peers.emplace(id, peer);
                    ++self->peerCount;
                    peer->read();
                }
            }
            if (self->stopRequested.load())
                return;
            if (!error)
                self->accept();
            else
            {
                self->acceptTimer.expires_after(10ms);
                self->acceptTimer.async_wait(
                    [self](auto waitError)
                    {
                        if (!waitError)
                            self->accept();
                    }
                );
            }
        }
    );
}

void ServerState::admit(
    const std::shared_ptr<ServerPeer>& peer,
    arena::rpc::Request request
)
{
    peer->requestId = request.request_id();
    peer->correlationId = correlation(request.correlation_id())
                              ? request.correlation_id()
                              : std::string{};
    const auto reject = [&](arena::rpc::Status status)
    {
        ++rejected;
        eraseSecret(*request.mutable_service_token());
        eraseSecret(*request.mutable_payload());
        peer->failure(status);
    };
    if (!request.request_id() ||
        !correlation(request.correlation_id()) ||
        !method(request.method()) ||
        request.idempotency_key().size() > 128 ||
        request.service_token().size() > 256 ||
        !std::all_of(
            request.idempotency_key().begin(),
            request.idempotency_key().end(),
            [](unsigned char c) { return c >= 32 && c <= 126; }
        ))
        return reject(arena::rpc::INVALID_ARGUMENT);
    if (request.protocol_version() != protocolVersion)
        return reject(arena::rpc::INCOMPATIBLE_VERSION);
    if (request.target_service() != config.service ||
        !identity(request.caller_service()))
        return reject(arena::rpc::INVALID_ARGUMENT);
    const auto credential = config.callerTokens.find(
        request.caller_service()
    );
    if (credential == config.callerTokens.end() ||
        !secretEqual(request.service_token(), credential->second))
        return reject(arena::rpc::UNAUTHENTICATED);
    eraseSecret(*request.mutable_service_token());
    if (request.service_path_size() < 1)
        return reject(arena::rpc::INVALID_ARGUMENT);
    if (request.service_path_size() >=
        static_cast<int>(maxServiceHops))
        return reject(arena::rpc::CALL_CYCLE);
    std::set<std::string> visited;
    for (const auto& node : request.service_path())
    {
        if (!identity(node))
            return reject(arena::rpc::INVALID_ARGUMENT);
        if (node == config.service || !visited.insert(node).second)
            return reject(arena::rpc::CALL_CYCLE);
    }
    if (request.service_path(request.service_path_size() - 1) !=
        request.caller_service())
        return reject(arena::rpc::INVALID_ARGUMENT);
    const auto handler = handlers.find(request.method());
    if (handler == handlers.end())
        return reject(arena::rpc::NOT_FOUND);
    if (stopRequested.load())
        return reject(arena::rpc::UNAVAILABLE);
    if (jobs.size() >= config.maxActive)
        return reject(arena::rpc::OVERLOADED);
    if (config.admission)
        try
        {
            if (!config.admission(request))
                return reject(arena::rpc::UNAVAILABLE);
        }
        catch (...)
        {
            return reject(arena::rpc::UNAVAILABLE);
        }
    const auto
        now = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch()
        )
                  .count();
    if (now < 0 ||
        request.deadline_unix_ms() <= static_cast<std::uint64_t>(now))
        return reject(arena::rpc::DEADLINE_EXCEEDED);
    const auto remaining = std::min<std::uint64_t>(
        request.deadline_unix_ms() - now,
        config.maxBudget.count()
    );
    Context context{
        Clock::now() + std::chrono::milliseconds(remaining),
        request.correlation_id(),
        {}
    };
    for (const auto& node : request.service_path())
        context.servicePath.push_back(node);
    context.servicePath.push_back(config.service);
    auto gate = std::make_shared<CompletionGate>();
    gate->owner = shared_from_this();
    gate->id = peer->id;
    auto job = std::make_shared<ServerJob>(
        shared_from_this(),
        peer,
        gate
    );
    jobs.emplace(peer->id, job);
    peer->jobId = peer->id;
    ++activeCount;
    ++accepted;
    job->deadline.expires_at(context.deadline);
    job->deadline.async_wait(
        [job](auto error)
        {
            if (!error && !job->gate->claimed.load())
            {
                job->requestCancel();
                if (const auto peer = job->peer.lock())
                    peer->failure(arena::rpc::DEADLINE_EXCEEDED);
            }
        }
    );
    peer->monitor();
    try
    {
        job->cancel = handler->second(
            std::move(request),
            std::move(context),
            [gate](auto response)
            { gate->submit(std::move(response)); }
        );
    }
    catch (...)
    {
        arena::rpc::Response response;
        response.set_status(arena::rpc::INTERNAL);
        gate->submit(std::move(response));
    }
    job->returned = true;
    job->invokeCancel();
}

void ServerState::complete(
    std::uint64_t id, arena::rpc::Response result
)
{
    OwnedResponse owned(std::move(result));
    auto& response = owned.value;
    const auto found = jobs.find(id);
    if (found == jobs.end())
        return;
    auto job = std::move(found->second);
    jobs.erase(found);
    boost::system::error_code ignored;
    job->deadline.cancel(ignored);
    --activeCount;
    ++completed;
    if (const auto peer = job->peer.lock())
    {
        if (Clock::now() >= job->deadline.expiry())
        {
            response.set_status(arena::rpc::DEADLINE_EXCEEDED);
            response.set_retryable(false);
        }
        peer->reply(std::move(response));
    }
    checkStopped();
}

void ServerState::beginStop(std::chrono::milliseconds grace)
{
    boost::system::error_code ignored;
    listener.cancel(ignored);
    listener.close(ignored);
    acceptTimer.cancel(ignored);
    std::vector<std::shared_ptr<ServerPeer>> idle;
    for (const auto& [id, peer] : peers)
        if (!peer->jobId)
            idle.push_back(peer);
    for (auto& peer : idle)
        peer->close();
    if (grace == 0ms)
        forceStop();
    else
    {
        stopTimer.expires_after(grace);
        stopTimer.async_wait(
            [self = shared_from_this()](auto error)
            {
                if (!error)
                    self->forceStop();
            }
        );
    }
    checkStopped();
}

void ServerState::forceStop()
{
    for (const auto& [id, job] : jobs)
        job->requestCancel();
    std::vector<std::shared_ptr<ServerPeer>> closing;
    for (const auto& [id, peer] : peers)
        closing.push_back(peer);
    for (auto& peer : closing)
        peer->close();
    checkStopped();
}
} // namespace detail

Server::Server(
    boost::asio::any_io_executor executor,
    Config config,
    Handlers handlers
)
    : m_state(
          std::make_shared<detail::ServerState>(
              std::move(executor),
              std::move(config),
              std::move(handlers)
          )
      )
{
    boost::asio::post(
        m_state->executor,
        [state = m_state] { state->accept(); }
    );
}
Server::~Server() noexcept
{
    try
    {
        stop();
    }
    catch (...)
    {
    }
}
std::uint16_t Server::port() const noexcept
{
    return m_state->boundPort;
}
Server::Stats Server::stats() const noexcept
{
    const auto& state = *m_state;
    return Stats{
        state.peerCount.load(),
        state.activeCount.load(),
        state.accepted.load(),
        state.rejected.load(),
        state.completed.load(),
        state.cancelled.load(),
        state.stopRequested.load(),
        state.stopped.load()
    };
}
void Server::stop(std::chrono::milliseconds grace)
{
    if (grace < 0ms || grace > 10s)
        throw std::invalid_argument("invalid RPC shutdown grace");
    if (m_state->stopRequested.exchange(true))
        return;
    boost::asio::post(
        m_state->executor,
        [state = m_state, grace] { state->beginStop(grace); }
    );
}
} // namespace arena::internal_rpc
