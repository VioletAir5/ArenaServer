/** @file
 * @brief Owns bounded RPC operations on one strand without owning
 * threads.
 */
#include "rpc/Client.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>

namespace arena::internal_rpc
{
namespace
{
bool correlationValid(std::string_view value)
{
    if (value.empty() || value.size() > 64)
        return false;
    for (const auto c : value)
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '-'))
            return false;
    return true;
}
bool methodValid(std::string_view value)
{
    if (value.empty() || value.size() > 64 || value.front() < 'a' ||
        value.front() > 'z')
        return false;
    for (const auto c : value)
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
              c == '_' || c == '.'))
            return false;
    return true;
}
} // namespace

namespace detail
{
using Clock = std::chrono::steady_clock;
using Strand = boost::asio::strand<boost::asio::any_io_executor>;
using Tcp = boost::asio::ip::tcp;

struct ClientState
{
    ClientState(
        boost::asio::any_io_executor executor,
        Tcp::endpoint target,
        std::size_t limit
    )
        : executor(boost::asio::make_strand(std::move(executor)))
        , endpoint(std::move(target))
        , maximum(limit)
    {
    }
    /// Every socket, timer and accepted callback runs on this strand.
    Strand executor;
    Tcp::endpoint endpoint;
    const std::size_t maximum;
    /// Only admission and registry removal require cross-thread
    /// locking.
    mutable std::mutex mutex;
    std::unordered_map<std::uint64_t, std::shared_ptr<Operation>>
        operations;
    std::uint64_t sequence{0};
    bool accepting{true};
};

struct Operation : std::enable_shared_from_this<Operation>
{
    Operation(
        const std::shared_ptr<ClientState>& owner,
        const arena::rpc::Request& request,
        Clock::time_point deadline,
        Client::Completion completion
    )
        : state(owner)
        , executor(owner->executor)
        , socket(executor)
        , timer(executor)
        , endpoint(owner->endpoint)
        , deadline(deadline)
        , requestId(request.request_id())
        , correlationId(request.correlation_id())
        , completion(std::move(completion))
    {
        const auto bytes = request.SerializeAsString();
        const auto size = static_cast<std::uint32_t>(bytes.size());
        outbound.resize(4);
        for (unsigned i = 0; i != 4; ++i)
            outbound[i] = static_cast<char>(size >> (24 - 8 * i));
        outbound += bytes;
    }

    void cancel()
    {
        if (cancelRequested.exchange(true))
            return;
        boost::asio::post(
            executor,
            [self = shared_from_this()]
            { self->finish(arena::rpc::CANCELLED); }
        );
    }

    bool interrupted()
    {
        if (finished)
            return true;
        if (cancelRequested.load())
        {
            finish(arena::rpc::CANCELLED);
            return true;
        }
        if (Clock::now() >= deadline)
        {
            finish(arena::rpc::DEADLINE_EXCEEDED);
            return true;
        }
        return false;
    }

    template <class Action> void advance(Action action)
    {
        if (interrupted())
            return;
        try
        {
            action();
        }
        catch (...)
        {
            finish(arena::rpc::INTERNAL);
        }
    }

    void start()
    {
        advance(
            [this]
            {
                timer.expires_at(deadline);
                timer.async_wait(
                    [self = shared_from_this()](auto error)
                    {
                        if (!error)
                            self->finish(
                                self->cancelRequested.load()
                                    ? arena::rpc::CANCELLED
                                    : arena::rpc::DEADLINE_EXCEEDED
                            );
                    }
                );
                socket.async_connect(
                    endpoint,
                    [self = shared_from_this()](auto error)
                    {
                        self->advance(
                            [&]
                            {
                                if (error)
                                {
                                    self->transportFailure();
                                    return;
                                }
                                // A write can partially reach the
                                // peer before error. Conservatively
                                // forbid transparent replay from
                                // here.
                                self->sent = true;
                                boost::asio::async_write(
                                    self->socket,
                                    boost::asio::buffer(
                                        self->outbound
                                    ),
                                    [self](auto error, std::size_t)
                                    {
                                        self->advance(
                                            [&]
                                            {
                                                if (error)
                                                    self->transportFailure();
                                                else
                                                    self->readHeader();
                                            }
                                        );
                                    }
                                );
                            }
                        );
                    }
                );
            }
        );
    }

    void readHeader()
    {
        // No write references remain after async_write completion.
        outbound.clear();
        boost::asio::async_read(
            socket,
            boost::asio::buffer(header),
            [self = shared_from_this()](auto error, std::size_t)
            {
                self->advance(
                    [&]
                    {
                        if (error)
                        {
                            self->transportFailure();
                            return;
                        }
                        std::uint32_t size = 0;
                        for (const auto byte : self->header)
                            size = (size << 8) | byte;
                        if (!size || size > Client::maxFrame)
                        {
                            self->finish(arena::rpc::INTERNAL);
                            return;
                        }
                        self->inbound.resize(size);
                        self->readBody();
                    }
                );
            }
        );
    }

    void readBody()
    {
        boost::asio::async_read(
            socket,
            boost::asio::buffer(inbound),
            [self = shared_from_this()](auto error, std::size_t)
            {
                self->advance(
                    [&]
                    {
                        if (error)
                        {
                            self->transportFailure();
                            return;
                        }
                        arena::rpc::Response response;
                        if (!response.ParseFromArray(
                                self->inbound.data(),
                                static_cast<int>(self->inbound.size())
                            ) ||
                            response.request_id() !=
                                self->requestId ||
                            response.correlation_id() !=
                                self->correlationId ||
                            !arena::rpc::Status_IsValid(
                                response.status()
                            ))
                        {
                            self->finish(arena::rpc::INTERNAL);
                            return;
                        }
                        if (response.status() == arena::rpc::OK ||
                            response.status() ==
                                arena::rpc::OUTCOME_UNKNOWN)
                            response.set_retryable(false);
                        self->complete(std::move(response));
                    }
                );
            }
        );
    }

    void transportFailure()
    {
        finish(
            sent ? arena::rpc::OUTCOME_UNKNOWN
                 : arena::rpc::UNAVAILABLE,
            !sent
        );
    }

    void finish(arena::rpc::Status status, bool retryable = false)
    {
        if (finished)
            return;
        arena::rpc::Response response;
        response.set_request_id(requestId);
        response.set_correlation_id(correlationId);
        response.set_status(status);
        response.set_retryable(retryable);
        complete(std::move(response));
    }

    void complete(arena::rpc::Response response)
    {
        if (finished)
            return;
        finished = true;
        boost::system::error_code ignored;
        timer.cancel(ignored);
        socket.cancel(ignored);
        socket.close(ignored);
        if (const auto owner = state.lock())
        {
            std::lock_guard lock(owner->mutex);
            owner->operations.erase(requestId);
        }
        auto callback = std::move(completion);
        try
        {
            callback(std::move(response));
        }
        catch (...)
        {
            // User callbacks cannot unwind through unrelated socket
            // handlers.
        }
    }

    /// Weak back-reference prevents registry/operation retention
    /// cycles.
    std::weak_ptr<ClientState> state;
    Strand executor;
    Tcp::socket socket;
    boost::asio::steady_timer timer;
    Tcp::endpoint endpoint;
    Clock::time_point deadline;
    std::uint64_t requestId;
    std::string correlationId;
    Client::Completion completion;
    std::string outbound, inbound;
    std::array<unsigned char, 4> header{};
    /// Cancellation can be requested before the strand starts this
    /// operation.
    std::atomic<bool> cancelRequested{false};
    /// Only the strand observes transport progress and completion.
    bool sent{false}, finished{false};
};
} // namespace detail

Call::Call(std::weak_ptr<detail::Operation> operation)
    : m_operation(std::move(operation))
{
}

void Call::cancel() const
{
    if (const auto operation = m_operation.lock())
        operation->cancel();
}

Client::Client(
    boost::asio::any_io_executor executor,
    boost::asio::ip::tcp::endpoint endpoint,
    std::size_t maxInflight
)
{
    const auto address = endpoint.address();
    if (!endpoint.port() ||
        !(address == boost::asio::ip::address_v4::loopback() ||
          address == boost::asio::ip::address_v6::loopback()) ||
        !maxInflight || maxInflight > 4096)
        throw std::invalid_argument(
            "invalid internal RPC endpoint or limit"
        );
    m_state = std::make_shared<detail::ClientState>(
        std::move(executor),
        std::move(endpoint),
        maxInflight
    );
}

Client::~Client() noexcept
{
    try
    {
        stop();
    }
    catch (...)
    {
        // Allocation failure cannot make a destructor throw. Existing
        // finite deadlines still settle operations when the executor
        // is pumped.
    }
}

std::expected<Call, arena::rpc::Status> Client::call(
    arena::rpc::Request request,
    Options options,
    Completion completion
)
{
    using namespace std::chrono;
    const auto now = detail::Clock::now();
    if (!completion || options.timeout <= milliseconds::zero() ||
        options.timeout > seconds(30) ||
        !methodValid(request.method()) ||
        request.idempotency_key().size() > 128 ||
        request.service_token().size() > 256 ||
        (options.context &&
         !correlationValid(options.context->correlationId)) ||
        request.ByteSizeLong() > maxFrame)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    auto deadline = now + options.timeout;
    if (options.context)
    {
        deadline = std::min(deadline, options.context->deadline);
        request.set_correlation_id(options.context->correlationId);
    }
    if (deadline <= now)
        return std::unexpected(arena::rpc::DEADLINE_EXCEEDED);
    const auto owner = m_state;
    std::lock_guard lock(owner->mutex);
    if (!owner->accepting)
        return std::unexpected(arena::rpc::UNAVAILABLE);
    if (owner->operations.size() >= owner->maximum)
        return std::unexpected(arena::rpc::OVERLOADED);
    if (owner->sequence == std::numeric_limits<std::uint64_t>::max())
        return std::unexpected(arena::rpc::INTERNAL);
    const auto id = ++owner->sequence;
    request.set_request_id(id);
    if (request.correlation_id().empty())
        request.set_correlation_id("rpc-" + std::to_string(id));
    if (!correlationValid(request.correlation_id()))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);

    const auto admittedAt = detail::Clock::now();
    if (deadline <= admittedAt)
        return std::unexpected(arena::rpc::DEADLINE_EXCEEDED);
    const auto remaining = ceil<milliseconds>(deadline - admittedAt);
    const auto unixNow = duration_cast<milliseconds>(
                             system_clock::now().time_since_epoch()
    )
                             .count();
    if (unixNow < 0)
        return std::unexpected(arena::rpc::INTERNAL);
    request.set_deadline_unix_ms(
        static_cast<std::uint64_t>(unixNow + remaining.count())
    );
    if (request.ByteSizeLong() > maxFrame)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    auto operation = std::make_shared<detail::Operation>(
        owner,
        request,
        deadline,
        std::move(completion)
    );
    owner->operations.emplace(id, operation);
    try
    {
        boost::asio::post(
            owner->executor,
            [operation] { operation->start(); }
        );
    }
    catch (...)
    {
        owner->operations.erase(id);
        return std::unexpected(arena::rpc::INTERNAL);
    }
    return Call(operation);
}

void Client::stop()
{
    const auto owner = m_state;
    std::lock_guard lock(owner->mutex);
    if (!owner->accepting)
        return;
    owner->accepting = false;
    for (const auto& [id, operation] : owner->operations)
    {
        (void)id;
        operation->cancelRequested.store(true);
    }
    boost::asio::post(
        owner->executor,
        [owner]
        {
            std::unordered_map<
                std::uint64_t,
                std::shared_ptr<detail::Operation>>
                abandoned;
            {
                std::lock_guard lock(owner->mutex);
                abandoned.swap(owner->operations);
            }
            for (const auto& [id, operation] : abandoned)
            {
                (void)id;
                operation->finish(arena::rpc::CANCELLED);
            }
        }
    );
}

std::size_t Client::inflight() const
{
    std::lock_guard lock(m_state->mutex);
    return m_state->operations.size();
}
} // namespace arena::internal_rpc
