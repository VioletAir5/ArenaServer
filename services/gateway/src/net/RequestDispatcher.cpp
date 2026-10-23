/** @file
 * @brief Implements bounded request state independently of
 * frame/protobuf codecs.
 */
#include "net/RequestDispatcher.h"

#include <atomic>
#include <thread>
#include <unordered_map>
#include <utility>

namespace service::gateway
{
struct RequestDispatcher::State
{
    struct Pending
    {
        ReplyMetadata metadata;
        std::uint64_t generation;
        ConnectionClock::time_point deadline;
        ConnectionTimer timer;
    };

    ConnectionContext::Ptr context;
    std::shared_ptr<const MessageRegistry> registry;
    ReplySink reply;
    std::shared_ptr<spdlog::logger> logger;
    const std::thread::id owner{std::this_thread::get_id()};
    std::unordered_map<std::uint64_t, Pending> pending;
    std::uint64_t generation{0};
    bool stopped{false};
    bool draining{false};
    unsigned replyDepth{0};
    // The host supplies a weak owner endpoint; no channel reference
    // is retained.
    std::function<void()> drained;

    void notifyDrained()
    {
        if (!stopped && draining && pending.empty() && !replyDepth)
            if (auto callback = std::exchange(drained, {}))
                callback();
    }

    void
    log(std::string_view event,
        const RequestMessage& request,
        ErrorCode code,
        std::string_view outcome = "reply") noexcept
    {
        try
        {
            logger->info(
                "event={} connection_id={} request_id={} msg_id={} "
                "code={} outcome={}",
                event,
                request.connectionId,
                request.requestId,
                request.msgId,
                static_cast<int>(code),
                outcome
            );
        }
        catch (...)
        {
        }
    }

    void respond(
        ConnectionChannel& channel,
        const ReplyMetadata& metadata,
        MessageReply result
    )
    {
        if (!channel.connected() || context->session->superseded())
        {
            log("request_end",
                *metadata.request,
                arena::protocol::ERR_OK,
                "cancelled");
            return;
        }
        ++replyDepth;
        ErrorCode code;
        try
        {
            code = reply(channel, metadata, std::move(result));
        }
        catch (...)
        {
            --replyDepth;
            throw;
        }
        --replyDepth;
        log("request_end", *metadata.request, code);
    }

    void finish(
        ConnectionChannel& channel,
        std::uint64_t id,
        std::uint64_t ticket,
        MessageReply result
    )
    {
        const auto found = pending.find(id);
        if (stopped || found == pending.end() ||
            found->second.generation != ticket)
            return;
        auto entry = std::move(found->second);
        pending.erase(found);
        entry.timer.reset();
        if (ConnectionClock::now() >= entry.deadline)
            result = std::unexpected(arena::protocol::ERR_TIMEOUT);
        respond(channel, entry.metadata, std::move(result));
        notifyDrained();
    }

    void stop() noexcept
    {
        stopped = true;
        drained = {};
        for (const auto& [id, entry] : pending)
        {
            (void)id;
            log("request_end",
                *entry.metadata.request,
                arena::protocol::ERR_OK,
                "cancelled");
        }
        pending.clear();
    }
};

RequestDispatcher::RequestDispatcher(
    ConnectionContext::Ptr context,
    std::shared_ptr<const MessageRegistry> registry,
    ReplySink sink,
    std::shared_ptr<spdlog::logger> logger
)
{
    if (!context || !registry || !registry->sealed() || !sink)
        throw std::invalid_argument(
            "dispatcher requires context, "
            "sealed registry and reply sink"
        );
    m_state = std::make_shared<State>();
    m_state->context = std::move(context);
    m_state->registry = std::move(registry);
    m_state->reply = std::move(sink);
    m_state->logger = logger ? std::move(logger)
                             : spdlog::default_logger();
}

RequestDispatcher::~RequestDispatcher()
{
    stop();
}

void RequestDispatcher::stop() noexcept
{
    m_state->stop();
}

void RequestDispatcher::drain(std::function<void()> drained)
{
    if (std::this_thread::get_id() != m_state->owner)
        throw std::logic_error(
            "request drain requires connection owner"
        );
    if (m_state->stopped || m_state->draining)
        return;
    if (!drained)
        throw std::invalid_argument(
            "request drain requires completion callback"
        );
    m_state->draining = true;
    m_state->drained = std::move(drained);
    m_state->notifyDrained();
}

void RequestDispatcher::dispatch(
    ConnectionChannel& channel,
    ReplyMetadata metadata,
    bool validRequest
)
{
    if (std::this_thread::get_id() != m_state->owner)
        throw std::logic_error(
            "request dispatcher requires connection owner"
        );
    if (m_state->stopped || m_state->draining || !channel.connected())
        return;
    const auto& request = metadata.request;
    if (!request || channel.id() != m_state->context->connectionId ||
        request->connectionId != channel.id())
        throw std::invalid_argument(
            "request belongs to another connection"
        );
    if (m_state->context->session->superseded())
    {
        m_state->context->rejections->record(
            RejectionReason::Unauthenticated
        );
        channel.close();
        return;
    }
    if (const auto found = m_state->pending.find(request->requestId);
        found != m_state->pending.end())
    {
        m_state->finish(
            channel,
            request->requestId,
            found->second.generation,
            std::unexpected(arena::protocol::ERR_DUPLICATE_REQUEST)
        );
        return;
    }
    m_state->log("request_start", *request, arena::protocol::ERR_OK);
    const auto reject = [&](ErrorCode code)
    {
        m_state->respond(channel, metadata, std::unexpected(code));
        m_state->notifyDrained();
    };
    if (!validRequest || !request->requestId)
        return reject(arena::protocol::ERR_INVALID_REQUEST);
    const auto* route = m_state->registry->find(request->msgId);
    if (!route)
        return reject(arena::protocol::ERR_UNKNOWN_MESSAGE);
    const auto login = m_state->context->session->loginState();
    if (login == ConnectionSession::LoginState::Expired ||
        (route->options.requiresAuthentication &&
         login != ConnectionSession::LoginState::Authenticated))
        return reject(arena::protocol::ERR_UNAUTHENTICATED);
    if (m_state->pending.size() >= kMaxPendingRequests)
        return reject(arena::protocol::ERR_TOO_MANY_REQUESTS);

    const auto id = request->requestId;
    const auto ticket = ++m_state->generation;
    const auto deadline = ConnectionClock::now() +
                          route->options.timeout;
    m_state->pending.emplace(
        id,
        State::Pending{metadata, ticket, deadline, ConnectionTimer{}}
    );
    const std::weak_ptr<State> weak = m_state;
    const auto claimed = std::make_shared<std::atomic<bool>>(false);
    const RequestCompletion complete =
        [weak, post = channel.poster(), claimed, id, ticket](
            MessageReply result
        )
    {
        if (claimed->exchange(true, std::memory_order_relaxed))
            return;
        // No state or channel is acquired on the business thread. A
        // full mailbox leaves the bounded request pending until its
        // existing deadline fires.
        post(
            [weak, id, ticket, result = std::move(result)](
                ConnectionChannel& owner
            ) mutable
            {
                if (auto state = weak.lock())
                    state->finish(
                        owner,
                        id,
                        ticket,
                        std::move(result)
                    );
            }
        );
    };
    try
    {
        auto timer = channel.scheduleAt(
            deadline,
            [weak, id, ticket](ConnectionChannel& owner)
            {
                if (auto state = weak.lock())
                    state->finish(
                        owner,
                        id,
                        ticket,
                        std::unexpected(arena::protocol::ERR_TIMEOUT)
                    );
            }
        );
        m_state->pending.at(id).timer = std::move(timer);
        m_state->log(
            "request_handler",
            *request,
            arena::protocol::ERR_OK
        );
        route->handler(request, complete);
    }
    catch (const BusinessError& error)
    {
        complete(std::unexpected(error.code()));
    }
    catch (...)
    {
        complete(std::unexpected(arena::protocol::ERR_INTERNAL));
    }
}
} // namespace service::gateway
