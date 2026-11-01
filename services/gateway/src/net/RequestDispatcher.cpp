/** @file
 * @brief Implements bounded request state independently of
 * frame/protobuf codecs.
 */
#include "net/RequestDispatcher.h"

#include <limits>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "net/RequestExecution.h"

#include "runtime/OwnerScheduler.h"

namespace service::gateway
{
struct RequestDispatcher::State
{
    struct Pending
    {
        ReplyMetadata metadata;
        std::uint64_t generation;
        arena::runtime::OwnerClock::time_point deadline;
        arena::runtime::OwnerTimer timer;
    };

    ConnectionContext::Ptr context;
    std::shared_ptr<const MessageRegistry> registry;
    ReplySink reply;
    std::shared_ptr<spdlog::logger> logger;
    RequestAdmission admission;
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

    struct CompletionScope
    {
        State& state;
        explicit CompletionScope(State& owner)
            : state(owner)
        {
            ++state.replyDepth;
        }
        ~CompletionScope()
        {
            --state.replyDepth;
            try
            {
                state.notifyDrained();
            }
            catch (...)
            {
            }
        }
    };

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
        RequestExecution::OwnerAction action,
        bool cancelled = false
    )
    {
        const auto found = pending.find(id);
        if (stopped || found == pending.end() ||
            found->second.generation != ticket)
            return;
        // Removing the pending entry must not let a reentrant drain
        // overtake the owner action or its accepted reply.
        CompletionScope completion(*this);
        auto entry = std::move(found->second);
        pending.erase(found);
        entry.timer.reset();
        const bool expired = arena::runtime::OwnerClock::now() >=
                             entry.deadline;
        const bool disconnected = !channel.connected() ||
                                  context->session->superseded();
        entry.metadata.request->execution->retire(
            cancelled || expired || disconnected
        );
        // Cancellation can reenter the host and close it inline.
        if (stopped || !channel.connected() ||
            context->session->superseded())
        {
            log("request_end",
                *entry.metadata.request,
                arena::protocol::ERR_OK,
                "cancelled");
            return;
        }
        MessageReply result = std::unexpected(
            arena::protocol::ERR_TIMEOUT
        );
        if (!expired)
        {
            try
            {
                result = action();
            }
            catch (const BusinessError& error)
            {
                result = std::unexpected(error.code());
            }
            catch (...)
            {
                result = std::unexpected(
                    arena::protocol::ERR_INTERNAL
                );
            }
        }
        respond(channel, entry.metadata, std::move(result));
        notifyDrained();
    }

    void stop() noexcept
    {
        if (stopped)
            return;
        stopped = true;
        drained = {};
        // Cancellation hooks may reenter shutdown. Remove all entries
        // before invoking them and never hold an iterator across one.
        auto cancelled = std::move(pending);
        pending.clear();
        for (auto& [id, entry] : cancelled)
        {
            (void)id;
            entry.timer.reset();
            entry.metadata.request->execution->retire(true);
            log("request_end",
                *entry.metadata.request,
                arena::protocol::ERR_OK,
                "cancelled");
        }
    }
};

RequestDispatcher::RequestDispatcher(
    ConnectionContext::Ptr context,
    std::shared_ptr<const MessageRegistry> registry,
    ReplySink sink,
    std::shared_ptr<spdlog::logger> logger,
    RequestAdmission admission
)
{
    if (!context || !registry || !registry->sealed() || !sink)
        throw std::invalid_argument(
            "dispatcher requires context, "
            "sealed registry and reply sink"
        );
    m_state = std::make_shared<State>();
    m_state->admission = std::move(admission);
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
            []() -> MessageReply
            {
                return std::unexpected(
                    arena::protocol::ERR_DUPLICATE_REQUEST
                );
            },
            true
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
    if (m_state->admission)
    {
        bool allowed = false;
        try
        {
            allowed = m_state->admission(request->msgId);
        }
        catch (...)
        {
        }
        if (!allowed)
            return reject(arena::protocol::ERR_UNAVAILABLE);
    }
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
    if (m_state->generation ==
        std::numeric_limits<std::uint64_t>::max())
        return reject(arena::protocol::ERR_UNAVAILABLE);

    const auto id = request->requestId;
    const auto ticket = ++m_state->generation;
    const auto deadline = arena::runtime::OwnerClock::now() +
                          route->options.timeout;
    const std::weak_ptr<State> weak = m_state;
    auto execution = std::make_shared<RequestExecution>(
        deadline,
        "connection-" + std::to_string(request->connectionId) +
            "-request-" + std::to_string(id),
        [weak, post = channel.poster(), id, ticket](
            RequestExecution::OwnerAction action
        )
        {
            // Business threads retain only a weak state and the
            // bounded owner endpoint, never a borrowed channel.
            return post(
                [weak, id, ticket, action = std::move(action)](
                    ConnectionChannel& owner
                ) mutable
                {
                    if (auto state = weak.lock())
                        state->finish(
                            owner,
                            id,
                            ticket,
                            std::move(action)
                        );
                }
            );
        }
    );
    auto owned = std::make_shared<RequestMessage>(*request);
    owned->execution = execution;
    metadata.request = owned;
    m_state->pending.emplace(
        id,
        State::Pending{
            metadata,
            ticket,
            deadline,
            arena::runtime::OwnerTimer{}
        }
    );
    const RequestCompletion complete =
        [execution](MessageReply result)
    { execution->complete(std::move(result)); };
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
                        []() -> MessageReply
                        {
                            return std::unexpected(
                                arena::protocol::ERR_TIMEOUT
                            );
                        },
                        true
                    );
            }
        );
        m_state->pending.at(id).timer = std::move(timer);
    }
    catch (...)
    {
        // Without a timer, a rejected mailbox post could otherwise
        // leave an admitted request permanently pending.
        m_state->finish(
            channel,
            id,
            ticket,
            []() -> MessageReply
            {
                return std::unexpected(arena::protocol::ERR_INTERNAL);
            },
            true
        );
        return;
    }
    try
    {
        m_state
            ->log("request_handler", *owned, arena::protocol::ERR_OK);
        route->handler(std::move(owned), complete);
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
