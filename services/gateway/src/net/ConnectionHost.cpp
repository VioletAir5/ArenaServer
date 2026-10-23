/** @file
 * @brief Owns transport lifetime, read flow, and bounded owner
 * dispatch.
 */
#include "net/ConnectionHost.h"

#include <mutex>
#include <stdexcept>
#include <utility>

#include "net/BackpressureFeature.h"
namespace service::gateway
{
// A retained poster owns only this mailbox. In particular foreign
// callers never lock a weak Host: its final release must remain on
// the owner thread.
struct ConnectionHost::Mailbox : std::enable_shared_from_this<Mailbox>
{
    Mailbox(
        std::thread::id ownerThread,
        std::function<void(ConnectionScheduler::Action)> schedule
    )
        : owner(ownerThread)
        , defer(std::move(schedule))
    {
    }

    bool post(ConnectionAction action)
    {
        if (!action)
            return false;
        std::unique_lock lock(mutex);
        if (!active)
            return false;
        if (std::this_thread::get_id() == owner)
        {
            auto target = host;
            lock.unlock();
            if (auto self = target.lock())
            {
                self->runAction(std::move(action));
                return true;
            }
            return false;
        }
        if (pending >= kMaxPostedActions)
            return false;
        ++pending;
        try
        {
            // Hold the admission lock until enqueue returns. close()
            // then forms a barrier before the executor may be
            // released.
            defer(
                [mailbox = shared_from_this(),
                 action = std::move(action)]() mutable
                {
                    // A broken inline foreign scheduler must not
                    // reenter the admission mutex or mutate owner
                    // state.
                    if (std::this_thread::get_id() != mailbox->owner)
                        throw std::logic_error(
                            "connection scheduler "
                            "must enqueue on owner"
                        );
                    std::weak_ptr<ConnectionHost> target;
                    {
                        std::lock_guard guard(mailbox->mutex);
                        --mailbox->pending;
                        if (!mailbox->active)
                            return;
                        target = mailbox->host;
                    }
                    if (auto self = target.lock())
                        self->runAction(std::move(action));
                }
            );
            return true;
        }
        catch (...)
        {
            --pending;
            return false;
        }
    }

    void close() noexcept
    {
        std::lock_guard lock(mutex);
        active = false;
        host.reset();
        defer = {};
    }
    /// Serializes foreign enqueue with owner closure and executor
    /// retirement.
    std::mutex mutex;
    /// Sole thread allowed to acquire and mutate the weak host
    /// target.
    const std::thread::id owner;
    /// Owner enqueue function cleared when completion admission
    /// closes.
    std::function<void(ConnectionScheduler::Action)> defer;
    /// Non-owning target, acquired only on the owner thread.
    std::weak_ptr<ConnectionHost> host;
    /// Accepted foreign actions still waiting in the owner executor.
    std::size_t pending{0};
    /// Whether new completion actions may be accepted.
    bool active{false};
};
ConnectionTimer::ConnectionTimer(std::function<void()> cancel)
    : m_cancel(std::move(cancel))
{
}
ConnectionTimer::~ConnectionTimer()
{
    reset();
}
ConnectionTimer::ConnectionTimer(ConnectionTimer&& other) noexcept
    : m_cancel(std::exchange(other.m_cancel, {}))
{
}
ConnectionTimer&
ConnectionTimer::operator=(ConnectionTimer&& other) noexcept
{
    if (this != &other)
    {
        reset();
        m_cancel = std::exchange(other.m_cancel, {});
    }
    return *this;
}
void ConnectionTimer::reset() noexcept
{
    if (auto cancel = std::exchange(m_cancel, {}))
    {
        try
        {
            cancel();
        }
        catch (...)
        {
            std::terminate();
        }
    }
}
ConnectionHost::ConnectionHost(
    ITransport::Ptr transport,
    ConnectionContext::Ptr context,
    ConnectionScheduler scheduler,
    ConnectionHandler handler
)
    : m_transport(std::move(transport))
    , m_context(std::move(context))
    , m_scheduler(std::move(scheduler))
    , m_handler(std::move(handler))
    , m_mailbox(std::make_shared<Mailbox>(m_owner, m_scheduler.defer))
{
    if (!m_transport || !m_context || !m_scheduler.defer ||
        !m_scheduler.at || !m_handler.onMessage ||
        m_transport->id() != m_context->connectionId)
        throw std::invalid_argument(
            "invalid connection host dependencies"
        );
}
ConnectionHost::~ConnectionHost() noexcept
{
    if (std::this_thread::get_id() != m_owner)
        std::terminate();
    stopPosting();
    notifyClosed();
}
void ConnectionHost::requireOwner() const
{
    if (std::this_thread::get_id() != m_owner)
        throw std::logic_error("connection host requires owner");
}
ConnectionPipeline& ConnectionHost::features()
{
    requireOwner();
    return m_features;
}
void ConnectionHost::start()
{
    requireOwner();
    if (m_started || m_closing || m_draining)
        return;
    if (weak_from_this().expired())
        throw std::logic_error(
            "host start requires shared ownership"
        );
    auto keep = shared_from_this();
    {
        std::lock_guard lock(m_mailbox->mutex);
        m_mailbox->host = weak_from_this();
        m_mailbox->active = true;
    }
    m_started = true;
    m_features.m_sealed = true;
    ++m_eventDepth;
    try
    {
        for (auto& feature : m_features.m_features)
        {
            feature->onConnected(*this);
            if (m_closing || m_draining)
                break;
        }
        if (!m_closing && !m_draining && m_handler.onStarted)
            m_handler.onStarted(*this);
    }
    catch (...)
    {
        m_context->rejections->record(
            RejectionReason::CallbackFailure
        );
        close();
    }
    finishEvent();
    outputChanged(m_transport->pendingBytes());
}
void ConnectionHost::received(std::span<const std::byte> data)
{
    requireOwner();
    if (!acceptingInput())
        return;
    auto keep = shared_from_this();
    ++m_eventDepth;
    try
    {
        m_handler.onMessage(*this, data);
    }
    catch (...)
    {
        m_context->rejections->record(
            RejectionReason::CallbackFailure
        );
        close();
    }
    finishEvent();
}
void ConnectionHost::outputChanged(std::size_t bytes)
{
    requireOwner();
    m_nextOutput = bytes;
    if (!m_started || m_closing || m_notifyingOutput)
        return;
    auto keep = shared_from_this();
    m_notifyingOutput = true;
    ++m_eventDepth;
    try
    {
        while (!m_closing && m_reportedOutput != m_nextOutput)
        {
            const auto before = m_reportedOutput;
            const auto after = m_nextOutput;
            m_reportedOutput = after;
            for (auto& feature : m_features.m_features)
            {
                feature->onOutputBufferChanged(*this, before, after);
                if (m_closing)
                    break;
            }
        }
    }
    catch (...)
    {
        m_context->rejections->record(
            RejectionReason::CallbackFailure
        );
        close();
    }
    m_notifyingOutput = false;
    finishEvent();
}
void ConnectionHost::transportClosed()
{
    requireOwner();
    auto keep = shared_from_this();
    m_closing = true;
    stopPosting();
    m_closed = true;
    if (!m_eventDepth)
        notifyClosed();
}
void ConnectionHost::finishEvent() noexcept
{
    --m_eventDepth;
    if (!m_eventDepth && m_closed)
        notifyClosed();
}
void ConnectionHost::notifyClosed() noexcept
{
    if (m_closedNotified)
        return;
    m_closedNotified = true;
    try
    {
        if (m_handler.onClosed)
            m_handler.onClosed();
    }
    catch (...)
    {
        m_context->rejections->record(
            RejectionReason::CallbackFailure
        );
    }
    for (auto& feature : m_features.m_features)
        feature->onClosed();
}
std::uint64_t ConnectionHost::id() const noexcept
{
    return m_transport->id();
}
void ConnectionHost::send(std::span<const std::byte> data)
{
    requireOwner();
    if (!m_closing && !m_writeShutdown)
        m_transport->send(data);
}
void ConnectionHost::shutdown()
{
    requireOwner();
    if (m_closing || m_draining)
        return;
    auto keep = shared_from_this();
    m_draining = true;
    ++m_eventDepth;
    try
    {
        // Once input is quiescent, reads only discard bytes or
        // observe EOF. A stale application/authentication pause must
        // not hide peer FIN.
        if (m_pauseReasons)
        {
            m_pauseReasons = 0;
            m_transport->resumeRead();
        }
        if (m_closing)
        {
            finishEvent();
            return;
        }
        if (m_handler.onDraining)
            m_handler.onDraining(
                [weak = weak_from_this(), owner = m_owner]
                {
                    // The protocol drains its request map on the
                    // owner. Check before weak acquisition to
                    // preserve owner-only final release.
                    if (std::this_thread::get_id() != owner)
                        throw std::logic_error(
                            "protocol drain completion requires owner"
                        );
                    if (auto self = weak.lock())
                        self->finishDrain();
                }
            );
        else
            finishDrain();
    }
    catch (...)
    {
        m_context->rejections->record(
            RejectionReason::CallbackFailure
        );
        close();
    }
    finishEvent();
}
void ConnectionHost::finishDrain()
{
    requireOwner();
    if (!m_draining || m_closing || m_writeShutdown)
        return;
    auto keep = shared_from_this();
    m_writeShutdown = true;
    ++m_eventDepth;
    try
    {
        // Native reads continue to observe peer EOF; received()
        // discards application bytes as soon as draining starts.
        m_transport->shutdown();
    }
    catch (...)
    {
        m_context->rejections->record(
            RejectionReason::CallbackFailure
        );
        close();
    }
    finishEvent();
}
bool ConnectionHost::connected() const noexcept
{
    return !m_closing && m_transport->connected();
}
bool ConnectionHost::acceptingInput() const noexcept
{
    return m_started && !m_closing && !m_draining;
}
void ConnectionHost::setReadPaused(
    ReadPauseReason reason, bool paused
)
{
    requireOwner();
    if (m_closing || m_draining)
        return;
    const auto bit = static_cast<std::uint32_t>(reason);
    const auto before = m_pauseReasons;
    if (paused)
        m_pauseReasons |= bit;
    else
        m_pauseReasons &= ~bit;
    if (!before && m_pauseReasons)
        m_transport->pauseRead();
    // Native resume may synchronously read and reenter
    // received/outputChanged.
    if (before && !m_pauseReasons)
        m_transport->resumeRead();
}
void ConnectionHost::close()
{
    requireOwner();
    if (m_closing)
        return;
    auto keep = shared_from_this();
    m_closing = true;
    stopPosting();
    auto weak = weak_from_this();
    try
    {
        m_transport->pauseRead();
        m_scheduler.defer(
            [weak]
            {
                if (auto self = weak.lock())
                {
                    self->m_transport->close();
                }
            }
        );
    }
    catch (...)
    {
        m_transport->close();
    }
}
const ConnectionContext& ConnectionHost::context() const noexcept
{
    return *m_context;
}
ConnectionTimer ConnectionHost::scheduleAt(
    ConnectionClock::time_point when, ConnectionAction callback
)
{
    requireOwner();
    if (m_closing)
        return ConnectionTimer{};
    return m_scheduler.at(
        when,
        [weak = weak_from_this(), callback = std::move(callback)]
        {
            if (auto self = weak.lock(); self && !self->m_closing)
            {
                ++self->m_eventDepth;
                try
                {
                    callback(*self);
                }
                catch (...)
                {
                    self->m_context->rejections->record(
                        RejectionReason::CallbackFailure
                    );
                    self->close();
                }
                self->finishEvent();
            }
        }
    );
}
ConnectionPost ConnectionHost::poster() const
{
    return [mailbox = m_mailbox](ConnectionAction action)
    { return mailbox->post(std::move(action)); };
}
void ConnectionHost::stopPosting() noexcept
{
    m_mailbox->close();
}
void ConnectionHost::runAction(ConnectionAction action)
{
    requireOwner();
    if (!m_started || m_closing)
        return;
    ++m_eventDepth;
    try
    {
        action(*this);
    }
    catch (...)
    {
        m_context->rejections->record(
            RejectionReason::CallbackFailure
        );
        close();
    }
    finishEvent();
}
std::shared_ptr<ConnectionHost> makeConnectionHost(
    ITransport::Ptr transport,
    ConnectionContext::Ptr context,
    ConnectionScheduler scheduler,
    const ConnectionHandlerFactory& handlers,
    const ConnectionFeatureFactory& features
)
{
    if (!context)
        throw std::invalid_argument("missing connection context");
    auto handler = handlers ? handlers(context)
                            : ConnectionHandler(
                                  [](ConnectionChannel& channel,
                                     std::span<const std::byte> bytes)
                                  { channel.send(bytes); }
                              );
    auto host = std::make_shared<ConnectionHost>(
        std::move(transport),
        std::move(context),
        std::move(scheduler),
        std::move(handler)
    );
    if (features)
        features(host->features(), host->context());
    else
        host->features().add<BackpressureFeature>();
    return host;
}
} // namespace service::gateway
