#include "net/ConnectionRuntime.h"

#include <stdexcept>
#include <utility>
namespace service::gateway
{
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
ConnectionTimer& ConnectionTimer::operator=(ConnectionTimer&& other
) noexcept
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
ConnectionRuntime::ConnectionRuntime(
    IConnection::Ptr transport,
    ConnectionContext::Ptr context,
    ConnectionScheduler scheduler,
    ConnectionMessageCallback handler
)
    : m_transport(std::move(transport))
    , m_context(std::move(context))
    , m_scheduler(std::move(scheduler))
    , m_handler(std::move(handler))
{
    if (!m_transport || !m_context || !m_scheduler.defer ||
        !m_scheduler.at || !m_handler ||
        m_transport->id() != m_context->connectionId)
        throw std::invalid_argument(
            "invalid connection runtime dependencies"
        );
}
ConnectionRuntime::~ConnectionRuntime() noexcept
{
    if (std::this_thread::get_id() != m_owner)
        std::terminate();
    notifyClosed();
}
void ConnectionRuntime::requireOwner() const
{
    if (std::this_thread::get_id() != m_owner)
        throw std::logic_error("connection runtime requires owner");
}
ConnectionPipeline& ConnectionRuntime::features()
{
    requireOwner();
    return m_features;
}
void ConnectionRuntime::start()
{
    requireOwner();
    if (m_started || m_closing)
        return;
    if (weak_from_this().expired())
        throw std::logic_error(
            "runtime start requires shared ownership"
        );
    auto keep = shared_from_this();
    m_started = true;
    m_features.m_sealed = true;
    ++m_eventDepth;
    try
    {
        for (auto& feature : m_features.m_features)
        {
            feature->onConnected(*this);
            if (m_closing)
                break;
        }
    }
    catch (...)
    {
        requestClose();
    }
    finishEvent();
    outputChanged(m_transport->pendingBytes());
}
void ConnectionRuntime::received(std::span<const std::byte> data)
{
    requireOwner();
    if (!m_started || m_closing)
        return;
    auto keep = shared_from_this();
    ++m_eventDepth;
    try
    {
        m_handler(*this, data);
    }
    catch (...)
    {
        requestClose();
    }
    finishEvent();
}
void ConnectionRuntime::outputChanged(std::size_t bytes)
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
        requestClose();
    }
    m_notifyingOutput = false;
    finishEvent();
}
void ConnectionRuntime::transportClosed()
{
    requireOwner();
    auto keep = shared_from_this();
    m_closing = true;
    m_closed = true;
    if (!m_eventDepth)
        notifyClosed();
}
void ConnectionRuntime::finishEvent() noexcept
{
    --m_eventDepth;
    if (!m_eventDepth && m_closed)
        notifyClosed();
}
void ConnectionRuntime::notifyClosed() noexcept
{
    if (m_closedNotified)
        return;
    m_closedNotified = true;
    for (auto& feature : m_features.m_features)
        feature->onClosed();
}
std::uint64_t ConnectionRuntime::id() const noexcept
{
    return m_transport->id();
}
void ConnectionRuntime::send(std::span<const std::byte> data)
{
    if (!m_closing.load())
        m_transport->send(data);
}
void ConnectionRuntime::shutdown()
{
    requireOwner();
    m_transport->shutdown();
}
void ConnectionRuntime::close()
{
    requestClose();
}
bool ConnectionRuntime::connected() const noexcept
{
    return !m_closing.load() && m_transport->connected();
}
void ConnectionRuntime::pauseRead()
{
    setReadPaused(ReadPauseReason::Application, true);
}
void ConnectionRuntime::resumeRead()
{
    setReadPaused(ReadPauseReason::Application, false);
}
std::size_t ConnectionRuntime::pendingBytes() const noexcept
{
    return m_transport->pendingBytes();
}
void ConnectionRuntime::setReadPaused(
    ReadPauseReason reason, bool paused
)
{
    requireOwner();
    if (m_closing)
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
void ConnectionRuntime::requestClose()
{
    requireOwner();
    if (m_closing.exchange(true))
        return;
    auto weak = weak_from_this();
    try
    {
        m_scheduler.defer(
            [weak]
            {
                if (auto self = weak.lock())
                {
                    self->m_transport->close();
                    self->transportClosed();
                }
            }
        );
    }
    catch (...)
    {
        m_transport->close();
        transportClosed();
    }
}
const ConnectionContext& ConnectionRuntime::context() const noexcept
{
    return *m_context;
}
ConnectionTimer ConnectionRuntime::scheduleAt(
    ConnectionClock::time_point when,
    std::function<void(IConnectionControl&)> callback
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
                    self->requestClose();
                }
                self->finishEvent();
            }
        }
    );
}
FeatureResult ConnectionRuntime::inspectPacket(PacketView packet)
{
    requireOwner();
    if (!m_started || m_closing)
        return FeatureResult::Close;
    auto keep = shared_from_this();
    ++m_eventDepth;
    auto result = FeatureResult::Continue;
    try
    {
        for (auto& feature : m_features.m_features)
        {
            result = feature->onPacket(*this, packet);
            if (m_closing)
                result = FeatureResult::Close;
            if (result != FeatureResult::Continue)
                break;
        }
    }
    catch (...)
    {
        result = FeatureResult::Close;
    }
    if (result == FeatureResult::Close)
        requestClose();
    finishEvent();
    return result;
}
} // namespace service::gateway