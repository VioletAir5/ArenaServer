# services/gateway/include/net/ConnectionRuntime.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；105行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Coordinates transport, context, features, and packet dispatch.
 */
#pragma once
#include <atomic>
#include <thread>

#include "net/ConnectionFeature.h"
#include "net/ConnectionHandler.h"

namespace service::gateway
{
// Registry owns Runtime; Runtime owns transport, context and
// features. Transport callbacks capture weak Runtime. All
// events/control/final release belong to owner; send() alone may
// delegate from foreign callers.
/** @brief Coordinates transport, context, features, and packet dispatch. */
class ConnectionRuntime final
    : public IConnection,
      public IConnectionControl,
      public PacketFeatureHost,
      public std::enable_shared_from_this<ConnectionRuntime>
{
public:
    ConnectionRuntime(
        IConnection::Ptr,
        ConnectionContext::Ptr,
        ConnectionScheduler,
        ConnectionMessageCallback
    );
    /** @brief Releases owned resources. */
    ~ConnectionRuntime() noexcept override;
    /** @brief Returns the connection policy pipeline. */
    ConnectionPipeline& features();
    void start();
    void received(std::span<const std::byte>);
    void outputChanged(std::size_t);
    void transportClosed();
    /** @brief Returns the process-wide connection identifier. */
    std::uint64_t id() const noexcept override;
    /** @brief Queues bytes for transmission. */
    void send(std::span<const std::byte>) override;
    void shutdown() override;
    void close() override;
    /** @brief Reports whether the connection is still active. */
    bool connected() const noexcept override;
    /** @brief Pauses inbound reads. */
    void pauseRead() override;
    /** @brief Resumes inbound reads. */
    void resumeRead() override;
    /** @brief Returns the number of bytes awaiting transmission. */
    std::size_t pendingBytes() const noexcept override;
    /** @brief Sets read paused. */
    void setReadPaused(ReadPauseReason, bool) override;
    /** @brief Requests connection closure on the owner thread. */
    void requestClose() override;
    /** @brief Returns the immutable connection context. */
    const ConnectionContext& context() const noexcept override;
    ConnectionTimer
        scheduleAt(ConnectionClock::time_point, std::function<void(IConnectionControl&)>)
            override;
    /** @brief Inspects packet. */
    FeatureResult inspectPacket(PacketView) override;

private:
    void requireOwner() const;
    void finishEvent() noexcept;
    /** @brief Notifies closed. */
    void notifyClosed() noexcept;
    /// Thread that owns mutable connection state.
    const std::thread::id m_owner{std::this_thread::get_id()};
    /// Underlying connection transport.
    IConnection::Ptr m_transport;
    /// Identity and session context for this connection.
    ConnectionContext::Ptr m_context;
    /// Stores the scheduler.
    ConnectionScheduler m_scheduler;
    /// Stores the handler.
    ConnectionMessageCallback m_handler;
    /// Policies applied to connection events.
    ConnectionPipeline m_features;
    /// Stores the closing.
    std::atomic<bool> m_closing{false};
    /// Stores the started.
    bool m_started{false}, m_closed{false}, m_closedNotified{false};
    /// Stores the notifying output.
    bool m_notifyingOutput{false};
    /// Stores the event depth.
    unsigned m_eventDepth{0};
    /// Stores the pause reasons.
    std::uint32_t m_pauseReasons{0};
    /// Stores the reported output.
    std::size_t m_reportedOutput{0}, m_nextOutput{0};
};
// Empty feature factory selects BackpressureFeature. A supplied
// factory builds the complete policy set; transport hard limits
// always remain.
std::shared_ptr<ConnectionRuntime> makeConnectionRuntime(
    IConnection::Ptr,
    ConnectionContext::Ptr,
    ConnectionScheduler,
    const ConnectionHandlerFactory&,
    const ConnectionFeatureFactory& = {}
);
} // namespace service::gateway
~~~
