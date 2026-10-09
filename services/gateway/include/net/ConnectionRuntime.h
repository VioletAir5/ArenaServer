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
    ~ConnectionRuntime() noexcept override;
    ConnectionPipeline& features();
    void start();
    void received(std::span<const std::byte>);
    void outputChanged(std::size_t);
    void transportClosed();
    std::uint64_t id() const noexcept override;
    void send(std::span<const std::byte>) override;
    void shutdown() override;
    void close() override;
    bool connected() const noexcept override;
    void pauseRead() override;
    void resumeRead() override;
    std::size_t pendingBytes() const noexcept override;
    void setReadPaused(ReadPauseReason, bool) override;
    void requestClose() override;
    const ConnectionContext& context() const noexcept override;
    ConnectionTimer
        scheduleAt(ConnectionClock::time_point, std::function<void(IConnectionControl&)>)
            override;
    FeatureResult inspectPacket(PacketView) override;

private:
    void requireOwner() const;
    void finishEvent() noexcept;
    void notifyClosed() noexcept;
    const std::thread::id m_owner{std::this_thread::get_id()};
    IConnection::Ptr m_transport;
    ConnectionContext::Ptr m_context;
    ConnectionScheduler m_scheduler;
    ConnectionMessageCallback m_handler;
    ConnectionPipeline m_features;
    std::atomic<bool> m_closing{false};
    bool m_started{false}, m_closed{false}, m_closedNotified{false};
    bool m_notifyingOutput{false};
    unsigned m_eventDepth{0};
    std::uint32_t m_pauseReasons{0};
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
