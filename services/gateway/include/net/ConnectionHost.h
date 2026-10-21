/** @file
 * @brief Owns one transport, protocol callbacks, and connection read
 * flow.
 */
#pragma once

#include <thread>

#include "net/ConnectionHandler.h"
#include "net/ITransport.h"

namespace service::gateway
{
/** @brief Coordinates owner-thread connection lifetime and transport
 * policies.
 *
 * The worker registry owns the host. The host owns transport and
 * protocol; transport callbacks retain a weak host. All state belongs
 * to the owner, except the independently synchronized bounded
 * completion mailbox.
 */
class ConnectionHost final
    : public ConnectionChannel,
      public std::enable_shared_from_this<ConnectionHost>
{
public:
    static constexpr std::size_t kMaxPostedActions = 256;
    /** @brief Composes a transport with protocol callbacks on its
     * owner. */
    ConnectionHost(
        ITransport::Ptr,
        ConnectionContext::Ptr,
        ConnectionScheduler,
        ConnectionHandler
    );
    ~ConnectionHost() noexcept override;
    /** @brief Returns transport policies for registration before
     * start. */
    ConnectionPipeline& features();
    /** @brief Seals policies and activates the protocol on the owner.
     */
    void start();
    /** @brief Delivers borrowed transport bytes to the protocol. */
    void received(std::span<const std::byte>);
    /** @brief Applies transport output pressure to connection
     * policies. */
    void outputChanged(std::size_t);
    /** @brief Reports transport closure and defers notification until
     * unwind. */
    void transportClosed();
    std::uint64_t id() const noexcept override;
    void send(std::span<const std::byte>) override;
    void shutdown() override;
    void close() override;
    bool connected() const noexcept override;
    bool acceptingInput() const noexcept override;
    void pauseRead() override;
    void resumeRead() override;
    std::size_t pendingBytes() const noexcept override;
    void setReadPaused(ReadPauseReason, bool) override;
    void requestClose() override;
    const ConnectionContext& context() const noexcept override;
    ConnectionTimer scheduleAt(
        ConnectionClock::time_point,
        std::function<void(IConnectionControl&)>
    ) override;
    bool post(ConnectionAction) override;
    ConnectionPost poster() const override;

private:
    struct Mailbox;
    void requireOwner() const;
    void runAction(ConnectionAction);
    void finishDrain();
    void finishEvent() noexcept;
    void notifyClosed() noexcept;
    void stopPosting() noexcept;
    /// Sole thread permitted to mutate connection and protocol state.
    const std::thread::id m_owner{std::this_thread::get_id()};
    /// Concrete socket adapter, never exposed as a protocol channel.
    ITransport::Ptr m_transport;
    /// Immutable identity and owner-thread authentication context.
    ConnectionContext::Ptr m_context;
    /// Owner executor and monotonic timers.
    ConnectionScheduler m_scheduler;
    /// Per-connection protocol lifecycle and byte consumer.
    ConnectionHandler m_handler;
    /// Transport pressure and lifecycle policies only.
    ConnectionPipeline m_features;
    /// Cross-thread completion admission independent of host
    /// lifetime.
    std::shared_ptr<Mailbox> m_mailbox;
    /// Owner-thread close state gates all protocol work.
    bool m_closing{false};
    /// Quiesces protocol admission while accepted requests may still
    /// reply.
    bool m_draining{false};
    /// Makes the protocol-drained callback shut down transport writes
    /// once.
    bool m_writeShutdown{false};
    bool m_started{false}, m_closed{false}, m_closedNotified{false};
    /// Prevents recursive output notifications from traversing policy
    /// twice.
    bool m_notifyingOutput{false};
    /// Delays close hooks until the outermost callback returns.
    unsigned m_eventDepth{0};
    /// Independent causes that must all clear before reads resume.
    std::uint32_t m_pauseReasons{0};
    /// Coalesces output changes caused by synchronous read/write
    /// reentry.
    std::size_t m_reportedOutput{0}, m_nextOutput{0};
};

/** @brief Builds a host with fresh protocol state and transport
 * policies.
 *
 * An empty feature factory installs default backpressure. A supplied
 * factory builds the complete transport policy set; transport hard
 * limits remain.
 */
std::shared_ptr<ConnectionHost> makeConnectionHost(
    ITransport::Ptr,
    ConnectionContext::Ptr,
    ConnectionScheduler,
    const ConnectionHandlerFactory&,
    const ConnectionFeatureFactory& = {}
);
} // namespace service::gateway
