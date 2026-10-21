/** @file
 * @brief Defines connection policies and their lifecycle hooks.
 */
#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include "net/ConnectionContext.h"

namespace service::gateway
{
using ConnectionClock = std::chrono::steady_clock;
// Destroy/reset on the owner to cancel. The scheduler outlives
// tokens.
/** @brief Cancels an owner-thread timer when its token is reset or
 * destroyed. */
class ConnectionTimer
{
public:
    explicit ConnectionTimer(std::function<void()> cancel = {});
    ~ConnectionTimer();
    ConnectionTimer(ConnectionTimer&&) noexcept;
    ConnectionTimer& operator=(ConnectionTimer&&) noexcept;
    ConnectionTimer(const ConnectionTimer&) = delete;
    /** @brief Cancels the held timer and clears its action. */
    void reset() noexcept;

private:
    /// Cancellation action for the scheduled timer.
    std::function<void()> m_cancel;
};
/** @brief Provides owner-thread callback and timer scheduling to
 * features. */
struct ConnectionScheduler
{
    using Action = std::function<void()>;
    /// Defers an action to the connection owner.
    std::function<void(Action)> defer;
    /// Schedules an action for a monotonic deadline.
    std::function<
        ConnectionTimer(ConnectionClock::time_point, Action)>
        at;
};
/** @brief Represents read pause reason. */
enum class ReadPauseReason : std::uint32_t
{
    Backpressure = 1,
    RateLimit = 2,
    Authenticating = 4,
    Application = 8
};
/** @brief Borrows a decoded packet type and payload for one callback.
 */
struct PacketView
{
    /// Packet type from the decoded header.
    std::uint16_t type;
    /// Borrowed payload valid for the current callback.
    std::span<const std::byte> payload;
};
/** @brief Controls packet policy processing. */
enum class FeatureResult
{
    Continue,
    Drop,
    Close
};
/** @brief Exposes owner-thread connection controls to policy
 * features. */
class IConnectionControl
{
public:
    virtual ~IConnectionControl() = default;
    /** @brief Sets read paused. */
    virtual void setReadPaused(ReadPauseReason, bool) = 0;
    /** @brief Returns the number of bytes awaiting transmission. */
    virtual std::size_t pendingBytes() const noexcept = 0;
    /** @brief Requests connection closure on the owner thread. */
    virtual void requestClose() = 0;
    /** @brief Returns the immutable connection context. */
    virtual const ConnectionContext& context() const noexcept = 0;
    // Callback runs on owner, only while the runtime remains open.
    virtual ConnectionTimer scheduleAt(
        ConnectionClock::time_point,
        std::function<void(IConnectionControl&)>
    ) = 0;
};
/** @brief Receives connection lifecycle and packet policy hooks. */
class IConnectionFeature
{
public:
    virtual ~IConnectionFeature() = default;
    virtual void onConnected(IConnectionControl&)
    {
    }
    virtual FeatureResult onPacket(IConnectionControl&, PacketView)
    {
        return FeatureResult::Continue;
    }
    virtual void onOutputBufferChanged(
        IConnectionControl&, std::size_t, std::size_t
    )
    {
    }
    virtual void onClosed() noexcept
    {
    }
};
// Build before Runtime::start(). Nodes are per connection; no hot
// removal.
/** @brief Owns the ordered policy features for one connection. */
class ConnectionPipeline
{
public:
    ConnectionPipeline() = default;
    ConnectionPipeline(const ConnectionPipeline&) = delete;
    ConnectionPipeline& operator=(const ConnectionPipeline&) = delete;
    ConnectionPipeline(ConnectionPipeline&&) = delete;
    ConnectionPipeline& operator=(ConnectionPipeline&&) = delete;
    template <class T, class... Args> T& add(Args&&... args)
    {
        if (m_sealed)
            throw std::logic_error("connection pipeline is sealed");
        auto node = std::make_unique<T>(std::forward<Args>(args)...);
        auto& result = *node;
        m_features.push_back(std::move(node));
        return result;
    }

private:
    friend class ConnectionRuntime;
    /// Whether feature registration has ended.
    bool m_sealed{false};
    /// Policies applied to connection events.
    std::vector<std::unique_ptr<IConnectionFeature>> m_features;
};
// Copied into workers; builds fresh per-connection nodes on owner.
// Shared captures/configuration need their own synchronization.
// Empty server factory selects default BackpressureFeature.
using ConnectionFeatureFactory = std::function<
    void(ConnectionPipeline&, const ConnectionContext&)>;
// PacketCodec calls this after framing, before protobuf/business
// handlers.
/** @brief Allows the packet decoder to apply connection policies. */
class PacketFeatureHost
{
public:
    virtual ~PacketFeatureHost() = default;
    /** @brief Inspects packet. */
    virtual FeatureResult inspectPacket(PacketView) = 0;
};
} // namespace service::gateway
