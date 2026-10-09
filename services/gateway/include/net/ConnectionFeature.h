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
class ConnectionTimer
{
public:
    explicit ConnectionTimer(std::function<void()> cancel = {});
    ~ConnectionTimer();
    ConnectionTimer(ConnectionTimer&&) noexcept;
    ConnectionTimer& operator=(ConnectionTimer&&) noexcept;
    ConnectionTimer(const ConnectionTimer&) = delete;
    void reset() noexcept;

private:
    std::function<void()> m_cancel;
};
struct ConnectionScheduler
{
    using Action = std::function<void()>;
    std::function<void(Action)> defer;
    std::function<
        ConnectionTimer(ConnectionClock::time_point, Action)>
        at;
};
enum class ReadPauseReason : std::uint32_t
{
    Backpressure = 1,
    RateLimit = 2,
    Authenticating = 4,
    Application = 8
};
struct PacketView
{
    std::uint16_t type;
    std::span<const std::byte> payload;
};
enum class FeatureResult
{
    Continue,
    Drop,
    Close
};
class IConnectionControl
{
public:
    virtual ~IConnectionControl() = default;
    virtual void setReadPaused(ReadPauseReason, bool) = 0;
    virtual std::size_t pendingBytes() const noexcept = 0;
    virtual void requestClose() = 0;
    virtual const ConnectionContext& context() const noexcept = 0;
    // Callback runs on owner, only while the runtime remains open.
    virtual ConnectionTimer
        scheduleAt(ConnectionClock::time_point, std::function<void(IConnectionControl&)>) = 0;
};
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
    bool m_sealed{false};
    std::vector<std::unique_ptr<IConnectionFeature>> m_features;
};
// Copied into workers; builds fresh per-connection nodes on owner.
// Shared captures/configuration need their own synchronization.
// Empty server factory selects default BackpressureFeature.
using ConnectionFeatureFactory = std::function<
    void(ConnectionPipeline&, const ConnectionContext&)>;
// PacketCodec calls this after framing, before protobuf/business
// handlers.
class PacketFeatureHost
{
public:
    virtual ~PacketFeatureHost() = default;
    virtual FeatureResult inspectPacket(PacketView) = 0;
};
} // namespace service::gateway
