# services/gateway/include/net/RejectionCounters.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；57行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Counts rejected connections by reason.
 */
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace service::gateway
{
// Fixed labels bound metric cardinality. These count rejected
// packets/policy events, not every transport disconnect (EOF, reset,
// stop, etc.).
/** @brief Represents rejection reason. */
enum class RejectionReason : std::size_t
{
    PacketRateExceeded,
    PacketTypeNotAllowed,
    Unauthenticated,
    InvalidFrame,
    InvalidEnvelope,
    HandlerRejected,
    CallbackFailure,
    SlowClient,
    Count
};

/** @brief Counts rejected connections by reason. */
class RejectionCounters final
{
public:
    using Ptr = std::shared_ptr<RejectionCounters>;
    static constexpr auto kReasons = static_cast<std::size_t>(
        RejectionReason::Count
    );
    using Snapshot = std::array<std::uint64_t, kReasons>;

    void record(RejectionReason reason) noexcept;
    std::uint64_t count(RejectionReason reason) const noexcept;
    // Each element is atomic; a live snapshot is not one transaction.
    Snapshot snapshot() const noexcept;
    static std::string_view name(RejectionReason reason) noexcept;

private:
    /// Stores the counts.
    std::array<std::atomic<std::uint64_t>, kReasons> m_counts{};
};

// Process-wide default, shared by TCP/WS and all workers. Inject a
// separate instance into ConnectionContext/packet handlers for
// isolated scopes/tests.
RejectionCounters::Ptr defaultRejectionCounters();
} // namespace service::gateway
~~~
