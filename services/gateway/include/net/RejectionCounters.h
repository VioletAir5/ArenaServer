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
    std::array<std::atomic<std::uint64_t>, kReasons> m_counts{};
};

// Process-wide default, shared by TCP/WS and all workers. Inject a
// separate instance into ConnectionContext/packet handlers for
// isolated scopes/tests.
RejectionCounters::Ptr defaultRejectionCounters();
} // namespace service::gateway