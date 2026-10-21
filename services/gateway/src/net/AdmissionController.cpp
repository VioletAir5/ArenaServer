/** @file
 * @brief Implements shared admission reservations and exact
 * reclamation.
 */
#include "net/AdmissionController.h"

#include <algorithm>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace service::gateway
{
struct AdmissionController::State
{
    explicit State(Limits configured)
        : limits(configured)
    {
    }

    const Limits limits;
    mutable std::mutex mutex;
    Snapshot counters;
    std::unordered_map<std::uint32_t, std::size_t> perIp;
};

namespace
{
std::uint32_t ipKey(const InetAddress& peer) noexcept
{
    // InetAddress currently represents IPv4 only. Keeping the binary
    // address avoids string allocation and excludes the ephemeral
    // port.
    return reinterpret_cast<const sockaddr_in*>(peer.sockaddrPtr())
        ->sin_addr.s_addr;
}
} // namespace

AdmissionController::AdmissionController(Limits limits)
    : m_state(std::make_shared<State>(limits))
{
    if (limits.maxConnections == 0 || limits.maxConnectionsPerIp == 0)
    {
        throw std::invalid_argument(
            "admission limits must be positive"
        );
    }
}

AdmissionController::Lease::Lease(
    std::shared_ptr<State> state, std::uint32_t ip
)
    : m_state(std::move(state))
    , m_ip(ip)
{
}

AdmissionController::Lease::~Lease() noexcept
{
    if (!m_reserved)
    {
        return;
    }
    std::lock_guard lock(m_state->mutex);
    const auto entry = m_state->perIp.find(m_ip);
    if (--entry->second == 0)
    {
        m_state->perIp.erase(entry);
    }
    --m_state->counters.active;
    m_state->counters.trackedIps = m_state->perIp.size();
}

std::shared_ptr<AdmissionController::Lease>
AdmissionController::tryAcquire(const InetAddress& peer)
{
    const auto key = ipKey(peer);
    // Allocate before mutating counts. Allocation failure must never
    // leave a reservation without an owner to return it.
    auto lease = std::shared_ptr<Lease>(new Lease(m_state, key));
    std::lock_guard lock(m_state->mutex);
    auto& counters = m_state->counters;
    if (counters.active >= m_state->limits.maxConnections)
    {
        ++counters.rejectedTotal;
        ++counters.rejectedGlobal;
        return {};
    }
    const auto found = m_state->perIp.find(key);
    if (found != m_state->perIp.end() &&
        found->second >= m_state->limits.maxConnectionsPerIp)
    {
        ++counters.rejectedTotal;
        ++counters.rejectedPerIp;
        return {};
    }
    auto [entry, inserted] = m_state->perIp.try_emplace(key, 0);
    (void)inserted;
    ++entry->second;
    ++counters.active;
    counters.peak = std::max(counters.peak, counters.active);
    counters.trackedIps = m_state->perIp.size();
    lease->m_reserved = true;
    return lease;
}

void AdmissionController::recordLocalRejection()
{
    std::lock_guard lock(m_state->mutex);
    ++m_state->counters.rejectedTotal;
    ++m_state->counters.rejectedLocal;
}

AdmissionController::Snapshot AdmissionController::snapshot() const
{
    std::lock_guard lock(m_state->mutex);
    return m_state->counters;
}

std::size_t
AdmissionController::activeForIp(const InetAddress& peer) const
{
    std::lock_guard lock(m_state->mutex);
    const auto found = m_state->perIp.find(ipKey(peer));
    return found == m_state->perIp.end() ? 0 : found->second;
}

AdmissionController::Limits
AdmissionController::limits() const noexcept
{
    return m_state->limits;
}
} // namespace service::gateway
