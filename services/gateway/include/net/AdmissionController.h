/** @file
 * @brief Shares bounded connection admission across gateway
 * listeners.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "net/InetAddress.h"

namespace service::gateway
{
/** @brief Reserves process-local capacity before connection setup.
 *
 * A controller may be shared by TCP and WebSocket listeners. Counts
 * include queued worker handoffs and incomplete WebSocket upgrades.
 */
class AdmissionController final
{
    struct State;

public:
    struct Limits
    {
        /// Maximum live reservations across all attached listeners.
        std::size_t maxConnections;
        /// Maximum reservations for one IPv4 address, excluding port.
        std::size_t maxConnectionsPerIp;
    };

    struct Snapshot
    {
        std::size_t active{0};
        std::size_t peak{0};
        std::size_t trackedIps{0};
        std::uint64_t rejectedTotal{0};
        std::uint64_t rejectedGlobal{0};
        std::uint64_t rejectedPerIp{0};
        std::uint64_t rejectedLocal{0};
    };

    /** @brief Releases one reservation when its last owner retires.
     */
    class Lease final
    {
    public:
        ~Lease() noexcept;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;

    private:
        friend class AdmissionController;
        Lease(std::shared_ptr<State> state, std::uint32_t ip);
        /// Shared accounting outlives the public controller if
        /// needed.
        std::shared_ptr<State> m_state;
        std::uint32_t m_ip;
        bool m_reserved{false};
    };

    /** @brief Constructs a controller with positive global/IP limits.
     */
    explicit AdmissionController(Limits limits);

    /** @brief Atomically reserves capacity, or returns null on
     * rejection.
     * @param[in] peer Kernel-reported remote address; port is
     * ignored.
     */
    std::shared_ptr<Lease> tryAcquire(const InetAddress& peer);
    /** @brief Counts a listener's own capacity rejection. */
    void recordLocalRejection();
    /** @brief Returns a consistent accounting snapshot from any
     * thread. */
    Snapshot snapshot() const;
    /** @brief Returns live reservations for this peer's IPv4 address.
     */
    std::size_t activeForIp(const InetAddress& peer) const;
    Limits limits() const noexcept;

private:
    /// Short count transactions are shared across listener/owner
    /// threads.
    std::shared_ptr<State> m_state;
};
} // namespace service::gateway
