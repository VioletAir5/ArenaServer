/** @file
 * @brief Runs registry reclamation on the management lifetime, not a
 * connection.
 */
#pragma once

#include <chrono>
#include <memory>

#include "net/ConnectionFeature.h"
#include "net/OnlineRegistry.h"

namespace service::gateway
{
/** @brief Sweeps logical sessions independently of transport and
 * worker teardown.
 *
 * Construct, stop, shut down, and finally release on the management
 * owner. Stop periodic work before draining servers, then shutdown
 * the registry once connection owners have retired. The scheduler
 * must outlive this object.
 */
class OnlineRegistryMaintenance final
{
public:
    /** @brief Starts periodic sweeps on the supplied management
     * scheduler.
     * @param[in] registry Shared process-local session and roster
     * authority.
     * @param[in] scheduler Owner timer service independent of
     * ConnectionHost.
     * @param[in] interval Positive sweep interval; defaults to 100
     * milliseconds.
     */
    OnlineRegistryMaintenance(
        std::shared_ptr<OnlineRegistry> registry,
        ConnectionScheduler scheduler,
        std::chrono::milliseconds interval =
            std::chrono::milliseconds(100)
    );
    ~OnlineRegistryMaintenance() noexcept;
    OnlineRegistryMaintenance(const OnlineRegistryMaintenance&) =
        delete;
    OnlineRegistryMaintenance&
    operator=(const OnlineRegistryMaintenance&) = delete;
    /** @brief Cancels upkeep without cancelling pending connection
     * replies.
     * @throws std::logic_error When called outside the management
     * owner.
     */
    void stop();
    /** @brief Cancels upkeep and permanently clears the shared
     * authority. */
    void shutdown() noexcept;
    /** @brief Reports owner-thread maintenance state. */
    bool running() const noexcept;

private:
    struct State;
    /// Weak timer callbacks never extend the management lifetime.
    std::shared_ptr<State> m_state;
};
} // namespace service::gateway
