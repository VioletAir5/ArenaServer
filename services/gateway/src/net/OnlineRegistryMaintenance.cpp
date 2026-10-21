/** @file
 * @brief Performs proactive logical session expiry independently of
 * traffic.
 */
#include "net/OnlineRegistryMaintenance.h"

#include <stdexcept>
#include <thread>
#include <utility>

#include <spdlog/spdlog.h>

namespace service::gateway
{
struct OnlineRegistryMaintenance::State
    : std::enable_shared_from_this<State>
{
    State(
        std::shared_ptr<OnlineRegistry> value,
        ConnectionScheduler service,
        std::chrono::milliseconds period
    )
        : registry(std::move(value))
        , scheduler(std::move(service))
        , interval(period)
    {
    }

    void requireOwner() const
    {
        if (std::this_thread::get_id() != owner)
            throw std::logic_error(
                "online maintenance requires management owner"
            );
    }
    void arm()
    {
        timer = scheduler.at(
            ConnectionClock::now() + interval,
            [weak = weak_from_this()]
            {
                if (auto self = weak.lock())
                    self->tick();
            }
        );
    }
    void tick()
    {
        requireOwner();
        if (!active)
            return;
        timer.reset();
        try
        {
            const auto reclaimed = registry->sweep();
            const auto retained = registry->retainedCount();
            if (reclaimed)
            {
                try
                {
                    spdlog::info(
                        "event=online_sweep active={} retained={} "
                        "rooms={}",
                        registry->activeCount(),
                        retained,
                        registry->roomCount()
                    );
                }
                catch (...)
                {
                }
            }
            if (active)
                arm();
        }
        catch (...)
        {
            // Failed timer rearm must not silently leave retained
            // players forever.
            stop();
            registry->shutdown();
        }
    }
    void stop()
    {
        requireOwner();
        if (!active)
            return;
        active = false;
        timer.reset();
        scheduler = {};
    }

    /// Registry has one lifetime shared across listeners and
    /// connection owners.
    std::shared_ptr<OnlineRegistry> registry;
    /// Management executor used only on the owner and cleared when
    /// upkeep stops.
    ConnectionScheduler scheduler;
    /// Bounds reclamation delay after a monotonic deadline.
    const std::chrono::milliseconds interval;
    /// Management thread owns the timer token and all maintenance
    /// mutation.
    const std::thread::id owner{std::this_thread::get_id()};
    /// Cancelled before the base executor is released.
    ConnectionTimer timer;
    /// Prevents cancelled or already-ready timer callbacks from
    /// rearming.
    bool active{true};
};

OnlineRegistryMaintenance::OnlineRegistryMaintenance(
    std::shared_ptr<OnlineRegistry> registry,
    ConnectionScheduler scheduler,
    std::chrono::milliseconds interval
)
{
    if (!registry || !scheduler.at ||
        interval <= std::chrono::milliseconds::zero())
        throw std::invalid_argument(
            "invalid online maintenance dependencies"
        );
    m_state = std::make_shared<State>(
        std::move(registry),
        std::move(scheduler),
        interval
    );
    m_state->arm();
}
OnlineRegistryMaintenance::~OnlineRegistryMaintenance() noexcept
{
    m_state->stop();
}
void OnlineRegistryMaintenance::stop()
{
    m_state->stop();
}
void OnlineRegistryMaintenance::shutdown() noexcept
{
    m_state->stop();
    m_state->registry->shutdown();
}
bool OnlineRegistryMaintenance::running() const noexcept
{
    return m_state->active;
}
} // namespace service::gateway
