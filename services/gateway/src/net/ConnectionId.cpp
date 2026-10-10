/** @file
 * @brief Allocates process-wide connection identifiers.
 */
#include "ConnectionId.h"

#include <atomic>
#include <limits>
#include <stdexcept>

namespace service::gateway::detail
{
std::uint64_t nextConnectionId()
{
    static std::atomic_uint64_t next{1};
    auto value = next.load(std::memory_order_relaxed);
    for (;;)
    {
        if (value == 0)
        {
            throw std::overflow_error("connection identity exhausted"
            );
        }
        const auto successor = value == std::numeric_limits<
                                            std::uint64_t>::max()
                                   ? 0
                                   : value + 1;
        if (next.compare_exchange_weak(
                value,
                successor,
                std::memory_order_relaxed
            ))
        {
            return value;
        }
    }
}
} // namespace service::gateway::detail
