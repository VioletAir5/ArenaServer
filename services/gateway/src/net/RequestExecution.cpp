/** @file
 * @brief Coordinates request completion and downstream cancellation
 * races.
 */
#include "net/RequestExecution.h"

#include <stdexcept>
#include <utility>

namespace service::gateway
{
namespace
{
void invokeCancellation(
    RequestExecution::Cancellation callback
) noexcept
{
    try
    {
        if (callback)
            callback();
    }
    catch (...)
    {
        // Cancellation must not unwind through timer or shutdown
        // callbacks.
    }
}
} // namespace

RequestExecution::RequestExecution(
    std::chrono::steady_clock::time_point deadline,
    std::string correlationId,
    Submit submit
)
    : m_deadline(deadline)
    , m_correlationId(std::move(correlationId))
    , m_submit(std::move(submit))
{
    if (!m_submit || m_correlationId.empty())
        throw std::invalid_argument(
            "request execution requires owner endpoint"
        );
}

std::chrono::steady_clock::time_point
RequestExecution::deadline() const noexcept
{
    return m_deadline;
}

const std::string& RequestExecution::correlationId() const noexcept
{
    return m_correlationId;
}

void RequestExecution::complete(MessageReply reply)
{
    completeOnOwner([reply = std::move(reply)]() mutable
                    { return std::move(reply); });
}

bool RequestExecution::completeOnOwner(OwnerAction action) noexcept
{
    if (!action || m_claimed.exchange(true))
        return false;
    try
    {
        return m_submit(std::move(action));
    }
    catch (...)
    {
        return false;
    }
}

void RequestExecution::setCancellation(Cancellation callback)
{
    if (!callback)
        return;
    bool invoke = false;
    {
        std::lock_guard lock(m_mutex);
        if (m_registered)
            return;
        m_registered = true;
        if (m_retired)
            invoke = m_cancelled;
        else
        {
            m_cancel = std::move(callback);
            return;
        }
    }
    if (invoke)
        invokeCancellation(std::move(callback));
}

void RequestExecution::retire(bool cancelled) noexcept
{
    m_claimed.store(true);
    Cancellation callback;
    {
        std::lock_guard lock(m_mutex);
        if (m_retired)
            return;
        m_retired = true;
        m_cancelled = cancelled;
        callback = std::move(m_cancel);
    }
    if (cancelled)
        invokeCancellation(std::move(callback));
}
} // namespace service::gateway
