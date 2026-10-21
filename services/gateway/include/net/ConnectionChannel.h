/** @file
 * @brief Defines the explicit owner-thread channel exposed to
 * protocols.
 */
#pragma once

#include <functional>
#include <span>

#include "net/ConnectionFeature.h"

namespace service::gateway
{
class ConnectionChannel;
using ConnectionAction = std::function<void(ConnectionChannel&)>;
using ConnectionPost = std::function<bool(ConnectionAction)>;

/** @brief Offers protocol I/O and bounded delivery to a connection
 * owner.
 *
 * All operations require the owner thread, except immutable id(),
 * post(), and poster(). Protocols retain poster(), never a borrowed
 * channel reference, when completing work on a foreign thread.
 */
class ConnectionChannel : public IConnectionControl
{
public:
    ~ConnectionChannel() override = default;
    /** @brief Returns the immutable process-wide connection
     * identifier. */
    virtual std::uint64_t id() const noexcept = 0;
    /** @brief Copies reply bytes into the transport on its owner
     * thread. */
    virtual void send(std::span<const std::byte>) = 0;
    /** @brief Quiesces protocol input, drains pending requests and
     * output. The transport write half shuts down only after protocol
     * drain finishes.
     */
    virtual void shutdown() = 0;
    /** @brief Requests an idempotent deferred transport close. */
    virtual void close() = 0;
    /** @brief Reports owner-thread connection availability. */
    virtual bool connected() const noexcept = 0;
    /** @brief Reports whether new protocol input may be admitted on
     * the owner. Draining channels remain connected for
     * already-admitted request replies.
     */
    virtual bool acceptingInput() const noexcept = 0;
    /** @brief Pauses application input independently of other pause
     * reasons. */
    virtual void pauseRead() = 0;
    /** @brief Clears the application's read pause reason. */
    virtual void resumeRead() = 0;
    /** @brief Runs on owner or queues within the connection's mailbox
     * limit.
     *
     * Returns false for an empty action, closed channel, full
     * mailbox, or scheduler failure. Accepted queued actions become
     * no-ops after closure. An owner-thread call executes immediately
     * and is reentrant.
     */
    virtual bool post(ConnectionAction) = 0;
    /** @brief Returns a thread-safe posting handle valid after host
     * teardown.
     *
     * The handle does not retain the host, transport, or owner
     * executor after teardown. Calling it after closure returns
     * false.
     */
    virtual ConnectionPost poster() const = 0;
};
} // namespace service::gateway
