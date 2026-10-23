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

/** @brief Identifies independent causes that must all clear before
 * reading. */
enum class ReadPauseReason : std::uint32_t
{
    Backpressure = 1,
    Application = 2
};

/** @brief Offers protocol and policy controls with bounded delivery
 * to a connection owner.
 *
 * All operations require the owner thread, except immutable id() and
 * poster(). Protocols retain poster(), never a borrowed
 * channel reference, when completing work on a foreign thread.
 */
class ConnectionChannel
{
public:
    virtual ~ConnectionChannel() = default;
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
    /** @brief Sets or clears one read pause cause without changing
     * other causes. */
    virtual void setReadPaused(ReadPauseReason, bool) = 0;
    /** @brief Returns immutable metadata and the owner's
     * authentication context. */
    virtual const ConnectionContext& context() const noexcept = 0;
    /** @brief Schedules an owner callback only while the host remains
     * open. Reset the returned token on owner to cancel;
     * already-ready callbacks also check host lifetime and logical
     * closure before running.
     */
    virtual ConnectionTimer
        scheduleAt(ConnectionClock::time_point, ConnectionAction) = 0;
    /** @brief Returns a thread-safe posting handle valid after host
     * teardown.
     *
     * The handle does not retain the host, transport, or owner
     * executor after teardown. The handle executes immediately on
     * owner (and is reentrant), or queues within the foreign-action
     * limit. An empty action, closure, capacity or enqueue failure
     * returns false; queued actions become no-ops after closure.
     */
    virtual ConnectionPost poster() const = 0;
};
} // namespace service::gateway
