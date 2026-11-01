/** @file
 * @brief Fences asynchronous replies and side effects to one live
 * request.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>

#include "net/MessageRegistry.h"

namespace service::gateway
{
class RequestDispatcher;

/** @brief Owns the deadline, once claim and cancellation handoff of
 * one request.
 *
 * Completion and cancellation registration are thread-safe. The
 * dispatcher rechecks request identity, generation, connection and
 * deadline before running an OwnerAction; business threads never
 * access a channel or mutable session. This is an execution contract
 * for trusted handlers, not a security sandbox.
 */
class RequestExecution final
{
public:
    using OwnerAction = std::function<MessageReply()>;
    using Cancellation = std::function<void()>;
    using Submit = std::function<bool(OwnerAction)>;

    /** @brief Installs immutable metadata and a weak owner submission
     * endpoint. */
    RequestExecution(
        std::chrono::steady_clock::time_point deadline,
        std::string correlationId,
        Submit submit
    );
    RequestExecution(const RequestExecution&) = delete;
    RequestExecution& operator=(const RequestExecution&) = delete;

    std::chrono::steady_clock::time_point deadline() const noexcept;
    const std::string& correlationId() const noexcept;

    /** @brief Claims a reply once and submits it for owner
     * validation. */
    void complete(MessageReply);
    /** @brief Claims a lazy owner action once; no action runs on this
     * thread.
     * @return False if already claimed/retired or the bounded owner
     * post fails. A failed post leaves the existing request timer
     * responsible for cleanup.
     */
    bool completeOnOwner(OwnerAction) noexcept;
    /** @brief Registers the accepted downstream operation's cancel
     * handle.
     *
     * A cancellation that won before registration invokes this handle
     * once. Normal completion discards late handles. Only one cancel
     * handle is accepted; repeated registration cannot consume the
     * slot twice. Callbacks run outside the registration mutex and
     * must be thread-safe.
     */
    void setCancellation(Cancellation);

private:
    friend class RequestDispatcher;
    /** @brief Revokes completion on the owner and consumes the cancel
     * slot. */
    void retire(bool cancelled) noexcept;

    const std::chrono::steady_clock::time_point m_deadline;
    const std::string m_correlationId;
    const Submit m_submit;
    /// Completion admission is independent of cancellation
    /// registration.
    std::atomic<bool> m_claimed{false};
    /// Protects retirement versus a late foreign-thread registration.
    std::mutex m_mutex;
    Cancellation m_cancel;
    bool m_registered{false}, m_retired{false}, m_cancelled{false};
};
} // namespace service::gateway
