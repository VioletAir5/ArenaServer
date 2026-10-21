/** @file
 * @brief Manages owner-thread request admission, deadlines and
 * once-only completion.
 */
#pragma once

#include <memory>

#include <spdlog/spdlog.h>

#include "net/ConnectionChannel.h"
#include "net/MessageRegistry.h"

namespace service::gateway
{
/** @brief Carries correlation data from protocol decoding to reply
 * encoding. */
struct ReplyMetadata
{
    /// Owning request retained until completion, timeout or
    /// disconnect.
    std::shared_ptr<const RequestMessage> request;
    /// Outer 32-bit correlation ID preserved independently of
    /// protobuf ID.
    std::uint32_t wireRequestId;
    /// Legacy clients require zero response msg_id when it was
    /// omitted.
    bool explicitMsgId;
};

// The protocol owns encoding; return the error actually sent for
// structured logs.
using ReplySink = std::function<ErrorCode(
    ConnectionChannel&, const ReplyMetadata&, MessageReply
)>;

/** @brief Tracks owner-thread requests without retaining a socket or
 * channel.
 *
 * Business completions retain a weak state and bounded owner posting
 * handle. Duplicate or late completions cannot finish a reused
 * request ID. Mailbox saturation leaves the request pending until its
 * existing deadline expires; the count bound does not constrain
 * memory allocated by a business handler.
 */
class RequestDispatcher final
{
public:
    static constexpr std::size_t kMaxPendingRequests = 64;
    RequestDispatcher(
        ConnectionContext::Ptr,
        std::shared_ptr<const MessageRegistry>,
        ReplySink,
        std::shared_ptr<spdlog::logger> logger = {}
    );
    ~RequestDispatcher();
    RequestDispatcher(const RequestDispatcher&) = delete;
    RequestDispatcher& operator=(const RequestDispatcher&) = delete;

    /** @brief Admits a decoded request on the connection owner. */
    void
    dispatch(ConnectionChannel&, ReplyMetadata, bool validRequest);
    /** @brief Refuses new requests and notifies after pending replies
     * enqueue.
     * @param[in] drained Owner-thread callback; repeated drain calls
     * are ignored.
     */
    void drain(std::function<void()> drained);
    /** @brief Cancels pending work before the channel and scheduler
     * disappear. */
    void stop() noexcept;

private:
    struct State;
    /// Pending requests retained only by the owner; completion
    /// callbacks are weak.
    std::shared_ptr<State> m_state;
};
} // namespace service::gateway
