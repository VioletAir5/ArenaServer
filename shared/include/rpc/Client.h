/** @file
 * @brief Provides bounded asynchronous internal RPC on an external
 * executor.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "rpc.pb.h"

namespace arena::internal_rpc
{
namespace detail
{
struct Operation;
struct ClientState;
} // namespace detail

/** @brief Carries the parent's monotonic deadline and correlation
 * identity. */
struct Context
{
    /// Absolute local deadline; children cannot extend it.
    std::chrono::steady_clock::time_point deadline;
    /// Bounded public correlation identity; never include
    /// credentials.
    std::string correlationId;
};

struct Options
{
    /// Maximum duration including executor waiting, in (0, 30s].
    std::chrono::milliseconds timeout{1000};
    /// Optional upstream budget clips timeout and supplies
    /// correlationId.
    std::optional<Context> context;
    /// Exact route captured for this admission; no global client
    /// retargeting.
    std::optional<boost::asio::ip::tcp::endpoint> endpoint{};
};

/** @brief Cancels an accepted call without retaining its connection.
 */
class Call
{
public:
    Call() = default;
    /** @brief Requests cancellation from any thread; repeated calls
     * coalesce. */
    void cancel() const;

private:
    explicit Call(std::weak_ptr<detail::Operation> operation);
    friend class Client;
    /// Completion revokes this weak operation handle.
    std::weak_ptr<detail::Operation> m_operation;
};

/** @brief Exchanges one bounded protobuf request per loopback
 * connection.
 *
 * call(), stop() and inflight() are thread-safe. Only accepted calls
 * invoke completion, exactly once on a shared strand. Rejections
 * return synchronously without posting callbacks, so overload cannot
 * create a hidden callback queue. There are no automatic retries.
 * Transport loss after write admission returns OUTCOME_UNKNOWN, never
 * retryable. Deadline/cancellation also never imply that a remote
 * mutation rolled back; resolve it using the same idempotency key.
 *
 * The caller owns the executor and must keep pumping it through
 * cancellation and completion. This object owns no threads. Callbacks
 * must be short and must not block the strand. Destructor requests
 * stop; it does not join the executor.
 */
class Client final
{
public:
    static constexpr std::size_t maxFrame = 256 * 1024;
    using Completion = std::function<void(arena::rpc::Response)>;

    /** @brief Binds one exact loopback endpoint and finite admission
     * limit. */
    Client(
        boost::asio::any_io_executor,
        boost::asio::ip::tcp::endpoint,
        std::size_t maxInflight = 128
    );
    ~Client() noexcept;
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    /** @brief Copies a request and admits bounded work without
     * waiting.
     * @param[in] request Typed method/payload/key/token. IDs and
     * deadline are assigned here; a nonempty correlation_id is
     * preserved without a parent.
     * @param[in] options Local timeout and optional parent deadline.
     * @param[in] completion Invoked once for accepted calls;
     * exceptions are isolated from the executor.
     * @return Cancellation handle or immediate admission/validation
     * failure.
     */
    std::expected<Call, arena::rpc::Status> call(
        arena::rpc::Request request,
        Options options,
        Completion completion
    );

    /** @brief Closes admission immediately, then cancels accepted
     * work. */
    void stop();
    /** @brief Counts admitted calls before their completion is
     * dispatched. */
    std::size_t inflight() const;

private:
    /// Registry owns operations; each operation only weakly
    /// references it.
    std::shared_ptr<detail::ClientState> m_state;
};
} // namespace arena::internal_rpc
