/** @file
 * @brief Serves bounded authenticated RPC without owning executor
 * threads.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "rpc/Client.h"

namespace arena::internal_rpc
{
namespace detail
{
struct ServerState;
}

/** @brief Owns one strict-version loopback listener and its real
 * handler loans.
 *
 * The caller keeps the executor pumping until stats().stopped, then
 * releases dependencies. Handlers and cancellation run on one strand
 * and must not block. Completion may run on any thread and means the
 * actual job has ended. A deadline or peer close cancels the job and
 * retires its response, but retains its admission loan until
 * Completion. A handler that throws must not have started external
 * work. Constructor failure releases the listener immediately.
 */
class Server final
{
public:
    using Cancellation = std::function<void()>;
    using Completion = std::function<void(arena::rpc::Response)>;
    using Handler = std::function<
        Cancellation(arena::rpc::Request, Context, Completion)>;
    using Handlers = std::map<std::string, Handler>;

    struct Config
    {
        /// Exact target identity, also appended to the validated
        /// service path.
        std::string service;
        /// Loopback listener; port zero selects an ephemeral port.
        boost::asio::ip::tcp::endpoint endpoint{
            boost::asio::ip::address_v4::loopback(), 0
        };
        /// Each caller role has its own secret; request labels do not
        /// grant roles.
        std::map<std::string, std::string> callerTokens;
        /// Maximum owned health/read/write sockets, independently of
        /// jobs.
        std::size_t maxPeers{64};
        /// Includes cancelled jobs whose physical cleanup has not
        /// completed.
        std::size_t maxActive{32};
        /// Bound includes all protobuf metadata and payload bytes.
        std::size_t maxFrame{Client::maxFrame};
        /// Clipped once when admitting the incoming absolute parent
        /// deadline.
        std::chrono::milliseconds maxBudget{1000};
        /// Covers the entire header and body read, not each partial
        /// read.
        std::chrono::milliseconds readTimeout{250};
        /// A slow reader cannot retain a completed response
        /// indefinitely.
        std::chrono::milliseconds writeTimeout{250};
        /// Optional short owner-strand business admission gate.
        std::function<bool(const arena::rpc::Request&)> admission;
    };

    struct Stats
    {
        /// Actual sockets currently retained by the owner.
        std::size_t peers{0};
        /// Actual accepted handlers, including response-retired
        /// cleanup jobs.
        std::size_t active{0};
        /// Accepted business handlers, excluding metadata/transport
        /// rejection.
        std::uint64_t accepted{0};
        /// Requests or peers rejected before business admission.
        std::uint64_t rejected{0};
        /// Physical handler completions, each counted exactly once.
        std::uint64_t completed{0};
        /// Accepted jobs signalled for cancellation, each counted
        /// once.
        std::uint64_t cancelled{0};
        /// Admission has been fenced, even before the owner processes
        /// stop.
        bool stopping{false};
        /// Listener and peers are closed and every physical handler
        /// has ended.
        bool stopped{false};
    };

    /** @brief Validates and binds synchronously, then starts
     * accepting on owner. */
    Server(boost::asio::any_io_executor, Config, Handlers);
    ~Server() noexcept;
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    /** @brief Returns the actual bound TCP port. */
    std::uint16_t port() const noexcept;
    /** @brief Reads concurrent observation counters without executing
     * work. */
    Stats stats() const noexcept;
    /** @brief Fences admission immediately; the first call owns the
     * finite grace. Accepted handlers may finish during grace.
     * Afterwards cancellation is sent once; stats().stopped still
     * waits for their real Completion. No callbacks or threads are
     * created for repeated stop requests.
     */
    void stop(
        std::chrono::milliseconds grace =
            std::chrono::milliseconds::zero()
    );

private:
    /// Physical jobs retain state only until their once-only
    /// completion handoff.
    const std::shared_ptr<detail::ServerState> m_state;
};
} // namespace arena::internal_rpc