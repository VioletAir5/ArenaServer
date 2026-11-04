/** @file
 * @brief Owns one authenticated Room RPC process and its private
 * durable cache.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>

#include "rpc/Server.h"

namespace service::room
{
/** @brief Computes isolated proposals; only Lobby WAL can publish
 * authority. The external executor must keep pumping until
 * stats().stopped. Accepted disk jobs retain physical admission after
 * cancellation until their actual ACK. Stage and initial/new-epoch
 * activation are durable. Same-instance forward cache installation is
 * volatile because Lobby has already committed its image; restart
 * changes worker identity and requires a new-epoch activation.
 * Startup first validates the private journal, then durably releases
 * previous-instance images while retaining their epoch fences.
 * Readiness waits for every cleanup ACK.
 */
class RoomWorker final
{
public:
    struct Config
    {
        /// Stable configured node name; each process adds a random
        /// instance.
        std::string nodeId;
        /// Explicit Lobby-only credential, never logged or defaulted.
        std::string token;
        /// Private Room cache directory; never the Lobby journal
        /// directory.
        std::filesystem::path directory;
        /// Exact loopback listener; zero is useful for isolated
        /// tests.
        boost::asio::ip::tcp::endpoint endpoint{
            boost::asio::ip::address_v4::loopback(), 0
        };
        /// Includes queued, computing and physically pending disk
        /// work.
        std::size_t maxInflight{32};
        /// Includes retired routes because their epoch high-water is
        /// retained.
        std::size_t maxRooms{4096};
        /// Inactive transfer records retained separately from active
        /// routes.
        std::size_t maxStages{4096};
        /// Bound on canonical encoded private-cache key/value bytes.
        std::size_t maxCacheBytes{16 * 1024 * 1024};
        /// Short thread-safe operator intent predicate, without RPC
        /// or SQL.
        std::function<bool()> configuredReady;
    };
    struct Stats
    {
        /// Unique node:instance identity of this process.
        std::string workerId;
        /// Exact stable configured node name.
        std::string nodeId;
        /// Current-process routes explicitly activated by Lobby.
        std::size_t activeRoutes{0};
        /// Inactive transfer records, including lightweight epoch
        /// fences retained from previous instances.
        std::size_t stages{0};
        /// Canonical bytes currently retained in the private cache.
        std::size_t cacheBytes{0};
        /// Physical accepted handler jobs not yet completed.
        std::size_t activeJobs{0};
        /// Private journal jobs awaiting physical or owner
        /// completion.
        std::size_t journalPending{0};
        /// Actual acknowledged canonical journal length.
        std::size_t journalFileBytes{0};
        /// Actual RPC peers and retained handler loans.
        std::size_t rpcPeers{0}, rpcActive{0};
        /// The private writer and all its deliveries have ended.
        bool journalClosed{false};
        /// Listener/cache ready, including operator intent.
        bool ready{false};
        /// New work has been fenced permanently.
        bool stopping{false};
        /// Listener, peers, handlers and private writer are
        /// physically drained.
        bool stopped{false};
    };
    /** @brief Validates and recovers the private cache before
     * publishing readiness.
     * @param[in] executor Executor pumped until the worker reports
     * stopped.
     * @param[in] config Node identity, private journal and bounded
     * RPC policy.
     */
    RoomWorker(boost::asio::any_io_executor executor, Config config);
    ~RoomWorker() noexcept;
    RoomWorker(const RoomWorker&) = delete;
    RoomWorker& operator=(const RoomWorker&) = delete;
    /** @brief Returns the real bound listener port. */
    std::uint16_t port() const noexcept;
    /** @brief Returns passive bounded accounting from any thread. */
    Stats stats() const;
    /** @brief Fences admission and drains existing physical work. */
    void stop(
        std::chrono::milliseconds grace =
            std::chrono::milliseconds::zero()
    );

private:
    struct State;
    /// Queued owner work retains state only through its actual
    /// completion.
    std::shared_ptr<State> m_state;
};
} // namespace service::room