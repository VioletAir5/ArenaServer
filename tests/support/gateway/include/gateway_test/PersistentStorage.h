/** @file
 * @brief Bridges gateway authentication, identity allocation and
 * durable results to the bounded internal Storage RPC service.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <boost/asio/ip/tcp.hpp>

#include "net/MessageRegistry.h"

#include "auth/LoginAuthenticator.h"
#include "lobby/RoomService.h"
#include "storage.pb.h"

namespace arena::runtime
{
class ConfigStore;
}

namespace service::gateway
{

struct PersistentStorageConfig
{
    /// Exact loopback endpoint; no implicit discovery or database
    /// access.
    boost::asio::ip::tcp::endpoint endpoint{
        boost::asio::ip::address_v4::loopback(), 7105
    };
    /// Internal credential, never included in diagnostics.
    std::string serviceToken;
    /// Optional immutable discovery snapshots; absence uses the
    /// startup endpoint.
    std::shared_ptr<arena::runtime::ConfigStore> runtime;
    /// Enable bounded readiness probes when the control plane is
    /// configured.
    bool probeHealth{false};
    /// Private, exclusively locked directory containing durable
    /// result jobs.
    std::filesystem::path spoolDirectory;
    std::size_t maxAuth{64}, maxWorkPending{128};
    std::size_t maxSpoolFiles{128}, maxSpoolBytes{8 * 1024 * 1024};
    std::size_t maxRatings{100000};
    std::uint32_t idBlockSize{128};
    std::chrono::milliseconds rpcTimeout{1000};
    std::uint32_t maxResultAttempts{8};
    std::chrono::milliseconds retryBase{50}, retryMax{1000};
};

struct PersistentStorageMetrics
{
    std::size_t authActive{0}, workPending{0}, idsAvailable{0};
    std::size_t spoolFiles{0}, spoolBytes{0}, failedResults{0};
    std::uint64_t resultAttempts{0}, resultsCommitted{0}, rejected{0};
    std::uint64_t operatorRequeues{0}, cleanupFailures{0};
    bool accepting{false}, recovered{false}, spoolHealthy{false};
    /// Actual admitted wire calls, including the single readiness
    /// probe.
    std::size_t rpcActive{0};
    /// Current discovery version and the version of the latest valid
    /// probe.
    std::uint64_t routeVersion{0}, probeVersion{0};
    bool dependencyReady{false};
};

/** @brief Owns a dedicated RPC executor and a finite CPU/filesystem
 * worker.
 *
 * Public admission and lookup methods never wait for disk, KDF or
 * RPC. prime() is startup-only. stop()/destruction must run outside
 * adapter callbacks; they close admission, cancel RPC, join workers
 * and preserve durable jobs. A result completion reports true only
 * after a validated PostgreSQL COMMIT acknowledgement, never merely
 * after spool admission or a transport timeout.
 */
class PersistentStorage final
{
public:
    using Clock = std::chrono::steady_clock;
    using AuthResult = std::
        expected<arena::domain::PlayerId, service::auth::AuthError>;
    using AuthCompletion = std::function<void(AuthResult)>;
    using Cancellation = std::function<void()>;
    using SaveCompletion = std::function<void(bool)>;
    using LookupResult = std::
        expected<service::lobby::RoomSettlement, ErrorCode>;
    using LookupCompletion = std::function<void(LookupResult)>;

    explicit PersistentStorage(PersistentStorageConfig);
    ~PersistentStorage() noexcept;
    PersistentStorage(const PersistentStorage&) = delete;
    PersistentStorage& operator=(const PersistentStorage&) = delete;

    /** @brief Waits only during startup for spool validation and a
     * durable ID block. */
    bool prime(std::chrono::milliseconds timeout);
    /** @brief Allocates an already committed ID without waiting; low
     * stock prefetches. */
    std::optional<std::uint64_t> takeTicketId();
    /** @brief Returns a canonical rating learned from a successful
     * password login. */
    std::optional<std::uint32_t>
        rating(arena::domain::PlayerId) const;

    std::expected<Cancellation, service::auth::AuthError>
    authenticate(
        std::string account,
        std::string password,
        Clock::time_point deadline,
        std::string correlationId,
        AuthCompletion
    );
    /** @brief Admits one bounded result; callback follows durable PG
     * acknowledgement. */
    bool save(arena::storage::MatchRecord, SaveCompletion);
    /** @brief Copies a frozen managed result and canonical cohort
     * ratings. */
    bool save(
        const service::lobby::ManagedRoom&,
        const service::lobby::RoomSettlement&,
        SaveCompletion
    );
    /** @brief Explicit operator action; failed jobs never reset
     * budgets automatically. */
    bool requeueFailed(std::uint64_t matchId);

    /** @brief Queries a durable result after restart and checks
     * caller membership. */
    std::expected<Cancellation, ErrorCode> settlement(
        arena::domain::PlayerId,
        std::uint64_t matchId,
        Clock::time_point deadline,
        std::string correlationId,
        LookupCompletion
    );

    /** @brief Reports actual dependency readiness for the current
     * route version. */
    bool ready() const;
    PersistentStorageMetrics metrics() const;
    void stop() noexcept;

private:
    struct State;
    std::shared_ptr<State> m_state;
};
} // namespace service::gateway
