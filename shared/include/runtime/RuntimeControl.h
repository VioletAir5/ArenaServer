/** @file
 * @brief Owns bounded runtime reload and loopback health control.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "runtime/RuntimeConfig.h"

namespace arena::runtime
{
/** @brief Runs control I/O independently of business reactors and
 * room ticks.
 *
 * Construction binds the health listener before returning. Reloads
 * read files on one worker and publish on the control owner. stop()
 * and destruction must run outside control callbacks; they join the
 * reader and all control I/O. Dependency and inflight providers must
 * be thread-safe, short and nonblocking.
 */
class RuntimeControl final
{
public:
    struct Config
    {
        /// Optional atomically published runtime configuration.
        std::shared_ptr<ConfigStore> store;
        /// Optional regular file read only by the dedicated loader.
        std::filesystem::path configFile;
        /// Loopback health port; zero selects an ephemeral port.
        std::uint16_t healthPort{0};
        /// Bounded public service identity included in health
        /// replies.
        std::string service{"gateway"};
        /// Optional bounded public node identity.
        std::string nodeId;
        /// Cached dependency readiness; never perform RPC or SQL
        /// here.
        std::function<bool()> dependencyReady;
        /// Actual business work retained by the service,
        /// independently of peers.
        std::function<std::size_t()> inflight;
        /// Optional observer invoked after a successful publication.
        ConfigStore::Notify notify;
    };

    struct Metrics
    {
        /// Complete newer snapshots successfully published.
        std::uint64_t applied{0};
        /// Invalid, stale or unavailable reload requests.
        std::uint64_t rejected{0};
        /// Requests folded into an already queued or active reload.
        std::uint64_t coalesced{0};
        /// Actual file loaders currently executing, always zero or
        /// one.
        std::size_t active{0};
        /// Maximum simultaneous file loaders observed.
        std::size_t peakActive{0};
        /// Health peers currently owned by the control reactor.
        std::size_t peers{0};
        /// Health peers rejected at the finite capacity bound.
        std::uint64_t rejectedPeers{0};
        /// Latest rejected reload classification, without file
        /// contents.
        std::optional<ConfigError> lastError;
        /// Terminal admission closure, independent of operator
        /// quiescence.
        bool draining{false};
        /// Both worker and control owners have exited.
        bool closed{false};
    };

    /** @brief Starts the bounded loader and binds the loopback health
     * listener.
     * @param[in] config Explicit ownership and short observation
     * callbacks.
     */
    explicit RuntimeControl(Config config);
    ~RuntimeControl() noexcept;
    RuntimeControl(const RuntimeControl&) = delete;
    RuntimeControl& operator=(const RuntimeControl&) = delete;

    /** @brief Returns the actual bound loopback port. */
    std::uint16_t port() const noexcept;
    /** @brief Checks intent, terminal drain and cached dependency
     * readiness. */
    bool ready() const noexcept;
    /** @brief Combines global and matching-node intent, independently
     * of dependencies and drain. */
    bool configuredReady() const noexcept;
    /** @brief Permanently closes business readiness before the caller
     * drains. */
    void setDraining() noexcept;
    /** @brief Distinguishes terminal drain from reversible operator
     * quiescence. */
    bool draining() const noexcept;
    /** @brief Queues bounded reload work; overlapping requests
     * coalesce. */
    bool requestReload();
    /** @brief Samples ownership and reload counters without waiting
     * for I/O. */
    Metrics metrics() const noexcept;
    /** @brief Joins all owners; call outside this object's callbacks.
     * Repeated calls after closure are harmless from any outside
     * thread.
     */
    void stop();

private:
    struct State;
    /// Shared callback lifetime; sessions only weakly reference this
    /// state.
    const std::shared_ptr<State> m_state;
    /// Serializes simultaneous outside-thread shutdown callers.
    std::mutex m_stopMutex;
};
} // namespace arena::runtime