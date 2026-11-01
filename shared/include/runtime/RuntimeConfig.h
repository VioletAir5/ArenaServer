/** @file
 * @brief Publishes validated immutable discovery and readiness
 * snapshots.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace arena::runtime
{
/** @brief Identifies one statically configured internal service node.
 */
struct Node
{
    /// Logical service selected by the caller.
    std::string service;
    /// Stable identity used to order equally ready nodes.
    std::string id;
    /// Exact loopback address; resolution does not perform DNS.
    std::string host;
    /// Validated nonzero TCP port.
    std::uint16_t port;
    /// Whether new calls may select this node.
    bool ready;
};

enum class ConfigError
{
    /// The complete document violates the schema or its bounds.
    Invalid,
    /// A bounded regular input file could not be opened or inspected.
    File,
    /// Input changed or ended while the loader was reading it.
    Changed,
    /// The proposed publication version is not newer than the current
    /// version.
    Stale,
    /// Another publication is still delivering its notification.
    Busy
};

/** @brief Retains one complete version without exposing mutable
 * protobuf state. */
class ConfigSnapshot final
{
public:
    using Ptr = std::shared_ptr<const ConfigSnapshot>;
    static constexpr std::size_t maxBytes = 64 * 1024;

    /** @brief Validates a complete JSON document before constructing
     * a snapshot. */
    static std::expected<Ptr, ConfigError>
    parse(std::string_view json);
    /** @brief Reads one bounded regular file; ownership belongs to a
     * loader worker. */
    static std::expected<Ptr, ConfigError>
    read(const std::filesystem::path& path);
    /** @brief Returns the publication version, including for content
     * rollbacks. */
    std::uint64_t version() const noexcept
    {
        return m_version;
    }
    /** @brief Returns the operator's admission intent, independently
     * of liveness. */
    bool desiredReady() const noexcept
    {
        return m_ready;
    }
    /** @brief Returns a validated drain duration between zero and ten
     * seconds. */
    std::uint32_t drainGraceMs() const noexcept
    {
        return m_drainGraceMs;
    }
    /** @brief Returns the immutable canonical node order. */
    const std::vector<Node>& nodes() const noexcept
    {
        return m_nodes;
    }
    /** @brief Selects the first ready node, retaining no external
     * mutable state. */
    std::optional<Node> resolve(std::string_view service) const;

private:
    ConfigSnapshot() = default;
    /// Nonzero, strictly increasing publication identity.
    std::uint64_t m_version{0};
    /// Explicit readiness intent from the complete document.
    bool m_ready{false};
    /// Finite drain bound, separate from request deadlines.
    std::uint32_t m_drainGraceMs{0};
    /// Sorted by logical service and stable node identity.
    std::vector<Node> m_nodes;
};

/** @brief Atomically swaps whole snapshots; retained readers keep
 * their version.
 *
 * Notifications run outside the publication lock. Recursive
 * publication rejects with Busy instead of recursing or queueing
 * work. A callback exception cannot roll back an already published
 * snapshot and is counted separately.
 */
class ConfigStore final
{
public:
    using Notify = std::function<void(ConfigSnapshot::Ptr)>;
    /** @brief Starts with one validated immutable version.
     * @param[in] initial Non-null snapshot retained by the store.
     */
    explicit ConfigStore(ConfigSnapshot::Ptr initial);
    /** @brief Acquires one coherent immutable version from any
     * thread. */
    ConfigSnapshot::Ptr snapshot() const noexcept;
    /** @brief Publishes only a newer validated candidate; rejection
     * keeps the old version. */
    std::expected<void, ConfigError>
    publish(ConfigSnapshot::Ptr candidate, Notify notify = {});
    /** @brief Counts failed notifications without changing
     * publication success. */
    std::uint64_t notificationFailures() const noexcept;

private:
    /// Release/acquire publication owns the immutable snapshot's
    /// lifetime.
    std::atomic<ConfigSnapshot::Ptr> m_current;
    /// Serializes version checks and guards notification reentry.
    mutable std::mutex m_mutex;
    /// Remains true through the outside-lock callback.
    bool m_notifying{false};
    /// Diagnostic failures, independent of admission and publication
    /// version.
    std::atomic<std::uint64_t> m_notificationFailures{0};
};
} // namespace arena::runtime