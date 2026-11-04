/** @file
 * @brief Persists atomic authority mutations before publishing
 * immutable state.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/asio/any_io_executor.hpp>

namespace arena::persistence
{
/** @brief Owns a bounded journal and its single disk writer.
 *
 * Construction performs bounded recovery and takes an exclusive
 * directory lock. commit() and stop() never wait for disk. Accepted
 * completions run serially on the supplied executor, after the batch
 * is synced or definitively rejected. The executor must remain alive
 * and pumping until stats().closed. Destruction requests shutdown and
 * joins the real worker; destroy outside a latency-sensitive owner,
 * or after closed is observed. A callback cannot retain this object.
 *
 * A successful revision acknowledges local fdatasync, not replicated
 * durability. OutcomeUnknown fences further writes; reopen and
 * inspect state to reconcile it.
 */
class AtomicJournal final
{
    struct State;

public:
    struct Config
    {
        /// Explicit directory containing authority.wal and its
        /// exclusive lock.
        std::filesystem::path directory;
        /// Maximum canonical journal size, including checkpoint
        /// frames. Overwritten history is compacted before this bound
        /// is reached.
        std::size_t maxBytes{256 * 1024 * 1024};
        /// Maximum encoded mutation payload; never greater than four
        /// MiB.
        std::size_t maxRecordBytes{4 * 1024 * 1024};
        /// Accepted jobs, including disk work and undelivered
        /// completions.
        std::size_t maxPending{64};
        /// Retained mutation capacities and estimated encoded
        /// buffers.
        std::size_t maxPendingBytes{16 * 1024 * 1024};
        /// Bounds detached key/value copies independently of journal
        /// growth. The transition proof separately retains at most
        /// maxPendingBytes of admitted mutation capacity.
        std::size_t maxStateBytes{32 * 1024 * 1024};
        /// Maximum live keys in a detached state snapshot.
        std::size_t maxKeys{65536};
    };

    struct Mutation
    {
        /// Nonempty binary key, bounded to 256 bytes.
        std::string key;
        /// Missing value deletes the key; present empty value stores
        /// empty bytes.
        std::optional<std::string> value;
    };

    struct Snapshot
    {
        /// Strictly increasing revision of one complete durable
        /// mutation batch.
        std::uint64_t revision{0};
        /// Detached values; no worker-owned mutable map escapes
        /// publication.
        std::map<std::string, std::string> values;

        Snapshot() = default;
        Snapshot(
            std::uint64_t initialRevision,
            std::map<std::string, std::string> initialValues
        )
            : revision(initialRevision)
            , values(std::move(initialValues))
        {
        }
        /** @brief Verifies the complete result of one namespace
         * transition.
         * @param[in] base Exact published predecessor, not a copied
         * revision.
         * @param[in] prefix Nonempty binary namespace prefix.
         * @param[in] expected Sorted, unique mutations belonging to
         * the prefix. Unmentioned keys must remain unchanged.
         * Unrelated namespaces and same-value writes are permitted.
         * The identical snapshot verifies only an empty expected
         * batch; recovered views have no predecessor. Checking costs
         * depend on this write set, not retained history.
         */
        bool verifiesNamespaceTransition(
            const Snapshot& base,
            std::string_view prefix,
            std::span<const Mutation> expected
        ) const noexcept;

    private:
        friend struct AtomicJournal::State;
        /// Weak provenance never retains an unbounded chain of old
        /// snapshots.
        std::weak_ptr<const Snapshot> m_predecessor;
        /// Copies cannot reuse another published object's transition
        /// proof.
        std::weak_ptr<const Snapshot> m_identity;
        /// Moved before I/O; retained capacity is bounded by
        /// maxPendingBytes.
        std::vector<Mutation> m_writes;
    };

    enum class Error
    {
        /// The caller's expected revision no longer matches current
        /// durable state.
        Conflict,
        /// A journal, record, state or key bound prevented any write.
        Capacity,
        /// An earlier uncertain write quarantined this writer.
        Unavailable,
        /// A write or sync failed after write admission; disk may
        /// contain the batch.
        OutcomeUnknown
    };

    struct Stats
    {
        /// Fully synced and published batches.
        std::uint64_t committed{0};
        /// Complete checkpoint replacements; logical revision is
        /// unchanged.
        std::uint64_t compactions{0};
        /// Expected-revision failures, with no disk mutation.
        std::uint64_t conflicts{0};
        /// Immediate invalid, full or closed admission rejections.
        std::uint64_t rejected{0};
        /// Uncertain write or sync failures.
        std::uint64_t ioFailures{0};
        /// User callback exceptions, isolated from the owner
        /// executor.
        std::uint64_t callbackFailures{0};
        /// Executor handoff failures; closed remains false if
        /// delivery is lost.
        std::uint64_t deliveryFailures{0};
        /// Accepted jobs still writing, queued or awaiting owner
        /// delivery.
        std::size_t pending{0};
        /// Conservative retained charge for all accepted jobs.
        std::size_t pendingBytes{0};
        /// Last fully acknowledged file prefix, excluding uncertain
        /// writes.
        std::size_t fileBytes{0};
        /// Admission is open and no uncertain write has occurred.
        bool accepting{true};
        /// Subsequent disk mutations are forbidden after uncertainty.
        bool poisoned{false};
        /// The worker has closed all journal, directory and lock
        /// descriptors.
        bool writerStopped{false};
        /// All physical I/O has ended and accepted completions were
        /// delivered.
        bool closed{false};
    };

    using Result = std::expected<std::uint64_t, Error>;
    using Completion = std::function<void(Result)>;

    /** @brief Recovers one validated prefix and starts a private disk
     * worker. */
    AtomicJournal(
        boost::asio::any_io_executor executor, Config config
    );
    ~AtomicJournal() noexcept;
    AtomicJournal(const AtomicJournal&) = delete;
    AtomicJournal& operator=(const AtomicJournal&) = delete;

    /** @brief Returns one immutable durable version from any thread.
     */
    std::shared_ptr<const Snapshot> snapshot() const noexcept;
    /** @brief Admits one bounded CAS batch without waiting for the
     * writer.
     * @return False rejects immediately and never invokes completion.
     */
    bool commit(
        std::uint64_t expectedRevision,
        std::vector<Mutation> mutations,
        Completion completion
    );
    /** @brief Rejects new jobs and drains accepted work; safe to
     * repeat. */
    void stop() noexcept;
    /** @brief Observes accounting without waiting for disk or the
     * owner loop. */
    Stats stats() const noexcept;

private:
    /// The worker and queued owner deliveries retain state
    /// independently of callers.
    std::shared_ptr<State> m_state;
};
} // namespace arena::persistence