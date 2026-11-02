/** @file
 * @brief Plans owner-epoch transitions for the existing single
 * journal writer.
 */
#pragma once

#include <expected>
#include <variant>

#include "lobby/RoomDirectory.h"

namespace service::lobby
{
/** @brief Describes one opaque replay extent produced and validated
 * by a worker. */
struct RoomReplayExtent
{
    std::uint64_t firstTick{0};
    std::uint64_t lastTick{0};
    std::string bytes;
    bool operator==(const RoomReplayExtent&) const = default;
};
struct RoomAppliedReply
{
    std::uint64_t sequence{0};
    std::string response;
};
struct RoomCreate
{
    RoomIdentity room;
    std::string owner;
    RoomWorkerImage image;
};
struct RoomAccept
{
    RoomIdentity room;
    std::uint64_t epoch{0};
    RoomOperation operation;
};
struct RoomApply
{
    RoomIdentity room;
    std::string owner;
    std::uint64_t epoch{0};
    RoomWorkerImage image;
    std::vector<RoomAppliedReply> replies;
    std::optional<RoomReplayExtent> replay;
};
struct RoomPrepare
{
    RoomIdentity room;
    std::uint64_t epoch{0};
    std::uint64_t migration{0};
    std::string target;
    std::int64_t deadline{0};
};
struct RoomFreeze
{
    RoomIdentity room;
    std::uint64_t migration{0};
    /// On source failure, fence new proposals and use only its last
    /// durable image. Unapplied ordered ingress remains queued for
    /// the target; no tick is invented.
    bool recoverCommittedImage{false};
};
struct RoomFrozen
{
    RoomIdentity room;
    std::uint64_t migration{0};
    std::string fingerprint;
};
struct RoomTransferred
{
    RoomIdentity room;
    std::uint64_t migration{0};
    std::string target;
    std::uint64_t epoch{0};
    std::string fingerprint; ///< Authenticated target's durable Stage
                             ///< receipt.
};
struct RoomCommit
{
    RoomIdentity room;
    std::uint64_t migration{0};
};
struct RoomAbort
{
    RoomIdentity room;
    std::uint64_t migration{0};
};
struct RoomPruneReceipts
{
    RoomIdentity room;
};
using RoomDirectoryCommand = std::variant<
    RoomCreate,
    RoomAccept,
    RoomApply,
    RoomPrepare,
    RoomFreeze,
    RoomFrozen,
    RoomTransferred,
    RoomCommit,
    RoomAbort,
    RoomPruneReceipts>;

/** @brief Uses value candidates, never a second writer, executor or
 * simulation.
 *
 * The authority validates/authenticates worker messages before
 * planning. Opaque checkpoint/replay bytes are not semantically
 * verified here. The authority combines mutations with its registry
 * metadata in ONE DurableStateStore CAS. Only publish(), after real
 * journal ACK, exposes the resulting route/receipt.
 */
class RoomMigration final
{
public:
    using Error = RoomDirectoryError;
    class Candidate final
    {
    public:
        Candidate(Candidate&&) noexcept = default;
        Candidate& operator=(Candidate&&) noexcept = default;
        Candidate(const Candidate&) = delete;
        std::uint64_t expectedRevision() const noexcept;
        const std::vector<DurableStateStore::Mutation>&
        mutations() const noexcept;

    private:
        friend class RoomMigration;
        Candidate() = default;
        std::shared_ptr<RoomDirectory> m_planned;
        std::vector<DurableStateStore::Mutation> m_mutations;
        std::uint64_t m_expected{0};
    };

    /** @brief Checks finite bounds and the same-Linux-boot identity.
     */
    RoomMigration(RoomDirectoryLimits, std::string bootId);
    /** @brief Recovers committed routes; unknown schema or boot
     * mismatch fails closed. */
    std::expected<std::shared_ptr<const RoomDirectory>, Error>
    load(const DurableStateStore&) const;
    /** @brief Produces a disposable bounded candidate without
     * publishing side effects. */
    std::expected<Candidate, Error> plan(
        const RoomDirectory&,
        const std::vector<RoomDirectoryCommand>&,
        std::int64_t nowNanos
    ) const;
    /** @brief Validates the actual synced version and all owned bytes
     * on the owner. A no-op may share one authority CAS at base + 1.
     * Success consumes the candidate; failure preserves it for
     * revalidation. Publication only transfers preallocated immutable
     * data and cannot allocate.
     */
    std::expected<std::shared_ptr<const RoomDirectory>, Error>
    publish(
        Candidate&&,
        const DurableStateStore&,
        std::uint64_t ackRevision
    ) const noexcept;

private:
    RoomDirectoryLimits m_limits;
    std::string m_boot;
};
} // namespace service::lobby