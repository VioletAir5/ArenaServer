/** @file
 * @brief Defines committed routing, ordered ingress and opaque room
 * images.
 */
#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "lobby/DurableStateStore.h"

namespace service::lobby
{
/** @brief Identifies one room lifetime independently of its owner. */
struct RoomIdentity
{
    std::uint64_t room{0};        ///< Stable public room identity.
    std::uint64_t incarnation{0}; ///< Never reused actor lifetime.
    auto operator<=>(const RoomIdentity&) const = default;
};
/** @brief Contains worker-produced bytes without hosting a
 * simulation. */
struct RoomWorkerImage
{
    std::uint64_t tick{0}; ///< Last committed logical tick.
    std::uint64_t checksum{
        0
    }; ///< Worker-reported canonical checksum.
    std::uint64_t cursor{
        0
    }; ///< Last consumed ordered ingress record.
    std::string checkpoint; ///< Opaque canonical worker checkpoint.
    bool operator==(const RoomWorkerImage&) const = default;
};
enum class RoomIngressKind : std::uint8_t
{
    Input,
    Ready,
    Tick,
    Settlement,
    Membership
};
/** @brief Captures one logical operation, independent of a socket
 * callback. */
struct RoomOperation
{
    std::string id; ///< Stable caller operation key, never a
                    ///< transport pointer.
    RoomIngressKind kind{RoomIngressKind::Input};
    std::uint64_t tick{
        0
    }; ///< Nonzero only for a logical Tick operation.
    std::string payload; ///< Validated public/domain command bytes;
                         ///< no credentials.
    bool operator==(const RoomOperation&) const = default;
};
struct RoomOrderedInput
{
    std::uint64_t sequence{
        0
    }; ///< Contiguous per-room acceptance cursor.
    RoomOperation operation;
    bool operator==(const RoomOrderedInput&) const = default;
};
/** @brief Preserves pending and applied results within a bounded
 * retry horizon. */
struct RoomReceipt
{
    std::uint64_t sequence{0};
    std::string
        fingerprint; ///< SHA-256 of the full logical operation.
    std::int64_t expiresAt{0}; ///< Same-boot monotonic nanoseconds.
    bool applied{
        false
    }; ///< Pending receipts cannot be garbage-collected.
    std::string response; ///< Immutable worker result, empty for
                          ///< internal ticks.
    bool operator==(const RoomReceipt&) const = default;
};
enum class RoomMigrationPhase : std::uint8_t
{
    Preparing,
    Freezing,
    Frozen,
    Transferred,
    Committed,
    Aborted
};
/** @brief Records the durable handoff and both prospective fencing
 * epochs. */
struct RoomHandoff
{
    std::uint64_t id{
        0
    }; ///< Strictly increasing within this room lifetime.
    std::string source;
    std::string target;
    std::uint64_t sourceEpoch{0};
    std::uint64_t targetEpoch{
        0
    }; ///< sourceEpoch + 1, checked before Prepare.
    std::uint64_t abortEpoch{
        0
    }; ///< sourceEpoch + 2 fences both old workers.
    std::uint64_t cutoff{
        0
    }; ///< Source may consume only through this cursor.
    std::int64_t deadline{
        0
    }; ///< Persisted recovery budget, never renewed on load.
    RoomMigrationPhase phase{RoomMigrationPhase::Preparing};
    std::string
        frozenFingerprint; ///< Full image and identity digest.
    bool operator==(const RoomHandoff&) const = default;
};
/** @brief Is immutable after publication; it contains no actor or
 * live callbacks. */
struct RoomRouteRecord
{
    RoomIdentity identity;
    std::string owner; ///< Explicit process/worker identity, resolved
                       ///< by the caller.
    std::uint64_t ownerEpoch{
        1
    }; ///< Required by ingress and every worker proposal.
    RoomWorkerImage image;
    std::uint64_t nextSequence{1};
    std::uint64_t admittedTick{
        0
    }; ///< Includes durable ticks not yet applied.
    std::uint64_t initialTick{
        0
    }; ///< First opaque replay-history anchor.
    std::vector<RoomOrderedInput> inbox;
    std::map<std::string, RoomReceipt> receipts;
    std::uint64_t migrationWatermark{0};
    std::optional<RoomHandoff> handoff;
    bool operator==(const RoomRouteRecord&) const = default;
};
struct RoomDirectoryLimits
{
    std::size_t maxRooms{4096};
    std::size_t maxInbox{512};
    std::size_t maxReceipts{2048};
    std::size_t maxOperationBytes{16 * 1024};
    std::size_t maxImageBytes{256 * 1024};
    std::size_t maxReplayBytes{1024 * 1024};
    std::size_t maxCommands{128};
    std::int64_t receiptRetentionNanos{10'000'000'000};
    bool operator==(const RoomDirectoryLimits&) const = default;
};
enum class RoomDirectoryError
{
    Invalid,
    Capacity,
    NotFound,
    StaleEpoch,
    Conflict,
    Phase,
    Exhausted,
    NotDurable,
    BootMismatch
};
class RoomMigration;
/** @brief Exposes only state reconstructed from the real durable
 * store. */
class RoomDirectory final
{
public:
    std::uint64_t revision() const noexcept;
    std::size_t size() const noexcept;
    const RoomRouteRecord* find(RoomIdentity) const noexcept;

private:
    friend class RoomMigration;
    RoomDirectory() = default;
    std::uint64_t m_revision{0};
    std::string m_boot;
    RoomDirectoryLimits m_limits;
    std::map<RoomIdentity, std::shared_ptr<const RoomRouteRecord>>
        m_rooms;
    std::shared_ptr<const DurableStateStore::Snapshot> m_durable;
};
/** @brief Hashes metadata and opaque image bytes; it does not verify
 * gameplay. */
std::string
roomImageFingerprint(RoomIdentity, const RoomWorkerImage&);
} // namespace service::lobby