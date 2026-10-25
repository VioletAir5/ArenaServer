/** @file
 * @brief Owns process-local authentication, presence and bounded room
 * rosters.
 */
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "auth/JwtTokens.h"

namespace service::gateway
{
enum class Presence
{
    Online,
    Detached
};

enum class OnlineError
{
    NotCurrent,
    InvalidRoom,
    RoomFull,
    Capacity
};

struct SessionBinding
{
    /// Authenticated account identity.
    arena::domain::PlayerId playerId;
    /// Physical connection permitted to use this binding.
    std::uint64_t connectionId;
    /// Monotonic authority generation that fences older connections.
    std::uint64_t epoch;
};

struct OnlineSnapshot
{
    /// Account whose logical session is described.
    arena::domain::PlayerId playerId;
    /// Logical identity retained across authorized JWT recovery.
    arena::domain::SessionId sessionId;
    /// Zero while detached; epoch remains the recovery fence.
    std::uint64_t connectionId;
    /// Current transport authorization generation.
    std::uint64_t epoch;
    /// Online transport or a disconnected session retained in grace.
    Presence presence;
    /// Authoritative gateway roster binding, absent outside a room.
    std::optional<arena::domain::RoomId> roomId;
    /// Exclusive monotonic recovery deadline, present only in grace.
    std::optional<std::chrono::steady_clock::time_point>
        graceDeadline;
    /// Logical-session membership generation; changes on actual
    /// move/leave.
    std::uint64_t roomEpoch{0};
};

struct OnlineState
{
    /// Current caller state captured in the same transaction as
    /// members.
    OnlineSnapshot session;
    /// Owned and player-sorted roster, empty when the caller has no
    /// room.
    std::vector<OnlineSnapshot> members;
};

/** @brief Identifies an authenticated logical participant without a
 * transport. */
struct MatchParticipant
{
    /// Account participating in the trusted match.
    arena::domain::PlayerId playerId;
    /// Logical session sampled by the matching owner.
    arena::domain::SessionId sessionId;
    bool operator==(const MatchParticipant&) const = default;
};

/** @brief Freezes one managed-room membership independently of
 * reconnects. */
struct MatchRoomMember
{
    /// Fixed account in the allocation cohort.
    arena::domain::PlayerId playerId;
    /// Reconnect-stable identity; fresh password sessions differ.
    arena::domain::SessionId sessionId;
    /// Exact membership fence captured by atomic allocation.
    std::uint64_t roomEpoch;
    bool operator==(const MatchRoomMember&) const = default;
};

/** @brief Binds one server allocation to exactly two immutable
 * memberships. */
struct ManagedRoom
{
    /// Nonzero match identity allocated by the queue owner.
    std::uint64_t matchId;
    /// Reserved server identity, inaccessible to client room joins.
    arena::domain::RoomId roomId;
    /// Canonical immutable cohort retained until owner cleanup.
    std::array<MatchRoomMember, 2> members;
};

struct OnlineRegistryConfig
{
    /// Bounds retained Online and Detached players together.
    std::size_t maxPlayers{100000};
    /// Positive monotonic reconnect window, at most one hour.
    std::chrono::milliseconds grace{30000};
    /// Maximum rosters, including managed rooms awaiting owner
    /// cleanup.
    std::size_t maxRooms{10000};
    /// Online and Detached slots share this per-room capacity.
    std::size_t maxRoomMembers{64};
    /// Called under the registry lock; must not reenter this
    /// registry.
    std::function<std::chrono::steady_clock::time_point()> now{
        std::chrono::steady_clock::now
    };
};

class OnlineRegistry;

/** @brief Fences one transport binding without retaining its
 * connection. */
class SessionLease final
{
public:
    /// Immutable transport authorization tuple.
    const SessionBinding binding;
    /** @brief Checks revocation and JWT expiry from any thread. */
    bool current() const;
    /** @brief Revokes this lease and conditionally starts grace once.
     */
    void release() noexcept;

private:
    friend class OnlineRegistry;
    SessionLease(
        SessionBinding,
        std::int64_t,
        std::function<std::int64_t()>,
        std::function<void()>,
        std::weak_ptr<OnlineRegistry>
    );
    /// JWT wall-clock expiry, independent of the monotonic grace
    /// clock.
    const std::int64_t m_expiresAt;
    /// Thread-safe authentication clock retained with the lease.
    const std::function<std::int64_t()> m_now;
    /// Durable owner posting handle; never a raw connection
    /// reference.
    const std::function<void()> m_kick;
    /// Authority may retire before a saved lease receives final
    /// release.
    const std::weak_ptr<OnlineRegistry> m_directory;
    /// Publishes revocation before deferred physical socket closure.
    std::atomic<bool> m_valid{true};
};

struct SessionGrant
{
    /// Current transport authorization installed by this transaction.
    std::shared_ptr<SessionLease> lease;
    /// Newly issued JWT; callers must not log it.
    std::string token;
    /// Stable logical session and room recovered or freshly created.
    OnlineSnapshot snapshot;
    /// True for authorized JWT recovery, false for password
    /// replacement.
    bool resumed;
};

/** @brief Serializes auth leases, presence and room membership in one
 * authority.
 *
 * All public operations are thread-safe. Records are owned values
 * rather than owner-affine domain entities. User kick callbacks
 * execute unlocked.
 */
class OnlineRegistry final
    : public std::enable_shared_from_this<OnlineRegistry>
{
public:
    /** @brief Uses default limits with an explicit retained-player
     * cap. */
    explicit OnlineRegistry(std::size_t maximum = 100000);
    /** @brief Validates limits and installs the supplied monotonic
     * clock. */
    explicit OnlineRegistry(OnlineRegistryConfig);
    ~OnlineRegistry() noexcept;
    OnlineRegistry(const OnlineRegistry&) = delete;
    OnlineRegistry& operator=(const OnlineRegistry&) = delete;

    /** @brief Installs a fresh password session or recovers a current
     * JWT.
     * @param[in] player Identity established by credential
     * verification.
     * @param[in] connectionId Nonzero physical connection identity.
     * @param[in] resume Verified claims, absent for fresh password
     * login.
     * @param[in] tokens Token issuer and thread-safe wall clock.
     * @param[in] kick Durable close handle invoked outside the
     * registry lock.
     */
    std::expected<SessionGrant, service::auth::AuthError> install(
        arena::domain::PlayerId player,
        std::uint64_t connectionId,
        std::optional<service::auth::TokenClaims> resume,
        const service::auth::JwtTokens& tokens,
        std::function<void()> kick
    );
    /** @brief Starts grace once for the exact current transport
     * binding. */
    void detach(SessionBinding) noexcept;
    /** @brief Counts currently valid Online leases, excluding logical
     * expiry. */
    std::size_t activeCount() const;
    /** @brief Returns physical records; maintenance uses sweep for
     * reclamation. */
    std::size_t retainedCount() const;
    /** @brief Returns physical rosters, including managed allocations
     * awaiting owner cleanup and slots retained during grace. */
    std::size_t roomCount() const;
    /** @brief Finds the unexpired retained authority generation. */
    std::optional<std::uint64_t>
        currentEpoch(arena::domain::PlayerId) const;
    /** @brief Copies one unexpired session without scanning other
     * players. */
    std::optional<OnlineSnapshot>
        snapshot(arena::domain::PlayerId) const;
    /** @brief Returns a consistent session and bounded roster for its
     * owner. */
    std::expected<OnlineState, OnlineError> state(SessionBinding);
    /** @brief Atomically moves a current binding while preserving it
     * on failure. */
    std::expected<OnlineState, OnlineError>
        joinRoom(SessionBinding, arena::domain::RoomId);
    /** @brief Clears room membership while retaining the logical
     * session. */
    std::expected<OnlineState, OnlineError> leaveRoom(SessionBinding);
    /** @brief Atomically binds two current logical sessions to a
     * server room. Detached sessions remain eligible until their
     * existing grace expires. Managed IDs occupy a namespace
     * inaccessible to ordinary room joins. Retrying the same match
     * and cohort returns the existing allocation.
     */
    std::expected<ManagedRoom, OnlineError> allocateMatch(
        std::uint64_t matchId,
        std::array<MatchParticipant, 2> participants
    );
    /** @brief Copies the immutable cohort, including expired or
     * replaced members. */
    std::optional<ManagedRoom>
        managedRoom(arena::domain::RoomId) const;
    /** @brief Clears only memberships still belonging to this exact
     * allocation. */
    bool finishMatch(arena::domain::RoomId, std::uint64_t matchId);
    /** @brief Copies one roster after pruning expired members in that
     * room. */
    std::vector<OnlineSnapshot>
        roomMembers(arena::domain::RoomId) const;
    /** @brief Reclaims expired records/rosters and returns the
     * removed count. */
    std::size_t sweep();
    /** @brief Revokes every record; later release/install cannot
     * revive it. */
    void shutdown() noexcept;

private:
    struct State;
    /// One mutex-protected authority for player records and both room
    /// indexes.
    std::unique_ptr<State> m_state;
};
} // namespace service::gateway
