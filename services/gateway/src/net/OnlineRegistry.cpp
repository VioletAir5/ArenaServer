/** @file
 * @brief Implements one fenced authority for retained players and
 * room rosters.
 */
#include "net/OnlineRegistry.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <openssl/rand.h>
#include <spdlog/spdlog.h>

namespace service::gateway
{
namespace
{
// Ordinary clients cannot reserve a future managed-room identity.
constexpr std::uint64_t managedRoomBit = std::uint64_t{1} << 63;

OnlineRegistryConfig withMaximum(std::size_t maximum)
{
    OnlineRegistryConfig config;
    config.maxPlayers = maximum;
    return config;
}

bool sameBinding(
    const SessionBinding& left, const SessionBinding& right
)
{
    return left.playerId == right.playerId &&
           left.connectionId == right.connectionId &&
           left.epoch == right.epoch;
}
} // namespace

struct OnlineRegistry::State
{
    struct Entry
    {
        OnlineSnapshot snapshot;
        SessionBinding binding;
        std::int64_t expiresAt;
        std::function<std::int64_t()> wallNow;
        std::shared_ptr<SessionLease> active;
    };

    struct Room
    {
        /// Live and detached members, pruned independently of the
        /// cohort.
        std::unordered_set<std::uint64_t> players;
        /// Immutable allocation survives expiry until its owner
        /// retires it.
        std::optional<ManagedRoom> managed;
    };

    // Declare this before the transaction lock. Its destructor runs
    // only after that lock is released, including early returns and
    // exceptions.
    struct Kicks
    {
        std::vector<std::shared_ptr<SessionLease>> leases;
        ~Kicks()
        {
            for (const auto& lease : leases)
            {
                try
                {
                    lease->m_kick();
                }
                catch (...)
                {
                    // A committed replacement/expiry cannot be undone
                    // by an unavailable or already closed connection
                    // mailbox.
                }
            }
        }
    };

    explicit State(OnlineRegistryConfig configured)
        : config(std::move(configured))
    {
    }

    bool expired(
        const Entry& entry, std::chrono::steady_clock::time_point now
    ) const
    {
        return entry.wallNow() >= entry.expiresAt ||
               (entry.snapshot.graceDeadline &&
                now >= *entry.snapshot.graceDeadline);
    }

    void removeRoom(Entry& entry)
    {
        if (!entry.snapshot.roomId)
            return;
        const auto found = rooms.find(entry.snapshot.roomId->value());
        if (found != rooms.end())
        {
            found->second.players.erase(
                entry.snapshot.playerId.value()
            );
            if (found->second.players.empty() &&
                !found->second.managed)
                rooms.erase(found);
        }
        entry.snapshot.roomId.reset();
    }

    std::size_t
    prune(std::chrono::steady_clock::time_point now, Kicks& kicks)
    {
        std::size_t removed = 0;
        for (auto found = players.begin(); found != players.end();)
        {
            if (!expired(found->second, now))
            {
                ++found;
                continue;
            }
            if (found->second.active)
            {
                kicks.leases.push_back(found->second.active);
                found->second.active->m_valid.store(
                    false,
                    std::memory_order_release
                );
            }
            removeRoom(found->second);
            found = players.erase(found);
            ++removed;
        }
        return removed;
    }

    bool prunePlayer(
        std::uint64_t player,
        std::chrono::steady_clock::time_point now,
        Kicks& kicks
    )
    {
        const auto found = players.find(player);
        if (found == players.end() || !expired(found->second, now))
            return false;
        if (found->second.active)
        {
            kicks.leases.push_back(found->second.active);
            found->second.active->m_valid.store(
                false,
                std::memory_order_release
            );
        }
        removeRoom(found->second);
        players.erase(found);
        return true;
    }

    void pruneRoom(
        arena::domain::RoomId room,
        std::chrono::steady_clock::time_point now,
        Kicks& kicks
    )
    {
        const auto found = rooms.find(room.value());
        if (found == rooms.end())
            return;
        // The bounded copy survives removal of the last member/room.
        // Never scan unrelated players on a room request.
        const std::vector<std::uint64_t> members(
            found->second.players.begin(),
            found->second.players.end()
        );
        for (const auto player : members)
            prunePlayer(player, now, kicks);
    }

    Entry* current(SessionBinding binding)
    {
        const auto found = players.find(binding.playerId.value());
        if (stopped || found == players.end() ||
            !sameBinding(found->second.binding, binding) ||
            found->second.snapshot.presence != Presence::Online ||
            !found->second.active || !found->second.active->current())
            return nullptr;
        return &found->second;
    }

    Entry* prepare(
        SessionBinding binding,
        std::chrono::steady_clock::time_point now,
        Kicks& kicks
    )
    {
        prunePlayer(binding.playerId.value(), now, kicks);
        auto* entry = current(binding);
        if (!entry)
            return nullptr;
        const auto room = entry->snapshot.roomId;
        if (room)
            pruneRoom(*room, now, kicks);
        // Clock dependencies can advance between checks. Room pruning
        // may have erased this player, so no previous pointer
        // survives.
        prunePlayer(binding.playerId.value(), now, kicks);
        return current(binding);
    }

    std::vector<OnlineSnapshot>
    members(arena::domain::RoomId room) const
    {
        std::vector<OnlineSnapshot> result;
        const auto found = rooms.find(room.value());
        if (found == rooms.end())
            return result;
        result.reserve(found->second.players.size());
        for (const auto player : found->second.players)
            result.push_back(players.at(player).snapshot);
        std::sort(
            result.begin(),
            result.end(),
            [](const auto& left, const auto& right)
            { return left.playerId.value() < right.playerId.value(); }
        );
        return result;
    }

    OnlineState view(const Entry& entry) const
    {
        return OnlineState{
            entry.snapshot,
            entry.snapshot.roomId ? members(*entry.snapshot.roomId)
                                  : std::vector<OnlineSnapshot>{}
        };
    }

    const OnlineRegistryConfig config;
    mutable std::mutex mutex;
    std::unordered_map<std::uint64_t, Entry> players;
    std::unordered_map<std::uint64_t, Room> rooms;
    std::uint64_t epoch{0};
    std::uint64_t sessionId{0};
    bool stopped{false};
};

SessionLease::SessionLease(
    SessionBinding bindingValue,
    std::int64_t expires,
    std::function<std::int64_t()> now,
    std::function<void()> kick,
    std::weak_ptr<OnlineRegistry> directory
)
    : binding(bindingValue)
    , m_expiresAt(expires)
    , m_now(std::move(now))
    , m_kick(std::move(kick))
    , m_directory(std::move(directory))
{
}

bool SessionLease::current() const
{
    return m_valid.load(std::memory_order_acquire) &&
           m_now() < m_expiresAt;
}

void SessionLease::release() noexcept
{
    m_valid.store(false, std::memory_order_release);
    if (auto directory = m_directory.lock())
        directory->detach(binding);
}

OnlineRegistry::OnlineRegistry(std::size_t maximum)
    : OnlineRegistry(withMaximum(maximum))
{
}

OnlineRegistry::OnlineRegistry(OnlineRegistryConfig config)
    : m_state(std::make_unique<State>(std::move(config)))
{
    const auto& settings = m_state->config;
    if (!settings.maxPlayers || !settings.maxRooms ||
        !settings.maxRoomMembers ||
        settings.grace <= std::chrono::milliseconds::zero() ||
        settings.grace > std::chrono::hours(1) || !settings.now)
        throw std::invalid_argument(
            "invalid online registry limits or clock"
        );
    std::uint64_t seeds[2]{};
    if (RAND_bytes(
            reinterpret_cast<unsigned char*>(seeds),
            sizeof(seeds)
        ) != 1)
        throw std::runtime_error(
            "online registry entropy unavailable"
        );
    // Reserve growth space while avoiding deterministic identity
    // reuse after restart. This remains a process-local authority.
    m_state->epoch = seeds[0] & ((std::uint64_t{1} << 63) - 1);
    m_state->sessionId = seeds[1] & ((std::uint64_t{1} << 63) - 1);
}

OnlineRegistry::~OnlineRegistry() noexcept
{
    shutdown();
}

std::expected<SessionGrant, service::auth::AuthError>
OnlineRegistry::install(
    arena::domain::PlayerId player,
    std::uint64_t connection,
    std::optional<service::auth::TokenClaims> resume,
    const service::auth::JwtTokens& tokens,
    std::function<void()> kick
)
{
    using service::auth::AuthError;
    if (!connection || !kick || weak_from_this().expired())
        throw std::invalid_argument(
            "online install requires identity, shared registry and "
            "closer"
        );
    State::Kicks kicks;
    std::unique_lock lock(m_state->mutex);
    if (m_state->stopped)
        return std::unexpected(AuthError::Unavailable);
    const auto now = m_state->config.now();
    m_state->prunePlayer(player.value(), now, kicks);
    auto found = m_state->players.find(player.value());
    if (resume && (resume->playerId != player ||
                   found == m_state->players.end() ||
                   found->second.snapshot.epoch != resume->epoch ||
                   found->second.expiresAt != resume->expiresAt ||
                   tokens.now() >= resume->expiresAt))
        return std::unexpected(AuthError::Superseded);
    if (found == m_state->players.end() &&
        m_state->players.size() >= m_state->config.maxPlayers)
    {
        m_state->prune(now, kicks);
        found = m_state->players.find(player.value());
        if (found == m_state->players.end() &&
            m_state->players.size() >= m_state->config.maxPlayers)
            return std::unexpected(AuthError::Unavailable);
    }
    if (m_state->epoch == std::numeric_limits<std::uint64_t>::max() ||
        (!resume && m_state->sessionId ==
                        std::numeric_limits<std::uint64_t>::max()))
        return std::unexpected(AuthError::Unavailable);
    const auto nextEpoch = m_state->epoch + 1;
    auto token = tokens.issue(player, nextEpoch);
    if (!token)
        return std::unexpected(token.error());
    const auto identity = resume ? found->second.snapshot.sessionId
                                 : arena::domain::SessionId{
                                       m_state->sessionId + 1
                                   };
    OnlineSnapshot snapshot{
        player,
        identity,
        connection,
        nextEpoch,
        Presence::Online,
        resume ? found->second.snapshot.roomId : std::nullopt,
        std::nullopt,
        resume ? found->second.snapshot.roomEpoch : 0
    };
    auto lease = std::shared_ptr<SessionLease>(new SessionLease(
        {player, connection, nextEpoch},
        token->claims.expiresAt,
        tokens.clock(),
        std::move(kick),
        weak_from_this()
    ));
    State::Entry replacement{
        snapshot,
        lease->binding,
        token->claims.expiresAt,
        tokens.clock(),
        lease
    };
    if (found != m_state->players.end() && found->second.active)
        kicks.leases.push_back(found->second.active);
    // All throwing preparation precedes replacement of valid state.
    if (found == m_state->players.end())
        m_state->players.emplace(
            player.value(),
            std::move(replacement)
        );
    else
    {
        if (found->second.active)
            found->second.active->m_valid.store(
                false,
                std::memory_order_release
            );
        if (!resume)
            m_state->removeRoom(found->second);
        found->second = std::move(replacement);
    }
    m_state->epoch = nextEpoch;
    if (!resume)
        m_state->sessionId = identity.value();
    lock.unlock();
    try
    {
        spdlog::info(
            "event=session_install player_id={} connection_id={} "
            "epoch={} session_id={} resumed={}",
            player.value(),
            connection,
            nextEpoch,
            identity.value(),
            resume.has_value()
        );
    }
    catch (...)
    {
        // Observability cannot turn a committed install into a
        // failure.
    }
    return SessionGrant{
        std::move(lease),
        std::move(token->encoded),
        snapshot,
        resume.has_value()
    };
}

void OnlineRegistry::detach(SessionBinding binding) noexcept
{
    std::lock_guard lock(m_state->mutex);
    const auto found = m_state->players.find(
        binding.playerId.value()
    );
    if (m_state->stopped || found == m_state->players.end() ||
        !sameBinding(found->second.binding, binding) ||
        found->second.snapshot.presence != Presence::Online)
        return;
    auto& entry = found->second;
    if (entry.active)
        entry.active->m_valid.store(false, std::memory_order_release);
    entry.active.reset();
    try
    {
        const auto now = m_state->config.now();
        if (entry.wallNow() >= entry.expiresAt ||
            m_state->config.grace ==
                std::chrono::milliseconds::zero())
        {
            m_state->removeRoom(entry);
            m_state->players.erase(found);
            return;
        }
        entry.snapshot.connectionId = 0;
        entry.snapshot.presence = Presence::Detached;
        const auto
            latest = std::chrono::steady_clock::time_point::max() -
                     m_state->config.grace;
        entry.snapshot.graceDeadline =
            now > latest
                ? std::chrono::steady_clock::time_point::max()
                : now + m_state->config.grace;
    }
    catch (...)
    {
        // A failing injected clock cannot leave a disconnected player
        // online forever, nor escape a connection cleanup destructor.
        m_state->removeRoom(entry);
        m_state->players.erase(found);
    }
}

std::size_t OnlineRegistry::activeCount() const
{
    std::lock_guard lock(m_state->mutex);
    const auto now = m_state->config.now();
    return std::count_if(
        m_state->players.begin(),
        m_state->players.end(),
        [&](const auto& item)
        {
            return item.second.snapshot.presence ==
                       Presence::Online &&
                   !m_state->expired(item.second, now) &&
                   item.second.active &&
                   item.second.active->current();
        }
    );
}

std::size_t OnlineRegistry::retainedCount() const
{
    std::lock_guard lock(m_state->mutex);
    return m_state->players.size();
}

std::size_t OnlineRegistry::roomCount() const
{
    std::lock_guard lock(m_state->mutex);
    return m_state->rooms.size();
}

std::optional<OnlineSnapshot>
OnlineRegistry::snapshot(arena::domain::PlayerId player) const
{
    State::Kicks kicks;
    std::lock_guard lock(m_state->mutex);
    m_state
        ->prunePlayer(player.value(), m_state->config.now(), kicks);
    const auto found = m_state->players.find(player.value());
    if (found == m_state->players.end())
        return std::nullopt;
    return found->second.snapshot;
}

std::optional<std::uint64_t>
OnlineRegistry::currentEpoch(arena::domain::PlayerId player) const
{
    const auto found = snapshot(player);
    return found ? std::optional(found->epoch) : std::nullopt;
}

std::expected<OnlineState, OnlineError>
OnlineRegistry::state(SessionBinding binding)
{
    State::Kicks kicks;
    std::lock_guard lock(m_state->mutex);
    const auto* entry = m_state->prepare(
        binding,
        m_state->config.now(),
        kicks
    );
    if (!entry)
        return std::unexpected(OnlineError::NotCurrent);
    return m_state->view(*entry);
}

std::expected<OnlineState, OnlineError> OnlineRegistry::joinRoom(
    SessionBinding binding, arena::domain::RoomId room
)
{
    State::Kicks kicks;
    std::lock_guard lock(m_state->mutex);
    const auto now = m_state->config.now();
    auto* entry = m_state->prepare(binding, now, kicks);
    if (!entry)
        return std::unexpected(OnlineError::NotCurrent);
    if ((room.value() & managedRoomBit) ||
        (entry->snapshot.roomId &&
         m_state->rooms.at(entry->snapshot.roomId->value()).managed))
        return std::unexpected(OnlineError::InvalidRoom);
    m_state->pruneRoom(room, now, kicks);
    entry = m_state->prepare(binding, now, kicks);
    if (!entry)
        return std::unexpected(OnlineError::NotCurrent);
    if (entry->snapshot.roomId == room)
        return m_state->view(*entry);
    if (entry->snapshot.roomEpoch ==
        std::numeric_limits<std::uint64_t>::max())
        return std::unexpected(OnlineError::Capacity);
    auto target = m_state->rooms.find(room.value());
    if (target != m_state->rooms.end() &&
        target->second.players.size() >=
            m_state->config.maxRoomMembers)
        return std::unexpected(OnlineError::RoomFull);
    const auto atRoomCapacity = [&]
    {
        const bool retiresOld = entry->snapshot.roomId &&
                                m_state->rooms
                                        .at(entry->snapshot.roomId
                                                ->value())
                                        .players.size() == 1;
        return target == m_state->rooms.end() &&
               m_state->rooms.size() >= m_state->config.maxRooms &&
               !retiresOld;
    };
    if (atRoomCapacity())
    {
        // Whole-directory reclamation is reserved for maintenance and
        // capacity pressure, never the ordinary lookup/move path.
        m_state->prune(now, kicks);
        entry = m_state->prepare(binding, now, kicks);
        if (!entry)
            return std::unexpected(OnlineError::NotCurrent);
        target = m_state->rooms.find(room.value());
        if (atRoomCapacity())
            return std::unexpected(OnlineError::Capacity);
    }
    auto next = entry->snapshot;
    next.roomId = room;
    ++next.roomEpoch;
    auto members = m_state->members(room);
    members.push_back(next);
    std::sort(
        members.begin(),
        members.end(),
        [](const auto& left, const auto& right)
        { return left.playerId.value() < right.playerId.value(); }
    );
    OnlineState result{next, std::move(members)};
    // Prepare the complete owning reply and target index before
    // touching the old room. Allocation failure preserves both old
    // indexes.
    const auto [destination, created] = m_state->rooms.try_emplace(
        room.value()
    );
    try
    {
        destination->second.players.insert(binding.playerId.value());
    }
    catch (...)
    {
        if (created)
            m_state->rooms.erase(destination);
        throw;
    }
    m_state->removeRoom(*entry);
    entry->snapshot.roomId = room;
    entry->snapshot.roomEpoch = next.roomEpoch;
    return result;
}

std::expected<OnlineState, OnlineError>
OnlineRegistry::leaveRoom(SessionBinding binding)
{
    State::Kicks kicks;
    std::lock_guard lock(m_state->mutex);
    auto* entry = m_state->prepare(
        binding,
        m_state->config.now(),
        kicks
    );
    if (!entry)
        return std::unexpected(OnlineError::NotCurrent);
    if (!entry->snapshot.roomId)
        return m_state->view(*entry);
    if (m_state->rooms.at(entry->snapshot.roomId->value()).managed)
        return std::unexpected(OnlineError::InvalidRoom);
    if (entry->snapshot.roomEpoch ==
        std::numeric_limits<std::uint64_t>::max())
        return std::unexpected(OnlineError::Capacity);
    auto next = entry->snapshot;
    next.roomId.reset();
    ++next.roomEpoch;
    OnlineState result{next, {}};
    m_state->removeRoom(*entry);
    entry->snapshot.roomEpoch = next.roomEpoch;
    return result;
}

std::expected<ManagedRoom, OnlineError> OnlineRegistry::allocateMatch(
    std::uint64_t matchId,
    std::array<MatchParticipant, 2> participants
)
{
    if (!matchId || (matchId & managedRoomBit) ||
        participants[0].playerId == participants[1].playerId)
        return std::unexpected(OnlineError::InvalidRoom);
    if (participants[1].playerId.value() <
        participants[0].playerId.value())
        std::swap(participants[0], participants[1]);
    const arena::domain::RoomId room{managedRoomBit | matchId};
    State::Kicks kicks;
    std::lock_guard lock(m_state->mutex);
    if (m_state->stopped)
        return std::unexpected(OnlineError::NotCurrent);
    const auto now = m_state->config.now();
    for (const auto& participant : participants)
        m_state
            ->prunePlayer(participant.playerId.value(), now, kicks);
    if (m_state->rooms.size() >= m_state->config.maxRooms &&
        !m_state->rooms.contains(room.value()))
        m_state->prune(now, kicks);
    const auto existing = m_state->rooms.find(room.value());
    std::array<State::Entry*, 2> entries{};
    for (std::size_t index = 0; index < participants.size(); ++index)
    {
        const auto& participant = participants[index];
        const auto found = m_state->players.find(
            participant.playerId.value()
        );
        if (found == m_state->players.end() ||
            found->second.snapshot.sessionId !=
                participant.sessionId ||
            m_state->expired(found->second, now) ||
            (found->second.snapshot.presence == Presence::Online &&
             (!found->second.active ||
              !found->second.active->current())))
            return std::unexpected(OnlineError::NotCurrent);
        entries[index] = &found->second;
    }
    if (existing != m_state->rooms.end())
    {
        if (!existing->second.managed ||
            existing->second.managed->matchId != matchId)
            return std::unexpected(OnlineError::InvalidRoom);
        const auto& saved = *existing->second.managed;
        for (std::size_t index = 0; index < participants.size();
             ++index)
        {
            const auto& member = saved.members[index];
            const auto& current = entries[index]->snapshot;
            if (member.playerId != participants[index].playerId ||
                member.sessionId != participants[index].sessionId ||
                current.roomId != room ||
                current.roomEpoch != member.roomEpoch)
                return std::unexpected(OnlineError::InvalidRoom);
        }
        return saved;
    }
    if (m_state->config.maxRoomMembers < 2)
        return std::unexpected(OnlineError::RoomFull);
    if (m_state->rooms.size() >= m_state->config.maxRooms)
        return std::unexpected(OnlineError::Capacity);
    for (const auto* entry : entries)
    {
        if (entry->snapshot.roomId)
            return std::unexpected(OnlineError::InvalidRoom);
        // Reserve one further generation for unconditional owner
        // cleanup.
        if (entry->snapshot.roomEpoch >= UINT64_MAX - 1)
            return std::unexpected(OnlineError::Capacity);
    }
    const ManagedRoom allocation{
        matchId,
        room,
        {{{participants[0].playerId,
           participants[0].sessionId,
           entries[0]->snapshot.roomEpoch + 1},
          {participants[1].playerId,
           participants[1].sessionId,
           entries[1]->snapshot.roomEpoch + 1}}}
    };
    State::Room prepared;
    prepared.players.reserve(2);
    for (const auto& participant : participants)
        prepared.players.insert(participant.playerId.value());
    prepared.managed = allocation;
    // All allocation happens before either participant changes. A
    // failure cannot leave one player bound or consume the old room
    // indexes.
    m_state->rooms.emplace(room.value(), std::move(prepared));
    for (std::size_t index = 0; index < entries.size(); ++index)
    {
        entries[index]->snapshot.roomId = room;
        entries[index]->snapshot.roomEpoch = allocation.members[index]
                                                 .roomEpoch;
    }
    return allocation;
}

std::optional<ManagedRoom>
OnlineRegistry::managedRoom(arena::domain::RoomId room) const
{
    std::lock_guard lock(m_state->mutex);
    const auto found = m_state->rooms.find(room.value());
    return found == m_state->rooms.end() ? std::nullopt
                                         : found->second.managed;
}

bool OnlineRegistry::finishMatch(
    arena::domain::RoomId room, std::uint64_t matchId
)
{
    std::lock_guard lock(m_state->mutex);
    const auto found = m_state->rooms.find(room.value());
    if (found == m_state->rooms.end() || !found->second.managed ||
        found->second.managed->matchId != matchId)
        return false;
    for (const auto& member : found->second.managed->members)
    {
        const auto player = m_state->players.find(
            member.playerId.value()
        );
        if (player == m_state->players.end())
            continue;
        auto& current = player->second.snapshot;
        if (current.sessionId == member.sessionId &&
            current.roomId == room &&
            current.roomEpoch == member.roomEpoch)
        {
            current.roomId.reset();
            ++current.roomEpoch;
        }
    }
    m_state->rooms.erase(found);
    return true;
}

std::vector<OnlineSnapshot>
OnlineRegistry::roomMembers(arena::domain::RoomId room) const
{
    State::Kicks kicks;
    std::lock_guard lock(m_state->mutex);
    m_state->pruneRoom(room, m_state->config.now(), kicks);
    return m_state->members(room);
}

std::size_t OnlineRegistry::sweep()
{
    State::Kicks kicks;
    std::lock_guard lock(m_state->mutex);
    return m_state->prune(m_state->config.now(), kicks);
}

void OnlineRegistry::shutdown() noexcept
{
    decltype(m_state->players) retiring;
    {
        std::lock_guard lock(m_state->mutex);
        m_state->stopped = true;
        for (const auto& [id, entry] : m_state->players)
        {
            (void)id;
            if (entry.active)
                entry.active->m_valid.store(
                    false,
                    std::memory_order_release
                );
        }
        retiring.swap(m_state->players);
        m_state->rooms.clear();
    }
    for (const auto& [id, entry] : retiring)
    {
        (void)id;
        if (!entry.active)
            continue;
        try
        {
            entry.active->m_kick();
        }
        catch (...)
        {
        }
    }
}
} // namespace service::gateway
