/** @file
 * @brief Copies legacy production values without executing a room
 * kernel.
 */
#include "lobby/LegacyRoomImport.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <map>
#include <new>
#include <set>
#include <type_traits>

namespace service::lobby
{
namespace
{
using Error = LegacyRoomImportError;
namespace runtime = service::room;
using Counter = std::uint64_t RoomMetrics::*;
constexpr std::array<Counter, 11> counters{
    &RoomMetrics::ticks,
    &RoomMetrics::applied,
    &RoomMetrics::rejected,
    &RoomMetrics::duplicates,
    &RoomMetrics::battlesStarted,
    &RoomMetrics::battlesCompleted,
    &RoomMetrics::battlesAborted,
    &RoomMetrics::persistenceSubmitted,
    &RoomMetrics::persistenceRejected,
    &RoomMetrics::persistenceFailed,
    &RoomMetrics::persistenceCommitted
};
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
[[noreturn]] void fail(Error error)
{
    throw error;
}
void clearGauges(RoomMetrics& m)
{
    m.actors = m.managedMatches = m.retainedResults = 0;
    m.pendingSettlements = m.blockedSettlements = 0;
}
bool noGauges(const RoomMetrics& m)
{
    return !m.actors && !m.managedMatches && !m.retainedResults &&
           !m.pendingSettlements && !m.blockedSettlements;
}
void add(RoomMetrics& to, const RoomMetrics& from)
{
    for (const auto field : counters)
        if (from.*field > maximum - to.*field)
            fail(Error::Invalid);
    for (const auto field : counters)
        to.*field += from.*field;
}
bool sameCounters(const RoomMetrics& a, const RoomMetrics& b)
{
    return a.skippedWallSlots == b.skippedWallSlots &&
           std::all_of(
               counters.begin(),
               counters.end(),
               [&](auto field) { return a.*field == b.*field; }
           );
}
runtime::RuntimePolicy policy(const RoomPolicy& p)
{
    if (!p.externalTicks || !p.externalSettlement ||
        p.callbackSettlement || p.archives)
        fail(Error::Unsupported);
    const auto milliseconds = p.resultRetention.count();
    if (!p.maxRooms || !p.maxPending || !p.maxPendingPerRoom ||
        !p.maxCatchUp || !p.actor.maxMembers ||
        p.actor.maxMembers > 65536 || !p.actor.maxInputsPerTick ||
        p.actor.maxInputsPerTick > 65536 || !p.actor.historyTicks ||
        p.actor.historyTicks > 65536 || p.actor.positionLimit <= 0 ||
        p.maxPendingPerRoom > p.actor.maxInputsPerTick ||
        !p.battleDurationTicks || !p.readyTimeoutTicks ||
        p.battleDurationTicks >= maximum - p.readyTimeoutTicks ||
        milliseconds <= 0 || milliseconds > 3'600'000 ||
        milliseconds >
            std::numeric_limits<std::int64_t>::max() / 1'000'000 ||
        !p.persistenceAdmissionAttempts ||
        p.persistenceAdmissionAttempts > 32 ||
        p.persistenceRetryBase.count() <= 0 ||
        p.persistenceRetryMax < p.persistenceRetryBase ||
        p.persistenceRetryMax > std::chrono::minutes(1))
        fail(Error::Invalid);
    return {
        p.actor,
        p.maxPendingPerRoom,
        p.battleDurationTicks,
        p.readyTimeoutTicks,
        milliseconds * 1'000'000
    };
}
std::int64_t nanos(arena::runtime::OwnerClock::time_point time)
{
    using Duration = arena::runtime::OwnerClock::duration;
    using Rep = Duration::rep;
    static_assert(
        std::is_integral_v<Rep> && std::is_signed_v<Rep> &&
            std::numeric_limits<Rep>::digits == 63 &&
            std::ratio_equal_v<Duration::period, std::nano>,
        "Legacy import requires the fenced Linux signed-64 "
        "nanosecond clock ABI"
    );
    const auto value = time.time_since_epoch().count();
    if (value < 0 || value > std::numeric_limits<std::int64_t>::max())
        fail(Error::Invalid);
    return static_cast<std::int64_t>(value);
}
runtime::RuntimeManaged managed(const ManagedMatchCheckpoint& m)
{
    if (m.retryAt || m.persistenceOutcome)
        fail(Error::Unsupported);
    if (static_cast<unsigned>(m.phase) >
        static_cast<unsigned>(RoomPhase::Finished))
        fail(Error::Invalid);
    const bool terminal = m.phase == RoomPhase::Settling ||
                          m.phase == RoomPhase::Finished;
    if (terminal ? (!m.result || !m.persistenceAccepted ||
                    m.admissionAttempts != 1)
                 : (m.result.has_value() || m.persistenceAccepted ||
                    m.admissionAttempts || m.persistenceBlocked))
        fail(Error::Invalid);
    if ((m.phase == RoomPhase::Finished) != m.expires.has_value() ||
        (m.phase == RoomPhase::Finished && m.persistenceBlocked))
        fail(Error::Invalid);
    for (const auto presence : m.presence)
        if (presence != Presence::Online &&
            presence != Presence::Detached)
            fail(Error::Invalid);
    const auto& a = m.allocation.members[0];
    const auto& b = m.allocation.members[1];
    runtime::RuntimeAllocation allocation{
        m.allocation.matchId,
        {{{a.playerId, a.sessionId, a.roomEpoch},
          {b.playerId, b.sessionId, b.roomEpoch}}},
        {a.initialMmr, b.initialMmr}
    };
    runtime::RuntimeManaged out{
        allocation,
        m.room,
        m.battle,
        static_cast<runtime::RuntimePhase>(m.phase),
        m.ready,
        {m.presence[0] == Presence::Online,
         m.presence[1] == Presence::Online},
        m.createdUnixMs,
        m.startedUnixMs,
        m.waitedTicks,
        m.startedTick,
        m.finishedTick,
        m.playedTicks,
        {},
        {},
        m.persistenceBlocked
    };
    if (m.expires)
        out.expiresAtNanos = nanos(*m.expires);
    if (m.result)
    {
        const auto& r = *m.result;
        if (static_cast<unsigned>(r.reason) >
            static_cast<unsigned>(SettlementReason::ReadyTimeout))
            fail(Error::Invalid);
        out.result.emplace(
            runtime::RuntimeSettlement{
                r.resultId,
                r.matchId,
                r.roomId,
                r.finalTick,
                r.finalChecksum,
                static_cast<runtime::RuntimeReason>(r.reason),
                r.winnerPlayerId,
                r.players,
                r.createdUnixMs,
                r.startedUnixMs,
                r.finishedUnixMs,
                r.startedTick,
                r.playedTicks
            }
        );
    }
    return out;
}
runtime::RuntimeImage image(
    const RoomPolicy& p,
    const runtime::Snapshot& actor,
    const ManagedMatchCheckpoint* match,
    const RoomCounterCheckpoint& contribution
)
{
    const auto& c = contribution.counters;
    if (contribution.roomId != actor.roomId || !noGauges(c) ||
        c.skippedWallSlots ||
        contribution.knownSinceTick > actor.tick ||
        (contribution.completeHistory &&
         contribution.knownSinceTick) ||
        c.ticks != actor.tick - contribution.knownSinceTick)
        fail(Error::Invalid);
    runtime::RuntimeImage out{
        policy(p),
        actor,
        {},
        {c.ticks,
         c.applied,
         c.rejected,
         c.duplicates,
         c.battlesStarted,
         c.battlesCompleted,
         c.battlesAborted,
         c.persistenceSubmitted,
         c.persistenceCommitted,
         c.persistenceFailed},
        actor.tick,
        contribution.knownSinceTick,
        contribution.completeHistory
    };
    if (match)
    {
        if (match->allocation.roomId != actor.roomId)
            fail(Error::Invalid);
        out.managed = managed(*match);
    }
    // Semantic kernel/FSM/checksum validation deliberately happens at
    // worker Stage.
    return out;
}
} // namespace

std::expected<LegacyRoomImport, LegacyRoomImportError>
splitLegacyRooms(const RoomCheckpoint& saved)
try
{
    const auto convertedPolicy = policy(saved.policy);
    (void)convertedPolicy;
    if (!saved.running || !saved.started || !saved.accepting)
        fail(Error::Unsupported);
    if (saved.actors.size() > saved.policy.maxRooms ||
        saved.actors.size() > 65536 ||
        saved.matches.size() > saved.actors.size() ||
        saved.roomCounters.size() > saved.actors.size())
        fail(Error::Capacity);
    if (saved.counterVersion > 1)
        fail(Error::Unsupported);
    if (!saved.counterVersion && !saved.roomCounters.empty())
        fail(Error::Invalid);
    LegacyRoomImport out{{}, {}, saved.incarnationHighWater};
    out.images.reserve(saved.actors.size());
    std::map<std::uint64_t, const runtime::Snapshot*> actors;
    std::set<std::uint64_t> incarnations;
    std::uint64_t previous = 0;
    for (const auto& actor : saved.actors)
    {
        if (actor.roomId.value() <= previous || !actor.incarnation ||
            actor.incarnation > saved.incarnationHighWater ||
            !incarnations.insert(actor.incarnation).second)
            fail(Error::Invalid);
        previous = actor.roomId.value();
        actors.emplace(previous, &actor);
    }
    std::map<std::uint64_t, const ManagedMatchCheckpoint*> matches;
    std::set<std::uint64_t> matchIds;
    RoomMetrics gauges;
    std::uint64_t started = 0, completed = 0, aborted = 0,
                  submitted = 0, failed = 0, committed = 0;
    previous = 0;
    for (const auto& match : saved.matches)
    {
        const auto id = match.allocation.roomId.value();
        if (id <= previous || !actors.contains(id) ||
            !matchIds.insert(match.allocation.matchId).second)
            fail(Error::Invalid);
        previous = id;
        matches.emplace(id, &match);
        if (match.phase == RoomPhase::Finished)
            ++gauges.retainedResults;
        else
            ++gauges.managedMatches;
        if (match.phase == RoomPhase::Settling)
        {
            ++gauges.pendingSettlements;
            gauges.blockedSettlements += match.persistenceBlocked;
        }
        started += match.playedTicks != 0;
        submitted += match.persistenceAccepted;
        failed += match.persistenceBlocked;
        committed += match.phase == RoomPhase::Finished;
        completed += match.phase == RoomPhase::Finished &&
                     match.result &&
                     match.result->reason ==
                         SettlementReason::Completed;
        aborted += match.phase == RoomPhase::Finished &&
                   match.result &&
                   match.result->reason !=
                       SettlementReason::Completed;
    }
    if (saved.metrics.actors != actors.size() ||
        saved.metrics.managedMatches != gauges.managedMatches ||
        saved.metrics.retainedResults != gauges.retainedResults ||
        saved.metrics.pendingSettlements !=
            gauges.pendingSettlements ||
        saved.metrics.blockedSettlements != gauges.blockedSettlements)
        fail(Error::Invalid);
    std::map<std::uint64_t, const RoomCounterCheckpoint*>
        contributions;
    if (!saved.counterVersion)
    {
        RoomMetrics empty;
        if (!noGauges(saved.unattributedCounters) ||
            !sameCounters(empty, saved.unattributedCounters) ||
            saved.metrics.battlesStarted < started ||
            saved.metrics.battlesCompleted < completed ||
            saved.metrics.battlesAborted < aborted ||
            saved.metrics.persistenceSubmitted < submitted ||
            saved.metrics.persistenceCommitted < committed ||
            saved.metrics.persistenceFailed < failed)
            fail(Error::Invalid);
        out.unattributed = saved.metrics;
        clearGauges(out.unattributed);
    }
    else
    {
        if (saved.roomCounters.size() != actors.size() ||
            !noGauges(saved.unattributedCounters) ||
            saved.unattributedCounters.skippedWallSlots !=
                saved.metrics.skippedWallSlots)
            fail(Error::Invalid);
        auto sum = saved.unattributedCounters;
        out.unattributed = saved.unattributedCounters;
        previous = 0;
        for (const auto& contribution : saved.roomCounters)
        {
            const auto id = contribution.roomId.value();
            if (id <= previous || !actors.contains(id) ||
                !noGauges(contribution.counters))
                fail(Error::Invalid);
            previous = id;
            contributions.emplace(id, &contribution);
            add(sum, contribution.counters);
            // The removed callback-admission metric has no worker
            // counterpart; retain its exact historical total in the
            // coordinator ledger.
            if (contribution.counters.persistenceRejected >
                maximum - out.unattributed.persistenceRejected)
                fail(Error::Invalid);
            out.unattributed
                .persistenceRejected += contribution.counters
                                            .persistenceRejected;
        }
        if (!sameCounters(sum, saved.metrics))
            fail(Error::Invalid);
    }
    for (const auto& actor : saved.actors)
    {
        const auto id = actor.roomId.value();
        RoomCounterCheckpoint unknown{
            actor.roomId,
            actor.tick,
            false,
            {}
        };
        const auto* contribution = saved.counterVersion
                                       ? contributions.at(id)
                                       : &unknown;
        const auto match = matches.find(id);
        out.images.push_back(image(
            saved.policy,
            actor,
            match == matches.end() ? nullptr : match->second,
            *contribution
        ));
    }
    return out;
}
catch (Error error)
{
    return std::unexpected(error);
}
catch (const std::bad_alloc&)
{
    return std::unexpected(Error::Capacity);
}
catch (...)
{
    return std::unexpected(Error::Invalid);
}

std::expected<LegacyRoomImage, LegacyRoomImportError>
importLegacyRoom(const RoomAggregate& saved)
try
{
    if (saved.version != 1)
        fail(Error::Unsupported);
    LegacyRoomImage out{
        image(
            saved.policy,
            saved.actor,
            saved.managed ? &*saved.managed : nullptr,
            saved.contribution
        ),
        saved.replay,
        {}
    };
    out.unattributed.persistenceRejected = saved.contribution.counters
                                               .persistenceRejected;
    return out;
}
catch (Error error)
{
    return std::unexpected(error);
}
catch (const std::bad_alloc&)
{
    return std::unexpected(Error::Capacity);
}
catch (...)
{
    return std::unexpected(Error::Invalid);
}
} // namespace service::lobby