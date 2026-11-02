/** @file
 * @brief Syncs complete disposable Lobby candidates before
 * publication and replies.
 */
#include "lobby/LobbyAuthority.h"

#include <algorithm>
#include <atomic>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/unknown_field_set.h>

#include "lobby/ReplayArchive.h"
#include "storage.pb.h"

namespace service::lobby
{
namespace
{
using Clock = LobbyAuthority::Clock;
using Error = LobbyAuthority::Error;
constexpr std::string_view coreKey = "authority/core";
constexpr std::string_view metadataKey = "authority/meta";
constexpr std::uint64_t idCeiling = std::uint64_t{1} << 63;

bool known(const google::protobuf::Message& message)
{
    return message.GetReflection()->GetUnknownFields(message).empty();
}
std::int64_t nanos(Clock::time_point point) noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               point.time_since_epoch()
    )
        .count();
}
bool elapsed(
    Clock::time_point now,
    Clock::time_point before,
    std::chrono::milliseconds duration
) noexcept
{
    if (now < before)
        return false;
    const auto ticks = static_cast<std::uint64_t>(
                           now.time_since_epoch().count()
                       ) -
                       static_cast<std::uint64_t>(
                           before.time_since_epoch().count()
                       );
    return std::chrono::duration_cast<
               std::chrono::duration<std::uint64_t, std::milli>>(
               std::chrono::duration<std::uint64_t, Clock::period>{
                   ticks
               }
           )
               .count() >=
           static_cast<std::uint64_t>(duration.count());
}

bool validMetadata(
    const LobbyAuthority::Snapshot& saved,
    const LobbyAuthority::Config& config
)
{
    const auto& meta = saved.meta;
    if (!known(meta) || meta.version() != 1 ||
        meta.boot_id() != config.bootId ||
        meta.reserved_epoch() < saved.core.registry.epochWatermark ||
        meta.incarnation_high_water() <
            saved.core.rooms.incarnationHighWater ||
        meta.transports_size() >
            static_cast<std::int64_t>(config.registry.maxPlayers) ||
        meta.receipts_size() >
            static_cast<std::int64_t>(config.maxReceipts) ||
        static_cast<std::size_t>(meta.transports_size()) +
                meta.receipts_size() + meta.settlements_size() >
            authorityCodecMaxItems ||
        static_cast<std::size_t>(meta.settlements_size()) >
            std::min(
                config.rooms.maxRooms,
                authorityCodecMaxItems / 2
            ) * 2 ||
        meta.ByteSizeLong() > authorityCodecMaxBytes)
        return false;
    const auto next = meta.ticket_next(), last = meta.ticket_last();
    if ((next == 0) != (last == 0) || last >= idCeiling ||
        (next && (next > last + 1 ||
                  next <= saved.core.queue.lastDurableId)) ||
        saved.core.queue.lastDurableId > last ||
        !saved.core.queue.externalIds)
        return false;
    std::map<std::uint64_t, const RegistryEntryCheckpoint*> entries;
    for (const auto& entry : saved.core.registry.entries)
        entries.emplace(entry.snapshot.playerId.value(), &entry);
    std::set<std::uint64_t> handles, players;
    std::set<std::pair<std::string, std::uint64_t>> external;
    for (const auto& transport : meta.transports())
    {
        const auto entry = entries.find(transport.player_id());
        if (!known(transport) ||
            transport.gateway_instance().empty() ||
            transport.gateway_instance().size() > 128 ||
            !transport.connection_id() || !transport.handle() ||
            transport.handle() > meta.transport_high_water() ||
            !transport.player_id() || !transport.epoch() ||
            transport.epoch() > meta.reserved_epoch() ||
            transport.token_expires_at_unix() <= 0 ||
            !handles.insert(transport.handle()).second ||
            !players.insert(transport.player_id()).second ||
            !external
                 .emplace(
                     transport.gateway_instance(),
                     transport.connection_id()
                 )
                 .second ||
            entry == entries.end())
            return false;
        const auto& current = *entry->second;
        if (current.snapshot.presence != Presence::Online ||
            current.snapshot.connectionId != transport.handle() ||
            current.snapshot.epoch != transport.epoch() ||
            current.expiresAt != transport.token_expires_at_unix())
            return false;
    }
    for (const auto& entry : saved.core.registry.entries)
        if (entry.snapshot.presence == Presence::Online &&
            !players.contains(entry.snapshot.playerId.value()))
            return false;
    std::set<std::string> receipts;
    for (const auto& receipt : meta.receipts())
    {
        arena::protocol::Envelope reply;
        if (!known(receipt) || receipt.key().empty() ||
            receipt.key().size() > 256 ||
            !receipts.insert(receipt.key()).second ||
            !receipt.method() ||
            receipt.request_fingerprint().empty() ||
            receipt.request_fingerprint().size() > 64 ||
            !receipt.player_id() || !receipt.session_id() ||
            receipt.session_id() >
                saved.core.registry.sessionWatermark ||
            !receipt.epoch() ||
            receipt.epoch() > meta.reserved_epoch() ||
            receipt.account().size() > 128 ||
            receipt.credential_kind() > 1 ||
            receipt.reply().empty() ||
            receipt.reply().size() > authorityCodecMaxBytes ||
            !reply.ParseFromString(receipt.reply()) ||
            reply.payload_case() ==
                arena::protocol::Envelope::PAYLOAD_NOT_SET ||
            reply.has_login_request())
            return false;
        if ((!receipt.prior_epoch()) !=
                (!receipt.prior_expires_at_unix()) ||
            receipt.prior_expires_at_unix() < 0 ||
            receipt.prior_epoch() > meta.reserved_epoch())
            return false;
    }
    std::set<std::uint64_t> settlements;
    for (const auto& delivery : meta.settlements())
    {
        arena::storage::SaveResultRequest request;
        if (!known(delivery) || !delivery.match_id() ||
            !settlements.insert(delivery.match_id()).second ||
            delivery.stable_idempotency_key().empty() ||
            delivery.stable_idempotency_key().size() > 256 ||
            delivery.max_attempts() == 0 ||
            delivery.max_attempts() > 64 ||
            delivery.attempts() > delivery.max_attempts() ||
            delivery.phase() <
                arena::lobby_meta::SettlementDelivery::PENDING ||
            delivery.phase() >
                arena::lobby_meta::SettlementDelivery::ACKNOWLEDGED ||
            delivery.retry_base_ms() == 0 ||
            delivery.retry_base_ms() > delivery.retry_max_ms() ||
            delivery.retry_max_ms() > 30000 ||
            delivery.deadline_nanos() <=
                delivery.first_attempt_nanos() ||
            static_cast<std::uint64_t>(delivery.deadline_nanos()) -
                    static_cast<std::uint64_t>(
                        delivery.first_attempt_nanos()
                    ) >
                3600000000000ULL ||
            delivery.next_attempt_nanos() <
                delivery.first_attempt_nanos() ||
            delivery.next_attempt_nanos() >
                delivery.deadline_nanos() ||
            ((delivery.phase() ==
                  arena::lobby_meta::SettlementDelivery::IN_FLIGHT ||
              delivery.phase() ==
                  arena::lobby_meta::SettlementDelivery::
                      OUTCOME_UNKNOWN ||
              delivery.phase() ==
                  arena::lobby_meta::SettlementDelivery::
                      ACKNOWLEDGED) &&
             !delivery.attempts()) ||
            delivery.request().empty() ||
            delivery.request().size() > authorityCodecMaxBytes ||
            !request.ParseFromString(delivery.request()) ||
            !known(request) || !request.has_match() ||
            !known(request.match()) ||
            request.match().match_id() != delivery.match_id() ||
            !request.match().room_id() ||
            request.match().participants_size() != 2 ||
            request.match().result_version() != 1 ||
            (request.match().state() != "completed" &&
             request.match().state() != "aborted"))
            return false;
        const auto& record = request.match();
        if (record.room_id() !=
                ((std::uint64_t{1} << 63) | record.match_id()) ||
            record.match_id() >= idCeiling)
            return false;
        for (int index = 0; index < record.participants_size();
             ++index)
        {
            const auto& member = record.participants(index);
            if (!known(member) || !member.player_id() ||
                !member.session_id() || !member.membership_epoch() ||
                member.session_id() >
                    saved.core.registry.sessionWatermark ||
                member.slot() != static_cast<unsigned>(index + 1) ||
                member.initial_mmr() > 10000 ||
                member.last_dx() < -1 || member.last_dx() > 1 ||
                member.last_dy() < -1 || member.last_dy() > 1 ||
                member.final_x() <
                    -config.rooms.actor.positionLimit ||
                member.final_x() > config.rooms.actor.positionLimit ||
                member.final_y() <
                    -config.rooms.actor.positionLimit ||
                member.final_y() > config.rooms.actor.positionLimit ||
                (index &&
                 record.participants(index - 1).player_id() >=
                     member.player_id()))
                return false;
        }
        const auto match = std::find_if(
            saved.core.rooms.matches.begin(),
            saved.core.rooms.matches.end(),
            [&](const auto& value)
            {
                return value.allocation.matchId ==
                       delivery.match_id();
            }
        );
        if (match == saved.core.rooms.matches.end())
        {
            if (delivery.phase() !=
                arena::lobby_meta::SettlementDelivery::ACKNOWLEDGED)
                return false;
        }
        else
        {
            if (!match->result ||
                match->result->finalTick != record.final_tick() ||
                match->result->finalChecksum !=
                    record.final_checksum() ||
                match->allocation.roomId.value() !=
                    record.room_id() ||
                (delivery.phase() ==
                     arena::lobby_meta::SettlementDelivery::
                         ACKNOWLEDGED &&
                 match->phase != RoomPhase::Finished))
                return false;
            for (std::size_t index = 0;
                 index < match->allocation.members.size();
                 ++index)
            {
                const auto& member = match->allocation.members[index];
                const auto& stored = record.participants(
                    static_cast<int>(index)
                );
                if (member.playerId.value() != stored.player_id() ||
                    member.sessionId.value() != stored.session_id() ||
                    member.roomEpoch != stored.membership_epoch() ||
                    member.initialMmr != stored.initial_mmr())
                    return false;
            }
        }
    }
    return true;
}
Error mapError(DurableStateStore::Error error) noexcept
{
    switch (error)
    {
    case DurableStateStore::Error::Capacity:
        return Error::Capacity;
    case DurableStateStore::Error::OutcomeUnknown:
        return Error::OutcomeUnknown;
    case DurableStateStore::Error::Conflict:
        return Error::InvalidState;
    case DurableStateStore::Error::Unavailable:
        return Error::Unavailable;
    }
    return Error::Unavailable;
}
std::string replayKey(const service::room::ReplayLog& log)
{
    return "replay/" + std::to_string(log.checkpoint.roomId.value()) +
           "/" + std::to_string(log.checkpoint.incarnation) + "/" +
           std::to_string(log.ticks.front().tick);
}
} // namespace

struct LobbyAuthority::WorkingState::Auxiliary
{
    arena::lobby_meta::Metadata meta;
    Clock::time_point now;
    std::int64_t wall;
    bool collect{true};
    std::vector<SessionBinding> revoked;
};
LobbyAuthority::WorkingState::WorkingState(
    std::shared_ptr<Auxiliary> auxiliary
)
    : m_aux(std::move(auxiliary))
    , meta(m_aux->meta)
    , now(m_aux->now)
    , wallNow(m_aux->wall)
{
}
LobbyAuthority::WorkingState::~WorkingState()
{
    m_aux->collect = false;
    rooms.reset();
    queue.reset();
    online.reset();
}
std::function<std::int64_t()>
LobbyAuthority::WorkingState::leaseClock() const
{
    return [aux = m_aux] { return aux->wall; };
}
OnlineRegistry::KickFactory
LobbyAuthority::WorkingState::kickFactory() const
{
    return [aux = m_aux](SessionBinding binding)
    {
        return [aux, binding]
        {
            if (aux->collect)
                aux->revoked.push_back(binding);
        };
    };
}

std::shared_ptr<arena::lobby_meta::Metadata>
LobbyAuthority::WorkingState::metadataHandle() const
{
    return {m_aux, &m_aux->meta};
}

struct LobbyAuthority::State : std::enable_shared_from_this<State>
{
    struct CaptureSlot
    {
        std::optional<Reply> reply;
        bool open{true}, invalid{false};
    };
    struct Job
    {
        Mutation mutation;
        Completion complete;
        Clock::time_point deadline;
        std::shared_ptr<std::atomic<bool>> cancelled;
        bool atTick;
    };
    struct Batch
    {
        std::shared_ptr<const Snapshot> snapshot;
        std::vector<std::shared_ptr<Job>> jobs;
        std::vector<CommitReply> replies;
        std::vector<SessionBinding> revoked;
        bool bootstrap{false};
    };
    State(boost::asio::any_io_executor executor, Config configured)
        : owner(boost::asio::make_strand(executor))
        , config(std::move(configured))
        , tickTimer(owner)
        , closeTimer(owner)
    {
    }

    std::unique_ptr<WorkingState> working(
        const Snapshot* source,
        const WorkingState::Auxiliary* frozen = nullptr
    )
    {
        auto aux = std::make_shared<WorkingState::Auxiliary>();
        aux->now = frozen ? frozen->now : config.now();
        aux->wall = frozen ? frozen->wall : config.wallNow();
        if (aux->wall < 0 ||
            aux->wall >
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::duration::max()
                )
                    .count())
            throw std::invalid_argument(
                "invalid authority wall clock"
            );
        if (source)
            aux->meta = source->meta;
        else
        {
            aux->meta.set_version(1);
            aux->meta.set_boot_id(config.bootId);
        }
        auto candidate = std::unique_ptr<WorkingState>(
            new WorkingState(aux)
        );
        auto registryConfig = config.registry;
        registryConfig.now = [aux] { return aux->now; };
        if (source)
        {
            auto registry = OnlineRegistry::restore(
                source->core.registry,
                registryConfig,
                candidate->leaseClock(),
                candidate->kickFactory()
            );
            if (!registry)
                throw std::invalid_argument(
                    "invalid registry checkpoint"
                );
            candidate->online = std::move(*registry);
        }
        else
        {
            candidate->online = std::make_shared<OnlineRegistry>(
                registryConfig
            );
            aux->meta.set_reserved_epoch(
                candidate->online->epochWatermark()
            );
        }
        auto queueConfig = config.queue;
        queueConfig.ticketIdSource =
            [aux]() -> std::optional<std::uint64_t>
        {
            const auto value = aux->meta.ticket_next();
            if (!value || value > aux->meta.ticket_last() ||
                value >= idCeiling)
                return {};
            aux->meta.set_ticket_next(value + 1);
            return value;
        };
        auto eligibility =
            [registry = candidate->online](auto player, auto session)
        {
            const auto current = registry->snapshot(player);
            if (!current || current->sessionId != session ||
                current->roomId)
                return service::match::Availability::Gone;
            return current->presence == Presence::Online
                       ? service::match::Availability::Ready
                       : service::match::Availability::Paused;
        };
        if (source)
        {
            auto queue = service::match::MatchQueue::restore(
                source->core.queue,
                queueConfig,
                eligibility
            );
            if (!queue)
                throw std::invalid_argument(
                    "invalid queue checkpoint"
                );
            candidate->queue = std::move(*queue);
        }
        else
            candidate->queue = std::make_unique<
                service::match::MatchQueue>(queueConfig, eligibility);
        auto roomConfig = config.rooms;
        roomConfig.externalTicks = true;
        roomConfig.externalSettlement = true;
        roomConfig.persistSettlement = {};
        roomConfig.replays.reset();
        roomConfig.now = [aux] { return aux->now; };
        roomConfig.wallNow = [aux]
        {
            return std::chrono::system_clock::time_point{
                std::chrono::seconds(aux->wall)
            };
        };
        roomConfig.incarnationSource = [aux]
        {
            const auto high = aux->meta.incarnation_high_water();
            if (high == UINT64_MAX)
                throw std::overflow_error(
                    "actor incarnation exhausted"
                );
            aux->meta.set_incarnation_high_water(high + 1);
            return high + 1;
        };
        if (source)
        {
            auto rooms = RoomService::restore(
                candidate->online,
                source->core.rooms,
                roomConfig
            );
            if (!rooms)
                throw std::invalid_argument(
                    "invalid room checkpoint"
                );
            candidate->rooms = std::move(*rooms);
        }
        else
        {
            candidate->rooms = std::make_unique<RoomService>(
                candidate->online,
                roomConfig
            );
            candidate->rooms->start({});
        }
        return candidate;
    }
    void closeSlots(
        const std::vector<std::shared_ptr<CaptureSlot>>& slots
    ) noexcept
    {
        for (const auto& slot : slots)
            slot->open = false;
    }
    void deliver(
        const std::shared_ptr<Job>& job,
        std::expected<CommitReply, Error> result
    ) noexcept
    {
        try
        {
            job->complete(std::move(result));
        }
        catch (...)
        {
            std::lock_guard lock(mutex);
            ++counts.callbackFailures;
        }
        std::lock_guard lock(mutex);
        --counts.pending;
        if (job->atTick)
            --counts.roomPending;
    }
    void failJobs(
        const std::vector<std::shared_ptr<Job>>& jobs, Error error
    ) noexcept
    {
        for (const auto& job : jobs)
            deliver(job, std::unexpected(error));
    }
    bool expired(const Job& job) const noexcept
    {
        return job.cancelled->load(std::memory_order_acquire) ||
               Clock::now() >= job.deadline;
    }
    void scheduleLocked()
    {
        if (wakeQueued)
            return;
        wakeQueued = true;
        try
        {
            boost::asio::post(
                owner,
                [self = shared_from_this()]
                {
                    {
                        std::lock_guard lock(self->mutex);
                        self->wakeQueued = false;
                    }
                    self->pump();
                }
            );
        }
        catch (...)
        {
            wakeQueued = false;
            throw;
        }
    }
    void schedule()
    {
        std::lock_guard lock(mutex);
        scheduleLocked();
    }
    void resume() noexcept
    {
        try
        {
            schedule();
        }
        catch (...)
        {
            fail();
        }
    }
    void armTick()
    {
        if (!config.autoTicks || stopping.load())
            return;
        tickTimer.expires_after(RoomService::tickInterval);
        tickTimer.async_wait(
            [self = shared_from_this()](auto error)
            {
                if (error || self->stopping.load())
                    return;
                self->tickDue = true;
                try
                {
                    self->armTick();
                    self->pump();
                }
                catch (...)
                {
                    self->fail();
                }
            }
        );
    }
    bool tickWork(
        const Snapshot& current,
        Clock::time_point now,
        std::int64_t wall
    ) const
    {
        if (!current.core.rooms.actors.empty() ||
            current.core.queue.metrics.queued)
            return true;
        for (const auto& entry : current.core.registry.entries)
            if (entry.expiresAt <= wall ||
                (entry.snapshot.graceDeadline &&
                 now >= *entry.snapshot.graceDeadline))
                return true;
        for (const auto& entry : current.core.queue.entries)
            if (entry.reclaimFrom && elapsed(
                                         now,
                                         *entry.reclaimFrom,
                                         config.queue.retention
                                     ))
                return true;
        for (const auto& transport : current.meta.transports())
            if (transport.lease_deadline_nanos() <= nanos(now))
                return true;
        for (const auto& receipt : current.meta.receipts())
            if (receipt.expires_nanos() <= nanos(now))
                return true;
        for (const auto& delivery : current.meta.settlements())
            if (delivery.phase() ==
                    arena::lobby_meta::SettlementDelivery::
                        ACKNOWLEDGED &&
                delivery.retention_deadline_nanos() <= nanos(now) &&
                std::none_of(
                    current.core.rooms.matches.begin(),
                    current.core.rooms.matches.end(),
                    [&](const auto& match)
                    {
                        return match.allocation.matchId ==
                               delivery.match_id();
                    }
                ))
                return true;
        return false;
    }
    void expireMetadata(WorkingState& candidate)
    {
        auto* transports = candidate.meta.mutable_transports();
        for (int index = transports->size(); index-- > 0;)
        {
            const auto& transport = transports->Get(index);
            if (transport.lease_deadline_nanos() <=
                nanos(candidate.now))
            {
                candidate.online->detach(
                    {arena::domain::PlayerId{transport.player_id()},
                     transport.handle(),
                     transport.epoch()}
                );
                transports->DeleteSubrange(index, 1);
            }
        }
        auto* receipts = candidate.meta.mutable_receipts();
        for (int index = receipts->size(); index-- > 0;)
            if (receipts->Get(index).expires_nanos() <=
                nanos(candidate.now))
                receipts->DeleteSubrange(index, 1);
    }
    void reconcileTransports(WorkingState& candidate)
    {
        const auto registry = candidate.online->checkpoint();
        std::map<std::uint64_t, const RegistryEntryCheckpoint*>
            entries;
        for (const auto& entry : registry.entries)
            entries.emplace(entry.snapshot.playerId.value(), &entry);
        auto* transports = candidate.meta.mutable_transports();
        for (int index = transports->size(); index-- > 0;)
        {
            const auto& transport = transports->Get(index);
            const auto found = entries.find(transport.player_id());
            if (found == entries.end() ||
                found->second->snapshot.presence !=
                    Presence::Online ||
                found->second->snapshot.connectionId !=
                    transport.handle() ||
                found->second->snapshot.epoch != transport.epoch())
                transports->DeleteSubrange(index, 1);
        }
        const auto rooms = candidate.rooms->checkpoint();
        if (!rooms)
            throw std::invalid_argument(
                "room metadata cutoff is busy"
            );
        auto* settlements = candidate.meta.mutable_settlements();
        for (int index = settlements->size(); index-- > 0;)
        {
            const auto& delivery = settlements->Get(index);
            if (delivery.phase() ==
                    arena::lobby_meta::SettlementDelivery::
                        ACKNOWLEDGED &&
                delivery.retention_deadline_nanos() <=
                    nanos(candidate.now) &&
                std::none_of(
                    rooms->matches.begin(),
                    rooms->matches.end(),
                    [&](const auto& match)
                    {
                        return match.allocation.matchId ==
                               delivery.match_id();
                    }
                ))
                settlements->DeleteSubrange(index, 1);
        }
    }
    void advance(WorkingState& candidate)
    {
        expireMetadata(candidate);
        candidate.online->sweep();
        const auto pairs = candidate.queue->advance(candidate.now);
        for (const auto& pair : pairs)
        {
            auto room = candidate.online->allocateMatch(
                pair.first.matchId,
                {{{pair.first.playerId,
                   pair.first.sessionId,
                   pair.first.mmr},
                  {pair.second.playerId,
                   pair.second.sessionId,
                   pair.second.mmr}}}
            );
            bool hosted = false;
            if (room)
            {
                hosted = candidate.rooms->openMatch(*room);
                if (!hosted)
                    candidate.online->finishMatch(
                        room->roomId,
                        room->matchId
                    );
            }
            candidate.queue->release(
                pair.first.matchId,
                candidate.now,
                hosted ? std::optional(room->roomId) : std::nullopt
            );
        }
        if (!candidate.rooms->advance())
            throw std::invalid_argument(
                "room cutoff could not complete"
            );
    }
    std::shared_ptr<Snapshot> freeze(
        WorkingState& candidate,
        std::uint64_t revision,
        const Snapshot* previous = nullptr
    )
    {
        auto result = std::make_shared<Snapshot>();
        result->revision = revision;
        result->core.registry = candidate.online->checkpoint();
        result->core.queue = candidate.queue->checkpoint();
        auto rooms = candidate.rooms->checkpoint();
        if (!rooms)
            throw std::invalid_argument(
                "room mailbox is not quiescent"
            );
        result->core.rooms = std::move(*rooms);
        result->meta = candidate.meta;
        if (!validMetadata(*result, config))
            throw std::invalid_argument("invalid authority metadata");
        if (previous)
        {
            const auto& old = previous->meta;
            if (result->meta.reserved_epoch() <
                    old.reserved_epoch() ||
                result->meta.transport_high_water() <
                    old.transport_high_water() ||
                result->meta.incarnation_high_water() <
                    old.incarnation_high_water() ||
                result->meta.ticket_last() < old.ticket_last() ||
                result->meta.ticket_next() < old.ticket_next() ||
                result->core.registry.epochWatermark <
                    previous->core.registry.epochWatermark ||
                result->core.registry.sessionWatermark <
                    previous->core.registry.sessionWatermark ||
                result->core.queue.lastTicketId <
                    previous->core.queue.lastTicketId ||
                result->core.queue.lastDurableId <
                    previous->core.queue.lastDurableId ||
                result->core.rooms.incarnationHighWater <
                    previous->core.rooms.incarnationHighWater)
                throw std::invalid_argument(
                    "authority watermarks cannot decrease"
                );
            for (const auto& receipt : old.receipts())
            {
                const auto found = std::find_if(
                    result->meta.receipts().begin(),
                    result->meta.receipts().end(),
                    [&](const auto& value)
                    { return value.key() == receipt.key(); }
                );
                if (found != result->meta.receipts().end() &&
                    found->expires_nanos() != receipt.expires_nanos())
                    throw std::invalid_argument(
                        "receipt retention cannot be renewed"
                    );
            }
            for (const auto& delivery : old.settlements())
            {
                const auto found = std::find_if(
                    result->meta.settlements().begin(),
                    result->meta.settlements().end(),
                    [&](const auto& value)
                    {
                        return value.match_id() ==
                               delivery.match_id();
                    }
                );
                if (found == result->meta.settlements().end())
                {
                    if (delivery.phase() !=
                            arena::lobby_meta::SettlementDelivery::
                                ACKNOWLEDGED ||
                        delivery.retention_deadline_nanos() >
                            nanos(candidate.now) ||
                        std::any_of(
                            result->core.rooms.matches.begin(),
                            result->core.rooms.matches.end(),
                            [&](const auto& match)
                            {
                                return match.allocation.matchId ==
                                       delivery.match_id();
                            }
                        ))
                        throw std::invalid_argument(
                            "unacknowledged settlement intent cannot "
                            "be discarded"
                        );
                }
                else if (
                    found->stable_idempotency_key() !=
                        delivery.stable_idempotency_key() ||
                    found->request() != delivery.request() ||
                    found->max_attempts() !=
                        delivery.max_attempts() ||
                    found->first_attempt_nanos() !=
                        delivery.first_attempt_nanos() ||
                    found->deadline_nanos() !=
                        delivery.deadline_nanos() ||
                    found->retry_base_ms() !=
                        delivery.retry_base_ms() ||
                    found->retry_max_ms() !=
                        delivery.retry_max_ms() ||
                    found->attempts() < delivery.attempts() ||
                    found->attempts() - delivery.attempts() > 1 ||
                    (delivery.phase() ==
                         arena::lobby_meta::SettlementDelivery::
                             ACKNOWLEDGED &&
                     (found->phase() != delivery.phase() ||
                      found->retention_deadline_nanos() !=
                          delivery.retention_deadline_nanos()))
                )
                    throw std::invalid_argument(
                        "settlement retry budget cannot be reset"
                    );
            }
        }
        auto validated = working(result.get(), candidate.m_aux.get());
        return result;
    }
    std::vector<DurableStateStore::Mutation>
    encode(const Snapshot& next, WorkingState* candidate)
    {
        auto core = encodeCoreCheckpoint(next.core);
        std::string metadata;
        if (!core || !next.meta.SerializeToString(&metadata) ||
            metadata.size() > authorityCodecMaxBytes)
            throw std::invalid_argument("authority encoding failed");
        std::vector<DurableStateStore::Mutation> mutations;
        mutations.reserve(next.core.rooms.actors.size() + 2);
        mutations.push_back({std::string(coreKey), std::move(*core)});
        mutations.push_back(
            {std::string(metadataKey), std::move(metadata)}
        );
        if (candidate)
        {
            const auto previous = store->snapshot();
            for (const auto& actor : next.core.rooms.actors)
            {
                const auto log = candidate->rooms->replayLog(
                    actor.roomId
                );
                if (!log || log->ticks.empty())
                    continue;
                auto value = encodeReplayLog(*log);
                auto key = replayKey(*log);
                if (!value || previous->values.contains(key))
                    throw std::invalid_argument(
                        "replay segment would overwrite committed "
                        "history"
                    );
                mutations.push_back(
                    {std::move(key), std::move(*value)}
                );
            }
        }
        return mutations;
    }

    void admit(
        std::shared_ptr<Batch> batch,
        std::vector<DurableStateStore::Mutation> mutations
    )
    try
    {
        writing = true;
        {
            std::lock_guard lock(mutex);
            counts.writeInFlight = true;
        }
        const auto accepted = store->commit(
            batch->snapshot->revision - 1,
            std::move(mutations),
            [self = shared_from_this(),
             batch](DurableStateStore::Result result)
            {
                self->writing = false;
                {
                    std::lock_guard lock(self->mutex);
                    self->counts.writeInFlight = false;
                }
                if (result && *result == batch->snapshot->revision)
                {
                    if (batch->bootstrap && !self->stopping.load())
                    {
                        try
                        {
                            self->armTick();
                        }
                        catch (...)
                        {
                            self->fail();
                            return;
                        }
                    }
                    self->published.store(
                        batch->snapshot,
                        std::memory_order_release
                    );
                    {
                        std::lock_guard lock(self->mutex);
                        ++self->counts.committed;
                        if (batch->bootstrap)
                        {
                            self->counts.initialized = true;
                            self->counts.ready = !self->stopping
                                                      .load();
                        }
                    }
                    for (const auto binding : batch->revoked)
                        if (self->config.onRevoked)
                            try
                            {
                                self->config.onRevoked(binding);
                            }
                            catch (...)
                            {
                                std::lock_guard lock(self->mutex);
                                ++self->counts.callbackFailures;
                            }
                    for (std::size_t i = 0; i < batch->jobs.size();
                         ++i)
                        self->deliver(
                            batch->jobs[i],
                            std::move(batch->replies[i])
                        );
                }
                else
                {
                    const auto error = result
                                           ? Error::OutcomeUnknown
                                           : mapError(result.error());
                    self->failJobs(batch->jobs, error);
                    if (batch->bootstrap ||
                        error == Error::OutcomeUnknown ||
                        error == Error::Unavailable ||
                        error == Error::InvalidState)
                        self->fail();
                }
                self->pump();
            }
        );
        if (!accepted)
        {
            writing = false;
            {
                std::lock_guard lock(mutex);
                counts.writeInFlight = false;
            }
            failJobs(
                batch->jobs,
                store->stats().poisoned ? Error::OutcomeUnknown
                                        : Error::Capacity
            );
            if (batch->bootstrap || store->stats().poisoned)
                fail();
            else
                resume();
        }
    }
    catch (...)
    {
        writing = false;
        {
            std::lock_guard lock(mutex);
            counts.writeInFlight = false;
        }
        failJobs(batch->jobs, Error::Capacity);
        if (batch->bootstrap)
            fail();
        else
            resume();
    }
    void transact(std::vector<std::shared_ptr<Job>> jobs, bool tick)
    {
        std::vector<std::shared_ptr<CaptureSlot>> slots;
        try
        {
            const auto source = published.load(
                std::memory_order_acquire
            );
            if (!source || source->revision == UINT64_MAX)
                throw std::invalid_argument(
                    "authority revision exhausted"
                );
            WorkingState::Auxiliary frozen;
            frozen.now = config.now();
            frozen.wall = config.wallNow();
            if (tick && jobs.empty() &&
                !tickWork(*source, frozen.now, frozen.wall))
            {
                resume();
                return;
            }
            auto candidate = working(source.get(), &frozen);
            slots.reserve(jobs.size());
            for (const auto& job : jobs)
            {
                auto slot = std::make_shared<CaptureSlot>();
                slots.push_back(slot);
                job->mutation(
                    *candidate,
                    [slot](Reply reply)
                    {
                        if (!slot->open)
                            return;
                        if (slot->reply)
                            slot->invalid = true;
                        else
                            slot->reply.emplace(std::move(reply));
                    }
                );
            }
            if (tick)
                advance(*candidate);
            reconcileTransports(*candidate);
            for (const auto& slot : slots)
                if (!slot->reply || slot->invalid ||
                    (slot->reply->has_value() &&
                     slot->reply->value().ByteSizeLong() >
                         authorityCodecMaxBytes))
                    throw std::invalid_argument(
                        "mutation did not capture exactly one "
                        "bounded reply"
                    );
            auto next = freeze(
                *candidate,
                source->revision + 1,
                source.get()
            );
            auto mutations = encode(*next, candidate.get());
            auto batch = std::make_shared<Batch>();
            batch->snapshot = next;
            batch->jobs = jobs;
            batch->replies.reserve(jobs.size());
            for (auto& slot : slots)
                batch->replies.push_back(
                    {std::move(*slot->reply), next}
                );
            batch->revoked = candidate->m_aux->revoked;
            closeSlots(slots);
            candidate.reset();
            if (stopping.load())
            {
                failJobs(jobs, Error::Cancelled);
                resume();
                return;
            }
            // Rebuild surviving jobs from committed state if a caller
            // cancelled during CPU preparation. No partial candidate
            // is ever written.
            std::vector<std::shared_ptr<Job>> cancelled;
            cancelled.reserve(jobs.size());
            for (const auto& job : jobs)
                if (expired(*job))
                    cancelled.push_back(job);
            if (!cancelled.empty())
            {
                {
                    std::lock_guard lock(mutex);
                    auto& target = tick ? roomJobs : controlJobs;
                    auto prepared = target;
                    for (auto it = jobs.rbegin(); it != jobs.rend();
                         ++it)
                        if (std::find(
                                cancelled.begin(),
                                cancelled.end(),
                                *it
                            ) == cancelled.end())
                            prepared.push_front(*it);
                    target.swap(prepared);
                }
                failJobs(cancelled, Error::Cancelled);
                if (tick)
                    tickDue = true;
                resume();
                return;
            }
            admit(std::move(batch), std::move(mutations));
        }
        catch (const std::bad_alloc&)
        {
            closeSlots(slots);
            failJobs(jobs, Error::Capacity);
            resume();
        }
        catch (...)
        {
            closeSlots(slots);
            failJobs(jobs, Error::InvalidState);
            resume();
        }
    }
    void pump()
    try
    {
        if (writing)
            return;
        if (stopping.load())
        {
            close();
            return;
        }
        if (tickRequested.exchange(false))
            tickDue = true;
        std::vector<std::shared_ptr<Job>> jobs;
        bool tick = false;
        {
            std::lock_guard lock(mutex);
            if (!counts.ready)
                return;
            if (tickDue && (controlJobs.empty() || !lastWasTick))
            {
                jobs.reserve(roomJobs.size());
                jobs.assign(roomJobs.begin(), roomJobs.end());
                roomJobs.clear();
                tick = true;
                tickDue = false;
            }
            else if (!controlJobs.empty())
            {
                jobs.reserve(1);
                jobs.push_back(std::move(controlJobs.front()));
                controlJobs.pop_front();
            }
        }
        lastWasTick = tick;
        for (auto it = jobs.begin(); it != jobs.end();)
            if (expired(**it))
            {
                auto job = *it;
                it = jobs.erase(it);
                {
                    std::lock_guard lock(mutex);
                    ++counts.cancelled;
                }
                deliver(job, std::unexpected(Error::Cancelled));
            }
            else
                ++it;
        if (jobs.empty())
        {
            if (!tick)
            {
                bool more;
                {
                    std::lock_guard lock(mutex);
                    more = !controlJobs.empty();
                }
                if (more)
                    resume();
                return;
            }
        }
        transact(std::move(jobs), tick);
    }
    catch (...)
    {
        fail();
    }

    void validateReplays(
        const DurableStateStore::Snapshot& durable,
        const Snapshot& current
    )
    {
        for (const auto& [key, value] : durable.values)
        {
            (void)value;
            if (key != coreKey && key != metadataKey &&
                !key.starts_with("replay/"))
                throw std::invalid_argument(
                    "unknown authority journal key"
                );
        }
        const auto multiply =
            [](std::uint64_t left, std::uint64_t right)
        {
            return right && left > UINT64_MAX / right ? UINT64_MAX
                                                      : left * right;
        };
        const auto add = [](std::uint64_t left, std::uint64_t right)
        {
            return right > UINT64_MAX - left ? UINT64_MAX
                                             : left + right;
        };
        service::room::ReplayFileLimits limits;
        limits.maxMembers = config.rooms.actor.maxMembers;
        limits.maxInputsPerTick = config.rooms.actor.maxInputsPerTick;
        limits.maxHistoryTicks = config.rooms.actor.historyTicks;
        // Each encoded logical component consumes at least one byte.
        // The largest expanded file component is a 48-byte
        // PlayerState; each complete archive additionally owns its
        // 104-byte header/footer. These are conservative bounds on
        // already admitted journal state, not the offline export
        // tool's default file/tick policy.
        limits.maxFileBytes = add(
            multiply(config.journal.maxStateBytes, 48),
            multiply(config.journal.maxKeys, 104)
        );
        limits.maxTicks = std::min<std::uint64_t>(
            config.journal.maxStateBytes,
            multiply(config.journal.maxKeys, authorityCodecMaxItems)
        );
        auto archives = assembleReplayArchives(
            durable,
            limits,
            config.journal.maxKeys
        );
        if (!archives)
            throw std::invalid_argument(
                "committed replay archive is invalid"
            );
        using Identity = std::pair<std::uint64_t, std::uint64_t>;
        std::map<Identity, const service::room::Snapshot*> actors;
        for (const auto& actor : current.core.rooms.actors)
            actors.emplace(
                Identity{actor.roomId.value(), actor.incarnation},
                &actor
            );
        std::set<Identity> histories;
        for (const auto& archive : *archives)
        {
            const auto& initial = archive.log.checkpoint;
            if (initial.incarnation >
                current.meta.incarnation_high_water())
                throw std::invalid_argument(
                    "committed replay exceeds incarnation watermark"
                );
            const Identity identity{
                initial.roomId.value(),
                initial.incarnation
            };
            histories.insert(identity);
            const auto actor = actors.find(identity);
            if (actor != actors.end() &&
                *actor->second != archive.finalSnapshot)
                throw std::invalid_argument(
                    "replay tail differs from committed actor"
                );
        }
        for (const auto& [identity, actor] : actors)
            if (actor->tick && !histories.contains(identity))
                throw std::invalid_argument(
                    "committed actor has no input history"
                );
    }
    void bootstrap()
    {
        try
        {
            if (stopping.load())
            {
                close();
                return;
            }
            auto opened = std::make_shared<DurableStateStore>(
                owner,
                config.journal
            );
            {
                std::lock_guard lock(mutex);
                store = opened;
            }
            if (stopping.load())
            {
                close();
                return;
            }
            const auto durable = store->snapshot();
            if (durable->values.empty())
            {
                if (durable->revision)
                    throw std::invalid_argument(
                        "nonzero authority revision has no core"
                    );
                auto candidate = working(nullptr);
                auto snapshot = freeze(*candidate, 1);
                auto mutations = encode(*snapshot, candidate.get());
                auto batch = std::make_shared<Batch>();
                batch->snapshot = snapshot;
                batch->bootstrap = true;
                candidate.reset();
                admit(std::move(batch), std::move(mutations));
                return;
            }
            const auto core = durable->values.find(
                std::string(coreKey)
            );
            const auto metadata = durable->values.find(
                std::string(metadataKey)
            );
            if (core == durable->values.end() ||
                metadata == durable->values.end() ||
                !durable->revision ||
                metadata->second.size() > authorityCodecMaxBytes)
                throw std::invalid_argument(
                    "incomplete authority checkpoint"
                );
            auto decoded = decodeCoreCheckpoint(core->second);
            auto snapshot = std::make_shared<Snapshot>();
            snapshot->revision = durable->revision;
            if (!decoded ||
                !snapshot->meta.ParseFromString(metadata->second))
                throw std::invalid_argument(
                    "authority decoding failed"
                );
            snapshot->core = std::move(*decoded);
            if (!validMetadata(*snapshot, config))
                throw std::invalid_argument(
                    "authority metadata or boot identity mismatch"
                );
            auto validated = working(snapshot.get());
            validateReplays(*durable, *snapshot);
            validated.reset();
            if (!stopping.load())
                armTick();
            published.store(
                std::move(snapshot),
                std::memory_order_release
            );
            {
                std::lock_guard lock(mutex);
                counts.initialized = true;
                counts.ready = !stopping.load();
            }
            if (stopping.load())
                close();
            else
                pump();
        }
        catch (...)
        {
            fail();
        }
    }
    void fail() noexcept
    {
        {
            std::lock_guard lock(mutex);
            counts.initialized = true;
            counts.ready = false;
            counts.failed = true;
        }
        stopping.store(true, std::memory_order_release);
        close();
    }
    void close() noexcept
    {
        if (writing)
            return;
        {
            std::lock_guard lock(mutex);
            counts.ready = false;
            counts.initialized = true;
        }
        for (;;)
        {
            std::shared_ptr<Job> job;
            {
                std::lock_guard lock(mutex);
                auto* queue = !controlJobs.empty() ? &controlJobs
                                                   : &roomJobs;
                if (queue->empty())
                    break;
                job = std::move(queue->front());
                queue->pop_front();
            }
            deliver(job, std::unexpected(Error::Cancelled));
        }
        boost::system::error_code ignored;
        tickTimer.cancel(ignored);
        if (!store)
        {
            std::lock_guard lock(mutex);
            counts.stopped = true;
            return;
        }
        store->stop();
        pollClosed();
    }
    void pollClosed() noexcept
    {
        if (store->stats().closed)
        {
            std::lock_guard lock(mutex);
            counts.stopped = true;
            return;
        }
        try
        {
            closeTimer.expires_after(std::chrono::milliseconds(5));
            closeTimer.async_wait(
                [self = shared_from_this()](auto error)
                {
                    if (!error)
                        self->pollClosed();
                }
            );
        }
        catch (...)
        {
            std::lock_guard lock(mutex);
            counts.failed = true;
        }
    }

    /// One serialized executor owns models, candidates and
    /// publication order.
    boost::asio::strand<boost::asio::any_io_executor> owner;
    const Config config;
    /// External admission and accounting; never held over client
    /// callbacks.
    mutable std::mutex mutex;
    Stats counts;
    std::shared_ptr<DurableStateStore> store;
    std::atomic<std::shared_ptr<const Snapshot>> published;
    std::deque<std::shared_ptr<Job>> controlJobs, roomJobs;
    std::atomic<bool> stopping{false}, tickRequested{false};
    bool writing{false}, wakeQueued{false}, tickDue{false},
        lastWasTick{false};
    boost::asio::steady_timer tickTimer, closeTimer;
};

LobbyAuthority::LobbyAuthority(
    boost::asio::any_io_executor executor, Config config
)
{
    if (config.bootId.empty() || config.bootId.size() > 128 ||
        !config.maxPending || !config.maxRoomPending ||
        !config.maxReceipts ||
        config.maxPending > authorityCodecMaxItems ||
        config.maxRoomPending > authorityCodecMaxItems ||
        config.maxReceipts > authorityCodecMaxItems || !config.now)
        throw std::invalid_argument(
            "invalid Lobby authority configuration"
        );
    if (!config.wallNow)
        config.wallNow = []
        {
            return std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now()
                           .time_since_epoch()
            )
                .count();
        };
    m_state = std::make_shared<State>(executor, std::move(config));
    boost::asio::post(
        m_state->owner,
        [state = m_state] { state->bootstrap(); }
    );
}
LobbyAuthority::~LobbyAuthority() noexcept
{
    stop();
}
std::shared_ptr<const LobbyAuthority::Snapshot>
LobbyAuthority::snapshot() const noexcept
{
    return m_state->published.load(std::memory_order_acquire);
}
std::expected<LobbyAuthority::Cancellation, LobbyAuthority::Error>
LobbyAuthority::submit(
    Mutation mutation,
    Clock::time_point deadline,
    Completion completion,
    bool atTick
)
try
{
    if (!mutation || !completion)
        return std::unexpected(Error::InvalidState);
    auto cancelled = std::make_shared<std::atomic<bool>>(false);
    auto job = std::make_shared<State::Job>(State::Job{
        std::move(mutation),
        std::move(completion),
        deadline,
        cancelled,
        atTick
    });
    Cancellation cancel = [cancelled]
    { cancelled->store(true, std::memory_order_release); };
    auto state = m_state;
    {
        std::lock_guard lock(state->mutex);
        if (!state->counts.ready || state->stopping.load())
            return std::unexpected(Error::Unavailable);
        const auto controls = state->counts.pending -
                              state->counts.roomPending;
        if ((atTick && state->counts.roomPending >=
                           state->config.maxRoomPending) ||
            (!atTick && controls >= state->config.maxPending))
        {
            ++state->counts.rejected;
            return std::unexpected(Error::Capacity);
        }
        (atTick ? state->roomJobs : state->controlJobs)
            .push_back(job);
        ++state->counts.pending;
        if (atTick)
            ++state->counts.roomPending;
        try
        {
            state->scheduleLocked();
        }
        catch (...)
        {
            (atTick ? state->roomJobs : state->controlJobs)
                .pop_back();
            --state->counts.pending;
            if (atTick)
                --state->counts.roomPending;
            return std::unexpected(Error::Capacity);
        }
    }
    return cancel;
}
catch (const std::bad_alloc&)
{
    return std::unexpected(Error::Capacity);
}
void LobbyAuthority::requestTick() noexcept
{
    const auto state = m_state;
    if (state->stopping.load())
        return;
    state->tickRequested.store(true, std::memory_order_release);
    try
    {
        state->schedule();
    }
    catch (...)
    {
    }
}
void LobbyAuthority::stop() noexcept
{
    const auto state = m_state;
    if (!state)
        return;
    state->stopping.store(true, std::memory_order_release);
    {
        std::lock_guard lock(state->mutex);
        state->counts.ready = false;
    }
    try
    {
        state->schedule();
    }
    catch (...)
    {
        std::lock_guard lock(state->mutex);
        state->counts.failed = true;
        if (state->store)
            state->store->stop();
    }
}
LobbyAuthority::Stats LobbyAuthority::stats() const noexcept
{
    std::lock_guard lock(m_state->mutex);
    auto result = m_state->counts;
    if (m_state->store)
        result.journal = m_state->store->stats();
    return result;
}
} // namespace service::lobby
