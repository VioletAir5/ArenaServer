/** @file
 * @brief Joins exact Room worker proposals to the single Lobby
 * transaction owner.
 */
#include "lobby/RoomCoordinator.h"

#include <algorithm>
#include <atomic>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <utility>

#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <openssl/crypto.h>

#include "lobby/LegacyRoomImport.h"
#include "lobby/Protocol.h"
#include "room/RoomWire.h"

namespace service::lobby
{
namespace
{
namespace pb = arena::room;
namespace rw = service::room::wire;
using Authority = LobbyAuthority;
using Status = arena::rpc::Status;
using Clock = RoomCoordinator::Clock;
using Image = service::room::RuntimeImage;
using Work = Authority::WorkingState;
using Mono = std::chrono::steady_clock;
constexpr std::int64_t migrationBudget = 8'000'000'000LL;
std::int64_t nanos(Clock::time_point value)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               value.time_since_epoch()
    )
        .count();
}
bool terminal(RoomMigrationPhase phase)
{
    return phase == RoomMigrationPhase::Committed ||
           phase == RoomMigrationPhase::Aborted;
}
Status authorityStatus(Authority::Error error)
{
    switch (error)
    {
    case Authority::Error::Cancelled:
        return arena::rpc::CANCELLED;
    case Authority::Error::Capacity:
        return arena::rpc::OVERLOADED;
    case Authority::Error::OutcomeUnknown:
        return arena::rpc::OUTCOME_UNKNOWN;
    case Authority::Error::InvalidState:
        return arena::rpc::CONFLICT;
    default:
        return arena::rpc::UNAVAILABLE;
    }
}
std::string initialTransfer(RoomIdentity id)
{
    return "initial-" + std::to_string(id.room) + "-" +
           std::to_string(id.incarnation);
}
std::string
migrationTransfer(RoomIdentity id, std::uint64_t migration)
{
    return "migration-" + std::to_string(id.room) + "-" +
           std::to_string(id.incarnation) + "-" +
           std::to_string(migration);
}
pb::Image wireImage(RoomIdentity id, const RoomWorkerImage& image)
{
    pb::Image value;
    value.set_version(1);
    value.set_room_id(id.room);
    value.set_incarnation(id.incarnation);
    value.set_physical_tick(image.tick);
    value.set_checksum(image.checksum);
    value.set_cursor(image.cursor);
    value.set_logical_tick(image.logicalTick);
    value.set_opaque(image.checkpoint);
    value.set_fingerprint(roomImageFingerprint(id, image));
    return value;
}
RoomWorkerImage directoryImage(const pb::Image& image)
{
    return {
        image.physical_tick(),
        image.checksum(),
        image.cursor(),
        image.opaque(),
        image.logical_tick()
    };
}
bool sameBase(
    const RoomRouteRecord* current, const RoomRouteRecord& before
)
{
    return current && current->identity == before.identity &&
           current->owner == before.owner &&
           current->ownerEpoch == before.ownerEpoch &&
           current->image == before.image &&
           current->lifecycle == before.lifecycle;
}
/** @brief Prepare fences ownership, not a tick image that may advance
 * in its queue. */
bool samePrepareBase(
    const RoomRouteRecord* current, const RoomRouteRecord& before
)
{
    return current && current->identity == before.identity &&
           current->owner == before.owner &&
           current->ownerEpoch == before.ownerEpoch &&
           current->lifecycle == before.lifecycle &&
           current->handoff == before.handoff &&
           current->migrationWatermark == before.migrationWatermark;
}
bool matchesStart(
    const RoomRouteRecord* route,
    std::uint64_t id,
    std::uint64_t sourceEpoch,
    std::string_view target
)
{
    return route && route->handoff && route->handoff->id == id &&
           route->handoff->sourceEpoch == sourceEpoch &&
           RoomClient::nodeOf(route->handoff->target) ==
               std::optional(std::string(target));
}
bool sameHandoff(
    const RoomRouteRecord* current, const RoomRouteRecord& before
)
{
    return sameBase(current, before) &&
           current->handoff == before.handoff &&
           current->migrationWatermark == before.migrationWatermark;
}
std::optional<ManagedRoom>
managed(const RegistryCheckpoint& registry, std::uint64_t room)
{
    const auto found = std::find_if(
        registry.rooms.begin(),
        registry.rooms.end(),
        [&](const auto& entry)
        { return entry.roomId.value() == room; }
    );
    return found != registry.rooms.end() ? found->managed
                                         : std::nullopt;
}
service::room::RuntimeAllocation allocation(const ManagedRoom& room)
{
    const service::room::Member first{
        room.members[0].playerId,
        room.members[0].sessionId,
        room.members[0].roomEpoch
    };
    const service::room::Member second{
        room.members[1].playerId,
        room.members[1].sessionId,
        room.members[1].roomEpoch
    };
    return {
        room.matchId,
        {first, second},
        {room.members[0].initialMmr, room.members[1].initialMmr}
    };
} /** @brief A stable rendezvous score chooses initial owners, never
     an alternate route. */
std::uint64_t score(RoomIdentity room, std::string_view node)
{
    std::uint64_t hash = 14695981039346656037ULL;
    for (auto value : {room.room, room.incarnation})
        for (unsigned shift = 0; shift < 64; shift += 8)
        {
            hash ^= (value >> shift) & 255;
            hash *= 1099511628211ULL;
        }
    for (unsigned char value : node)
    {
        hash ^= value;
        hash *= 1099511628211ULL;
    }
    return hash;
}
void removePending(Work& work, RoomIdentity id)
{
    auto* entries = work.meta.mutable_pending_rooms();
    for (int index = entries->size(); index-- > 0;)
        if (entries->Get(index).room_id() == id.room &&
            entries->Get(index).incarnation() == id.incarnation)
            entries->DeleteSubrange(index, 1);
}
const arena::lobby_meta::PendingRoom*
pending(const arena::lobby_meta::Metadata& metadata, RoomIdentity id)
{
    for (const auto& entry : metadata.pending_rooms())
        if (entry.room_id() == id.room &&
            entry.incarnation() == id.incarnation)
            return &entry;
    return nullptr;
}
bool manualPending(
    const Authority::Snapshot& snapshot, RoomIdentity id
)
{
    const auto* entry = pending(snapshot.meta, id);
    return entry && !entry->match_id();
}
/** @brief Returns a bounded public error without storing a caller
 * request ID. */
std::string publicReply(
    const service::room::RuntimeCommand& command,
    const pb::CommandResult& result,
    const Image& image
)
{
    if (command.kind ==
        service::room::RuntimeCommandKind::SettlementAck)
        return {};
    auto encoded = encodeRoomReply(command, result, image);
    arena::protocol::Envelope reply;
    if (encoded)
        reply = std::move(*encoded);
    else
    {
        reply.set_msg_id(arena::protocol::MSG_ERROR);
        reply.mutable_error()->set_code(encoded.error());
    }
    std::string bytes;
    if (!reply.SerializeToString(&bytes) || bytes.size() > 16 * 1024)
        throw std::invalid_argument("invalid bounded room receipt");
    return bytes;
}
} // namespace

struct RoomCoordinator::State : std::enable_shared_from_this<State>
{
    enum class Lane
    {
        Data,
        Control
    };
    struct JobKey
    {
        RoomIdentity room;
        std::string owner;
        std::uint64_t epoch{0};
        Lane lane{Lane::Control};
        auto operator<=>(const JobKey&) const = default;
    };
    using Token = std::uint64_t;
    /** @brief Restores the exact physical loan across nested
     * continuations. */
    struct JobScope
    {
        State& state;
        Token previous;
        JobScope(State& value, Token token)
            : state(value)
            , previous(value.currentJob)
        {
            state.currentJob = token;
        }
        ~JobScope()
        {
            state.currentJob = previous;
        }
    };
    struct Health
    {
        std::string worker;
        /// Only a positive probe from this discovery version
        /// qualifies.
        std::uint64_t version{0};
        Mono::time_point due{};
        bool ready{false}, probing{false};
    };
    struct Installed
    {
        std::string owner, fingerprint;
        std::uint64_t epoch{0};
    };
    struct Action
    {
        RoomIdentity room;
        bool isolated{false};
        std::size_t cost{1};
        Clock::time_point deadline;
        std::function<bool(Work&)> mutate;
        std::function<void(bool, Status)> complete;
    };
    struct Admin
    {
        RoomIdentity room;
        std::uint64_t epoch{0}, migration{0};
        std::string target;
        Clock::time_point deadline;
        MigrationCompletion completion;
        std::atomic<bool> cancelled{false};
        bool finished{false};
    };
    State(
        boost::asio::any_io_executor executor,
        Authority& owner,
        Config value
    )
        : authority(owner)
        , config(std::move(value))
        , strand(boost::asio::make_strand(executor))
        , timer(strand)
        , client(executor, config.client)
        , controlClient(executor, config.client)
        , hintClient(executor, config.client)
    {
        if (!executor || !config.now || !config.wallNow ||
            !config.maxInflight || config.maxInflight > 256 ||
            !config.maxBatch || config.maxBatch > 128 ||
            config.poll < std::chrono::milliseconds(1) ||
            config.poll > std::chrono::milliseconds(100) ||
            config.probe < std::chrono::milliseconds(10) ||
            config.probe > std::chrono::milliseconds(1000))
            throw std::invalid_argument(
                "invalid Room coordinator configuration"
            );
    }
    ~State()
    {
        if (!config.client.token.empty())
            OPENSSL_cleanse(
                config.client.token.data(),
                config.client.token.size()
            );
    }
    std::vector<std::string> configured() const
    {
        std::vector<std::string> result;
        if (config.client.runtime)
        {
            for (const auto& node :
                 config.client.runtime->snapshot()->nodes())
                if (node.service == "room")
                    result.push_back(node.id);
        }
        else
            for (const auto& node : config.client.nodes)
                result.push_back(node.id);
        std::sort(result.begin(), result.end());
        result.erase(
            std::unique(result.begin(), result.end()),
            result.end()
        );
        return result;
    }
    std::uint64_t runtimeVersion() const noexcept
    {
        return config.client.runtime
                   ? config.client.runtime->snapshot()->version()
                   : 1;
    }
    std::optional<std::string> worker(std::string_view node) const
    {
        const auto found = health.find(std::string(node));
        if (found == health.end() || !found->second.ready ||
            found->second.version != runtimeVersion())
            return {};
        const auto allowed = client.nodes();
        if (std::find(allowed.begin(), allowed.end(), node) ==
            allowed.end())
            return {};
        return found->second.worker;
    }
    std::optional<std::string> initialOwner(RoomIdentity id) const
    {
        const auto nodes = client.nodes();
        std::optional<std::string> selected;
        std::uint64_t highest = 0;
        for (const auto& node : nodes)
            if (const auto live = worker(node))
            {
                const auto candidate = score(id, node);
                if (!selected || candidate > highest)
                {
                    selected = live;
                    highest = candidate;
                }
            }
        return selected;
    }
    arena::internal_rpc::Context context() const
    {
        return {
            Mono::now() + config.client.timeout,
            "lobby-room-coordinator",
            {"lobby"}
        };
    }
    void refresh()
    {
        auto snapshot = authority.snapshot();
        const auto version = runtimeVersion();
        std::size_t healthy = 0;
        for (const auto& [node, value] : health)
        {
            if (value.ready && worker(node))
                ++healthy;
        }
        std::lock_guard lock(mutex);
        counts.rpcInflight = client.inflight() +
                             controlClient.inflight() +
                             hintClient.inflight();
        counts.inflight = running.size() + transports;
        counts.walInflight = writing;
        counts.cacheHintPending = pendingHints.size();
        counts.cacheHintInflight = hintTransports;
        counts.healthyWorkers = healthy;
        countsVersion = version;
        counts.stopping = stopping.load();
        counts.ready = !counts.stopping && healthy && snapshot &&
                       snapshot->core.rooms.actors.empty() &&
                       authority.stats().ready;
        counts.stopped = counts.stopping && !timerPending &&
                         !writing && actions.empty() &&
                         running.empty() && !transports &&
                         !counts.migrations && !wakePosted.load();
    }
    void done(RoomIdentity id, bool success = false)
    {
        const auto found = running.find(currentJob);
        if (found == running.end() || found->second.room != id)
            return;
        const auto key = found->second;
        running.erase(found);
        if (!success)
        {
            retry[key] = Mono::now() + config.probe;
            std::lock_guard lock(mutex);
            ++counts.rejected;
        }
    }
    RoomClient& jobClient()
    {
        const auto found = running.find(currentJob);
        return found != running.end() &&
                       found->second.lane == Lane::Data
                   ? client
                   : controlClient;
    }
    /** @brief Keeps the physical RPC loan until its owner
     * continuation executes. */
    template <class Reply, class Start, class Completion>
    void rpc(RoomIdentity id, Start start, Completion completion)
    {
        auto handler = std::make_shared<Completion>(
            std::move(completion)
        );
        ++transports;
        auto self = shared_from_this();
        const auto token = currentJob;
        auto received = [self, id, handler, token](
                            RoomClient::Result<Reply> result
                        ) mutable
        {
            boost::asio::post(
                self->strand,
                [self,
                 id,
                 handler,
                 token,
                 result = std::move(result)]() mutable
                {
                    JobScope scope(*self, token);
                    --self->transports;
                    try
                    {
                        (*handler)(std::move(result));
                    }
                    catch (...)
                    {
                        self->done(id);
                    }
                    self->pass();
                }
            );
        };
        RoomClient::Admission admitted = std::unexpected(
            arena::rpc::INTERNAL
        );
        try
        {
            admitted = start(received);
        }
        catch (...)
        {
            // Match an admission rejection so private completion
            // accounting drains.
            admitted = std::unexpected(arena::rpc::INTERNAL);
        }
        if (!admitted)
        {
            --transports;
            try
            {
                (*handler)(RoomClient::Result<Reply>(
                    std::unexpected(admitted.error())
                ));
            }
            catch (...)
            {
                done(id);
            }
        }
    }
    void enqueue(std::shared_ptr<Action> action)
    {
        auto completion = std::move(action->complete);
        action->complete = [self = shared_from_this(),
                            token = currentJob,
                            completion = std::move(completion)](
                               bool ok,
                               Status status
                           )
        {
            JobScope scope(*self, token);
            completion(ok, status);
        };

        if (stopping.load())
        {
            action->complete(false, arena::rpc::UNAVAILABLE);
            return;
        }
        if (actions.size() >= 2 * config.maxInflight)
        {
            action->complete(false, arena::rpc::OVERLOADED);
            return;
        }
        actions.push_back(std::move(action));
    }
    /** @brief Batches independent, revalidated candidates into one
     * Authority WAL. */
    void flush()
    {
        if (writing || actions.empty())
            return;
        auto batch = std::make_shared<
            std::vector<std::shared_ptr<Action>>>();
        std::size_t cost = 0;
        auto deadline = config.now() + std::chrono::seconds(1);
        std::set<RoomIdentity> rooms;
        const bool isolated = actions.front()->isolated;
        while (!actions.empty() &&
               cost + actions.front()->cost <= config.maxBatch &&
               !rooms.contains(actions.front()->room) &&
               (batch->empty() ||
                (!isolated && !actions.front()->isolated)))
        {
            rooms.insert(actions.front()->room);
            auto item = std::move(actions.front());
            actions.pop_front();
            cost += item->cost;
            deadline = std::min(deadline, item->deadline);
            batch->push_back(std::move(item));
        }
        if (batch->empty())
        {
            auto item = std::move(actions.front());
            actions.pop_front();
            item->complete(false, arena::rpc::OVERLOADED);
            return;
        }
        auto accepted = std::make_shared<std::vector<bool>>(
            batch->size(),
            false
        );
        writing = true;
        auto submitted = authority.submit(
            [batch, accepted](Work& work, Authority::Capture capture)
            {
                for (std::size_t index = 0; index < batch->size();
                     ++index)
                    (*accepted)[index] = (*batch)[index]->mutate(
                        work
                    );
                arena::protocol::Envelope reply;
                reply.set_msg_id(arena::protocol::MSG_PONG);
                reply.mutable_pong();
                capture(std::move(reply));
            },
            deadline,
            [self = shared_from_this(), batch, accepted](auto result)
            {
                boost::asio::post(
                    self->strand,
                    [self,
                     batch,
                     accepted,
                     result = std::move(result)]() mutable
                    {
                        self->writing = false;
                        const auto status = result
                                                ? arena::rpc::OK
                                                : authorityStatus(
                                                      result.error()
                                                  );
                        for (std::size_t index = 0;
                             index < batch->size();
                             ++index)
                        {
                            try
                            {
                                (*batch)[index]->complete(
                                    result && (*accepted)[index],
                                    status
                                );
                            }
                            catch (...)
                            {
                            }
                        }
                        self->pass();
                    }
                );
            }
        );
        if (!submitted)
        {
            writing = false;
            for (const auto& item : *batch)
                item->complete(
                    false,
                    authorityStatus(submitted.error())
                );
        }
    }
    void change(
        RoomRouteRecord route,
        RoomDirectoryCommand command,
        std::function<void(bool, Status)> completed = {}
    )
    {
        const auto id = route.identity;
        auto action = std::make_shared<Action>();
        action->room = id;
        action->deadline = config.now() + std::chrono::seconds(1);
        action->mutate = [route = std::move(route),
                          command = std::move(command)](Work& work)
        {
            if (!sameHandoff(
                    work.base->directory->find(route.identity),
                    route
                ))
                return false;
            work.roomChanges.push_back(command);
            return true;
        };
        action->complete =
            [self = shared_from_this(),
             id,
             completed = std::move(completed)](bool ok, Status status)
        {
            self->done(id, ok);
            if (completed)
                completed(ok, status);
        };
        enqueue(std::move(action));
    }
    void probeNodes()
    {
        const auto version = runtimeVersion();
        const auto nodes = configured();
        std::erase_if(
            health,
            [&](const auto& entry)
            {
                return !entry.second.probing && std::find(
                                                    nodes.begin(),
                                                    nodes.end(),
                                                    entry.first
                                                ) == nodes.end();
            }
        );
        for (const auto& node : nodes)
        {
            auto& current = health[node];
            if (current.probing || Mono::now() < current.due ||
                controlClient.inflight() >= config.client.maxInflight)
                continue;
            current.probing = true;
            current.due = Mono::now() + config.probe;
            rpc<pb::HealthReply>(
                {},
                [&](auto cb)
                {
                    return jobClient()
                        .probe(node, context(), std::move(cb));
                },
                [self = shared_from_this(),
                 node,
                 version](auto result)
                {
                    const auto found = self->health.find(node);
                    if (found == self->health.end())
                        return;
                    auto& state = found->second;
                    state.probing = false;
                    state.ready = result && result->ready() &&
                                  version == self->runtimeVersion();
                    state.version = version;
                    if (state.ready)
                        state.worker = result->worker_id();
                }
            );
        }
    }
    void completeAdmin(
        const std::shared_ptr<Admin>& admin,
        std::expected<RoomHandoff, Status> result
    )
    {
        if (admin->finished)
            return;
        admin->finished = true;
        {
            std::lock_guard lock(mutex);
            --counts.migrations;
        }
        auto completion = std::move(admin->completion);
        try
        {
            completion(std::move(result));
        }
        catch (...)
        {
        }
    }
    void prepare(const std::shared_ptr<Admin>& admin)
    {
        if (stopping.load() || admin->cancelled.load())
        {
            completeAdmin(
                admin,
                std::unexpected(arena::rpc::CANCELLED)
            );
            return;
        }
        if (config.now() >= admin->deadline)
        {
            completeAdmin(
                admin,
                std::unexpected(arena::rpc::DEADLINE_EXCEEDED)
            );
            return;
        }
        const auto snapshot = authority.snapshot();
        const auto* route = snapshot && snapshot->directory
                                ? snapshot->directory->find(
                                      admin->room
                                  )
                                : nullptr;
        if (!route || route->lifecycle != RoomRouteLifecycle::Active)
        {
            completeAdmin(
                admin,
                std::unexpected(arena::rpc::NOT_FOUND)
            );
            return;
        }
        if (route->handoff && route->handoff->id == admin->migration)
        {
            const auto& h = *route->handoff;
            if (h.sourceEpoch != admin->epoch ||
                RoomClient::nodeOf(h.target) !=
                    std::optional(admin->target))
                completeAdmin(
                    admin,
                    std::unexpected(arena::rpc::CONFLICT)
                );
            else
                completeAdmin(admin, h);
            return;
        }
        const auto target = worker(admin->target);
        if (!target)
        {
            completeAdmin(
                admin,
                std::unexpected(arena::rpc::UNAVAILABLE)
            );
            return;
        }
        if (route->ownerEpoch != admin->epoch ||
            route->owner == *target ||
            route->migrationWatermark >= admin->migration ||
            (route->handoff && !terminal(route->handoff->phase)))
        {
            completeAdmin(
                admin,
                std::unexpected(arena::rpc::CONFLICT)
            );
            return;
        }
        const auto now = nanos(config.now());
        if (now < 0 || now > INT64_MAX - migrationBudget)
        {
            completeAdmin(
                admin,
                std::unexpected(arena::rpc::OVERLOADED)
            );
            return;
        }
        auto action = std::make_shared<Action>();
        action->room = admin->room;
        action->isolated = true;
        action->deadline = admin->deadline;
        const auto before = *route;
        const auto cutoff = now + migrationBudget;
        action->mutate =
            [admin, before, target = *target, cutoff](Work& work)
        {
            if (admin->cancelled.load() ||
                work.now >= admin->deadline)
                return false;
            const auto* current = work.base->directory->find(
                before.identity
            );
            // A duplicate queued behind the first durable Prepare
            // keeps its original target instance and deadline,
            // including after Commit.
            if (matchesStart(
                    current,
                    admin->migration,
                    admin->epoch,
                    admin->target
                ))
                return true;
            if (!samePrepareBase(current, before))
                return false;
            work.roomChanges.push_back(
                RoomPrepare{
                    before.identity,
                    admin->epoch,
                    admin->migration,
                    target,
                    cutoff
                }
            );
            return true;
        };
        action->complete =
            [self = shared_from_this(), admin](bool, Status status)
        {
            auto snapshot = self->authority.snapshot();
            auto* route = snapshot
                              ? snapshot->directory->find(admin->room)
                              : nullptr;
            if (matchesStart(
                    route,
                    admin->migration,
                    admin->epoch,
                    admin->target
                ))
                self->completeAdmin(admin, *route->handoff);
            else
                self->completeAdmin(
                    admin,
                    std::unexpected(
                        status == arena::rpc::OK
                            ? (admin->cancelled.load()
                                   ? arena::rpc::CANCELLED
                                   : arena::rpc::CONFLICT)
                            : status
                    )
                );
        };
        enqueue(std::move(action));
    } /** @brief Publishes only worker-validated state with its
         original reservation. */
    void createRoom(
        arena::lobby_meta::PendingRoom reservation, std::string owner
    )
    {
        const RoomIdentity id{
            reservation.room_id(),
            reservation.incarnation()
        };
        const auto snapshot = authority.snapshot();
        pb::CreateRequest request;
        request.set_version(1);
        request.set_worker_id(owner);
        request.set_room_id(id.room);
        request.set_incarnation(id.incarnation);
        auto policy = rw::packPolicy(config.policy);
        if (!policy)
        {
            done(id);
            return;
        }
        *request.mutable_policy() = std::move(*policy);
        request.set_created_wall_millis(
            reservation.created_unix_ms()
        );
        std::optional<ManagedRoom> cohort;
        if (reservation.match_id())
        {
            cohort = managed(snapshot->core.registry, id.room);
            if (!cohort || cohort->matchId != reservation.match_id())
            {
                done(id);
                return;
            }
            auto value = rw::packAllocation(allocation(*cohort));
            if (!value)
            {
                done(id);
                return;
            }
            *request.mutable_allocation() = std::move(*value);
        }
        rpc<pb::CreateReply>(
            id,
            [&](auto cb)
            {
                return jobClient()
                    .create(request, context(), std::move(cb));
            },
            [self = shared_from_this(),
             id,
             reservation,
             owner,
             cohort](auto result)
            {
                if (!result || self->stopping.load())
                {
                    self->done(id);
                    return;
                }
                // No worker cache is needed until Lobby durably owns
                // this image. Expired reservations therefore leave no
                // private Stage behind.
                auto action = std::make_shared<Action>();
                action->room = id;
                action->deadline = self->config.now() +
                                   std::chrono::seconds(1);
                action->mutate = [id,
                                  reservation,
                                  owner,
                                  cohort,
                                  image = result->image()](Work& work)
                {
                    const auto* current = pending(work.meta, id);
                    if (!current ||
                        current->SerializeAsString() !=
                            reservation.SerializeAsString() ||
                        current->deadline_nanos() <=
                            nanos(work.now) ||
                        work.base->directory->find(id))
                        return false;
                    if (cohort)
                    {
                        const auto actual = work.online->managedRoom(
                            arena::domain::RoomId{id.room}
                        );
                        if (!actual ||
                            actual->matchId != cohort->matchId ||
                            actual->roomId != cohort->roomId ||
                            actual->members != cohort->members)
                            return false;
                    }
                    work.roomChanges.push_back(
                        RoomCreate{id, owner, directoryImage(image)}
                    );
                    if (reservation.match_id())
                    {
                        work.queue->release(
                            reservation.match_id(),
                            work.now,
                            arena::domain::RoomId{id.room}
                        );
                        removePending(work, id);
                    }
                    return true;
                };
                action->complete = [self, id](bool ok, Status)
                {
                    self->done(id, ok);
                    if (ok)
                    {
                        std::lock_guard lock(self->mutex);
                        ++self->counts.created;
                    }
                };
                self->enqueue(std::move(action));
            }
        );
    }
    /** @brief Validates an already durable legacy value on its worker
     * before adoption. */
    void stageInitial(
        RoomIdentity id,
        const std::string& owner,
        const pb::Image& image,
        std::function<void(bool)> complete
    )
    {
        if (stopping.load())
        {
            complete(false);
            return;
        }
        pb::StageRequest request;
        request.set_version(1);
        request.set_worker_id(owner);
        request.set_owner_epoch(1);
        request.set_transfer_id(initialTransfer(id));
        *request.mutable_image() = image;
        rpc<pb::StageReceipt>(
            id,
            [&](auto cb)
            {
                return jobClient()
                    .stage(request, context(), std::move(cb));
            },
            [complete = std::move(complete)](auto result)
            { complete(bool(result)); }
        );
    }
    void adopt(Image image, std::string owner)
    {
        const RoomIdentity id{
            image.actor.roomId.value(),
            image.actor.incarnation
        };
        auto encoded = rw::packImage(image, 0);
        if (!encoded)
        {
            done(id);
            return;
        }
        const auto bytes = encoded->opaque();
        stageInitial(
            id,
            owner,
            *encoded,
            [self = shared_from_this(),
             id,
             owner,
             encoded = *encoded,
             bytes](bool ok)
            {
                if (!ok)
                {
                    self->done(id);
                    return;
                }
                auto action = std::make_shared<Action>();
                action->room = id;
                action->deadline = self->config.now() +
                                   std::chrono::seconds(1);
                action->mutate =
                    [id, owner, encoded, bytes](Work& work)
                {
                    if (work.base->directory->find(id))
                        return false;
                    const auto values = splitLegacyRooms(
                        work.legacyRooms
                    );
                    if (!values)
                        return false;
                    const auto found = std::find_if(
                        values->images.begin(),
                        values->images.end(),
                        [&](const auto& value)
                        {
                            return value.actor.roomId.value() ==
                                       id.room &&
                                   value.actor.incarnation ==
                                       id.incarnation;
                        }
                    );
                    if (found == values->images.end())
                        return false;
                    const auto current = rw::encodeImage(*found);
                    if (!current || *current != bytes)
                        return false;
                    for (const auto& contribution :
                         work.legacyRooms.roomCounters)
                        if (contribution.roomId.value() == id.room)
                        {
                            const auto
                                count = contribution.counters
                                            .persistenceRejected;
                            if (count >
                                UINT64_MAX - work.legacyRooms
                                                 .unattributedCounters
                                                 .persistenceRejected)
                                return false;
                            work.legacyRooms.unattributedCounters
                                .persistenceRejected += count;
                        }
                    std::erase_if(
                        work.legacyRooms.actors,
                        [&](const auto& value)
                        { return value.roomId.value() == id.room; }
                    );
                    std::erase_if(
                        work.legacyRooms.matches,
                        [&](const auto& value)
                        {
                            return value.allocation.roomId.value() ==
                                   id.room;
                        }
                    );
                    std::erase_if(
                        work.legacyRooms.roomCounters,
                        [&](const auto& value)
                        { return value.roomId.value() == id.room; }
                    );
                    work.roomChanges.push_back(
                        RoomCreate{id, owner, directoryImage(encoded)}
                    );
                    return true;
                };
                action->complete = [self, id](bool applied, Status)
                {
                    self->done(id, applied);
                    if (applied)
                    {
                        std::lock_guard lock(self->mutex);
                        ++self->counts.created;
                    }
                };
                self->enqueue(std::move(action));
            }
        );
    }
    void expire(arena::lobby_meta::PendingRoom reservation)
    {
        const RoomIdentity id{
            reservation.room_id(),
            reservation.incarnation()
        };
        auto action = std::make_shared<Action>();
        action->room = id;
        action->deadline = config.now() + std::chrono::seconds(1);
        action->mutate = [id, reservation](Work& work)
        {
            const auto* current = pending(work.meta, id);
            if (!current ||
                current->SerializeAsString() !=
                    reservation.SerializeAsString() ||
                current->deadline_nanos() > nanos(work.now))
                return false;
            if (reservation.match_id())
            {
                work.online->finishMatch(
                    arena::domain::RoomId{id.room},
                    reservation.match_id()
                );
                work.queue->release(reservation.match_id(), work.now);
            }
            else if (
                const auto* route = work.base->directory->find(id)
            )
            {
                const auto registry = work.online->checkpoint();
                const bool bound = std::any_of(
                    registry.entries.begin(),
                    registry.entries.end(),
                    [&](const auto& entry)
                    {
                        return entry.snapshot.roomId &&
                               entry.snapshot.roomId->value() ==
                                   id.room;
                    }
                );
                if (!bound &&
                    route->lifecycle != RoomRouteLifecycle::Retired)
                    work.roomChanges.push_back(
                        RoomRetire{
                            id,
                            route->owner,
                            route->ownerEpoch,
                            route->image.cursor,
                            roomImageFingerprint(id, route->image)
                        }
                    );
            }
            removePending(work, id);
            return true;
        };
        action->complete =
            [self = shared_from_this(), id](bool ok, Status)
        { self->done(id, ok); };
        enqueue(std::move(action));
    } /** @brief Reinstalls a WAL-authorized cache before any isolated
         calculation. */
    void run(RoomRouteRecord route, std::uint64_t revision)
    {
        const auto id = route.identity;
        const auto proof = roomImageFingerprint(id, route.image);
        auto found = installed.find(id);
        if (found != installed.end() &&
            found->second.owner == route.owner &&
            found->second.epoch == route.ownerEpoch &&
            found->second.fingerprint == proof)
        {
            execute(std::move(route));
            return;
        }
        pb::ActivateRequest request;
        request.set_version(1);
        request.set_worker_id(route.owner);
        request.set_owner_epoch(route.ownerEpoch);
        request.set_directory_revision(revision);
        *request.mutable_image() = wireImage(id, route.image);
        if (route.handoff &&
            route.handoff->phase == RoomMigrationPhase::Committed &&
            route.owner == route.handoff->target &&
            route.ownerEpoch == route.handoff->targetEpoch)
            request.set_transfer_id(
                migrationTransfer(id, route.handoff->id)
            );
        rpc<pb::CacheReceipt>(
            id,
            [&](auto cb)
            {
                return jobClient()
                    .activate(request, context(), std::move(cb));
            },
            [self = shared_from_this(),
             route = std::move(route),
             proof](auto result) mutable
            {
                if (!result || self->stopping.load())
                {
                    self->done(route.identity);
                    return;
                }
                const auto snapshot = self->authority.snapshot();
                if (!snapshot ||
                    !sameBase(
                        snapshot->directory->find(route.identity),
                        route
                    ))
                {
                    self->done(route.identity, true);
                    return;
                }
                self->installed[route.identity] = {
                    route.owner,
                    proof,
                    route.ownerEpoch
                };
                self->execute(std::move(route));
            }
        );
    }
    void execute(RoomRouteRecord route)
    {
        const auto id = route.identity;
        if (stopping.load())
        {
            done(id);
            return;
        }
        const auto final = std::find_if(
            route.inbox.begin(),
            route.inbox.end(),
            [](const auto& value)
            { return value.operation.kind == RoomIngressKind::Tick; }
        );
        if (final == route.inbox.end())
        {
            done(id, true);
            return;
        }
        pb::ExecuteRequest request;
        request.set_version(1);
        request.set_worker_id(route.owner);
        request.set_owner_epoch(route.ownerEpoch);
        *request.mutable_base() = wireImage(id, route.image);
        const auto count = static_cast<std::size_t>(std::distance(
                               route.inbox.begin(),
                               final
                           )) +
                           1;
        if (count > rw::kMaxOperations)
        {
            done(id);
            return;
        }
        std::vector<service::room::RuntimeCommand> commands;
        commands.reserve(count - 1);
        for (std::size_t index = 0; index < count; ++index)
        {
            const auto& input = route.inbox[index];
            auto& operation = *request.add_operations();
            operation.set_cursor(input.sequence);
            operation.set_logical_tick(input.operation.tick);
            operation.set_payload(input.operation.payload);
            if (input.operation.kind == RoomIngressKind::Tick)
                operation.set_kind(pb::OP_TICK);
            else
            {
                operation.set_kind(pb::OP_COMMAND);
                auto command = rw::decodeCommand(
                    input.operation.payload
                );
                if (!command)
                {
                    done(id);
                    return;
                }
                commands.push_back(std::move(*command));
            }
        }
        rpc<pb::ExecuteReply>(
            id,
            [&](auto cb)
            {
                return jobClient()
                    .execute(request, context(), std::move(cb));
            },
            [self = shared_from_this(),
             route = std::move(route),
             commands = std::move(commands),
             count](auto result) mutable
            {
                if (!result || self->stopping.load())
                {
                    self->done(route.identity);
                    return;
                }
                self->proposal(
                    std::move(route),
                    std::move(commands),
                    count,
                    std::move(*result)
                );
            }
        );
    }
    void proposal(
        RoomRouteRecord before,
        std::vector<service::room::RuntimeCommand> commands,
        std::size_t count,
        pb::ExecuteReply reply
    )
    {
        const auto id = before.identity;
        auto image = rw::unpackImage(reply.image());
        if (!image ||
            reply.results_size() != static_cast<int>(commands.size()))
        {
            done(id);
            return;
        }
        RoomApply apply{
            id,
            before.owner,
            before.ownerEpoch,
            directoryImage(reply.image()),
            {},
            std::nullopt
        };
        for (std::size_t index = 0; index < commands.size(); ++index)
            apply.replies.push_back(
                {before.inbox[index].sequence,
                 publicReply(
                     commands[index],
                     reply.results(static_cast<int>(index)),
                     *image
                 )}
            );
        apply.replies.push_back(
            {before.inbox[count - 1].sequence, {}}
        );
        if (reply.has_replay())
        {
            auto tick = rw::decodeTick(reply.replay());
            auto previous = rw::decodeImage(before.image.checkpoint);
            if (!tick || !previous)
            {
                done(id);
                return;
            }
            auto segment = rw::encodeReplayLog(
                {previous->policy.actor, previous->actor, {*tick}}
            );
            if (!segment)
            {
                done(id);
                return;
            }
            apply.replay = RoomReplayExtent{
                tick->tick,
                tick->tick,
                std::move(*segment)
            };
        }
        const bool release = reply.release_membership(),
                   retire = reply.retire();
        const auto proof = reply.image().fingerprint();
        auto action = std::make_shared<Action>();
        action->cost = retire ? 2 : 1;
        action->room = id;
        action->deadline = config.now() + std::chrono::seconds(1);
        action->mutate = [before = std::move(before),
                          apply = std::move(apply),
                          count,
                          release,
                          retire,
                          proof,
                          image = std::move(*image)](Work& work)
        {
            const auto* current = work.base->directory->find(
                before.identity
            );
            if (!sameBase(current, before) ||
                current->inbox.size() < count ||
                (current->handoff &&
                 !terminal(current->handoff->phase) &&
                 current->handoff->phase !=
                     RoomMigrationPhase::Preparing))
                return false;
            for (std::size_t index = 0; index < count; ++index)
                if (current->inbox[index] != before.inbox[index])
                    return false;
            if (release)
            {
                if (!image.managed)
                    return false;
                const auto match = image.managed->allocation.matchId;
                const auto currentCohort = work.online->managedRoom(
                    arena::domain::RoomId{before.identity.room}
                );
                if (currentCohort && currentCohort->matchId != match)
                    return false;
                work.online->finishMatch(
                    arena::domain::RoomId{before.identity.room},
                    match
                );
            }
            work.roomChanges.push_back(apply);
            if (retire &&
                (image.managed ||
                 work.online
                     ->roomMembers(
                         arena::domain::RoomId{before.identity.room}
                     )
                     .empty()))
                work.roomChanges.push_back(
                    RoomRetire{
                        before.identity,
                        before.owner,
                        before.ownerEpoch,
                        apply.image.cursor,
                        proof
                    }
                );
            return true;
        };
        action->complete =
            [self = shared_from_this(), id](bool ok, Status)
        {
            self->done(id, ok);
            if (ok)
            {
                std::lock_guard lock(self->mutex);
                ++self->counts.applied;
            }
        };
        enqueue(std::move(action));
    }
    void progress(RoomRouteRecord route)
    {
        const auto id = route.identity;
        const auto h = *route.handoff;
        if (nanos(config.now()) >= h.deadline)
        {
            change(std::move(route), RoomAbort{id, h.id});
            return;
        }
        switch (h.phase)
        {
        case RoomMigrationPhase::Preparing:
        case RoomMigrationPhase::Freezing:
            change(std::move(route), RoomFreeze{id, h.id, true});
            return;
        case RoomMigrationPhase::Frozen:
        {
            const auto node = RoomClient::nodeOf(h.target);
            const auto live = node ? worker(*node) : std::nullopt;
            if (!live || *live != h.target)
            {
                done(id);
                return;
            }
            pb::StageRequest request;
            request.set_version(1);
            request.set_worker_id(h.target);
            request.set_owner_epoch(h.targetEpoch);
            request.set_transfer_id(migrationTransfer(id, h.id));
            *request.mutable_image() = wireImage(id, route.image);
            rpc<pb::StageReceipt>(
                id,
                [&](auto cb)
                {
                    return jobClient()
                        .stage(request, context(), std::move(cb));
                },
                [self = shared_from_this(),
                 route = std::move(route),
                 h,
                 id](auto result) mutable
                {
                    if (!result || self->stopping.load())
                    {
                        self->done(id);
                        return;
                    }
                    self->change(
                        std::move(route),
                        RoomTransferred{
                            id,
                            h.id,
                            h.target,
                            h.targetEpoch,
                            result->fingerprint()
                        }
                    );
                }
            );
            return;
        }
        case RoomMigrationPhase::Transferred:
            change(
                std::move(route),
                RoomCommit{id, h.id},
                [self = shared_from_this()](bool ok, Status)
                {
                    if (ok)
                    {
                        std::lock_guard lock(self->mutex);
                        ++self->counts.migrated;
                    }
                }
            );
            return;
        default:
            done(id, true);
            return;
        }
    }
    void
    recoverOwner(RoomRouteRecord route, const std::string& target)
    {
        if (route.migrationWatermark == UINT64_MAX ||
            route.ownerEpoch > UINT64_MAX - 2)
        {
            done(route.identity);
            return;
        }
        const auto now = nanos(config.now());
        if (now < 0 || now > INT64_MAX - migrationBudget)
        {
            done(route.identity);
            return;
        }
        change(
            route,
            RoomPrepare{
                route.identity,
                route.ownerEpoch,
                route.migrationWatermark + 1,
                target,
                now + migrationBudget
            }
        );
    }
    bool eligible(
        RoomIdentity id,
        Lane lane = Lane::Control,
        std::string owner = {},
        std::uint64_t epoch = 0
    )
    {
        if (lane == Lane::Control && owner.empty())
        {
            const auto snapshot = authority.snapshot();
            const auto* route = snapshot && snapshot->directory
                                    ? snapshot->directory->find(id)
                                    : nullptr;
            if (route)
            {
                owner = route->owner;
                epoch = route->ownerEpoch;
            }
        }
        JobKey key{id, std::move(owner), epoch, lane};
        std::size_t count = 0;
        for (const auto& [token, job] : running)
        {
            if (job.lane == lane)
                ++count;
            if (job == key || (lane == Lane::Control &&
                               job.lane == lane && job.room == id))
                return false;
        }
        if (count >= (lane == Lane::Data ? 2 * config.maxInflight
                                         : config.maxInflight))
            return false;
        const auto found = retry.find(key);
        if (found != retry.end() && Mono::now() < found->second)
            return false;
        if (nextToken == UINT64_MAX)
            return false;
        retry.erase(key);
        currentJob = ++nextToken;
        running.emplace(currentJob, std::move(key));
        return true;
    }
    void schedule(
        const std::shared_ptr<const Authority::Snapshot>& snapshot
    )
    {
        if (!snapshot || !snapshot->directory)
            return;
        std::erase_if(
            retry,
            [&](const auto& entry)
            {
                const auto* route = snapshot->directory->find(
                    entry.first.room
                );
                return Mono::now() >= entry.second ||
                       (!route &&
                        !pending(snapshot->meta, entry.first.room)) ||
                       (route && entry.first.lane == Lane::Data &&
                        (route->owner != entry.first.owner ||
                         route->ownerEpoch != entry.first.epoch));
            }
        );
        for (const auto& reservation : snapshot->meta.pending_rooms())
        {
            const RoomIdentity id{
                reservation.room_id(),
                reservation.incarnation()
            };
            if (nanos(config.now()) >= reservation.deadline_nanos())
            {
                if (eligible(id))
                    expire(reservation);
                continue;
            }
            if (snapshot->directory->find(id))
                continue;
            auto owner = initialOwner(id);
            if (owner && eligible(id))
                createRoom(reservation, std::move(*owner));
        }
        if (!snapshot->core.rooms.actors.empty())
        {
            const auto legacy = splitLegacyRooms(
                snapshot->core.rooms
            );
            if (legacy)
                for (const auto& image : legacy->images)
                {
                    const RoomIdentity id{
                        image.actor.roomId.value(),
                        image.actor.incarnation
                    };
                    auto owner = initialOwner(id);
                    if (owner && eligible(id))
                        adopt(image, std::move(*owner));
                }
        }
        std::erase_if(
            installed,
            [&](const auto& entry)
            {
                const auto* route = snapshot->directory->find(
                    entry.first
                );
                return !route || route->lifecycle ==
                                     RoomRouteLifecycle::Retired;
            }
        );
        // The coherent authority view indexes Active and Closing
        // routes only. Values still come from this pass's directory
        // and live worker probes.
        const auto& ids = snapshot->roomImages;
        auto start = routeAfter ? ids.upper_bound(*routeAfter)
                                : ids.begin();
        if (start == ids.end())
            start = ids.begin();
        if (ids.empty())
            return;
        auto current = start;
        do
        {
            const auto id = current->first;
            if (++current == ids.end())
                current = ids.begin();
            const auto* route = snapshot->directory->find(id);
            if (!route ||
                route->lifecycle == RoomRouteLifecycle::Retired ||
                manualPending(*snapshot, id))
                continue;
            if (route->handoff && !terminal(route->handoff->phase))
            {
                if (eligible(id))
                    progress(*route);
                continue;
            }
            const auto node = RoomClient::nodeOf(route->owner);
            const auto live = node ? worker(*node) : std::nullopt;
            if (!live)
            {
                const auto nodes = client.nodes();
                if (node &&
                    std::find(nodes.begin(), nodes.end(), *node) !=
                        nodes.end())
                {
                    const auto observed = health.find(*node);
                    // A changed view is unknown until its bounded
                    // probe returns. Only a fresh negative or
                    // explicit removal starts recovery.
                    if (observed == health.end() ||
                        observed->second.version != runtimeVersion())
                        continue;
                }
                std::optional<std::string> target;
                std::uint64_t best = 0;
                for (const auto& nodeName : nodes)
                    if (const auto candidate = worker(nodeName);
                        candidate && *candidate != route->owner)
                    {
                        const auto weight = score(id, nodeName);
                        if (!target || weight > best)
                        {
                            target = candidate;
                            best = weight;
                        }
                    }
                // This is a new durable epoch handoff, never an RPC
                // route fallback.
                if (target && eligible(id))
                    recoverOwner(*route, *target);
                continue;
            }
            if (*live != route->owner)
            {
                if (eligible(id))
                    recoverOwner(*route, *live);
                continue;
            }
            const bool expired = std::any_of(
                route->receipts.begin(),
                route->receipts.end(),
                [&](const auto& row)
                {
                    return row.second.applied &&
                           row.second.expiresAt <=
                               nanos(config.now());
                }
            );
            if (expired)
            {
                if (eligible(id))
                    change(*route, RoomPruneReceipts{id});
                continue;
            }
            const bool hasTick = std::any_of(
                route->inbox.begin(),
                route->inbox.end(),
                [](const auto& input)
                {
                    return input.operation.kind ==
                           RoomIngressKind::Tick;
                }
            );
            if (hasTick && eligible(
                               id,
                               Lane::Data,
                               route->owner,
                               route->ownerEpoch
                           ))
            {
                routeAfter = id;
                run(*route, snapshot->revision);
            }
        } while (current != start);
    }
    using HintKey = std::pair<RoomIdentity, std::string>;
    struct Hint
    {
        std::uint64_t epoch{0};
        bool retire{false}, inflight{false}, finished{false},
            wanted{true};
        unsigned attempts{0};
        Mono::time_point due{};
    };
    /** @brief Rebuilds intents only when published owner control
     * facts change. */
    void reconcileHints(const Authority::Snapshot& snapshot)
    {
        if (hintControlRevision ==
            snapshot.directory->controlRevision())
            return;
        std::map<HintKey, std::pair<std::uint64_t, bool>> desired;
        for (const auto id : snapshot.directory->ids())
        {
            const auto& route = *snapshot.directory->find(id);
            if (route.lifecycle == RoomRouteLifecycle::Retired)
                desired[{id, route.owner}] = {route.ownerEpoch, true};
            if (route.handoff)
            {
                const auto& h = *route.handoff;
                if (h.phase == RoomMigrationPhase::Frozen ||
                    h.phase == RoomMigrationPhase::Transferred ||
                    h.phase == RoomMigrationPhase::Committed)
                    desired[{id, h.source}] = {
                        h.sourceEpoch,
                        h.phase == RoomMigrationPhase::Committed
                    };
                if (h.phase == RoomMigrationPhase::Aborted)
                    desired[{id, h.target}] = {h.targetEpoch, true};
            }
        }
        for (auto& [key, hint] : hints)
            hint.wanted = false;
        for (const auto& [key, value] : desired)
        {
            auto& hint = hints[key];
            hint.wanted = true;
            if (hint.epoch != value.first ||
                hint.retire != value.second)
            {
                const bool inflight = hint.inflight;
                hint = Hint{value.first, value.second, inflight};
            }
            if (!hint.finished && !hint.inflight)
                pendingHints.insert(key);
        }
        for (auto it = hints.begin(); it != hints.end();)
        {
            if (!it->second.wanted)
            {
                pendingHints.erase(it->first);
                if (!it->second.inflight)
                {
                    it = hints.erase(it);
                    continue;
                }
            }
            ++it;
        }
        hintControlRevision = snapshot.directory->controlRevision();
        std::lock_guard lock(mutex);
        ++counts.cacheReconciliations;
    }
    /** @brief Dispatches only unfinished intents, with independent
     * finite loans. */
    void cleanup(const Authority::Snapshot& snapshot)
    {
        reconcileHints(snapshot);
        for (auto pending = pendingHints.begin();
             pending != pendingHints.end() && hintTransports < 16;)
        {
            const auto key = *pending++;
            const auto found = hints.find(key);
            if (found == hints.end())
            {
                pendingHints.erase(key);
                continue;
            }
            auto& hint = found->second;
            if (!hint.wanted || hint.finished || hint.inflight)
            {
                pendingHints.erase(key);
                continue;
            }
            if (Mono::now() < hint.due)
                continue;
            const auto node = RoomClient::nodeOf(key.second);
            const auto live = node ? worker(*node) : std::nullopt;
            // A restarted process has already invalidated recovered
            // active caches.
            if (live && *live != key.second)
            {
                hint.finished = true;
                pendingHints.erase(key);
                continue;
            }
            if (!live)
                continue;
            pb::HintRequest request;
            request.set_version(1);
            request.set_worker_id(key.second);
            request.set_owner_epoch(hint.epoch);
            request.set_directory_revision(snapshot.revision);
            request.set_room_id(key.first.room);
            request.set_incarnation(key.first.incarnation);
            pendingHints.erase(key);
            hint.inflight = true;
            ++hint.attempts;
            ++hintTransports;
            const auto expected = std::pair{hint.epoch, hint.retire};
            rpc<pb::CacheReceipt>(
                {},
                [&](auto cb)
                {
                    return hint.retire ? hintClient.retire(
                                             request,
                                             context(),
                                             std::move(cb)
                                         )
                                       : hintClient.freeze(
                                             request,
                                             context(),
                                             std::move(cb)
                                         );
                },
                [self = shared_from_this(),
                 key,
                 expected](auto result)
                {
                    --self->hintTransports;
                    const auto found = self->hints.find(key);
                    if (found == self->hints.end())
                        return;
                    auto& hint = found->second;
                    hint.inflight = false;
                    if (!hint.wanted)
                    {
                        self->pendingHints.erase(key);
                        self->hints.erase(found);
                        return;
                    }
                    // An old physical loan cannot resolve a newer
                    // desired epoch or kind.
                    if (std::pair{hint.epoch, hint.retire} !=
                        expected)
                    {
                        self->pendingHints.insert(key);
                        return;
                    }
                    hint.finished = bool(result) ||
                                    (!result &&
                                     (result.error() ==
                                          arena::rpc::NOT_FOUND ||
                                      result.error() ==
                                          arena::rpc::CONFLICT)) ||
                                    hint.attempts >= 3;
                    hint.due = Mono::now() + self->config.probe;
                    if (!hint.finished)
                        self->pendingHints.insert(key);
                }
            );
        }
    }
    void arm()
    {
        if (timerPending || stopping.load())
            return;
        timerPending = true;
        timer.expires_after(config.poll);
        timer.async_wait(
            [self = shared_from_this()](auto error)
            {
                self->timerPending = false;
                if (!error)
                    self->pass();
                else
                    self->refresh();
            }
        );
    }
    void pass() noexcept
    {
        try
        {
            if (stopping.load())
            {
                while (!actions.empty())
                {
                    auto action = std::move(actions.front());
                    actions.pop_front();
                    action->complete(false, arena::rpc::UNAVAILABLE);
                }
                refresh();
                return;
            }
            JobScope scope(*this, 0);
            probeNodes();
            const auto snapshot = authority.snapshot();
            if (authority.stats().ready)
                schedule(snapshot);
            if (snapshot && snapshot->directory)
            {
                JobScope scope(*this, 0);
                cleanup(*snapshot);
            }
            flush();
            arm();
            refresh();
        }
        catch (...)
        {
            try
            {
                arm();
                refresh();
            }
            catch (...)
            {
            }
        }
    }
    Authority& authority;
    Config config;
    boost::asio::strand<boost::asio::any_io_executor> strand;
    boost::asio::steady_timer timer;
    RoomClient client, controlClient, hintClient;
    std::map<std::string, Health> health;
    std::map<RoomIdentity, Installed> installed;
    std::map<JobKey, Mono::time_point> retry;
    std::map<Token, JobKey> running;
    Token currentJob{0}, nextToken{0};
    std::optional<RoomIdentity> routeAfter;
    std::deque<std::shared_ptr<Action>> actions;
    std::map<HintKey, Hint> hints;
    std::set<HintKey> pendingHints;
    std::optional<std::uint64_t> hintControlRevision;
    std::size_t transports{0}, hintTransports{0};
    bool writing{false}, timerPending{false};
    std::atomic<bool> stopping{false}, wakePosted{false};
    mutable std::mutex mutex;
    /// Guards the published statistics' discovery provenance with
    /// counts.
    std::uint64_t countsVersion{0};
    Stats counts;
};
RoomCoordinator::RoomCoordinator(
    boost::asio::any_io_executor executor,
    LobbyAuthority& authority,
    Config config
)
    : m_state(
          std::make_shared<State>(
              executor, authority, std::move(config)
          )
      )
{
    boost::asio::post(
        m_state->strand,
        [state = m_state] { state->pass(); }
    );
}
RoomCoordinator::~RoomCoordinator() noexcept
{
    stop();
}
void RoomCoordinator::wake() noexcept
{
    const auto state = m_state;
    if (state->stopping.load() || state->wakePosted.exchange(true))
        return;
    try
    {
        boost::asio::post(
            state->strand,
            [state]
            {
                state->wakePosted.store(false);
                state->pass();
            }
        );
    }
    catch (...)
    {
        state->wakePosted.store(false);
    }
}
std::expected<RoomCoordinator::Cancellation, Status>
RoomCoordinator::startMigration(
    RoomIdentity room,
    std::uint64_t epoch,
    std::uint64_t migration,
    std::string target,
    Clock::time_point deadline,
    MigrationCompletion completion
)
{
    const auto state = m_state;
    if (!room.room || !room.incarnation || !epoch || !migration ||
        !completion ||
        !RoomClient::nodeOf(
            target + ":00000000000000000000000000000000"
        ))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    if (deadline <= state->config.now())
        return std::unexpected(arena::rpc::DEADLINE_EXCEEDED);
    auto admin = std::make_shared<State::Admin>();
    admin->room = room;
    admin->epoch = epoch;
    admin->migration = migration;
    admin->target = std::move(target);
    admin->deadline = deadline;
    admin->completion = std::move(completion);
    {
        std::lock_guard lock(state->mutex);
        if (state->stopping.load())
            return std::unexpected(arena::rpc::UNAVAILABLE);
        if (state->counts.migrations >= state->config.maxInflight)
            return std::unexpected(arena::rpc::OVERLOADED);
        ++state->counts.migrations;
        state->counts.stopped = false;
    }
    try
    {
        boost::asio::post(
            state->strand,
            [state, admin]
            {
                try
                {
                    state->prepare(admin);
                }
                catch (...)
                {
                    state->completeAdmin(
                        admin,
                        std::unexpected(arena::rpc::INTERNAL)
                    );
                }
                state->pass();
            }
        );
    }
    catch (...)
    {
        std::lock_guard lock(state->mutex);
        --state->counts.migrations;
        return std::unexpected(arena::rpc::OVERLOADED);
    }
    return Cancellation(
        [weak = std::weak_ptr<State::Admin>(admin)]
        {
            if (auto admin = weak.lock())
                admin->cancelled.store(true);
        }
    );
}
RoomCoordinator::Stats RoomCoordinator::stats() const
{
    auto state = m_state;
    std::lock_guard lock(state->mutex);
    auto stats = state->counts;
    stats.rpcInflight = state->client.inflight() +
                        state->controlClient.inflight() +
                        state->hintClient.inflight();
    stats.stopping = state->stopping.load();
    if (state->countsVersion != state->runtimeVersion())
    {
        stats.healthyWorkers = 0;
        stats.ready = false;
    }
    stats.ready = stats.ready && !stats.stopping;
    return stats;
}
void RoomCoordinator::stop() noexcept
{
    const auto state = m_state;
    if (state->stopping.exchange(true))
        return;
    try
    {
        state->client.stop();
        state->controlClient.stop();
        state->hintClient.stop();
        boost::asio::post(
            state->strand,
            [state]
            {
                boost::system::error_code ignored;
                state->timer.cancel(ignored);
                state->pass();
            }
        );
    }
    catch (...)
    {
    }
}
} // namespace service::lobby