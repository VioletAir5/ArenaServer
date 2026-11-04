/** @file
 * @brief Runs authenticated Room proposals and durable,
 * non-authoritative cache loans.
 */
#include "room/RoomWorker.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>

#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <openssl/rand.h>

#include "persistence/AtomicJournal.h"
#include "room/RoomWire.h"

namespace service::room
{
namespace
{
namespace pb = arena::room;
using Server = arena::internal_rpc::Server;
using Journal = arena::persistence::AtomicJournal;
using Status = arena::rpc::Status;
struct Rejected
{
    Status status;
};
void check(bool value, Status status = arena::rpc::INVALID_ARGUMENT)
{
    if (!value)
        throw Rejected{status};
}
bool nodeName(std::string_view name)
{
    return !name.empty() && name.size() <= 64 &&
           name.front() >= 'a' && name.front() <= 'z' &&
           std::all_of(
               name.begin(),
               name.end(),
               [](unsigned char c)
               {
                   return (c >= 'a' && c <= 'z') ||
                          (c >= '0' && c <= '9') || c == '_' ||
                          c == '-';
               }
           );
}
bool hexProof(std::string_view proof, std::size_t size = 64)
{
    return proof.size() == size &&
           std::all_of(
               proof.begin(),
               proof.end(),
               [](unsigned char c)
               {
                   return (c >= '0' && c <= '9') ||
                          (c >= 'a' && c <= 'f');
               }
           );
}
bool workerName(std::string_view name)
{
    auto colon = name.find(':');
    return colon != std::string_view::npos &&
           nodeName(name.substr(0, colon)) &&
           hexProof(name.substr(colon + 1), 32);
}
bool transferName(std::string_view name)
{
    return !name.empty() && name.size() <= 128 &&
           std::all_of(
               name.begin(),
               name.end(),
               [](unsigned char c)
               {
                   return (c >= 'a' && c <= 'z') ||
                          (c >= 'A' && c <= 'Z') ||
                          (c >= '0' && c <= '9') || c == '-' ||
                          c == '_' || c == '.' || c == ':';
               }
           );
}
std::string instance(const std::string& node)
{
    std::array<unsigned char, 16> bytes{};
    if (RAND_bytes(bytes.data(), bytes.size()) != 1)
        throw std::runtime_error("Room instance entropy unavailable");
    constexpr char hex[] = "0123456789abcdef";
    std::string result = node + ":";
    for (auto b : bytes)
    {
        result += hex[b >> 4];
        result += hex[b & 15];
    }
    return result;
}
Status codecStatus(wire::CodecError error)
{
    return error == wire::CodecError::Capacity
               ? arena::rpc::OVERLOADED
           : error == wire::CodecError::Unavailable
               ? arena::rpc::UNAVAILABLE
               : arena::rpc::INVALID_ARGUMENT;
}
Status runtimeStatus(RuntimeError error)
{
    switch (error)
    {
    case RuntimeError::Capacity:
        return arena::rpc::OVERLOADED;
    case RuntimeError::Unauthenticated:
        return arena::rpc::UNAUTHENTICATED;
    case RuntimeError::BusinessFailure:
        return arena::rpc::CONFLICT;
    case RuntimeError::Unavailable:
        return arena::rpc::UNAVAILABLE;
    default:
        return arena::rpc::INVALID_ARGUMENT;
    }
}
template <class T> T decoded(std::expected<T, wire::CodecError> value)
{
    if (!value)
        throw Rejected{codecStatus(value.error())};
    return std::move(*value);
}
void validated(const RuntimeImage& image)
{
    auto valid = RoomRuntime::validate(image);
    if (!valid)
        throw Rejected{runtimeStatus(valid.error())};
}
std::string
key(bool stage, std::uint64_t room, std::uint64_t incarnation)
{
    return std::string(stage ? "stage/" : "route/") +
           std::to_string(room) + "/" + std::to_string(incarnation);
}
/** @brief Counts actual descriptors in this process, never a guessed
 * cache count. */
std::size_t descriptorCount()
{
    std::size_t count = 0;
    for (const auto& entry :
         std::filesystem::directory_iterator("/proc/self/fd"))
    {
        (void)entry;
        if (++count > 65536)
            throw Rejected{arena::rpc::OVERLOADED};
    }
    return count;
}
} // namespace

struct RoomWorker::State : std::enable_shared_from_this<State>
{
    struct Numbers
    {
        std::size_t routes{0}, active{0}, stages{0}, bytes{0};
    };
    struct Recovery
    {
        /// Detached image-free state published only after every
        /// startup ACK.
        std::map<std::string, std::string> values;
        /// Final bounded cache accounting for the detached state.
        Numbers numbers;
        /// Idempotent fence replacements retained through all
        /// physical writes.
        std::vector<Journal::Mutation> mutations;
        /// First replacement not yet admitted to the private writer.
        std::size_t next{0};
    };
    struct Job
    {
        arena::rpc::Request request;
        arena::internal_rpc::Context context;
        Server::Completion completion;
        std::shared_ptr<std::atomic<bool>> cancelled;
        bool finished{false};
    };
    State(boost::asio::any_io_executor executor, Config value)
        : config(std::move(value))
        , strand(boost::asio::make_strand(executor))
    {
        if (!executor || !nodeName(config.nodeId) ||
            config.token.size() < 32 || config.token.size() > 256 ||
            config.directory.empty() || !config.maxInflight ||
            config.maxInflight > 256 || !config.maxRooms ||
            config.maxRooms > 16384 || !config.maxStages ||
            config.maxStages > 16384 || config.maxCacheBytes < 1024 ||
            config.maxCacheBytes > 16 * 1024 * 1024)
            throw std::invalid_argument(
                "invalid Room worker configuration"
            );
        workerId = instance(config.nodeId);
        // A namespace directory prevents even a misconfigured parent
        // from opening its Lobby authority.wal. Different node caches
        // cannot alias each other.
        Journal::Config journalConfig;
        journalConfig.directory = config.directory /
                                  ("room-cache-" + config.nodeId);
        journal = std::make_unique<Journal>(
            strand,
            std::move(journalConfig)
        );
        try
        {
            recover();
        }
        catch (const Rejected&)
        {
            throw std::runtime_error("invalid Room private cache");
        }
    }
    void recover()
    {
        const auto snapshot = journal->snapshot();
        values = snapshot->values;
        for (const auto& [name, bytes] : values)
        {
            const bool stage = name.starts_with("stage/");
            check(stage || name.starts_with("route/"));
            pb::CacheRecord record;
            auto parsed = wire::parseMessage(bytes, record);
            check(bool(parsed));
            check(
                record.version() == 1 &&
                workerName(record.worker_id()) &&
                record.worker_id().starts_with(config.nodeId + ":") &&
                record.owner_epoch() && record.room_id() &&
                record.incarnation() &&
                hexProof(record.fingerprint()) &&
                key(stage, record.room_id(), record.incarnation()) ==
                    name
            );
            check(
                stage ? (record.directory_revision() == 0 &&
                         transferName(record.transfer_id()) &&
                         !record.active() &&
                         (record.has_image() ||
                          record.worker_id() != workerId))
                      : record.directory_revision() != 0
            );
            if (record.has_image())
            {
                auto image = decoded(
                    wire::unpackImage(record.image())
                );
                validated(image);
                check(
                    record.room_id() == record.image().room_id() &&
                    record.incarnation() ==
                        record.image().incarnation() &&
                    record.cursor() == record.image().cursor() &&
                    record.logical_tick() ==
                        record.image().logical_tick() &&
                    record.fingerprint() ==
                        record.image().fingerprint()
                );
            }
            else
                check(!record.active());
            if (stage)
                ++numbers.stages;
            else
                ++numbers.routes;
            // Previous-instance records retain fences, never active
            // execution rights.
            numbers.bytes += name.size() + bytes.size();
        }
        check(
            numbers.routes <= config.maxRooms &&
                numbers.stages <= config.maxStages &&
                numbers.bytes <= config.maxCacheBytes,
            arena::rpc::OVERLOADED
        );
    }
    /** @brief Quarantines startup if private cache normalization
     * cannot commit. */
    void recoveryFailed() noexcept
    {
        stopping.store(true);
        server->stop();
        busy = false;
        pump();
    }
    /** @brief Retains old epoch fences while releasing
     * previous-instance images. Each bounded batch must receive its
     * real journal ACK before readiness. A crash between batches
     * leaves valid fences and remaining old images; the next instance
     * validates and resumes normalization.
     */
    void
    writeRecovery(const std::shared_ptr<Recovery>& recovery) noexcept
    {
        try
        {
            if (stopping.load())
            {
                busy = false;
                pump();
                return;
            }
            if (recovery->next == recovery->mutations.size())
            {
                values.swap(recovery->values);
                {
                    std::lock_guard lock(mutex);
                    numbers = recovery->numbers;
                }
                initialized.store(true, std::memory_order_release);
                busy = false;
                pump();
                return;
            }
            // Image-free records contain only bounded identifiers and
            // fences. 256 records fit both the four-MiB frame and
            // pending-byte bound.
            const auto count = std::min<std::size_t>(
                256,
                recovery->mutations.size() - recovery->next
            );
            std::vector<Journal::Mutation> batch;
            batch.reserve(count);
            for (std::size_t i = 0; i < count; ++i)
                batch.push_back(
                    recovery->mutations[recovery->next + i]
                );
            recovery->next += count;
            if (!journal->commit(
                    journal->snapshot()->revision,
                    std::move(batch),
                    [self = shared_from_this(), recovery](auto result)
                    {
                        if (!result)
                            self->recoveryFailed();
                        else
                            self->writeRecovery(recovery);
                    }
                ))
                recoveryFailed();
        }
        catch (...)
        {
            recoveryFailed();
        }
    }
    /** @brief Prepares private-cache normalization outside all
     * admitted business work. */
    void releaseRecoveredImages() noexcept
    {
        try
        {
            auto recovery = std::make_shared<Recovery>();
            recovery->values = values;
            for (const auto& [name, bytes] : values)
            {
                pb::CacheRecord record;
                check(bool(wire::parseMessage(bytes, record)));
                if (record.worker_id() == workerId ||
                    !record.has_image())
                    continue;
                record.clear_image();
                record.set_active(false);
                auto encoded = decoded(
                    wire::serializeMessage(record)
                );
                recovery->values.at(name) = encoded;
                recovery->mutations.push_back(
                    {name, std::move(encoded)}
                );
            }
            recovery->numbers = planned(recovery->mutations);
            writeRecovery(recovery);
        }
        catch (...)
        {
            recoveryFailed();
        }
    }
    bool intent() const
    {
        try
        {
            return !config.configuredReady ||
                   config.configuredReady();
        }
        catch (...)
        {
            return false;
        }
    }
    std::optional<pb::CacheRecord> record(
        bool stage, std::uint64_t room, std::uint64_t incarnation
    ) const
    {
        auto found = values.find(key(stage, room, incarnation));
        if (found == values.end())
            return {};
        pb::CacheRecord value;
        auto parsed = wire::parseMessage(found->second, value);
        check(bool(parsed), arena::rpc::INTERNAL);
        return value;
    }
    static pb::CacheRecord cached(
        const pb::Image& image,
        const std::string& worker,
        std::uint64_t epoch,
        std::uint64_t revision,
        std::string transfer,
        bool active
    )
    {
        pb::CacheRecord r;
        r.set_version(1);
        r.set_worker_id(worker);
        r.set_owner_epoch(epoch);
        r.set_directory_revision(revision);
        *r.mutable_image() = image;
        r.set_transfer_id(std::move(transfer));
        r.set_active(active);
        r.set_room_id(image.room_id());
        r.set_incarnation(image.incarnation());
        r.set_cursor(image.cursor());
        r.set_logical_tick(image.logical_tick());
        r.set_fingerprint(image.fingerprint());
        return r;
    }
    pb::CacheReceipt receipt(const pb::CacheRecord& r) const
    {
        pb::CacheReceipt p;
        p.set_version(1);
        p.set_worker_id(workerId);
        p.set_owner_epoch(r.owner_epoch());
        p.set_directory_revision(r.directory_revision());
        p.set_room_id(r.room_id());
        p.set_incarnation(r.incarnation());
        p.set_cursor(r.cursor());
        p.set_logical_tick(r.logical_tick());
        p.set_fingerprint(r.fingerprint());
        p.set_active(r.active() && r.worker_id() == workerId);
        return p;
    }
    template <class Message> Message parse(const Job& job) const
    {
        Message value;
        auto parsed = wire::parseMessage(
            job.request.payload(),
            value
        );
        if (!parsed)
            throw Rejected{codecStatus(parsed.error())};
        check(
            value.version() == 1 && value.worker_id() == workerId,
            value.version() == 1 ? arena::rpc::CONFLICT
                                 : arena::rpc::INCOMPATIBLE_VERSION
        );
        return value;
    }
    template <class Message>
    arena::rpc::Response
    response(const Job& job, const Message& message) const
    {
        arena::rpc::Response r;
        r.set_request_id(job.request.request_id());
        r.set_correlation_id(job.request.correlation_id());
        r.set_protocol_version(arena::internal_rpc::protocolVersion);
        r.set_status(arena::rpc::OK);
        r.set_payload(decoded(wire::serializeMessage(message)));
        // The real complete envelope is checked too; the codec's
        // metadata reserve is not a substitute for the unchanged
        // native frame bound.
        check(
            r.ByteSizeLong() <= arena::internal_rpc::Client::maxFrame,
            arena::rpc::OVERLOADED
        );
        return r;
    }
    /** @brief Ends one physical loan exactly once; owner completion
     * never throws. */
    void finish(
        const std::shared_ptr<Job>& job,
        arena::rpc::Response result,
        bool running = true
    ) noexcept
    {
        if (job->finished)
            return;
        job->finished = true;
        {
            std::lock_guard lock(mutex);
            --activeJobs;
        }
        if (running)
            busy = false;
        auto completion = std::move(job->completion);
        try
        {
            completion(std::move(result));
        }
        catch (...)
        {
        }
    }
    void finish(
        const std::shared_ptr<Job>& job,
        Status status,
        bool running = true
    ) noexcept
    {
        arena::rpc::Response r;
        r.set_status(status);
        finish(job, std::move(r), running);
    }
    Numbers
    planned(const std::vector<Journal::Mutation>& mutations) const
    {
        Numbers next;
        {
            std::lock_guard lock(mutex);
            next = numbers;
        }
        for (const auto& m : mutations)
        {
            const bool stage = m.key.starts_with("stage/");
            auto old = values.find(m.key);
            if (old != values.end())
            {
                next.bytes -= m.key.size() + old->second.size();
                if (stage)
                    --next.stages;
                else
                {
                    --next.routes;
                    pb::CacheRecord r;
                    check(
                        bool(wire::parseMessage(old->second, r)),
                        arena::rpc::INTERNAL
                    );
                    if (r.active() && r.worker_id() == workerId)
                        --next.active;
                }
            }
            if (m.value)
            {
                next.bytes += m.key.size() + m.value->size();
                if (stage)
                    ++next.stages;
                else
                {
                    ++next.routes;
                    pb::CacheRecord r;
                    check(
                        bool(wire::parseMessage(*m.value, r)),
                        arena::rpc::INTERNAL
                    );
                    if (r.active() && r.worker_id() == workerId)
                        ++next.active;
                }
            }
        }
        check(
            next.routes <= config.maxRooms &&
                next.stages <= config.maxStages &&
                next.bytes <= config.maxCacheBytes,
            arena::rpc::OVERLOADED
        );
        return next;
    }
    void durable(
        const std::shared_ptr<Job>& job,
        std::vector<Journal::Mutation> mutations,
        arena::rpc::Response reply
    )
    {
        const auto next = planned(mutations);
        const auto revision = journal->snapshot()->revision;
        auto candidate = std::make_shared<
            std::map<std::string, std::string>>(values);
        for (const auto& mutation : mutations)
        {
            if (mutation.value)
                candidate->insert_or_assign(
                    mutation.key,
                    *mutation.value
                );
            else
                candidate->erase(mutation.key);
        }
        const bool accepted = journal->commit(
            revision,
            std::move(mutations),
            [self = shared_from_this(),
             job,
             next,
             candidate,
             reply = std::move(reply)](auto result) mutable
            {
                if (result)
                {
                    self->values.swap(*candidate);
                    {
                        std::lock_guard lock(self->mutex);
                        self->numbers = next;
                    }
                    self->finish(job, std::move(reply));
                    self->pump();
                    return;
                }
                const auto error = result.error();
                self->finish(
                    job,
                    error == Journal::Error::OutcomeUnknown
                        ? arena::rpc::OUTCOME_UNKNOWN
                    : error == Journal::Error::Capacity
                        ? arena::rpc::OVERLOADED
                    : error == Journal::Error::Conflict
                        ? arena::rpc::CONFLICT
                        : arena::rpc::UNAVAILABLE
                );
                self->pump();
            }
        );
        if (!accepted)
            finish(
                job,
                journal->stats().accepting ? arena::rpc::OVERLOADED
                                           : arena::rpc::UNAVAILABLE
            );
    }
    void create(const std::shared_ptr<Job>& job)
    {
        const auto request = parse<pb::CreateRequest>(*job);
        check(
            request.room_id() && request.incarnation() &&
            request.has_policy()
        );
        const auto policy = decoded(
            wire::unpackPolicy(request.policy())
        );
        std::optional<RuntimeAllocation> allocation;
        if (request.has_allocation())
            allocation = decoded(
                wire::unpackAllocation(request.allocation())
            );
        auto image = RoomRuntime::create(
            RoomId{request.room_id()},
            request.incarnation(),
            policy,
            allocation,
            request.created_wall_millis()
        );
        if (!image)
            throw Rejected{runtimeStatus(image.error())};
        pb::CreateReply reply;
        reply.set_version(1);
        reply.set_worker_id(workerId);
        *reply.mutable_image() = decoded(wire::packImage(*image, 0));
        finish(job, response(*job, reply));
    }
    void stage(const std::shared_ptr<Job>& job)
    {
        const auto request = parse<pb::StageRequest>(*job);
        check(
            request.owner_epoch() &&
            transferName(request.transfer_id()) && request.has_image()
        );
        const auto image = decoded(
            wire::unpackImage(request.image())
        );
        validated(image);
        if (request.has_legacy_replay())
        {
            const auto log = decoded(
                wire::decodeReplayLog(request.legacy_replay())
            );
            const auto& a = log.config;
            const auto& b = image.policy.actor;
            check(
                a.maxMembers == b.maxMembers &&
                a.maxInputsPerTick == b.maxInputsPerTick &&
                a.historyTicks == b.historyTicks &&
                a.positionLimit == b.positionLimit
            );
            const auto replay = RoomActor::replay(log);
            check(bool(replay) && replay->snapshot == image.actor);
        }
        auto desired = cached(
            request.image(),
            workerId,
            request.owner_epoch(),
            0,
            request.transfer_id(),
            false
        );
        const auto existing = record(
                       true,
                       image.actor.roomId.value(),
                       image.actor.incarnation
                   ),
                   route = record(
                       false,
                       image.actor.roomId.value(),
                       image.actor.incarnation
                   );
        pb::StageReceipt reply;
        reply.set_version(1);
        reply.set_worker_id(workerId);
        reply.set_transfer_id(request.transfer_id());
        reply.set_owner_epoch(request.owner_epoch());
        reply.set_room_id(request.image().room_id());
        reply.set_incarnation(request.image().incarnation());
        reply.set_physical_tick(request.image().physical_tick());
        reply.set_checksum(request.image().checksum());
        reply.set_cursor(request.image().cursor());
        reply.set_logical_tick(request.image().logical_tick());
        reply.set_fingerprint(request.image().fingerprint());
        if (existing && existing->SerializeAsString() ==
                            desired.SerializeAsString())
        {
            finish(job, response(*job, reply));
            return;
        }
        // An initial inactive proof may survive a crash before Lobby
        // RoomCreate. Revalidation by a new instance may replace only
        // the identical proof; no route (including a retired epoch
        // fence) may have existed here.
        const bool rebound = existing && !route &&
                             existing->worker_id() != workerId &&
                             !existing->active() &&
                             existing->owner_epoch() ==
                                 request.owner_epoch() &&
                             request.owner_epoch() == 1 &&
                             existing->transfer_id() ==
                                 request.transfer_id() &&
                             existing->fingerprint() ==
                                 request.image().fingerprint() &&
                             existing->cursor() ==
                                 request.image().cursor() &&
                             existing->logical_tick() ==
                                 request.image().logical_tick();
        check(
            rebound || ((!existing || existing->owner_epoch() <
                                          request.owner_epoch()) &&
                        (!route || route->owner_epoch() <
                                       request.owner_epoch())),
            arena::rpc::CONFLICT
        );
        durable(
            job,
            {{key(true,
                  request.image().room_id(),
                  request.image().incarnation()),
              decoded(wire::serializeMessage(desired))}},
            response(*job, reply)
        );
    }
    void activate(const std::shared_ptr<Job>& job)
    {
        const auto request = parse<pb::ActivateRequest>(*job);
        check(
            request.owner_epoch() && request.directory_revision() &&
            request.has_image()
        );
        const auto image = decoded(
            wire::unpackImage(request.image())
        );
        validated(image);
        const auto room = image.actor.roomId.value();
        const auto
            route = record(false, room, image.actor.incarnation),
            staged = record(true, room, image.actor.incarnation);
        if (route)
        {
            check(
                route->owner_epoch() <= request.owner_epoch() &&
                    route->directory_revision() <=
                        request.directory_revision(),
                arena::rpc::CONFLICT
            );
            if (route->owner_epoch() == request.owner_epoch())
            {
                check(
                    route->worker_id() == workerId &&
                        route->incarnation() ==
                            request.image().incarnation() &&
                        route->cursor() <= request.image().cursor() &&
                        route->logical_tick() <=
                            request.image().logical_tick() &&
                        route->has_image() &&
                        route->image().physical_tick() <=
                            request.image().physical_tick(),
                    arena::rpc::CONFLICT
                );
                if (route->cursor() == request.image().cursor() ||
                    route->logical_tick() ==
                        request.image().logical_tick())
                    check(
                        route->image().SerializeAsString() ==
                            request.image().SerializeAsString(),
                        arena::rpc::CONFLICT
                    );
                if (route->directory_revision() ==
                    request.directory_revision())
                {
                    check(
                        route->active() && route->has_image() &&
                            route->image().SerializeAsString() ==
                                request.image().SerializeAsString() &&
                            route->transfer_id() ==
                                request.transfer_id(),
                        arena::rpc::CONFLICT
                    );
                    finish(job, response(*job, receipt(*route)));
                    return;
                }
            }
        }
        check(
            !staged || staged->owner_epoch() <= request.owner_epoch(),
            arena::rpc::CONFLICT
        );
        if (!request.transfer_id().empty())
            check(
                (staged && staged->worker_id() == workerId &&
                 staged->owner_epoch() == request.owner_epoch() &&
                 staged->transfer_id() == request.transfer_id() &&
                 staged->image().SerializeAsString() ==
                     request.image().SerializeAsString()) ||
                    (!staged && route &&
                     route->worker_id() == workerId &&
                     route->owner_epoch() == request.owner_epoch() &&
                     route->has_image() &&
                     route->transfer_id() == request.transfer_id()),
                arena::rpc::CONFLICT
            );
        else
            check(
                !staged ||
                    staged->owner_epoch() != request.owner_epoch() ||
                    (request.owner_epoch() == 1 &&
                     staged->worker_id() == workerId &&
                     staged->transfer_id() ==
                         "initial-" + std::to_string(room) + "-" +
                             std::to_string(
                                 image.actor.incarnation
                             ) &&
                     staged->image().SerializeAsString() ==
                         request.image().SerializeAsString()),
                arena::rpc::CONFLICT
            );
        const auto desired = cached(
            request.image(),
            workerId,
            request.owner_epoch(),
            request.directory_revision(),
            request.transfer_id(),
            true
        );
        std::vector<Journal::Mutation> changes{
            {key(false, room, image.actor.incarnation),
             decoded(wire::serializeMessage(desired))}
        };
        if (staged)
            changes.push_back(
                {key(true, room, image.actor.incarnation),
                 std::nullopt}
            );
        auto reply = response(*job, receipt(desired));
        if (route && route->worker_id() == workerId &&
            route->owner_epoch() == request.owner_epoch() &&
            route->has_image() && !staged)
        {
            // Lobby has already durably published this image. Forward
            // same-epoch installation is process-local, avoiding one
            // fdatasync per room tick. Restart changes worker_id and
            // requires a new-epoch activation.
            const auto next = planned(changes);
            auto encoded = std::move(*changes.front().value);
            values.at(key(false, room, image.actor.incarnation))
                .swap(encoded);
            {
                std::lock_guard lock(mutex);
                numbers = next;
            }
            finish(job, std::move(reply));
            return;
        }
        durable(job, std::move(changes), std::move(reply));
    }
    void execute(const std::shared_ptr<Job>& job)
    {
        const auto request = parse<pb::ExecuteRequest>(*job);
        check(request.owner_epoch() && request.has_base());
        const auto base = decoded(wire::unpackImage(request.base()));
        validated(base);
        const auto route = record(
            false,
            base.actor.roomId.value(),
            base.actor.incarnation
        );
        check(
            route && route->active() &&
                route->worker_id() == workerId &&
                route->owner_epoch() == request.owner_epoch() &&
                route->incarnation() == base.actor.incarnation,
            arena::rpc::CONFLICT
        );
        check(
            route->cursor() <= request.base().cursor() &&
                route->logical_tick() <=
                    request.base().logical_tick() &&
                route->has_image() &&
                route->image().physical_tick() <=
                    request.base().physical_tick(),
            arena::rpc::CONFLICT
        );
        check(
            base.logicalCutoff !=
                std::numeric_limits<std::uint64_t>::max(),
            arena::rpc::OVERLOADED
        );
        check(
            request.operations_size() > 0 &&
                request.operations_size() <=
                    static_cast<int>(wire::kMaxOperations) &&
                request.operations_size() <=
                    static_cast<int>(base.policy.maxCommands + 1),
            arena::rpc::OVERLOADED
        );
        RuntimeCutoff cutoff;
        std::vector<RuntimeCommand> commands;
        std::uint64_t cursor = request.base().cursor();
        for (int index = 0; index < request.operations_size();
             ++index)
        {
            const auto& op = request.operations(index);
            check(
                cursor != std::numeric_limits<std::uint64_t>::max() &&
                op.cursor() == cursor + 1
            );
            ++cursor;
            if (index + 1 == request.operations_size())
            {
                check(
                    op.kind() == pb::OP_TICK &&
                    op.logical_tick() == base.logicalCutoff + 1
                );
                cutoff = decoded(wire::decodeCutoff(op.payload()));
            }
            else
            {
                check(
                    op.kind() == pb::OP_COMMAND &&
                    op.logical_tick() == 0
                );
                commands.push_back(
                    decoded(wire::decodeCommand(op.payload()))
                );
            }
        }
        cutoff.commands = std::move(commands);
        auto proposal = RoomRuntime::step(base, cutoff);
        if (!proposal)
            throw Rejected{runtimeStatus(proposal.error())};
        check(
            proposal->image.logicalCutoff == base.logicalCutoff + 1 &&
                proposal->results.size() == cutoff.commands.size(),
            arena::rpc::INTERNAL
        );
        pb::ExecuteReply reply;
        reply.set_version(1);
        reply.set_worker_id(workerId);
        reply.set_owner_epoch(request.owner_epoch());
        *reply.mutable_image() = decoded(
            wire::packImage(proposal->image, cursor)
        );
        for (const auto& result : proposal->results)
        {
            auto& value = *reply.add_results();
            if (result.error)
                value.set_error(
                    static_cast<pb::RuntimeError>(*result.error)
                );
            if (result.input)
                value.set_input(
                    static_cast<pb::InputStatus>(*result.input)
                );
        }
        if (proposal->replay)
            reply.set_replay(
                decoded(wire::encodeTick(*proposal->replay))
            );
        reply.set_release_membership(proposal->releaseMembership);
        reply.set_retire(proposal->retire);
        // The proposal never changes the active cache or any
        // canonical image.
        finish(job, response(*job, reply));
    }
    void hint(const std::shared_ptr<Job>& job, bool retire)
    {
        const auto request = parse<pb::HintRequest>(*job);
        check(
            request.owner_epoch() && request.directory_revision() &&
            request.room_id() && request.incarnation()
        );
        auto route = record(
            false,
            request.room_id(),
            request.incarnation()
        );
        const auto staged = record(
            true,
            request.room_id(),
            request.incarnation()
        );
        if (!route ||
            (retire && staged &&
             staged->owner_epoch() == request.owner_epoch() &&
             route->owner_epoch() < request.owner_epoch()))
        {
            check(
                retire && staged &&
                    staged->owner_epoch() == request.owner_epoch() &&
                    staged->worker_id() == workerId,
                arena::rpc::NOT_FOUND
            );
            route = staged;
        }
        check(
            route->worker_id() == workerId &&
                route->owner_epoch() == request.owner_epoch() &&
                route->incarnation() == request.incarnation() &&
                request.directory_revision() >=
                    route->directory_revision(),
            arena::rpc::CONFLICT
        );
        route->set_active(false);
        route->set_directory_revision(request.directory_revision());
        // Keep epoch/cursor/fingerprint fences while releasing the
        // heavy image.
        route->clear_image();
        std::vector<Journal::Mutation> changes{
            {key(false, request.room_id(), request.incarnation()),
             decoded(wire::serializeMessage(*route))}
        };
        if (staged && staged->worker_id() == workerId &&
            staged->owner_epoch() <= request.owner_epoch())
            changes.push_back(
                {key(true, request.room_id(), request.incarnation()),
                 std::nullopt}
            );
        durable(
            job,
            std::move(changes),
            response(*job, receipt(*route))
        );
    }
    /** @brief Serializes cache transitions while asynchronous durable
     * work is held. */
    void pump() noexcept
    {
        while (!busy)
        {
            if (queue.empty())
            {
                std::size_t count;
                {
                    std::lock_guard lock(mutex);
                    count = activeJobs;
                }
                if (stopping.load() && !count)
                    journal->stop();
                return;
            }
            auto job = std::move(queue.front());
            queue.pop_front();
            busy = true;
            try
            {
                if (job->cancelled->load())
                {
                    finish(job, arena::rpc::CANCELLED);
                    continue;
                }
                if (std::chrono::steady_clock::now() >=
                    job->context.deadline)
                {
                    finish(job, arena::rpc::DEADLINE_EXCEEDED);
                    continue;
                }
                if (!intent() || !journal->stats().accepting)
                {
                    finish(job, arena::rpc::UNAVAILABLE);
                    continue;
                }
                const auto& method = job->request.method();
                if (method == "room.create")
                    create(job);
                else if (method == "room.stage")
                    stage(job);
                else if (method == "room.activate")
                    activate(job);
                else if (method == "room.execute")
                    execute(job);
                else if (method == "room.freeze")
                    hint(job, false);
                else if (method == "room.retire")
                    hint(job, true);
                else
                    finish(job, arena::rpc::NOT_FOUND);
            }
            catch (const Rejected& error)
            {
                finish(job, error.status);
            }
            catch (const std::bad_alloc&)
            {
                finish(job, arena::rpc::OVERLOADED);
            }
            catch (...)
            {
                finish(job, arena::rpc::INTERNAL);
            }
        }
    }
    Server::Cancellation admit(
        arena::rpc::Request request,
        arena::internal_rpc::Context context,
        Server::Completion completion
    )
    {
        auto cancelled = std::make_shared<std::atomic<bool>>(false);
        auto job = std::make_shared<Job>(
            Job{std::move(request),
                std::move(context),
                std::move(completion),
                cancelled}
        );
        Status rejected = arena::rpc::OK;
        {
            std::lock_guard lock(mutex);
            if (stopping.load() ||
                !initialized.load(std::memory_order_acquire))
                rejected = arena::rpc::UNAVAILABLE;
            else if (activeJobs >= config.maxInflight)
                rejected = arena::rpc::OVERLOADED;
            else
                ++activeJobs;
        }
        if (rejected != arena::rpc::OK)
        {
            arena::rpc::Response r;
            r.set_status(rejected);
            job->completion(std::move(r));
            return {};
        }
        try
        {
            boost::asio::post(
                strand,
                [self = shared_from_this(), job]
                {
                    try
                    {
                        self->queue.push_back(job);
                    }
                    catch (...)
                    {
                        self->finish(
                            job,
                            arena::rpc::OVERLOADED,
                            false
                        );
                    }
                    self->pump();
                }
            );
        }
        catch (...)
        {
            std::lock_guard lock(mutex);
            --activeJobs;
            throw;
        }
        return [cancelled] { cancelled->store(true); };
    }
    arena::rpc::Response health(const arena::rpc::Request& request)
    {
        check(request.payload().empty());
        const auto state = observe();
        pb::HealthReply p;
        p.set_version(1);
        p.set_worker_id(workerId);
        p.set_node_id(config.nodeId);
        p.set_ready(state.ready);
        auto& s = *p.mutable_stats();
        s.set_active_routes(state.activeRoutes);
        s.set_inactive_stages(state.stages);
        s.set_cache_bytes(state.cacheBytes);
        s.set_journal_pending(state.journalPending);
        s.set_journal_file_bytes(state.journalFileBytes);
        s.set_active_jobs(state.activeJobs);
        s.set_rpc_peers(state.rpcPeers);
        s.set_rpc_active(state.rpcActive);
        s.set_physical_descriptors(descriptorCount());
        s.set_journal_closed(state.journalClosed);
        s.set_stopping(state.stopping);
        Job job{request, {}, {}, {}};
        auto result = response(job, p);
        if (!state.ready)
            result.set_status(arena::rpc::UNAVAILABLE);
        return result;
    }
    void listen(boost::asio::any_io_executor executor)
    {
        auto weak = weak_from_this();
        Server::Handlers handlers;
        auto roomHandler =
            [weak](auto request, auto context, auto complete)
        {
            if (auto self = weak.lock())
                return self->admit(
                    std::move(request),
                    std::move(context),
                    std::move(complete)
                );
            arena::rpc::Response r;
            r.set_status(arena::rpc::UNAVAILABLE);
            complete(std::move(r));
            return Server::Cancellation{};
        };
        handlers.emplace("room.create", roomHandler);
        handlers.emplace("room.stage", roomHandler);
        handlers.emplace("room.activate", roomHandler);
        handlers.emplace("room.execute", roomHandler);
        handlers.emplace("room.freeze", roomHandler);
        handlers.emplace("room.retire", roomHandler);
        handlers.emplace(
            "health.ready",
            [weak](auto request, auto, auto complete)
            {
                arena::rpc::Response r;
                try
                {
                    if (auto self = weak.lock())
                        r = self->health(request);
                    else
                        r.set_status(arena::rpc::UNAVAILABLE);
                }
                catch (const Rejected& error)
                {
                    r.set_status(error.status);
                }
                catch (...)
                {
                    r.set_status(arena::rpc::INTERNAL);
                }
                complete(std::move(r));
                return Server::Cancellation{};
            }
        );
        Server::Config c;
        c.service = "room";
        c.endpoint = config.endpoint;
        c.callerTokens.emplace("lobby", config.token);
        c.maxActive = config.maxInflight + 1;
        c.maxPeers = 2 * config.maxInflight + 16;
        server = std::make_unique<Server>(
            executor,
            std::move(c),
            std::move(handlers)
        );
        boost::asio::post(
            strand,
            [self = shared_from_this()]
            { self->releaseRecoveredImages(); }
        );
    }
    Stats observe() const
    {
        const auto disk = journal->stats();
        const auto rpc = server ? server->stats() : Server::Stats{};
        Stats s;
        s.workerId = workerId;
        s.nodeId = config.nodeId;
        {
            std::lock_guard lock(mutex);
            s.activeRoutes = numbers.active;
            s.stages = numbers.stages;
            s.cacheBytes = numbers.bytes;
            s.activeJobs = activeJobs;
        }
        s.journalPending = disk.pending;
        s.journalFileBytes = disk.fileBytes;
        s.journalClosed = disk.closed;
        s.rpcPeers = rpc.peers;
        s.rpcActive = rpc.active;
        s.stopping = stopping.load();
        s.ready = server &&
                  initialized.load(std::memory_order_acquire) &&
                  !s.stopping && disk.accepting && intent();
        s.stopped = s.stopping && !s.activeJobs && disk.closed &&
                    rpc.stopped;
        return s;
    }
    const Config config;
    std::string workerId;
    boost::asio::strand<boost::asio::any_io_executor> strand;
    std::unique_ptr<Journal> journal;
    std::unique_ptr<Server> server;
    mutable std::mutex mutex;
    Numbers numbers;
    std::size_t activeJobs{0};
    std::atomic<bool> stopping{false};
    /// Release-published only after every startup fence replacement
    /// is durable.
    std::atomic<bool> initialized{false};
    std::map<std::string, std::string> values;
    std::deque<std::shared_ptr<Job>> queue;
    bool busy{true};
};
RoomWorker::RoomWorker(
    boost::asio::any_io_executor executor, Config config
)
    : m_state(std::make_shared<State>(executor, std::move(config)))
{
    m_state->listen(std::move(executor));
}
RoomWorker::~RoomWorker() noexcept
{
    try
    {
        stop();
    }
    catch (...)
    {
    }
}
std::uint16_t RoomWorker::port() const noexcept
{
    return m_state->server->port();
}
RoomWorker::Stats RoomWorker::stats() const
{
    return m_state->observe();
}
void RoomWorker::stop(std::chrono::milliseconds grace)
{
    const auto state = m_state;
    if (state->stopping.exchange(true))
        return;
    state->server->stop(grace);
    boost::asio::post(state->strand, [state] { state->pump(); });
}
} // namespace service::room