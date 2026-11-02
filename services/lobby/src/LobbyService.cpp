/** @file
 * @brief Keeps all external effects outside durable owner mutations.
 */
#include "lobby/LobbyService.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include "lobby/Protocol.h"
#include "security/SecureErase.h"

namespace service::lobby
{
namespace
{
using namespace std::chrono_literals;
namespace wire = arena::protocol;
namespace rpc = arena::rpc;
namespace meta = arena::lobby_meta;
using Clock = LobbyAuthority::Clock;
using Context = arena::internal_rpc::Context;
using Envelope = wire::Envelope;
using Capture = LobbyAuthority::Capture;
using Work = LobbyAuthority::WorkingState;
using Snapshot = LobbyAuthority::Snapshot;

std::int64_t nanos(Clock::time_point time)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               time.time_since_epoch()
    )
        .count();
}
std::int64_t
expiry(Clock::time_point now, std::chrono::milliseconds duration)
{
    const auto value = nanos(now);
    const auto
        delta = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    duration
        )
                    .count();
    if (value > INT64_MAX - delta)
        throw std::overflow_error("Lobby deadline exhausted");
    return value + delta;
}
bool gateway(std::string_view value)
{
    return value.size() == 32 &&
           std::all_of(
               value.begin(),
               value.end(),
               [](char c)
               {
                   return (c >= '0' && c <= '9') ||
                          (c >= 'a' && c <= 'f');
               }
           );
}
Envelope internalReply()
{
    Envelope reply;
    reply.set_msg_id(wire::MSG_PONG);
    reply.mutable_pong();
    return reply;
}
Envelope failure(std::uint64_t request, wire::ErrorCode code)
{
    Envelope reply;
    reply.set_request_id(request);
    reply.set_msg_id(wire::MSG_ERROR);
    reply.mutable_error()->set_code(code);
    reply.mutable_error()->set_message("request failed");
    return reply;
}
rpc::Status authorityError(LobbyAuthority::Error error)
{
    switch (error)
    {
    case LobbyAuthority::Error::Cancelled:
        return rpc::CANCELLED;
    case LobbyAuthority::Error::Capacity:
        return rpc::OVERLOADED;
    case LobbyAuthority::Error::OutcomeUnknown:
        return rpc::OUTCOME_UNKNOWN;
    case LobbyAuthority::Error::Unavailable:
        return rpc::UNAVAILABLE;
    case LobbyAuthority::Error::InvalidState:
        return rpc::INTERNAL;
    }
    return rpc::INTERNAL;
}
int transport(
    const meta::Metadata& values,
    std::string_view instance,
    std::uint64_t connection
)
{
    for (int i = 0; i != values.transports_size(); ++i)
        if (values.transports(i).gateway_instance() == instance &&
            values.transports(i).connection_id() == connection)
            return i;
    return -1;
}
int receipt(const meta::Metadata& values, std::string_view key)
{
    for (int i = 0; i != values.receipts_size(); ++i)
        if (values.receipts(i).key() == key)
            return i;
    return -1;
}
int delivery(const meta::Metadata& values, std::uint64_t id)
{
    for (int i = 0; i != values.settlements_size(); ++i)
        if (values.settlements(i).match_id() == id)
            return i;
    return -1;
}
const RegistryEntryCheckpoint*
player(const Snapshot& snapshot, std::uint64_t id)
{
    const auto& entries = snapshot.core.registry.entries;
    const auto found = std::find_if(
        entries.begin(),
        entries.end(),
        [id](const auto& e)
        { return e.snapshot.playerId.value() == id; }
    );
    return found == entries.end() ? nullptr : &*found;
}
bool executeShape(const arena::lobby::ExecuteRequest& request)
{
    if (!gateway(request.gateway_instance()) ||
        !request.connection_id() || !request.has_message() ||
        !request.message().request_id() ||
        request.ByteSizeLong() > 16384)
        return false;
    const auto& message = request.message();
    const auto payload = static_cast<std::uint32_t>(
        message.payload_case()
    );
    if (message.msg_id() && message.msg_id() != payload)
        return false;
    switch (payload)
    {
    case wire::MSG_LOGIN_REQUEST:
    {
        const auto& login = message.login_request();
        if (login.client_version().size() > 64 ||
            (!login.operation_key().empty() &&
             (login.operation_key().size() < 16 ||
              login.operation_key().size() > 64 ||
              !std::all_of(
                  login.operation_key().begin(),
                  login.operation_key().end(),
                  [](unsigned char c)
                  {
                      return (c >= 'a' && c <= 'z') ||
                             (c >= 'A' && c <= 'Z') ||
                             (c >= '0' && c <= '9') || c == '_' ||
                             c == '-';
                  }
              ))))
            return false;
        if (login.credential_kind() == wire::CREDENTIAL_JWT)
            return login.account().empty() &&
                   !login.credential().empty() &&
                   login.credential().size() <= 4096;
        return login.credential_kind() == wire::CREDENTIAL_PASSWORD &&
               !login.account().empty() &&
               login.account().size() <= 128 &&
               !login.credential().empty() &&
               login.credential().size() <= 1024 &&
               std::all_of(
                   login.account().begin(),
                   login.account().end(),
                   [](unsigned char c) { return c >= 32 && c <= 126; }
               );
    }
    case wire::MSG_PING:
    case wire::MSG_SESSION_STATE_REQUEST:
    case wire::MSG_ROOM_JOIN_REQUEST:
    case wire::MSG_ROOM_LEAVE_REQUEST:
    case wire::MSG_MATCH_JOIN_REQUEST:
    case wire::MSG_MATCH_CANCEL_REQUEST:
    case wire::MSG_MATCH_STATUS_REQUEST:
    case wire::MSG_ROOM_INPUT_REQUEST:
    case wire::MSG_ROOM_SNAPSHOT_REQUEST:
    case wire::MSG_ROOM_READY_REQUEST:
    case wire::MSG_ROOM_STATUS_REQUEST:
    case wire::MSG_SETTLEMENT_REQUEST:
        return request.player_id() && request.session_epoch();
    default:
        return false;
    }
}

LobbyService::Config validated(LobbyService::Config config)
{
    if (!config.maxJobs || config.maxJobs > 4096 ||
        config.transportLease < 100ms ||
        config.transportLease > 30s || config.receiptRetention < 1s ||
        config.receiptRetention > 1h ||
        !config.settlementMaxAttempts ||
        config.settlementMaxAttempts > 64 ||
        config.settlementMaxElapsed < 1s ||
        config.settlementMaxElapsed > 1h || config.retryBase < 1ms ||
        config.retryMax < config.retryBase || config.retryMax > 30s)
    {
        arena::security::eraseSecret(config.auth.token);
        arena::security::eraseSecret(config.storage.token);
        throw std::invalid_argument(
            "invalid Lobby service configuration"
        );
    }
    return config;
}

/** @brief Constructs one immutable storage intent from the committed
 * managed cohort. */
arena::storage::SaveResultRequest
settlementRequest(const ManagedMatchCheckpoint& match)
{
    arena::storage::SaveResultRequest request;
    auto* record = request.mutable_match();
    const auto& result = *match.result;
    record->set_match_id(result.matchId);
    record->set_room_id(result.roomId.value());
    record->set_state(
        result.reason == SettlementReason::Completed ? "completed"
                                                     : "aborted"
    );
    record->set_result_version(1);
    record->set_winner_player_id(result.winnerPlayerId);
    record->set_final_tick(result.finalTick);
    record->set_final_checksum(result.finalChecksum);
    record->set_finish_reason(
        result.reason == SettlementReason::Completed ? "completed"
        : result.reason == SettlementReason::ReadyTimeout
            ? "ready_timeout"
            : "member_unavailable"
    );
    record->set_created_unix_ms(result.createdUnixMs);
    record->set_started_unix_ms(result.startedUnixMs);
    record->set_finished_unix_ms(result.finishedUnixMs);
    for (std::size_t i = 0; i != match.allocation.members.size(); ++i)
    {
        const auto& member = match.allocation.members[i];
        const auto found = std::find_if(
            result.players.begin(),
            result.players.end(),
            [&](const auto& p)
            { return p.member.playerId == member.playerId; }
        );
        if (found == result.players.end())
            throw std::logic_error("incomplete settlement cohort");
        auto* p = record->add_participants();
        p->set_player_id(member.playerId.value());
        p->set_slot(static_cast<std::uint32_t>(i + 1));
        p->set_session_id(member.sessionId.value());
        p->set_membership_epoch(member.roomEpoch);
        p->set_initial_mmr(member.initialMmr);
        p->set_last_input_seq(found->sequence);
        p->set_final_x(found->x);
        p->set_final_y(found->y);
        p->set_last_dx(found->lastDx);
        p->set_last_dy(found->lastDy);
    }
    return request;
}
} // namespace

struct LobbyService::State : std::enable_shared_from_this<State>
{
    struct Job
    {
        enum class Kind
        {
            Execute,
            Detach,
            Heartbeat
        } kind;
        std::uint64_t id{0};
        Context context;
        arena::lobby::ExecuteRequest execute;
        arena::lobby::DetachRequest detach;
        arena::lobby::HeartbeatRequest heartbeat;
        Completion<arena::lobby::ExecuteReply> executeComplete;
        Completion<arena::lobby::DetachReply> detachComplete;
        Completion<arena::lobby::HeartbeatReply> heartbeatComplete;
        std::atomic<int> cancelled{0};
        bool notified{false};
        arena::internal_rpc::Call child;
        LobbyAuthority::Cancellation transaction;
        std::uint64_t reserved{0};
        arena::auth::AuthenticateReply authenticated;
        std::string receiptKey;
        std::uint64_t proofEpoch{0};
        std::int64_t proofExpiry{0};
        bool receiptResume{false};
        std::uint64_t receiptEpoch{0}, receiptSession{0};
        std::uint32_t mmr{0};
        bool hasMmr{false};
        std::vector<std::uint64_t> revoked;
        ~Job()
        {
            if (execute.has_message() &&
                execute.message().has_login_request())
                arena::security::eraseSecret(
                    *execute.mutable_message()
                         ->mutable_login_request()
                         ->mutable_credential()
                );
            arena::security::eraseSecret(
                *authenticated.mutable_session_token()
            );
        }
    };
    State(boost::asio::any_io_executor executor, Config options)
        : strand(boost::asio::make_strand(executor))
        , config(validated(std::move(options)))
        , auth(strand, config.auth)
        , storage(strand, config.storage)
        , authority(strand, config.authority)
        , timer(strand)
    {
    }
    ~State()
    {
        arena::security::eraseSecret(config.auth.token);
        arena::security::eraseSecret(config.storage.token);
    }
    void start()
    {
        keepAlive = shared_from_this();
        try
        {
            boost::asio::post(
                strand,
                [self = shared_from_this()] { self->poll(); }
            );
        }
        catch (...)
        {
            keepAlive.reset();
            throw;
        }
    }
    rpc::Status abandoned(const std::shared_ptr<Job>& job) const
    {
        const auto reason = job->cancelled.load();
        if (reason)
            return static_cast<rpc::Status>(reason);
        if (Clock::now() >= job->context.deadline)
            return rpc::DEADLINE_EXCEEDED;
        if (stopping.load())
            return rpc::CANCELLED;
        return rpc::OK;
    }
    void notify(const std::shared_ptr<Job>& job, rpc::Status status)
    {
        if (job->notified)
            return;
        job->notified = true;
        try
        {
            if (job->executeComplete)
                job->executeComplete(std::unexpected(status));
            else if (job->detachComplete)
                job->detachComplete(std::unexpected(status));
            else
                job->heartbeatComplete(std::unexpected(status));
        }
        catch (...)
        {
        }
        job->executeComplete = {};
        job->detachComplete = {};
        job->heartbeatComplete = {};
    }
    void retire(const std::shared_ptr<Job>& job, rpc::Status status)
    {
        if (status != rpc::OK)
            notify(job, status);
        std::lock_guard lock(mutex);
        jobs.erase(job->id);
        active = jobs.size();
    }
    template <class Reply>
    void done(const std::shared_ptr<Job>& job, Reply reply)
    {
        const auto status = abandoned(job);
        if (status != rpc::OK)
        {
            retire(job, status);
            return;
        }
        if (!job->notified)
        {
            job->notified = true;
            try
            {
                if constexpr (
                    std::is_same_v<Reply, arena::lobby::ExecuteReply>
                )
                    job->executeComplete(std::move(reply));
                else if constexpr (
                    std::is_same_v<Reply, arena::lobby::DetachReply>
                )
                    job->detachComplete(std::move(reply));
                else
                    job->heartbeatComplete(std::move(reply));
            }
            catch (...)
            {
            }
        }
        retire(job, rpc::OK);
    }
    template <class Function>
    auto guarded(const std::shared_ptr<Job>& job, Function function)
    {
        return [self = shared_from_this(),
                job,
                function = std::move(function)](
                   auto result
               ) mutable noexcept
        {
            try
            {
                function(std::move(result));
            }
            catch (...)
            {
                self->retire(job, rpc::INTERNAL);
            }
        };
    }
    template <class Function> auto backgroundGuard(Function function)
    {
        return [self = shared_from_this(),
                function = std::move(function)](
                   auto result
               ) mutable noexcept
        {
            try
            {
                function(std::move(result));
            }
            catch (...)
            {
                self->fatal = true;
                self->stopping = true;
                try
                {
                    self->beginStop();
                }
                catch (...)
                {
                    self->authority.stop();
                }
            }
        };
    }
    Admission admit(std::shared_ptr<Job> job)
    {
        if (job->context.deadline <= Clock::now())
            return std::unexpected(rpc::DEADLINE_EXCEEDED);
        {
            std::lock_guard lock(mutex);
            if (stopping.load())
                return std::unexpected(rpc::UNAVAILABLE);
            if (jobs.size() >= config.maxJobs ||
                nextJob == UINT64_MAX)
                return std::unexpected(rpc::OVERLOADED);
            job->id = ++nextJob;
            jobs.emplace(job->id, job);
            active = jobs.size();
        }
        try
        {
            boost::asio::post(
                strand,
                [self = shared_from_this(), job]
                {
                    try
                    {
                        const auto status = self->abandoned(job);
                        if (status != rpc::OK)
                        {
                            self->retire(job, status);
                            return;
                        }
                        if (!self->authority.stats().ready)
                        {
                            self->retire(job, rpc::UNAVAILABLE);
                            return;
                        }
                        if (job->kind == Job::Kind::Execute &&
                            job->execute.message()
                                .has_login_request())
                            self->reserveLogin(job);
                        else if (
                            job->kind == Job::Kind::Execute &&
                            job->execute.message()
                                .has_match_join_request()
                        )
                            self->prepareMatch(job);
                        else
                            self->mutate(job);
                    }
                    catch (...)
                    {
                        self->retire(job, rpc::INTERNAL);
                    }
                }
            );
        }
        catch (...)
        {
            std::lock_guard lock(mutex);
            jobs.erase(job->id);
            active = jobs.size();
            return std::unexpected(rpc::OVERLOADED);
        }
        return Cancellation(
            [weak = weak_from_this(), held = std::weak_ptr<Job>(job)]
            {
                const auto job = held.lock();
                if (!job)
                    return;
                job->cancelled.store(rpc::CANCELLED);
                if (auto self = weak.lock())
                    try
                    {
                        boost::asio::post(
                            self->strand,
                            [self, job]
                            {
                                self->notify(job, rpc::CANCELLED);
                                job->child.cancel();
                                if (job->transaction)
                                    job->transaction();
                            }
                        );
                    }
                    catch (...)
                    {
                    }
            }
        );
    }
    void transact(
        const std::shared_ptr<Job>& job,
        LobbyAuthority::Mutation mutation,
        std::function<void(LobbyAuthority::CommitReply)> complete,
        bool tick = false
    )
    {
        const auto status = abandoned(job);
        if (status != rpc::OK)
        {
            retire(job, status);
            return;
        }
        auto checked = [job, mutation = std::move(mutation)](
                           Work& work,
                           Capture capture
                       ) mutable
        {
            if (job->cancelled.load() ||
                work.now >= job->context.deadline)
            {
                capture(std::unexpected(wire::ERR_TIMEOUT));
                return;
            }
            mutation(work, std::move(capture));
        };
        auto accepted = authority.submit(
            std::move(checked),
            job->context.deadline,
            guarded(
                job,
                [self = shared_from_this(),
                 job,
                 complete = std::move(complete)](auto result) mutable
                {
                    job->transaction = {};
                    if (!result)
                    {
                        self->retire(
                            job,
                            authorityError(result.error())
                        );
                        return;
                    }
                    const auto status = self->abandoned(job);
                    if (status != rpc::OK)
                    {
                        self->retire(job, status);
                        return;
                    }
                    complete(std::move(*result));
                }
            ),
            tick
        );
        if (!accepted)
        {
            retire(job, authorityError(accepted.error()));
            return;
        }
        job->transaction = std::move(*accepted);
    }
    void executeReply(
        const std::shared_ptr<Job>& job,
        LobbyAuthority::CommitReply committed
    )
    {
        arena::lobby::ExecuteReply reply;
        if (committed.reply)
            *reply.mutable_message() = std::move(*committed.reply);
        else
            *reply.mutable_message() = failure(
                job->execute.message().request_id(),
                committed.reply.error()
            );
        reply.mutable_message()->set_request_id(
            job->execute.message().request_id()
        );
        reply.set_committed_revision(committed.committed->revision);
        const auto index = transport(
            committed.committed->meta,
            job->execute.gateway_instance(),
            job->execute.connection_id()
        );
        if (index >= 0)
        {
            const auto& binding = committed.committed->meta
                                      .transports(index);
            const auto* entry = player(
                *committed.committed,
                binding.player_id()
            );
            if (entry && entry->snapshot.epoch == binding.epoch())
            {
                auto* projection = reply.mutable_session();
                projection->set_player_id(binding.player_id());
                projection->set_epoch(binding.epoch());
                projection->set_expires_at_unix(
                    binding.token_expires_at_unix()
                );
                projection->set_session_id(
                    entry->snapshot.sessionId.value()
                );
            }
        }
        done(job, std::move(reply));
    }

    void reserveLogin(const std::shared_ptr<Job>&);
    void authenticate(
        const std::shared_ptr<Job>&, std::string currentToken = {}
    );
    void afterIdentity(const std::shared_ptr<Job>&);
    void installLogin(const std::shared_ptr<Job>&);
    void prepareMatch(const std::shared_ptr<Job>&);
    void reject(const std::shared_ptr<Job>&, wire::ErrorCode);
    void mutate(const std::shared_ptr<Job>&);
    void poll();
    void probes();
    void reserveIds();
    void settlements();
    void sendSettlement(std::uint64_t, bool);
    void recordSettlement(
        std::uint64_t, rpc::Status, bool acknowledged = false
    );
    void beginStop()
    {
        if (stopApplied)
            return;
        stopApplied = true;
        std::vector<std::shared_ptr<Job>> pending;
        {
            std::lock_guard lock(mutex);
            for (const auto& [id, job] : jobs)
            {
                (void)id;
                pending.push_back(job);
            }
        }
        for (auto& job : pending)
        {
            job->cancelled.store(rpc::CANCELLED);
            notify(job, rpc::CANCELLED);
            try
            {
                job->child.cancel();
            }
            catch (...)
            {
            }
            try
            {
                if (job->transaction)
                    job->transaction();
            }
            catch (...)
            {
            }
        }
        try
        {
            auth.stop();
        }
        catch (...)
        {
        }
        try
        {
            storage.stop();
        }
        catch (...)
        {
        }
        authority.stop();
    }
    void stop()
    {
        if (stopping.exchange(true))
            return;
        try
        {
            boost::asio::post(
                strand,
                [self = shared_from_this()] { self->beginStop(); }
            );
        }
        catch (...)
        {
        } // The already armed owner poll observes stopping.
    }
    /// All mutable orchestration below is strand-owned unless atomic
    /// or mutex-protected.
    boost::asio::strand<boost::asio::any_io_executor> strand;
    Config config;
    AuthClient auth;
    StorageClient storage;
    LobbyAuthority authority;
    boost::asio::steady_timer timer;
    std::shared_ptr<State> keepAlive;
    std::mutex mutex;
    std::map<std::uint64_t, std::shared_ptr<Job>> jobs;
    std::uint64_t nextJob{0};
    std::atomic<std::size_t> active{0}, background{0};
    std::atomic<bool> stopping{false}, stopped{false}, fatal{false},
        authReady{false}, storageReady{false};
    bool stopApplied{false};
    bool authProbe{false}, storageProbe{false}, idPending{false},
        settlementPending{false};
    Clock::time_point nextAuth{}, nextStorage{}, nextIds{};
    std::uint32_t idFailures{0};
};
void LobbyService::State::reserveLogin(
    const std::shared_ptr<Job>& job
)
{
    transact(
        job,
        [](Work& work, Capture capture)
        {
            if (work.meta.reserved_epoch() == UINT64_MAX)
            {
                capture(std::unexpected(wire::ERR_UNAVAILABLE));
                return;
            }
            work.meta.set_reserved_epoch(
                work.meta.reserved_epoch() + 1
            );
            auto reply = internalReply();
            reply.mutable_pong()->set_nonce(
                work.meta.reserved_epoch()
            );
            capture(std::move(reply));
        },
        [self = shared_from_this(), job](auto committed)
        {
            if (!committed.reply)
            {
                self->executeReply(job, std::move(committed));
                return;
            }
            job->reserved = committed.reply->pong().nonce();
            self->authenticate(job);
        }
    );
}
void LobbyService::State::authenticate(
    const std::shared_ptr<Job>& job, std::string currentToken
)
{
    const auto status = abandoned(job);
    if (status != rpc::OK)
    {
        arena::security::eraseSecret(currentToken);
        retire(job, status);
        return;
    }
    const bool renewing = !currentToken.empty();
    const auto expectedPlayer = job->authenticated.player_id();
    const auto& login = job->execute.message().login_request();
    arena::auth::AuthenticateRequest request;
    request.set_proposed_epoch(job->reserved);
    request.set_credential_kind(
        renewing || login.credential_kind() == wire::CREDENTIAL_JWT
            ? arena::auth::AuthenticateRequest::JWT
            : arena::auth::AuthenticateRequest::PASSWORD
    );
    if (renewing)
        request.set_credential(std::move(currentToken));
    else
    {
        request.set_account(login.account());
        request.set_credential(login.credential());
    }
    auto accepted = auth.authenticate(
        std::move(request),
        job->context,
        guarded(
            job,
            [self = shared_from_this(),
             job,
             renewing,
             expectedPlayer](auto result)
            {
                struct Cleanup
                {
                    AuthClient::Result& value;
                    ~Cleanup()
                    {
                        if (value)
                            arena::security::eraseSecret(
                                *value->mutable_session_token()
                            );
                    }
                } cleanup{result};
                job->child = {};
                const auto status = self->abandoned(job);
                if (status != rpc::OK)
                {
                    self->retire(job, status);
                    return;
                }
                if (!result)
                {
                    self->retire(job, result.error());
                    return;
                }
                if (renewing && result->player_id() != expectedPlayer)
                {
                    self->retire(job, rpc::INTERNAL);
                    return;
                }
                arena::security::eraseSecret(
                    *job->authenticated.mutable_session_token()
                );
                job->authenticated = std::move(*result);
                if (renewing)
                {
                    self->installLogin(job);
                    return;
                }
                job->proofEpoch = job->authenticated.previous_epoch();
                job->proofExpiry = job->authenticated
                                       .previous_expires_at_unix();
                arena::security::eraseSecret(
                    *job->execute.mutable_message()
                         ->mutable_login_request()
                         ->mutable_credential()
                );
                self->afterIdentity(job);
            }
        )
    );
    if (!accepted)
    {
        retire(job, accepted.error());
        return;
    }
    job->child = std::move(*accepted);
}
void LobbyService::State::afterIdentity(
    const std::shared_ptr<Job>& job
)
{
    arena::storage::PlayerGetRequest request;
    request.set_player_id(job->authenticated.player_id());
    auto accepted = storage.player(
        std::move(request),
        job->context,
        guarded(
            job,
            [self = shared_from_this(), job](auto result)
            {
                job->child = {};
                const auto status = self->abandoned(job);
                if (status != rpc::OK)
                {
                    self->retire(job, status);
                    return;
                }
                if (!result)
                {
                    self->retire(job, result.error());
                    return;
                }
                const auto& login = job->execute.message()
                                        .login_request();
                job->receiptKey =
                    "login/" +
                    std::to_string(job->authenticated.player_id()) +
                    "/" +
                    (login.operation_key().empty()
                         ? job->execute.gateway_instance() + "/" +
                               std::to_string(
                                   job->execute.connection_id()
                               ) +
                               "/" +
                               std::to_string(
                                   job->execute.message().request_id()
                               )
                         : login.operation_key());
                const auto snapshot = self->authority.snapshot();
                const auto index = receipt(
                    snapshot->meta,
                    job->receiptKey
                );
                if (index < 0)
                {
                    self->installLogin(job);
                    return;
                }
                const auto& saved = snapshot->meta.receipts(index);
                if (saved.expires_nanos() <=
                    nanos(self->config.authority.now()))
                {
                    // The candidate rechecks its frozen cutoff.
                    // Expired receipts never supply a bearer token
                    // for a second Auth call.
                    self->installLogin(job);
                    return;
                }
                const auto* current = player(
                    *snapshot,
                    job->authenticated.player_id()
                );
                if (saved.credential_kind() !=
                        static_cast<std::uint32_t>(
                            login.credential_kind()
                        ) ||
                    saved.account() != login.account() ||
                    saved.prior_epoch() != job->proofEpoch ||
                    saved.prior_expires_at_unix() !=
                        job->proofExpiry ||
                    !current ||
                    current->snapshot.epoch != saved.epoch() ||
                    current->snapshot.sessionId.value() !=
                        saved.session_id())
                {
                    self->reject(job, wire::ERR_UNAUTHENTICATED);
                    return;
                }
                const auto physical = transport(
                    snapshot->meta,
                    job->execute.gateway_instance(),
                    job->execute.connection_id()
                );
                if (physical >= 0 &&
                    snapshot->meta.transports(physical).player_id() ==
                        saved.player_id() &&
                    snapshot->meta.transports(physical).epoch() ==
                        saved.epoch())
                {
                    self->installLogin(job);
                    return;
                }
                Envelope original;
                if (!original.ParseFromString(saved.reply()) ||
                    !original.has_login_response() ||
                    !original.login_response().accepted() ||
                    original.login_response().player_id() !=
                        saved.player_id() ||
                    original.login_response().session_id() !=
                        saved.session_id() ||
                    original.login_response().session_epoch() !=
                        saved.epoch() ||
                    original.login_response()
                        .session_token()
                        .empty() ||
                    original.login_response().session_token().size() >
                        4096)
                {
                    self->retire(job, rpc::INTERNAL);
                    return;
                }
                job->receiptResume = true;
                job->receiptEpoch = saved.epoch();
                job->receiptSession = saved.session_id();
                auto token = std::move(
                    *original.mutable_login_response()
                         ->mutable_session_token()
                );
                self->authenticate(job, std::move(token));
            }
        )
    );
    if (!accepted)
    {
        retire(job, accepted.error());
        return;
    }
    job->child = std::move(*accepted);
}
void LobbyService::State::installLogin(
    const std::shared_ptr<Job>& job
)
{
    const auto lease = config.transportLease;
    const auto retention = config.receiptRetention;
    const auto capacity = config.authority.maxReceipts;
    transact(
        job,
        [job, lease, retention, capacity](Work& work, Capture capture)
        {
            const auto& login = job->execute.message()
                                    .login_request();
            const auto id = job->authenticated.player_id();
            const auto savedIndex = receipt(
                work.meta,
                job->receiptKey
            );
            const auto current = work.online->snapshot(
                arena::domain::PlayerId{id}
            );
            if (savedIndex >= 0)
            {
                const auto& saved = work.meta.receipts(savedIndex);
                if (saved.expires_nanos() <= nanos(work.now))
                {
                    capture(
                        std::unexpected(wire::ERR_UNAUTHENTICATED)
                    );
                    return;
                }
                if (saved.player_id() != id ||
                    saved.credential_kind() !=
                        static_cast<std::uint32_t>(
                            login.credential_kind()
                        ) ||
                    saved.account() != login.account() ||
                    saved.prior_epoch() != job->proofEpoch ||
                    saved.prior_expires_at_unix() !=
                        job->proofExpiry ||
                    !current || current->epoch != saved.epoch() ||
                    current->sessionId.value() != saved.session_id())
                {
                    capture(
                        std::unexpected(wire::ERR_UNAUTHENTICATED)
                    );
                    return;
                }
                const auto physical = transport(
                    work.meta,
                    job->execute.gateway_instance(),
                    job->execute.connection_id()
                );
                if (!job->receiptResume && physical >= 0 &&
                    work.meta.transports(physical).player_id() ==
                        id &&
                    work.meta.transports(physical).epoch() ==
                        saved.epoch())
                {
                    Envelope previous;
                    if (!previous.ParseFromString(saved.reply()) ||
                        !previous.has_login_response() ||
                        !previous.login_response().accepted() ||
                        previous.login_response().player_id() != id ||
                        previous.login_response().session_epoch() !=
                            saved.epoch() ||
                        previous.login_response().session_id() !=
                            saved.session_id())
                    {
                        capture(std::unexpected(wire::ERR_INTERNAL));
                        return;
                    }
                    work.meta.mutable_transports(physical)
                        ->set_lease_deadline_nanos(
                            expiry(work.now, lease)
                        );
                    previous.set_request_id(
                        job->execute.message().request_id()
                    );
                    capture(std::move(previous));
                    return;
                }
                if (!job->receiptResume ||
                    saved.epoch() != job->receiptEpoch ||
                    saved.session_id() != job->receiptSession ||
                    job->authenticated.previous_epoch() !=
                        current->epoch)
                {
                    capture(
                        std::unexpected(wire::ERR_UNAUTHENTICATED)
                    );
                    return;
                }
            }
            else if (job->receiptResume)
            {
                capture(std::unexpected(wire::ERR_UNAUTHENTICATED));
                return;
            }
            else if (
                static_cast<std::size_t>(work.meta.receipts_size()) >=
                capacity
            )
            {
                capture(std::unexpected(wire::ERR_TOO_MANY_REQUESTS));
                return;
            }
            if (work.meta.transport_high_water() == UINT64_MAX)
            {
                capture(std::unexpected(wire::ERR_UNAVAILABLE));
                return;
            }
            const auto handle = work.meta.transport_high_water() + 1;
            std::optional<service::auth::TokenClaims> resume;
            if (job->receiptResume ||
                login.credential_kind() == wire::CREDENTIAL_JWT)
                resume = service::auth::TokenClaims{
                    arena::domain::PlayerId{id},
                    job->authenticated.previous_epoch(),
                    job->authenticated.previous_expires_at_unix()
                };
            service::auth::IssuedToken signedCandidate{
                job->authenticated.session_token(),
                {arena::domain::PlayerId{id},
                 job->authenticated.epoch(),
                 job->authenticated.expires_at_unix()}
            };
            auto installed = work.online->installVerified(
                std::move(signedCandidate),
                handle,
                resume,
                work.leaseClock(),
                work.kickFactory()(SessionBinding{
                    arena::domain::PlayerId{id},
                    handle,
                    job->authenticated.epoch()
                })
            );
            if (!installed)
            {
                capture(
                    std::unexpected(
                        installed.error() ==
                                service::auth::AuthError::Unavailable
                            ? wire::ERR_UNAVAILABLE
                            : wire::ERR_UNAUTHENTICATED
                    )
                );
                return;
            }
            work.meta.set_transport_high_water(handle);
            for (int i = work.meta.transports_size(); i-- > 0;)
            {
                const auto previous = work.meta.transports(i);
                const bool
                    physical = previous.gateway_instance() ==
                                   job->execute.gateway_instance() &&
                               previous.connection_id() ==
                                   job->execute.connection_id();
                if (previous.player_id() == id || physical)
                {
                    if (physical && previous.player_id() != id)
                        work.online->detach(
                            {arena::domain::PlayerId{
                                 previous.player_id()
                             },
                             previous.handle(),
                             previous.epoch()}
                        );
                    work.meta.mutable_transports()->DeleteSubrange(
                        i,
                        1
                    );
                }
            }
            auto* binding = work.meta.add_transports();
            binding->set_gateway_instance(
                job->execute.gateway_instance()
            );
            binding->set_connection_id(job->execute.connection_id());
            binding->set_handle(handle);
            binding->set_player_id(id);
            binding->set_epoch(installed->snapshot.epoch);
            binding->set_token_expires_at_unix(
                job->authenticated.expires_at_unix()
            );
            binding->set_lease_deadline_nanos(
                expiry(work.now, lease)
            );
            Envelope reply;
            reply.set_request_id(job->execute.message().request_id());
            reply.set_msg_id(wire::MSG_LOGIN_RESPONSE);
            auto* accepted = reply.mutable_login_response();
            accepted->set_accepted(true);
            accepted->set_session_token(installed->token);
            arena::security::eraseSecret(installed->token);
            accepted->set_player_id(id);
            accepted->set_session_epoch(installed->snapshot.epoch);
            accepted->set_session_id(
                installed->snapshot.sessionId.value()
            );
            accepted->set_room_id(
                installed->snapshot.roomId
                    ? installed->snapshot.roomId->value()
                    : 0
            );
            accepted->set_resumed(installed->resumed);
            auto* saved = savedIndex >= 0
                              ? work.meta.mutable_receipts(savedIndex)
                              : work.meta.add_receipts();
            if (savedIndex < 0)
                saved->set_expires_nanos(expiry(work.now, retention));
            saved->set_key(job->receiptKey);
            saved->set_method(wire::MSG_LOGIN_REQUEST);
            saved->set_request_fingerprint("login-v1");
            saved->set_player_id(id);
            saved->set_session_id(
                installed->snapshot.sessionId.value()
            );
            saved->set_epoch(installed->snapshot.epoch);
            saved->set_prior_epoch(job->proofEpoch);
            saved->set_prior_expires_at_unix(job->proofExpiry);
            saved->set_credential_kind(
                static_cast<std::uint32_t>(login.credential_kind())
            );
            saved->set_account(login.account());
            reply.SerializeToString(saved->mutable_reply());
            capture(std::move(reply));
        },
        [self = shared_from_this(), job](auto committed)
        { self->executeReply(job, std::move(committed)); }
    );
}
void LobbyService::State::prepareMatch(
    const std::shared_ptr<Job>& job
)
{
    const auto snapshot = authority.snapshot();
    const auto& request = job->execute;
    const auto index = transport(
        snapshot->meta,
        request.gateway_instance(),
        request.connection_id()
    );
    if (index < 0 ||
        snapshot->meta.transports(index).player_id() !=
            request.player_id() ||
        snapshot->meta.transports(index).epoch() !=
            request.session_epoch())
    {
        reject(job, wire::ERR_UNAUTHENTICATED);
        return;
    }
    if (!request.message().match_join_request().join_key())
    {
        reject(job, wire::ERR_INVALID_REQUEST);
        return;
    }
    const auto* entry = player(*snapshot, request.player_id());
    if (!entry)
    {
        reject(job, wire::ERR_UNAUTHENTICATED);
        return;
    }
    const auto
        key = request.message().match_join_request().join_key();
    const auto& tickets = snapshot->core.queue.entries;
    const bool replay = std::any_of(
        tickets.begin(),
        tickets.end(),
        [&](const auto& ticket)
        {
            return ticket.playerId.value() == request.player_id() &&
                   ticket.sessionId == entry->snapshot.sessionId &&
                   (ticket.joinKey == key || ticket.active);
        }
    );
    if (replay)
    {
        mutate(job);
        return;
    }
    arena::storage::PlayerGetRequest lookup;
    lookup.set_player_id(request.player_id());
    auto accepted = storage.player(
        std::move(lookup),
        job->context,
        guarded(
            job,
            [self = shared_from_this(), job](auto result)
            {
                job->child = {};
                const auto status = self->abandoned(job);
                if (status != rpc::OK)
                {
                    self->retire(job, status);
                    return;
                }
                if (!result)
                {
                    self->retire(job, result.error());
                    return;
                }
                job->mmr = result->player().mmr();
                job->hasMmr = true;
                self->mutate(job);
            }
        )
    );
    if (!accepted)
    {
        retire(job, accepted.error());
        return;
    }
    job->child = std::move(*accepted);
}
void LobbyService::State::reject(
    const std::shared_ptr<Job>& job, wire::ErrorCode code
)
{
    transact(
        job,
        [code](Work&, Capture capture)
        { capture(std::unexpected(code)); },
        [self = shared_from_this(), job](auto committed)
        { self->executeReply(job, std::move(committed)); }
    );
}
void LobbyService::State::mutate(const std::shared_ptr<Job>& job)
{
    const auto lease = config.transportLease;
    const bool tick = job->kind == Job::Kind::Execute &&
                      static_cast<unsigned>(
                          job->execute.message().payload_case()
                      ) >= 50;
    transact(
        job,
        [job, lease](Work& work, Capture capture)
        {
            if (job->kind == Job::Kind::Detach)
            {
                const auto& request = job->detach;
                const auto index = transport(
                    work.meta,
                    request.gateway_instance(),
                    request.connection_id()
                );
                if (index >= 0)
                {
                    const auto old = work.meta.transports(index);
                    if (old.player_id() == request.player_id() &&
                        old.epoch() == request.session_epoch())
                    {
                        work.online->detach(
                            {arena::domain::PlayerId{old.player_id()},
                             old.handle(),
                             old.epoch()}
                        );
                        work.meta.mutable_transports()
                            ->DeleteSubrange(index, 1);
                    }
                }
                capture(internalReply());
                return;
            }
            if (job->kind == Job::Kind::Heartbeat)
            {
                job->revoked.clear();
                for (const auto id : job->heartbeat.connection_ids())
                {
                    const auto index = transport(
                        work.meta,
                        job->heartbeat.gateway_instance(),
                        id
                    );
                    if (index < 0)
                        job->revoked.push_back(id);
                    else
                        work.meta.mutable_transports(index)
                            ->set_lease_deadline_nanos(
                                expiry(work.now, lease)
                            );
                }
                capture(internalReply());
                return;
            }
            const auto& request = job->execute;
            const auto index = transport(
                work.meta,
                request.gateway_instance(),
                request.connection_id()
            );
            if (index < 0 ||
                work.meta.transports(index).player_id() !=
                    request.player_id() ||
                work.meta.transports(index).epoch() !=
                    request.session_epoch())
            {
                capture(std::unexpected(wire::ERR_UNAUTHENTICATED));
                return;
            }
            const auto binding = work.meta.transports(index);
            const SessionBinding session{
                arena::domain::PlayerId{binding.player_id()},
                binding.handle(),
                binding.epoch()
            };
            auto authorized = work.online->state(session);
            if (!authorized)
            {
                capture(
                    std::unexpected(routeError(authorized.error()))
                );
                return;
            }
            work.meta.mutable_transports(index)
                ->set_lease_deadline_nanos(expiry(work.now, lease));
            const auto logical = authorized->session.sessionId;
            const auto& message = request.message();
            Envelope reply;
            reply.set_request_id(message.request_id());
            switch (message.payload_case())
            {
            case Envelope::kPing:
                reply.set_msg_id(wire::MSG_PONG);
                reply.mutable_pong()->set_nonce(
                    message.ping().nonce()
                );
                capture(std::move(reply));
                return;
            case Envelope::kSessionStateRequest:
                reply.set_msg_id(wire::MSG_SESSION_STATE_RESPONSE);
                encodeState(
                    *authorized,
                    *reply.mutable_session_state_response()
                );
                capture(std::move(reply));
                return;
            case Envelope::kRoomJoinRequest:
            {
                if (!message.room_join_request().room_id())
                {
                    capture(
                        std::unexpected(wire::ERR_INVALID_REQUEST)
                    );
                    return;
                }
                auto result = work.online->joinRoom(
                    session,
                    arena::domain::RoomId{
                        message.room_join_request().room_id()
                    }
                );
                if (!result)
                {
                    capture(
                        std::unexpected(routeError(result.error()))
                    );
                    return;
                }
                reply.set_msg_id(wire::MSG_ROOM_JOIN_RESPONSE);
                encodeState(
                    *result,
                    *reply.mutable_room_join_response()
                         ->mutable_state()
                );
                capture(std::move(reply));
                return;
            }
            case Envelope::kRoomLeaveRequest:
            {
                auto result = work.online->leaveRoom(session);
                if (!result)
                {
                    capture(
                        std::unexpected(routeError(result.error()))
                    );
                    return;
                }
                reply.set_msg_id(wire::MSG_ROOM_LEAVE_RESPONSE);
                encodeState(
                    *result,
                    *reply.mutable_room_leave_response()
                         ->mutable_state()
                );
                capture(std::move(reply));
                return;
            }
            case Envelope::kMatchJoinRequest:
            {
                const auto key = message.match_join_request()
                                     .join_key();
                if (!key)
                {
                    capture(
                        std::unexpected(wire::ERR_INVALID_REQUEST)
                    );
                    return;
                }
                const auto replay = work.queue->replay(
                    session.playerId,
                    logical,
                    key,
                    work.now
                );
                if (replay)
                {
                    reply.set_msg_id(wire::MSG_MATCH_JOIN_RESPONSE);
                    encodeTicket(
                        *replay,
                        *reply.mutable_match_join_response()
                             ->mutable_state()
                    );
                    capture(std::move(reply));
                    return;
                }
                if (!job->hasMmr)
                {
                    capture(std::unexpected(wire::ERR_UNAVAILABLE));
                    return;
                }
                auto result = work.queue->join(
                    session.playerId,
                    logical,
                    key,
                    job->mmr,
                    work.now
                );
                if (!result)
                {
                    capture(
                        std::unexpected(queueError(result.error()))
                    );
                    return;
                }
                reply.set_msg_id(wire::MSG_MATCH_JOIN_RESPONSE);
                encodeTicket(
                    *result,
                    *reply.mutable_match_join_response()
                         ->mutable_state()
                );
                capture(std::move(reply));
                return;
            }
            case Envelope::kMatchCancelRequest:
            case Envelope::kMatchStatusRequest:
            {
                const bool cancel = message
                                        .has_match_cancel_request();
                const auto id = cancel
                                    ? message.match_cancel_request()
                                          .ticket_id()
                                    : message.match_status_request()
                                          .ticket_id();
                if (!id)
                {
                    capture(
                        std::unexpected(wire::ERR_INVALID_REQUEST)
                    );
                    return;
                }
                auto result = cancel
                                  ? work.queue->cancel(
                                        session.playerId,
                                        logical,
                                        arena::domain::TicketId{id},
                                        work.now
                                    )
                                  : work.queue->status(
                                        session.playerId,
                                        logical,
                                        arena::domain::TicketId{id},
                                        work.now
                                    );
                if (!result)
                {
                    capture(
                        std::unexpected(queueError(result.error()))
                    );
                    return;
                }
                reply.set_msg_id(
                    cancel ? wire::MSG_MATCH_CANCEL_RESPONSE
                           : wire::MSG_MATCH_STATUS_RESPONSE
                );
                encodeTicket(
                    *result,
                    cancel ? *reply.mutable_match_cancel_response()
                                  ->mutable_state()
                           : *reply.mutable_match_status_response()
                                  ->mutable_state()
                );
                capture(std::move(reply));
                return;
            }
            default:
                break;
            }
            const auto limit = work.rooms->positionLimit();
            const auto requestId = message.request_id();
            bool admitted = false;
            if (message.has_room_status_request())
                admitted = work.rooms->status(
                    session,
                    logical,
                    [capture, limit, requestId](auto result)
                    {
                        if (!result)
                        {
                            capture(std::unexpected(result.error()));
                            return;
                        }
                        Envelope reply;
                        reply.set_request_id(requestId);
                        reply.set_msg_id(
                            wire::MSG_ROOM_STATUS_RESPONSE
                        );
                        encodeRoomStatus(
                            *result,
                            limit,
                            *reply.mutable_room_status_response()
                                 ->mutable_state()
                        );
                        capture(std::move(reply));
                    }
                );
            else if (message.has_settlement_request())
            {
                if (!message.settlement_request().match_id())
                {
                    capture(
                        std::unexpected(wire::ERR_INVALID_REQUEST)
                    );
                    return;
                }
                admitted = work.rooms->settlement(
                    session,
                    logical,
                    message.settlement_request().match_id(),
                    [capture, requestId](auto result)
                    {
                        if (!result)
                        {
                            capture(std::unexpected(result.error()));
                            return;
                        }
                        Envelope reply;
                        reply.set_request_id(requestId);
                        reply.set_msg_id(
                            wire::MSG_SETTLEMENT_RESPONSE
                        );
                        encodeSettlement(
                            *result,
                            *reply.mutable_settlement_response()
                                 ->mutable_state()
                        );
                        capture(std::move(reply));
                    }
                );
            }
            else
            {
                if (!authorized->session.roomId)
                {
                    capture(
                        std::unexpected(wire::ERR_BUSINESS_FAILURE)
                    );
                    return;
                }
                const auto room = *authorized->session.roomId;
                if (message.has_room_snapshot_request())
                    admitted = work.rooms->snapshot(
                        session,
                        logical,
                        room,
                        [capture, limit, requestId](auto result)
                        {
                            if (!result)
                            {
                                capture(
                                    std::unexpected(result.error())
                                );
                                return;
                            }
                            Envelope reply;
                            reply.set_request_id(requestId);
                            reply.set_msg_id(
                                wire::MSG_ROOM_SNAPSHOT_RESPONSE
                            );
                            encodeSnapshot(
                                *result,
                                limit,
                                *reply
                                     .mutable_room_snapshot_response()
                                     ->mutable_state()
                            );
                            capture(std::move(reply));
                        }
                    );
                else if (message.has_room_ready_request())
                {
                    const auto& value = message.room_ready_request();
                    if (!value.actor_incarnation() ||
                        !value.membership_epoch())
                    {
                        capture(
                            std::unexpected(wire::ERR_INVALID_REQUEST)
                        );
                        return;
                    }
                    admitted = work.rooms->ready(
                        session,
                        logical,
                        room,
                        value.actor_incarnation(),
                        value.membership_epoch(),
                        value.ready(),
                        [capture, limit, requestId](auto result)
                        {
                            if (!result)
                            {
                                capture(
                                    std::unexpected(result.error())
                                );
                                return;
                            }
                            Envelope reply;
                            reply.set_request_id(requestId);
                            reply.set_msg_id(
                                wire::MSG_ROOM_READY_RESPONSE
                            );
                            encodeRoomStatus(
                                *result,
                                limit,
                                *reply.mutable_room_ready_response()
                                     ->mutable_state()
                            );
                            capture(std::move(reply));
                        }
                    );
                }
                else if (message.has_room_input_request())
                {
                    const auto& value = message.room_input_request();
                    if (!value.actor_incarnation() ||
                        !value.membership_epoch() || !value.seq() ||
                        value.dx() < -1 || value.dx() > 1 ||
                        value.dy() < -1 || value.dy() > 1)
                    {
                        capture(
                            std::unexpected(wire::ERR_INVALID_REQUEST)
                        );
                        return;
                    }
                    admitted = work.rooms->input(
                        session,
                        logical,
                        room,
                        value.actor_incarnation(),
                        value.membership_epoch(),
                        value.seq(),
                        value.dx(),
                        value.dy(),
                        [capture, limit, requestId](auto result)
                        {
                            if (!result)
                            {
                                capture(
                                    std::unexpected(result.error())
                                );
                                return;
                            }
                            Envelope reply;
                            reply.set_request_id(requestId);
                            reply.set_msg_id(
                                wire::MSG_ROOM_INPUT_RESPONSE
                            );
                            encodeSnapshot(
                                result->snapshot,
                                limit,
                                *reply.mutable_room_input_response()
                                     ->mutable_state()
                            );
                            reply.mutable_room_input_response()
                                ->set_result(
                                    wireStatus(result->status)
                                );
                            capture(std::move(reply));
                        }
                    );
                }
            }
            if (!admitted)
                capture(std::unexpected(wire::ERR_UNAVAILABLE));
        },
        [self = shared_from_this(), job](auto committed)
        {
            if (job->kind == Job::Kind::Execute)
            {
                self->executeReply(job, std::move(committed));
                return;
            }
            if (job->kind == Job::Kind::Detach)
            {
                arena::lobby::DetachReply reply;
                reply.set_committed_revision(
                    committed.committed->revision
                );
                self->done(job, std::move(reply));
            }
            else
            {
                arena::lobby::HeartbeatReply reply;
                reply.set_committed_revision(
                    committed.committed->revision
                );
                for (const auto id : job->revoked)
                    reply.add_revoked_connection_ids(id);
                self->done(job, std::move(reply));
            }
        },
        tick
    );
}
void LobbyService::State::poll()
{
    try
    {
        if (stopping.load())
            beginStop();
        std::vector<std::shared_ptr<Job>> pending;
        {
            std::lock_guard lock(mutex);
            for (const auto& [id, job] : jobs)
            {
                (void)id;
                pending.push_back(job);
            }
        }
        for (auto& job : pending)
        {
            const auto status = abandoned(job);
            if (status != rpc::OK)
            {
                job->cancelled.store(status);
                notify(job, status);
                job->child.cancel();
                if (job->transaction)
                    job->transaction();
            }
        }
        if (!stopping.load())
        {
            probes();
            if (authority.stats().ready)
            {
                reserveIds();
                settlements();
            }
        }
        else if (
            !auth.inflight() && !storage.inflight() &&
            authority.stats().stopped
        )
        {
            if (fatal.load())
            {
                for (const auto& job : pending)
                    notify(job, rpc::INTERNAL);
                std::lock_guard lock(mutex);
                jobs.clear();
                active = 0;
                background = 0;
            }
            if (!active.load() && !background.load())
            {
                stopped = true;
                keepAlive.reset();
                return;
            }
        }
    }
    catch (...)
    {
        fatal = true;
        stopping = true;
        try
        {
            beginStop();
        }
        catch (...)
        {
            authority.stop();
        }
    }
    timer.expires_after(25ms);
    timer.async_wait(
        [self = shared_from_this()](auto error)
        {
            if (!error)
                self->poll();
        }
    );
}
void LobbyService::State::probes()
{
    const auto now = Clock::now();
    if (!authProbe && now >= nextAuth)
    {
        authProbe = true;
        ++background;
        auto accepted = auth.probe(
            {now + 300ms, "lobby-auth-probe", {"lobby"}},
            backgroundGuard(
                [self = shared_from_this()](auto result)
                {
                    self->authReady = result.has_value();
                    self->authProbe = false;
                    --self->background;
                    self->nextAuth = Clock::now() + 250ms;
                }
            )
        );
        if (!accepted)
        {
            authReady = false;
            authProbe = false;
            --background;
            nextAuth = now + 250ms;
        }
    }
    if (!storageProbe && now >= nextStorage)
    {
        storageProbe = true;
        ++background;
        auto accepted = storage.probe(
            {now + 300ms, "lobby-storage-probe", {"lobby"}},
            backgroundGuard(
                [self = shared_from_this()](auto result)
                {
                    self->storageReady = result.has_value();
                    self->storageProbe = false;
                    --self->background;
                    self->nextStorage = Clock::now() + 250ms;
                }
            )
        );
        if (!accepted)
        {
            storageReady = false;
            storageProbe = false;
            --background;
            nextStorage = now + 250ms;
        }
    }
}
void LobbyService::State::reserveIds()
{
    const auto snapshot = authority.snapshot();
    const auto& values = snapshot->meta;
    if (idPending || !storageReady.load() || Clock::now() < nextIds ||
        (values.ticket_next() &&
         values.ticket_next() <= values.ticket_last()))
        return;
    idPending = true;
    ++background;
    arena::storage::AllocateIdsRequest request;
    request.set_count(256);
    auto finish = [self = shared_from_this()](bool success)
    {
        self->idPending = false;
        --self->background;
        if (success)
            self->idFailures = 0;
        else
            self->idFailures = std::min<std::uint32_t>(
                self->idFailures + 1,
                8
            );
        const auto delay = success ? 100u
                                   : std::min(
                                         5000u,
                                         100u << self->idFailures
                                     );
        self->nextIds = Clock::now() +
                        std::chrono::milliseconds(
                            delay + (self->idFailures * 37u) % 97u
                        );
    };
    auto accepted = storage.allocateIds(
        std::move(request),
        {Clock::now() + 1000ms, "lobby-id-block", {"lobby"}},
        backgroundGuard(
            [self = shared_from_this(), finish](auto result)
            {
                if (!result || self->stopping.load())
                {
                    finish(false);
                    return;
                }
                const auto first = result->first(),
                           last = result->last();
                auto accepted = self->authority.submit(
                    [first, last](Work& work, Capture capture)
                    {
                        if (work.meta.ticket_next() &&
                            work.meta.ticket_next() <=
                                work.meta.ticket_last())
                        {
                            capture(internalReply());
                            return;
                        }
                        const auto highWater = std::max(
                            work.meta.ticket_last(),
                            work.queue->checkpoint().lastDurableId
                        );
                        if (first <= highWater)
                        {
                            capture(
                                std::unexpected(
                                    wire::ERR_BUSINESS_FAILURE
                                )
                            );
                            return;
                        }
                        work.meta.set_ticket_next(first);
                        work.meta.set_ticket_last(last);
                        capture(internalReply());
                    },
                    Clock::now() + 1000ms,
                    [finish](auto committed)
                    {
                        finish(
                            committed && committed->reply.has_value()
                        );
                    }
                );
                if (!accepted)
                    finish(false);
            }
        )
    );
    if (!accepted)
        finish(false);
}
void LobbyService::State::settlements()
{
    if (settlementPending)
        return;
    const auto snapshot = authority.snapshot();
    const auto now = nanos(config.authority.now());
    for (const auto& match : snapshot->core.rooms.matches)
        if (match.phase == RoomPhase::Settling && match.result &&
            delivery(snapshot->meta, match.allocation.matchId) < 0)
        {
            settlementPending = true;
            ++background;
            const auto id = match.allocation.matchId;
            const auto limit = config.settlementMaxAttempts;
            const auto elapsed = config.settlementMaxElapsed;
            const auto base = config.retryBase;
            const auto maximum = config.retryMax;
            auto accepted = authority.submit(
                [id, limit, elapsed, base, maximum](
                    Work& work,
                    Capture capture
                )
                {
                    if (delivery(work.meta, id) >= 0)
                    {
                        capture(internalReply());
                        return;
                    }
                    const auto rooms = work.rooms->checkpoint();
                    if (!rooms)
                    {
                        capture(
                            std::unexpected(wire::ERR_UNAVAILABLE)
                        );
                        return;
                    }
                    const auto found = std::find_if(
                        rooms->matches.begin(),
                        rooms->matches.end(),
                        [id](const auto& m)
                        { return m.allocation.matchId == id; }
                    );
                    if (found == rooms->matches.end() ||
                        !found->result ||
                        found->phase != RoomPhase::Settling)
                    {
                        capture(
                            std::unexpected(
                                wire::ERR_BUSINESS_FAILURE
                            )
                        );
                        return;
                    }
                    auto* intent = work.meta.add_settlements();
                    intent->set_match_id(id);
                    intent->set_stable_idempotency_key(
                        "lobby-settlement/" + std::to_string(id)
                    );
                    intent->set_max_attempts(limit);
                    intent->set_first_attempt_nanos(nanos(work.now));
                    intent->set_deadline_nanos(
                        expiry(work.now, elapsed)
                    );
                    intent->set_next_attempt_nanos(nanos(work.now));
                    intent->set_retry_base_ms(
                        static_cast<std::uint32_t>(base.count())
                    );
                    intent->set_retry_max_ms(
                        static_cast<std::uint32_t>(maximum.count())
                    );
                    intent->set_phase(
                        meta::SettlementDelivery::PENDING
                    );
                    settlementRequest(*found).SerializeToString(
                        intent->mutable_request()
                    );
                    capture(internalReply());
                },
                Clock::now() + 1000ms,
                [self = shared_from_this()](auto)
                {
                    self->settlementPending = false;
                    --self->background;
                }
            );
            if (!accepted)
            {
                settlementPending = false;
                --background;
            }
            return;
        }
    for (const auto& intent : snapshot->meta.settlements())
    {
        if (intent.phase() ==
                meta::SettlementDelivery::ACKNOWLEDGED ||
            intent.phase() == meta::SettlementDelivery::BLOCKED ||
            intent.next_attempt_nanos() > now)
            continue;
        settlementPending = true;
        ++background;
        const auto id = intent.match_id();
        if (intent.attempts() >= intent.max_attempts() ||
            now >= intent.deadline_nanos())
        {
            recordSettlement(id, rpc::DEADLINE_EXCEEDED);
            return;
        }
        if (!storageReady.load())
        {
            settlementPending = false;
            --background;
            return;
        }
        const bool
            reconcile = intent.phase() ==
                            meta::SettlementDelivery::IN_FLIGHT ||
                        intent.phase() ==
                            meta::SettlementDelivery::OUTCOME_UNKNOWN;
        auto accepted = authority.submit(
            [id](Work& work, Capture capture)
            {
                const auto index = delivery(work.meta, id);
                if (index < 0)
                {
                    capture(
                        std::unexpected(wire::ERR_BUSINESS_FAILURE)
                    );
                    return;
                }
                auto* current = work.meta.mutable_settlements(index);
                if (current->phase() ==
                        meta::SettlementDelivery::BLOCKED ||
                    current->phase() ==
                        meta::SettlementDelivery::ACKNOWLEDGED ||
                    current->attempts() >= current->max_attempts() ||
                    nanos(work.now) >= current->deadline_nanos())
                {
                    capture(std::unexpected(wire::ERR_UNAVAILABLE));
                    return;
                }
                current->set_attempts(current->attempts() + 1);
                current->set_phase(
                    meta::SettlementDelivery::IN_FLIGHT
                );
                capture(internalReply());
            },
            Clock::now() + 1000ms,
            backgroundGuard(
                [self = shared_from_this(),
                 id,
                 reconcile](auto committed)
                {
                    if (!committed || !committed->reply ||
                        self->stopping.load())
                    {
                        self->settlementPending = false;
                        --self->background;
                        return;
                    }
                    self->sendSettlement(id, reconcile);
                }
            )
        );
        if (!accepted)
        {
            settlementPending = false;
            --background;
        }
        return;
    }
}
void LobbyService::State::sendSettlement(
    std::uint64_t id, bool reconcile
)
{
    const auto snapshot = authority.snapshot();
    const auto index = delivery(snapshot->meta, id);
    arena::storage::SaveResultRequest request;
    if (index < 0 || !request.ParseFromString(
                         snapshot->meta.settlements(index).request()
                     ))
    {
        recordSettlement(id, rpc::INTERNAL);
        return;
    }
    const auto context = Context{
        Clock::now() + 1000ms,
        "lobby-settlement",
        {"lobby"}
    };
    if (!reconcile)
    {
        auto accepted = storage.saveResult(
            std::move(request),
            context,
            snapshot->meta.settlements(index)
                .stable_idempotency_key(),
            backgroundGuard(
                [self = shared_from_this(), id](auto result)
                {
                    self->recordSettlement(
                        id,
                        result ? rpc::OK : result.error(),
                        result.has_value()
                    );
                }
            )
        );
        if (!accepted)
            recordSettlement(id, accepted.error());
        return;
    }
    arena::storage::MatchGetRequest lookup;
    lookup.set_match_id(id);
    auto accepted = storage.match(
        std::move(lookup),
        context,
        backgroundGuard(
            [self = shared_from_this(),
             id,
             expected = std::move(*request.mutable_match()),
             context](auto result) mutable
            {
                if (!result)
                {
                    self->recordSettlement(id, result.error());
                    return;
                }
                auto actual = result->match();
                actual.DiscardUnknownFields();
                expected.DiscardUnknownFields();
                if (!expected.created_unix_ms())
                    actual.set_created_unix_ms(0);
                if (!expected.started_unix_ms())
                    actual.set_started_unix_ms(0);
                if (!expected.finished_unix_ms())
                    actual.set_finished_unix_ms(0);
                if (actual.SerializeAsString() !=
                    expected.SerializeAsString())
                {
                    self->recordSettlement(id, rpc::CONFLICT);
                    return;
                }
                arena::storage::SettlementGetRequest receipt;
                receipt.set_settlement_id(id);
                auto accepted = self->storage.getSettlement(
                    std::move(receipt),
                    context,
                    self->backgroundGuard(
                        [self, id](auto result)
                        {
                            self->recordSettlement(
                                id,
                                result
                                    ? (result->settlement()
                                                   .match_id() == id
                                           ? rpc::OK
                                           : rpc::CONFLICT)
                                    : result.error(),
                                result &&
                                    result->settlement().match_id() ==
                                        id
                            );
                        }
                    )
                );
                if (!accepted)
                    self->recordSettlement(id, accepted.error());
            }
        )
    );
    if (!accepted)
        recordSettlement(id, accepted.error());
}
void LobbyService::State::recordSettlement(
    std::uint64_t id, rpc::Status status, bool acknowledged
)
{
    if (stopping.load())
    {
        settlementPending = false;
        --background;
        return;
    }
    const auto retention = config.authority.rooms.resultRetention;
    auto accepted = authority.submit(
        [id,
         status,
         acknowledged,
         retention](Work& work, Capture capture)
        {
            const auto index = delivery(work.meta, id);
            if (index < 0)
            {
                capture(std::unexpected(wire::ERR_INTERNAL));
                return;
            }
            auto* intent = work.meta.mutable_settlements(index);
            intent->set_last_status(
                static_cast<std::uint32_t>(status)
            );
            if (acknowledged)
            {
                if (!work.rooms->acknowledgeSettlement(id, true))
                {
                    capture(std::unexpected(wire::ERR_INTERNAL));
                    return;
                }
                intent->set_phase(
                    meta::SettlementDelivery::ACKNOWLEDGED
                );
                intent->set_retention_deadline_nanos(
                    expiry(work.now, retention)
                );
                capture(internalReply());
                return;
            }
            const bool exhausted = intent->attempts() >=
                                       intent->max_attempts() ||
                                   nanos(work.now) >=
                                       intent->deadline_nanos();
            if (exhausted || status == rpc::CONFLICT ||
                status == rpc::INVALID_ARGUMENT ||
                status == rpc::INTERNAL)
            {
                if (!work.rooms->acknowledgeSettlement(id, false))
                {
                    capture(std::unexpected(wire::ERR_INTERNAL));
                    return;
                }
                intent->set_phase(meta::SettlementDelivery::BLOCKED);
            }
            else
            {
                intent->set_phase(
                    status == rpc::NOT_FOUND
                        ? meta::SettlementDelivery::PENDING
                        : meta::SettlementDelivery::OUTCOME_UNKNOWN
                );
                std::uint64_t delay = intent->retry_base_ms();
                for (std::uint32_t i = 1;
                     i < intent->attempts() &&
                     delay < intent->retry_max_ms();
                     ++i)
                    delay = std::min<std::uint64_t>(
                        intent->retry_max_ms(),
                        delay * 2
                    );
                const auto jitter = (id * 6364136223846793005ULL +
                                     intent->attempts() *
                                         1442695040888963407ULL) %
                                    (delay / 4 + 1);
                const auto next = expiry(
                    work.now,
                    std::chrono::milliseconds(delay + jitter)
                );
                intent->set_next_attempt_nanos(
                    std::min(next, intent->deadline_nanos())
                );
            }
            capture(internalReply());
        },
        Clock::now() + 1000ms,
        [self = shared_from_this()](auto)
        {
            self->settlementPending = false;
            --self->background;
        }
    );
    if (!accepted)
    {
        settlementPending = false;
        --background;
    }
}

LobbyService::LobbyService(
    boost::asio::any_io_executor executor, Config config
)
    : m_state(
          std::make_shared<State>(
              std::move(executor), std::move(config)
          )
      )
{
    m_state->start();
}
LobbyService::~LobbyService() noexcept
{
    stop();
}
LobbyService::Admission LobbyService::execute(
    arena::lobby::ExecuteRequest request,
    Context context,
    Completion<arena::lobby::ExecuteReply> complete
)
{
    if (!complete || !executeShape(request))
    {
        if (request.has_message() &&
            request.message().has_login_request())
            arena::security::eraseSecret(
                *request.mutable_message()
                     ->mutable_login_request()
                     ->mutable_credential()
            );
        return std::unexpected(rpc::INVALID_ARGUMENT);
    }
    auto job = std::make_shared<State::Job>();
    job->kind = State::Job::Kind::Execute;
    job->execute = std::move(request);
    job->context = std::move(context);
    job->executeComplete = std::move(complete);
    return m_state->admit(std::move(job));
}
LobbyService::Admission LobbyService::detach(
    arena::lobby::DetachRequest request,
    Context context,
    Completion<arena::lobby::DetachReply> complete
)
{
    if (!complete || !gateway(request.gateway_instance()) ||
        !request.connection_id() || !request.player_id() ||
        !request.session_epoch())
        return std::unexpected(rpc::INVALID_ARGUMENT);
    auto job = std::make_shared<State::Job>();
    job->kind = State::Job::Kind::Detach;
    job->detach = std::move(request);
    job->context = std::move(context);
    job->detachComplete = std::move(complete);
    return m_state->admit(std::move(job));
}
LobbyService::Admission LobbyService::heartbeat(
    arena::lobby::HeartbeatRequest request,
    Context context,
    Completion<arena::lobby::HeartbeatReply> complete
)
{
    if (!complete || !gateway(request.gateway_instance()) ||
        request.connection_ids_size() > 512)
        return std::unexpected(rpc::INVALID_ARGUMENT);
    std::set<std::uint64_t> distinct;
    for (const auto id : request.connection_ids())
        if (!id || !distinct.insert(id).second)
            return std::unexpected(rpc::INVALID_ARGUMENT);
    auto job = std::make_shared<State::Job>();
    job->kind = State::Job::Kind::Heartbeat;
    job->heartbeat = std::move(request);
    job->context = std::move(context);
    job->heartbeatComplete = std::move(complete);
    return m_state->admit(std::move(job));
}
void LobbyService::stop() noexcept
{
    try
    {
        m_state->stop();
    }
    catch (...)
    {
    }
}
LobbyService::Stats LobbyService::stats() const noexcept
{
    Stats result;
    result.authority = m_state->authority.stats();
    result.initialized = result.authority.initialized;
    result.failed = result.authority.failed || m_state->fatal.load();
    result.stopping = m_state->stopping.load();
    result.stopped = m_state->stopped.load();
    result.authInflight = m_state->auth.inflight();
    result.storageInflight = m_state->storage.inflight();
    result.active = m_state->active.load() +
                    m_state->background.load();
    const auto snapshot = m_state->authority.snapshot();
    if (snapshot)
    {
        result.committedRevision = snapshot->revision;
        result.onlineRetained = snapshot->core.registry.entries
                                    .size();
        for (const auto& entry : snapshot->core.registry.entries)
            if (entry.snapshot.presence == Presence::Online &&
                entry.leaseValid)
                ++result.onlineActive;
        result.roomIndexes = snapshot->core.registry.rooms.size();
        result.transports = snapshot->meta.transports_size();
        result.receipts = snapshot->meta.receipts_size();
        result.queued = snapshot->core.queue.metrics.queued;
        result.matchedPairs = snapshot->core.queue.metrics
                                  .matchedPairs;
        result.roomActors = snapshot->core.rooms.metrics.actors;
        result.roomTicks = snapshot->core.rooms.metrics.ticks;
        result.settlementsCommitted = snapshot->core.rooms.metrics
                                          .persistenceCommitted;
        result.settlementsBlocked = snapshot->core.rooms.metrics
                                        .blockedSettlements;
        result.ready = !result.stopping && result.authority.ready &&
                       m_state->authReady.load() &&
                       m_state->storageReady.load() &&
                       snapshot->meta.ticket_next() &&
                       snapshot->meta.ticket_next() <=
                           snapshot->meta.ticket_last();
    }
    return result;
}
} // namespace service::lobby