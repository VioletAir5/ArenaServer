/** @file
 * @brief Keeps credential work and signing out of Gateway and reactor
 * threads.
 */
#include "auth/AuthService.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <openssl/crypto.h>

#include "storage.pb.h"

namespace service::auth
{
namespace
{
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
using Status = arena::rpc::Status;

/** @brief Scrubs owned secret storage before clearing its logical
 * length. */
void clearSecret(std::string& value) noexcept
{
    if (!value.empty())
        OPENSSL_cleanse(value.data(), value.size());
    value.clear();
}

/** @brief Covers immediate rejections before ownership moves into a
 * job. */
struct RequestCleanup
{
    /// The incoming candidate owns its original credential buffer.
    arena::auth::AuthenticateRequest& request;
    ~RequestCleanup()
    {
        clearSecret(*request.mutable_credential());
    }
};

/** @brief Creates a payload-free public failure without driver
 * diagnostics. */
arena::rpc::Response failure(Status status)
{
    arena::rpc::Response response;
    response.set_status(status);
    return response;
}

/** @brief Rejects invalid credential profiles before taking a
 * business slot. */
bool validRequest(const arena::auth::AuthenticateRequest& request)
{
    if (!request.proposed_epoch() || request.credential().empty())
        return false;
    if (request.credential_kind() ==
        arena::auth::AuthenticateRequest::JWT)
        return request.account().empty() &&
               request.credential().size() <= 4096;
    return request.credential_kind() ==
               arena::auth::AuthenticateRequest::PASSWORD &&
           !request.account().empty() &&
           request.account().size() <= 128 &&
           request.credential().size() <= 1024 &&
           std::all_of(
               request.account().begin(),
               request.account().end(),
               [](unsigned char c) { return c >= 0x20 && c <= 0x7e; }
           );
}

/** @brief Credential failures never expose signer or digest
 * internals. */
Status authFailure(AuthError error)
{
    return error == AuthError::Unavailable
               ? arena::rpc::UNAVAILABLE
               : arena::rpc::UNAUTHENTICATED;
}
} // namespace

struct AuthService::State : std::enable_shared_from_this<State>
{
    struct Route
    {
        /// The exact endpoint copied from one immutable discovery
        /// snapshot.
        boost::asio::ip::tcp::endpoint endpoint;
        /// Identity prevents an old probe from authorizing a renamed
        /// node.
        std::string nodeId;
        bool operator==(const Route&) const = default;
    };
    struct Job
    {
        /// Internal admission identity, never a caller-selected
        /// player.
        std::uint64_t id;
        /// Credential storage moves to the worker and is scrubbed on
        /// completion.
        arena::auth::AuthenticateRequest request;
        /// One monotonic deadline and validated inherited call path.
        arena::internal_rpc::Context context;
        /// Retained until real lookup or KDF work has ended.
        Completion completion;
        /// Canonical Storage digest, consumed only by the KDF worker.
        std::string digest;
        /// Canonical identity obtained from Storage or verified JWT
        /// claims.
        std::uint64_t player{0};
        /// Password admission retains its route across later reloads.
        std::optional<Route> storageRoute;
        /// Cancellation can be set from any caller thread.
        std::atomic<bool> cancelled{false};
        /// Prevents duplicate completion during final teardown.
        std::atomic<bool> finished{false};
        /// Publishes the outbound cancellation handle without a data
        /// race.
        std::mutex callMutex;
        /// Weak transport handle; it cannot retain a finished RPC
        /// connection.
        arena::internal_rpc::Call call;
        ~Job()
        {
            clearSecret(*request.mutable_credential());
            clearSecret(digest);
        }

        void cancel()
        {
            cancelled.store(true);
            arena::internal_rpc::Call current;
            {
                std::lock_guard lock(callMutex);
                current = call;
            }
            current.cancel();
        }
    };

    /** @brief Validates resource bounds before any worker starts. */
    explicit State(
        boost::asio::any_io_executor executor, Config configured
    )
        : config(std::move(configured))
        , strand(boost::asio::make_strand(std::move(executor)))
        , client(
              strand, config.storageEndpoint, config.maxInflight + 1
          )
        , timer(strand)
        , tokens(std::move(config.jwt))
    {
        if (!config.storageEndpoint.address().is_loopback() ||
            !config.storageEndpoint.port() ||
            config.storageToken.size() < 32 ||
            config.storageToken.size() > 256 ||
            config.maxWorkers < 1 || config.maxWorkers > 16 ||
            config.maxQueued < 1 || config.maxQueued > 4096 ||
            config.maxInflight < 1 || config.maxInflight > 1024 ||
            config.rpcTimeout < 1ms || config.rpcTimeout > 1000ms)
            throw std::invalid_argument(
                "invalid Auth service configuration"
            );
    }

    /** @brief Joins finite workers and retires abandoned callbacks
     * without secrets. */
    ~State()
    {
        requestStop();
        for (auto& worker : workers)
            if (worker.joinable())
                worker.join();
        // Owner callbacks retain State while executing. Any jobs left
        // here therefore have no concurrent owner, and all KDF
        // workers have joined.
        for (auto& [id, job] : jobs)
        {
            (void)id;
            clearSecret(*job->request.mutable_credential());
            clearSecret(job->digest);
            if (!job->finished.exchange(true))
                try
                {
                    job->completion(failure(arena::rpc::CANCELLED));
                }
                catch (...)
                {
                }
        }
        clearSecret(config.storageToken);
    }

    /** @brief Starts a fixed number of workers and one serialized
     * health probe. */
    void start()
    {
        for (std::size_t index = 0; index < config.maxWorkers;
             ++index)
        {
            ++alive;
            try
            {
                workers.emplace_back([this] { worker(); });
            }
            catch (...)
            {
                --alive;
                throw;
            }
        }
        boost::asio::post(
            strand,
            [weak = weak_from_this()]
            {
                if (auto self = weak.lock())
                    self->probe();
            }
        );
    }

    /** @brief Acquires one ready route without DNS or a legacy
     * fallback. */
    std::optional<Route> route() const
    {
        if (!config.runtime)
            return Route{config.storageEndpoint, "startup"};
        const auto snapshot = config.runtime->snapshot();
        const auto node = snapshot->resolve("storage");
        if (!node)
            return std::nullopt;
        return Route{
            {boost::asio::ip::make_address(node->host), node->port},
            node->id
        };
    }

    /** @brief Releases admission only after physical cleanup, then
     * dispatches once. */
    void finish(
        const std::shared_ptr<Job>& job, arena::rpc::Response response
    )
    {
        if (job->finished.exchange(true))
            return;
        if (job->cancelled.load() ||
            Clock::now() >= job->context.deadline)
        {
            clearSecret(*response.mutable_payload());
            response.set_status(
                job->cancelled.load() ? arena::rpc::CANCELLED
                                      : arena::rpc::DEADLINE_EXCEEDED
            );
        }
        clearSecret(*job->request.mutable_credential());
        clearSecret(job->digest);
        {
            std::lock_guard lock(mutex);
            jobs.erase(job->id);
            ++completed;
        }
        auto completion = std::move(job->completion);
        try
        {
            completion(std::move(response));
        }
        catch (...)
        {
        }
    }

    /** @brief Rejects a full worker queue without blocking the
     * reactor. */
    bool enqueue(const std::shared_ptr<Job>& job)
    {
        std::lock_guard lock(mutex);
        if (stopping || queue.size() >= config.maxQueued)
            return false;
        queue.push_back(job);
        wake.notify_one();
        return true;
    }

    /** @brief Starts one captured Storage lookup or a bounded JWT
     * worker task. */
    void begin(const std::shared_ptr<Job>& job)
    {
        if (job->cancelled.load())
            return finish(job, failure(arena::rpc::CANCELLED));
        if (Clock::now() >= job->context.deadline)
            return finish(
                job,
                failure(arena::rpc::DEADLINE_EXCEEDED)
            );
        if (job->request.credential_kind() ==
            arena::auth::AuthenticateRequest::JWT)
        {
            if (!enqueue(job))
                finish(job, failure(arena::rpc::OVERLOADED));
            return;
        }
        const auto& endpoint = job->storageRoute;
        if (!endpoint)
            return finish(job, failure(arena::rpc::UNAVAILABLE));
        arena::storage::FindAccountRequest lookup;
        lookup.set_account_name(job->request.account());
        arena::rpc::Request request;
        request.set_method("account.find");
        request.set_caller_service("auth");
        request.set_target_service("storage");
        request.set_service_token(config.storageToken);
        request.set_payload(lookup.SerializeAsString());
        arena::internal_rpc::Options options;
        options.timeout = config.rpcTimeout;
        options.context = job->context;
        options.endpoint = endpoint->endpoint;
        auto accepted = client.call(
            std::move(request),
            std::move(options),
            [weak = weak_from_this(), job](auto response)
            {
                if (auto self = weak.lock())
                    self->lookupFinished(job, std::move(response));
            }
        );
        if (!accepted)
            return finish(job, failure(accepted.error()));
        {
            std::lock_guard lock(job->callMutex);
            job->call = *accepted;
        }
        if (job->cancelled.load())
            accepted->cancel();
    }

    /** @brief Validates the canonical account/player pair before
     * starting its KDF. */
    void lookupFinished(
        const std::shared_ptr<Job>& job, arena::rpc::Response response
    )
    {
        if (response.status() != arena::rpc::OK)
        {
            const auto status = response.status() ==
                                        arena::rpc::NOT_FOUND
                                    ? arena::rpc::UNAUTHENTICATED
                                    : response.status();
            clearSecret(*response.mutable_payload());
            return finish(job, failure(status));
        }
        arena::storage::FindAccountReply record;
        const bool decoded = record.ParseFromString(
            response.payload()
        );
        clearSecret(*response.mutable_payload());
        const bool valid = decoded && record.has_account() &&
                           record.has_player() &&
                           record.account().account_id() &&
                           record.account().account_name() ==
                               job->request.account() &&
                           record.player().account_id() ==
                               record.account().account_id() &&
                           record.player().player_id() &&
                           record.player().mmr() <= 10000 &&
                           !record.player().display_name().empty() &&
                           record.player().display_name().size() <=
                               128 &&
                           Pbkdf2PasswordDigest::validEncoding(
                               record.account().password_digest()
                           );
        if (!valid)
        {
            clearSecret(
                *record.mutable_account()->mutable_password_digest()
            );
            return finish(job, failure(arena::rpc::UNAVAILABLE));
        }
        if (record.account().state() != "active" ||
            record.player().state() != "active")
        {
            clearSecret(
                *record.mutable_account()->mutable_password_digest()
            );
            return finish(job, failure(arena::rpc::UNAUTHENTICATED));
        }
        if (job->cancelled.load() ||
            Clock::now() >= job->context.deadline)
        {
            clearSecret(
                *record.mutable_account()->mutable_password_digest()
            );
            return finish(job, failure(arena::rpc::CANCELLED));
        }
        job->player = record.player().player_id();
        job->digest = std::move(
            *record.mutable_account()->mutable_password_digest()
        );
        clearSecret(
            *record.mutable_account()->mutable_password_digest()
        );
        if (!enqueue(job))
            finish(job, failure(arena::rpc::OVERLOADED));
    }

    /** @brief Runs actual KDF and signing only on a dedicated worker.
     */
    arena::rpc::Response execute(const std::shared_ptr<Job>& job)
    {
        if (job->cancelled.load())
            return failure(arena::rpc::CANCELLED);
        if (Clock::now() >= job->context.deadline)
            return failure(arena::rpc::DEADLINE_EXCEEDED);
        std::optional<TokenClaims> prior;
        if (job->request.credential_kind() ==
            arena::auth::AuthenticateRequest::JWT)
        {
            auto claims = tokens.verify(job->request.credential());
            if (!claims)
                return failure(authFailure(claims.error()));
            if (job->request.proposed_epoch() <= claims->epoch)
                return failure(arena::rpc::UNAUTHENTICATED);
            prior = *claims;
            job->player = claims->playerId.value();
        }
        else
        {
            Pbkdf2PasswordDigest digest;
            auto verified = digest.verify(
                job->request.credential(),
                job->digest
            );
            if (!verified)
                return failure(authFailure(verified.error()));
            if (!*verified)
                return failure(arena::rpc::UNAUTHENTICATED);
        }
        if (job->cancelled.load())
            return failure(arena::rpc::CANCELLED);
        if (Clock::now() >= job->context.deadline)
            return failure(arena::rpc::DEADLINE_EXCEEDED);
        auto signedToken = tokens.issue(
            arena::domain::PlayerId{job->player},
            job->request.proposed_epoch()
        );
        if (!signedToken)
            return failure(authFailure(signedToken.error()));
        arena::auth::AuthenticateReply grant;
        grant.set_player_id(job->player);
        grant.set_epoch(job->request.proposed_epoch());
        grant.set_expires_at_unix(signedToken->claims.expiresAt);
        grant.set_session_token(std::move(signedToken->encoded));
        grant.set_resumed(prior.has_value());
        if (prior)
        {
            grant.set_previous_epoch(prior->epoch);
            grant.set_previous_expires_at_unix(prior->expiresAt);
        }
        arena::rpc::Response response;
        auto* payload = response.mutable_payload();
        payload->resize(grant.ByteSizeLong());
        const bool encoded = grant.SerializeToArray(
            payload->data(),
            static_cast<int>(payload->size())
        );
        clearSecret(*grant.mutable_session_token());
        if (!encoded)
        {
            clearSecret(*payload);
            return failure(arena::rpc::INTERNAL);
        }
        return response;
    }

    /** @brief Retains secrets and admission until uninterruptible
     * OpenSSL work returns. */
    void worker()
    {
        for (;;)
        {
            std::shared_ptr<Job> job;
            {
                std::unique_lock lock(mutex);
                wake.wait(
                    lock,
                    [this] { return stopping || !queue.empty(); }
                );
                if (queue.empty())
                    break;
                job = std::move(queue.front());
                queue.pop_front();
                ++busy;
            }
            arena::rpc::Response result;
            try
            {
                result = execute(job);
            }
            catch (...)
            {
                result = failure(arena::rpc::INTERNAL);
            }
            clearSecret(*job->request.mutable_credential());
            clearSecret(job->digest);
            {
                std::lock_guard lock(mutex);
                --busy;
            }
            boost::asio::post(
                strand,
                [weak = weak_from_this(),
                 job,
                 result = std::move(result)]() mutable
                {
                    if (auto self = weak.lock())
                        self->finish(job, std::move(result));
                }
            );
        }
        --alive;
    }

    /** @brief Schedules only after the previous finite probe has
     * completed. */
    void scheduleProbe()
    {
        {
            std::lock_guard lock(mutex);
            if (stopping)
                return;
        }
        timer.expires_after(250ms);
        timer.async_wait(
            [weak = weak_from_this()](auto error)
            {
                if (auto self = weak.lock(); self && !error)
                    self->probe();
            }
        );
    }

    /** @brief Observes dependency health without consuming a business
     * admission slot. */
    void probe()
    {
        {
            std::lock_guard lock(mutex);
            if (stopping)
                return;
        }
        const auto endpoint = route();
        if (!endpoint)
        {
            {
                std::lock_guard lock(mutex);
                remoteReady = false;
            }
            return scheduleProbe();
        }
        arena::rpc::Request request;
        request.set_method("health.ready");
        request.set_caller_service("auth");
        request.set_target_service("storage");
        request.set_service_token(config.storageToken);
        arena::internal_rpc::Options options;
        options.timeout = std::min(config.rpcTimeout, 300ms);
        options.context = arena::internal_rpc::Context{
            Clock::now() + options.timeout,
            "auth-health",
            {"auth"}
        };
        options.endpoint = endpoint->endpoint;
        auto accepted = client.call(
            std::move(request),
            std::move(options),
            [weak = weak_from_this(),
             selected = *endpoint](auto response)
            {
                if (auto self = weak.lock())
                {
                    const auto current = self->route();
                    {
                        std::lock_guard lock(self->mutex);
                        if (self->stopping)
                            return;
                        if (current && *current == selected)
                        {
                            self->remoteReady = response.status() ==
                                                    arena::rpc::OK &&
                                                response.payload()
                                                    .empty();
                            self->probeRoute = selected;
                            self->observed = Clock::now();
                        }
                    }
                    self->scheduleProbe();
                }
            }
        );
        if (!accepted)
        {
            {
                std::lock_guard lock(mutex);
                remoteReady = false;
            }
            scheduleProbe();
        }
    }

    /** @brief Fences admission and signals actual jobs exactly once
     * without joining owner I/O. */
    void requestStop() noexcept
    {
        {
            std::lock_guard lock(mutex);
            if (stopping)
                return;
            stopping = true;
            remoteReady = false;
            for (auto& [id, job] : jobs)
            {
                (void)id;
                try
                {
                    job->cancel();
                }
                catch (...)
                {
                }
            }
        }
        wake.notify_all();
        try
        {
            client.stop();
            boost::asio::post(
                strand,
                [weak = weak_from_this()]
                {
                    if (auto self = weak.lock())
                    {
                        boost::system::error_code ignored;
                        self->timer.cancel(ignored);
                        std::lock_guard lock(self->mutex);
                        self->probeStopped = true;
                    }
                }
            );
        }
        catch (...)
        {
        }
    }

    /// Immutable process settings; the signer takes ownership of its
    /// key.
    Config config;
    /// Serializes Storage responses, probe scheduling and normal
    /// completions.
    boost::asio::strand<boost::asio::any_io_executor> strand;
    /// Business capacity plus one reserved health-probe transport
    /// slot.
    arena::internal_rpc::Client client;
    /// Owner-strand probe timer; all rearming checks the stop fence.
    boost::asio::steady_timer timer;
    /// Auth alone owns JWT signing and verification policy.
    JwtTokens tokens;
    /// Guards admission, worker queue and externally sampled state.
    mutable std::mutex mutex;
    /// Wakes a finite set of idle workers on enqueue or shutdown.
    std::condition_variable wake;
    /// Includes Storage wait, worker execution and pending cleanup
    /// completion.
    std::map<std::uint64_t, std::shared_ptr<Job>> jobs;
    /// Finite queue of worker-ready jobs, never a blocking producer
    /// queue.
    std::deque<std::shared_ptr<Job>> queue;
    /// Actual threads joined after shutdown completes.
    std::vector<std::thread> workers;
    /// Exit counter remains observable until every real worker has
    /// returned.
    std::atomic<std::size_t> alive{0};
    /// KDF/signing executions currently owned by workers.
    std::size_t busy{0};
    /// Never reused during this Auth process lifetime.
    std::uint64_t sequence{0};
    /// Successfully admitted business jobs.
    std::uint64_t accepted{0};
    /// Bounded-admission failures, excluding unadmitted malformed
    /// input.
    std::uint64_t rejected{0};
    /// Actual once-only physical completions.
    std::uint64_t completed{0};
    /// Permanent business admission fence.
    bool stopping{false};
    /// Last successful observation for the retained route.
    bool remoteReady{false};
    /// Owner has processed probe cancellation before stopped is
    /// reported.
    bool probeStopped{false};
    /// Node identity and endpoint associated with the observed health
    /// reply.
    std::optional<Route> probeRoute;
    /// Monotonic observation freshness; civil time is not used for
    /// readiness.
    Clock::time_point observed{};
};

AuthService::AuthService(
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
AuthService::~AuthService() noexcept
{
    stop();
}

std::expected<AuthService::Cancellation, Status>
AuthService::authenticate(
    arena::auth::AuthenticateRequest request,
    arena::internal_rpc::Context context,
    Completion completion
)
{
    RequestCleanup clean{request};
    if (!completion || !validRequest(request) ||
        context.correlationId.empty() ||
        context.correlationId.size() > 128)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    if (context.deadline <= Clock::now())
        return std::unexpected(arena::rpc::DEADLINE_EXCEEDED);
    context.deadline = std::min(
        context.deadline,
        Clock::now() + 1000ms
    );
    auto state = m_state;
    auto job = std::make_shared<State::Job>();
    job->request = std::move(request);
    job->context = std::move(context);
    job->completion = std::move(completion);
    Cancellation cancellation(
        [weak = std::weak_ptr<State::Job>(job)]
        {
            if (auto current = weak.lock())
                current->cancel();
        }
    );
    if (job->request.credential_kind() ==
        arena::auth::AuthenticateRequest::PASSWORD)
    {
        job->storageRoute = state->route();
        if (!job->storageRoute)
            return std::unexpected(arena::rpc::UNAVAILABLE);
    }
    {
        std::lock_guard lock(state->mutex);
        if (state->stopping || state->sequence == UINT64_MAX ||
            state->jobs.size() >= state->config.maxInflight)
        {
            ++state->rejected;
            return std::unexpected(
                state->stopping ? arena::rpc::UNAVAILABLE
                                : arena::rpc::OVERLOADED
            );
        }
        job->id = ++state->sequence;
        state->jobs.emplace(job->id, job);
        ++state->accepted;
    }
    try
    {
        boost::asio::post(
            state->strand,
            [weak = state->weak_from_this(), job]
            {
                if (auto self = weak.lock())
                    self->begin(job);
            }
        );
    }
    catch (...)
    {
        std::lock_guard lock(state->mutex);
        state->jobs.erase(job->id);
        --state->accepted;
        ++state->rejected;
        return std::unexpected(arena::rpc::INTERNAL);
    }
    return std::expected<Cancellation, Status>(
        std::move(cancellation)
    );
}

AuthService::Stats AuthService::stats() const
{
    const auto state = m_state;
    const auto route = state->route();
    const auto rpc = state->client.inflight();
    std::lock_guard lock(state->mutex);
    const auto alive = state->alive.load();
    const bool ready = !state->stopping && state->remoteReady &&
                       route && state->probeRoute &&
                       *route == *state->probeRoute &&
                       Clock::now() - state->observed <= 1000ms;
    return Stats{
        state->jobs.size(),
        state->queue.size(),
        state->busy,
        alive,
        state->accepted,
        state->rejected,
        state->completed,
        ready,
        state->stopping,
        state->stopping && state->jobs.empty() && !alive && !rpc &&
            state->probeStopped
    };
}
void AuthService::stop() noexcept
{
    if (m_state)
        m_state->requestStop();
}
} // namespace service::auth
