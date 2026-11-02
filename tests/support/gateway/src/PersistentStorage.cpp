/** @file
 * @brief Executes bounded Storage RPC, password work and crash-safe
 * result jobs.
 */
#include "gateway_test/PersistentStorage.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <future>
#include <map>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <utility>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <openssl/crypto.h>
#include <openssl/sha.h>

#include "lobby/OnlineRegistry.h"
#include "lobby/RoomService.h"
#include "rpc/Client.h"
#include "runtime/RuntimeConfig.h"

namespace service::gateway
{
namespace
{
using AuthError = service::auth::AuthError;
using PlayerId = arena::domain::PlayerId;
using RpcClient = arena::internal_rpc::Client;
using Milliseconds = std::chrono::milliseconds;
using Clock = std::chrono::steady_clock;
constexpr std::size_t recordOverhead = 56;
constexpr std::string_view recordMagic = "ARSP0001";

struct Fd
{
    int value{-1};
    explicit Fd(int fd = -1)
        : value(fd)
    {
    }
    ~Fd()
    {
        if (value >= 0)
            ::close(value);
    }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
};

[[noreturn]] void diskError()
{
    throw std::runtime_error(
        "persistent result spool operation failed"
    );
}
void syncFd(int fd)
{
    while (::fsync(fd) != 0)
        if (errno != EINTR)
            diskError();
}
void writeAll(int fd, std::string_view bytes)
{
    while (!bytes.empty())
    {
        const auto written = ::write(fd, bytes.data(), bytes.size());
        if (written < 0 && errno == EINTR)
            continue;
        if (written <= 0)
            diskError();
        bytes.remove_prefix(static_cast<std::size_t>(written));
    }
}
std::string
readFile(int directory, const std::string& name, std::size_t limit)
{
    Fd fd(
        ::openat(
            directory,
            name.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW
        )
    );
    struct stat info{};
    if (fd.value < 0 || ::fstat(fd.value, &info) != 0 ||
        !S_ISREG(info.st_mode) || info.st_size < 0 ||
        static_cast<std::uintmax_t>(info.st_size) > limit)
        diskError();
    std::string bytes(static_cast<std::size_t>(info.st_size), '\0');
    std::size_t offset = 0;
    while (offset < bytes.size())
    {
        auto count = ::read(
            fd.value,
            bytes.data() + offset,
            bytes.size() - offset
        );
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0)
            diskError();
        offset += static_cast<std::size_t>(count);
    }
    return bytes;
}
void integer(std::string& out, std::uint64_t value, unsigned width)
{
    for (unsigned i = width; i != 0; --i)
        out.push_back(static_cast<char>(value >> ((i - 1) * 8)));
}
std::uint64_t
integer(std::string_view bytes, std::size_t offset, unsigned width)
{
    if (offset + width > bytes.size())
        diskError();
    std::uint64_t value = 0;
    for (unsigned i = 0; i != width; ++i)
        value = (value << 8) |
                static_cast<unsigned char>(bytes[offset + i]);
    return value;
}
std::uint64_t wallMillis()
{
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<Milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        )
            .count()
    );
}
bool correlationValid(std::string_view value)
{
    return !value.empty() && value.size() <= 64 &&
           std::all_of(
               value.begin(),
               value.end(),
               [](unsigned char c)
               {
                   return (c >= 'a' && c <= 'z') ||
                          (c >= 'A' && c <= 'Z') ||
                          (c >= '0' && c <= '9') || c == '_' ||
                          c == '-';
               }
           );
}
bool sameMatch(
    arena::storage::MatchRecord actual,
    arena::storage::MatchRecord expected
)
{
    // Storage fills receipt times only when a caller explicitly
    // leaves them zero.
    // Compare known result facts while tolerating future protobuf
    // fields.
    actual.DiscardUnknownFields();
    expected.DiscardUnknownFields();
    if (!expected.created_unix_ms())
        actual.set_created_unix_ms(0);
    if (!expected.started_unix_ms())
        actual.set_started_unix_ms(0);
    if (!expected.finished_unix_ms())
        actual.set_finished_unix_ms(0);
    return actual.SerializeAsString() == expected.SerializeAsString();
}
bool validMatch(const arena::storage::MatchRecord& value)
{
    if (!value.match_id() || value.match_id() >= (1ULL << 63) ||
        value.room_id() != ((1ULL << 63) | value.match_id()) ||
        value.result_version() != 1 ||
        value.participants_size() != 2 ||
        (value.state() != "completed" &&
         value.state() != "aborted") ||
        (value.state() == "completed") !=
            (value.finish_reason() == "completed") ||
        (value.finish_reason() != "completed" &&
         value.finish_reason() != "member_unavailable" &&
         value.finish_reason() != "ready_timeout") ||
        (value.state() == "aborted" && value.winner_player_id()))
        return false;
    const auto& a = value.participants(0);
    const auto& b = value.participants(1);
    for (const auto* member : {&a, &b})
        if (!member->player_id() || !member->session_id() ||
            !member->membership_epoch() || member->slot() < 1 ||
            member->slot() > 2 || member->initial_mmr() > 10000 ||
            member->last_dx() < -1 || member->last_dx() > 1 ||
            member->last_dy() < -1 || member->last_dy() > 1)
            return false;
    return a.slot() != b.slot() && a.player_id() != b.player_id() &&
           a.session_id() != b.session_id() &&
           (!value.winner_player_id() ||
            value.winner_player_id() == a.player_id() ||
            value.winner_player_id() == b.player_id());
}
template <class Callback, class Value>
void complete(Callback callback, Value value)
{
    if (callback)
        try
        {
            callback(std::move(value));
        }
        catch (...)
        {
        }
}
} // namespace

struct PersistentStorage::State : std::enable_shared_from_this<State>
{
    struct Route
    {
        boost::asio::ip::tcp::endpoint endpoint;
        std::uint64_t version{0};
        std::string nodeId;
    };
    struct AuthJob
    {
        std::uint64_t id;
        std::string account, password;
        Clock::time_point deadline;
        AuthCompletion completion;
        arena::internal_rpc::Call call;
        std::atomic<bool> cancelled{false};
    };
    struct Lookup
    {
        std::uint64_t id, matchId;
        PlayerId player{1};
        Clock::time_point deadline;
        std::string correlation;
        LookupCompletion completion;
        arena::internal_rpc::Call call;
        arena::storage::MatchRecord match;
        std::atomic<bool> cancelled{false};
        bool followup{false};
        /// Both reads retain the endpoint chosen before the first
        /// admission.
        boost::asio::ip::tcp::endpoint endpoint;
    };
    enum class Phase
    {
        Write,
        Ready,
        Inflight,
        Response,
        Failed
    };
    struct ResultJob
    {
        std::uint64_t id, nextDue{0};
        std::uint32_t attempts{0};
        std::string payload;
        Phase phase{Phase::Write};
        SaveCompletion completion;
        arena::rpc::Response response;
        bool processing{false}, operatorRequeue{false};
    };

    explicit State(PersistentStorageConfig value)
        : config(std::move(value))
        , guard(boost::asio::make_work_guard(io))
        , client(
              io.get_executor(), config.endpoint, config.maxAuth + 16
          )
        , generator(std::random_device{}())
    {
        if (!config.endpoint.address().is_loopback() ||
            !config.endpoint.port() ||
            config.serviceToken.size() < 32 ||
            config.serviceToken.size() > 256 ||
            config.spoolDirectory.empty() || config.maxAuth < 1 ||
            config.maxAuth > 1024 || config.maxWorkPending < 1 ||
            config.maxWorkPending > 4096 ||
            config.maxSpoolFiles < 1 || config.maxSpoolFiles > 4096 ||
            config.maxSpoolBytes < 1024 ||
            config.maxSpoolBytes > 256 * 1024 * 1024 ||
            config.maxRatings < 1 || config.maxRatings > 100000 ||
            config.idBlockSize < 1 || config.idBlockSize > 1024 ||
            config.rpcTimeout < Milliseconds{1} ||
            config.rpcTimeout > Milliseconds{5000} ||
            config.maxResultAttempts < 1 ||
            config.maxResultAttempts > 64 ||
            config.retryBase < Milliseconds{1} ||
            config.retryMax < config.retryBase ||
            config.retryMax > Milliseconds{5000})
            throw std::invalid_argument(
                "invalid persistent Storage configuration"
            );
        std::filesystem::create_directories(config.spoolDirectory);
        directory = ::open(
            config.spoolDirectory.c_str(),
            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW
        );
        if (directory < 0)
            diskError();
        lock = ::openat(
            directory,
            ".lock",
            O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW,
            0600
        );
        if (lock < 0 || ::flock(lock, LOCK_EX | LOCK_NB) != 0)
        {
            if (lock >= 0)
                ::close(lock);
            ::close(directory);
            directory = lock = -1;
            throw std::runtime_error(
                "persistent result spool is already owned"
            );
        }
    }
    ~State()
    {
        for (auto* thread : {&workThread, &ioThread})
            if (thread->joinable())
            {
                if (thread->get_id() == std::this_thread::get_id())
                    thread->detach();
                else
                    thread->join();
            }
        if (lock >= 0)
            ::close(lock);
        if (directory >= 0)
            ::close(directory);
    }
    PersistentStorageConfig config;
    boost::asio::io_context io;
    boost::asio::executor_work_guard<
        boost::asio::io_context::executor_type>
        guard;
    RpcClient client;
    /// Probe timer and its call run only on io; status is guarded by
    /// mutex.
    boost::asio::steady_timer healthTimer{io};
    bool remoteReady{false};
    std::uint64_t healthVersion{0};
    std::optional<Route> healthRoute;
    Clock::time_point healthObserved{};
    std::thread ioThread, workThread;
    mutable std::mutex mutex;
    std::mutex stopMutex;
    std::condition_variable changed;
    bool accepting{true}, stopping{false}, recovered{false},
        healthy{true};
    bool idFetching{false};
    std::uint64_t sequence{0}, lastReserved{0};
    int directory{-1}, lock{-1};
    std::deque<std::uint64_t> ids;
    std::map<std::uint64_t, std::uint32_t> ratings;
    std::map<std::uint64_t, std::shared_ptr<AuthJob>> auth;
    std::map<std::uint64_t, std::shared_ptr<Lookup>> lookups;
    std::map<std::uint64_t, std::shared_ptr<ResultJob>> results;
    std::deque<std::function<void()>> work;
    std::size_t bytes{0};
    std::mt19937_64 generator;
    PersistentStorageMetrics counts;

    /** @brief Copies one ready endpoint from one acquired immutable
     * version. */
    std::optional<Route>
    route(arena::runtime::ConfigSnapshot::Ptr current = {}) const
    {
        if (!config.runtime)
            return Route{config.endpoint, 0, "startup"};
        if (!current)
            current = config.runtime->snapshot();
        const auto node = current->resolve("storage");
        if (!node)
            return std::nullopt;
        return Route{
            {boost::asio::ip::make_address(node->host), node->port},
            current->version(),
            node->id
        };
    }

    /** @brief Schedules at most one probe after the preceding call
     * completes. */
    void scheduleProbe()
    {
        {
            std::lock_guard lock(mutex);
            if (stopping)
                return;
        }
        healthTimer.expires_after(Milliseconds{250});
        healthTimer.async_wait(
            [weak = weak_from_this()](auto error)
            {
                if (auto self = weak.lock(); self && !error)
                    self->probe();
            }
        );
    }

    /** @brief A reply from an obsolete route cannot make its
     * replacement ready. */
    void probe()
    {
        {
            std::lock_guard lock(mutex);
            if (stopping)
                return;
        }
        const auto selected = route();
        if (!selected)
        {
            {
                std::lock_guard lock(mutex);
                remoteReady = false;
            }
            return scheduleProbe();
        }
        arena::rpc::Request request;
        request.set_method("health.ready");
        request.set_protocol_version(
            arena::internal_rpc::protocolVersion
        );
        request.set_caller_service("gateway");
        request.set_target_service("storage");
        request.set_service_token(config.serviceToken);
        arena::internal_rpc::Options options;
        options.timeout = std::min(
            config.rpcTimeout,
            Milliseconds{300}
        );
        options.endpoint = selected->endpoint;
        auto accepted = client.call(
            std::move(request),
            options,
            [weak = weak_from_this(),
             selected = *selected](auto response)
            {
                const auto self = weak.lock();
                if (!self)
                    return;
                const auto current = self->route();
                {
                    std::lock_guard lock(self->mutex);
                    if (self->stopping)
                        return;
                    if (current &&
                        current->nodeId == selected.nodeId &&
                        current->endpoint == selected.endpoint)
                    {
                        self->healthVersion = selected.version;
                        self->healthRoute = selected;
                        self->healthObserved = Clock::now();
                        self->remoteReady = response.status() ==
                                                arena::rpc::OK &&
                                            response.payload()
                                                .empty();
                    }
                }
                self->scheduleProbe();
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

    std::string name(std::uint64_t id, bool failed = false) const
    {
        return std::to_string(id) + (failed ? ".failed" : ".pending");
    }

    std::string encode(const ResultJob& job) const
    {
        std::string out(recordMagic);
        integer(out, job.attempts, 4);
        integer(out, job.nextDue, 8);
        integer(out, job.payload.size(), 4);
        out += job.payload;
        std::array<unsigned char, SHA256_DIGEST_LENGTH> hash{};
        SHA256(
            reinterpret_cast<const unsigned char*>(out.data()),
            out.size(),
            hash.data()
        );
        out.append(
            reinterpret_cast<const char*>(hash.data()),
            hash.size()
        );
        return out;
    }
    std::shared_ptr<ResultJob>
    decode(std::string_view data, bool failed) const
    {
        if (data.size() < recordOverhead ||
            !data.starts_with(recordMagic) ||
            integer(data, 20, 4) != data.size() - recordOverhead)
            diskError();
        std::array<unsigned char, SHA256_DIGEST_LENGTH> hash{};
        SHA256(
            reinterpret_cast<const unsigned char*>(data.data()),
            data.size() - hash.size(),
            hash.data()
        );
        if (CRYPTO_memcmp(
                hash.data(),
                data.data() + data.size() - hash.size(),
                hash.size()
            ))
            diskError();
        auto job = std::make_shared<ResultJob>();
        job->attempts = static_cast<std::uint32_t>(
            integer(data, 8, 4)
        );
        job->nextDue = integer(data, 12, 8);
        job->payload = std::string(
            data.substr(24, data.size() - recordOverhead)
        );
        arena::storage::SaveResultRequest request;
        if (job->attempts > config.maxResultAttempts ||
            !request.ParseFromString(job->payload) ||
            !request.has_match() || !validMatch(request.match()))
            diskError();
        job->id = request.match().match_id();
        job->phase = failed ? Phase::Failed : Phase::Ready;
        return job;
    }
    void writeRecord(const ResultJob& job)
    {
        const auto destination = name(job.id);
        struct stat info{};
        if (::fstatat(
                directory,
                destination.c_str(),
                &info,
                AT_SYMLINK_NOFOLLOW
            ) == 0)
        {
            const auto old = decode(
                readFile(directory, destination, RpcClient::maxFrame),
                false
            );
            if (old->payload != job.payload ||
                old->attempts > job.attempts)
                diskError();
        }
        else if (errno != ENOENT)
            diskError();
        const auto temporary = destination + ".tmp";
        Fd fd(
            ::openat(
                directory,
                temporary.c_str(),
                O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                0600
            )
        );
        if (fd.value < 0)
            diskError();
        try
        {
            const auto encoded = encode(job);
            writeAll(fd.value, encoded);
            syncFd(fd.value);
            if (::renameat(
                    directory,
                    temporary.c_str(),
                    directory,
                    destination.c_str()
                ) != 0)
                diskError();
            syncFd(directory);
        }
        catch (...)
        {
            ::unlinkat(directory, temporary.c_str(), 0);
            throw;
        }
    }
    void recover()
    {
        std::map<std::uint64_t, std::shared_ptr<ResultJob>> loaded;
        std::size_t total = 0, entries = 0;
        for (const auto& entry : std::filesystem::directory_iterator(
                 config.spoolDirectory
             ))
        {
            const auto filename = entry.path().filename().string();
            if (filename == ".lock")
                continue;
            if (++entries > config.maxSpoolFiles * 3)
                diskError();
            if (filename.ends_with(".pending.tmp"))
            {
                const auto stem = filename.substr(
                    0,
                    filename.size() - 12
                );
                if (stem.empty() ||
                    !std::all_of(
                        stem.begin(),
                        stem.end(),
                        [](char c) { return c >= '0' && c <= '9'; }
                    ))
                    diskError();
                if (::unlinkat(directory, filename.c_str(), 0) != 0)
                    diskError();
                continue;
            }
            const bool failed = filename.ends_with(".failed");
            if (!failed && !filename.ends_with(".pending"))
                diskError();
            auto job = decode(
                readFile(directory, filename, RpcClient::maxFrame),
                failed
            );
            if (filename != name(job->id, failed))
                diskError();
            const auto previous = loaded.find(job->id);
            if (previous != loaded.end())
            {
                if (previous->second->payload != job->payload)
                    diskError();
                if (failed)
                    job->attempts = std::max(
                        job->attempts,
                        previous->second->attempts
                    );
                else
                    previous->second->attempts = std::max(
                        job->attempts,
                        previous->second->attempts
                    );
                if (failed)
                    loaded[job->id] = job;
                if (::unlinkat(directory, name(job->id).c_str(), 0) !=
                    0)
                    diskError();
                continue;
            }
            total += job->payload.size() + recordOverhead;
            if (loaded.size() >= config.maxSpoolFiles ||
                total > config.maxSpoolBytes)
                diskError();
            // Wall-clock corrections cannot turn a bounded retry
            // delay into years.
            job->nextDue = std::min(
                job->nextDue,
                wallMillis() + static_cast<std::uint64_t>(
                                   config.retryMax.count()
                               )
            );
            loaded.emplace(job->id, std::move(job));
        }
        syncFd(directory);
        std::lock_guard guardLock(mutex);
        results = std::move(loaded);
        bytes = total;
        recovered = true;
        changed.notify_all();
    }

    bool enqueue(std::function<void()> action)
    {
        std::lock_guard guardLock(mutex);
        if (stopping || work.size() >= config.maxWorkPending)
            return false;
        work.push_back(std::move(action));
        changed.notify_all();
        return true;
    }
    void start()
    {
        auto self = shared_from_this();
        ioThread = std::thread([self] { self->io.run(); });
        try
        {
            workThread = std::thread([self] { self->run(); });
        }
        catch (...)
        {
            guard.reset();
            if (ioThread.joinable())
                ioThread.join();
            throw;
        }
    }
    void
    authFinish(const std::shared_ptr<AuthJob>& job, AuthResult value)
    {
        AuthCompletion completion;
        {
            std::lock_guard guardLock(mutex);
            const auto found = auth.find(job->id);
            if (found == auth.end())
                return;
            completion = std::move(job->completion);
            auth.erase(found);
        }
        OPENSSL_cleanse(job->password.data(), job->password.size());
        job->password.clear();
        changed.notify_all();
        complete(std::move(completion), std::move(value));
    }
    void authResponse(
        const std::shared_ptr<AuthJob>& job,
        arena::rpc::Response response
    )
    {
        if (job->cancelled || Clock::now() >= job->deadline)
            return authFinish(
                job,
                std::unexpected(AuthError::Unavailable)
            );
        if (response.status() != arena::rpc::OK)
            return authFinish(
                job,
                std::unexpected(
                    response.status() == arena::rpc::NOT_FOUND
                        ? AuthError::InvalidCredentials
                        : AuthError::Unavailable
                )
            );
        arena::storage::FindAccountReply value;
        if (!value.ParseFromString(response.payload()) ||
            !value.has_account() || !value.has_player() ||
            !value.account().account_id() ||
            value.account().account_name() != job->account ||
            value.player().account_id() !=
                value.account().account_id() ||
            !value.player().player_id() ||
            value.player().mmr() > 10000 ||
            value.player().display_name().empty() ||
            value.player().display_name().size() > 128 ||
            !service::auth::Pbkdf2PasswordDigest::validEncoding(
                value.account().password_digest()
            ))
            return authFinish(
                job,
                std::unexpected(AuthError::Unavailable)
            );
        if (value.account().state() != "active" ||
            value.player().state() != "active")
            return authFinish(
                job,
                std::unexpected(AuthError::InvalidCredentials)
            );
        std::weak_ptr<State> weak = shared_from_this();
        if (!enqueue(
                [weak, job, value = std::move(value)]() mutable
                {
                    const auto state = weak.lock();
                    if (!state)
                        return;
                    if (job->cancelled ||
                        Clock::now() >= job->deadline)
                        return state->authFinish(
                            job,
                            std::unexpected(AuthError::Unavailable)
                        );
                    service::auth::Pbkdf2PasswordDigest digest;
                    auto verified = digest.verify(
                        job->password,
                        value.account().password_digest()
                    );
                    if (!verified || !*verified)
                        return state->authFinish(
                            job,
                            std::unexpected(
                                verified
                                    ? AuthError::InvalidCredentials
                                    : verified.error()
                            )
                        );
                    {
                        std::lock_guard guardLock(state->mutex);
                        if (!state->accepting || job->cancelled ||
                            Clock::now() >= job->deadline ||
                            (!state->ratings.contains(
                                 value.player().player_id()
                             ) &&
                             state->ratings.size() >=
                                 state->config.maxRatings))
                            verified = std::unexpected(
                                AuthError::Unavailable
                            );
                        else
                            state->ratings[value.player()
                                               .player_id()] =
                                value.player().mmr();
                    }
                    state->authFinish(
                        job,
                        verified ? AuthResult{PlayerId{
                                       value.player().player_id()
                                   }}
                                 : AuthResult{std::unexpected(
                                       verified.error()
                                   )}
                    );
                }
            ))
            authFinish(job, std::unexpected(AuthError::Unavailable));
    }

    void requestIds()
    {
        const auto selected = route();
        if (!selected)
            return;
        {
            std::lock_guard guardLock(mutex);
            if (!accepting || idFetching ||
                ids.size() > config.idBlockSize)
                return;
            idFetching = true;
        }
        arena::storage::AllocateIdsRequest payload;
        payload.set_count(config.idBlockSize);
        arena::rpc::Request request;
        request.set_method("match.allocate_ids");
        request.set_payload(payload.SerializeAsString());
        request.set_service_token(config.serviceToken);
        std::weak_ptr<State> weak = shared_from_this();
        auto result = client.call(
            std::move(request),
            {config.rpcTimeout, {}, selected->endpoint},
            [weak](auto response)
            {
                const auto state = weak.lock();
                if (!state)
                    return;
                arena::storage::AllocateIdsReply value;
                const bool valid = response.status() ==
                                       arena::rpc::OK &&
                                   value.ParseFromString(
                                       response.payload()
                                   ) &&
                                   value.first() &&
                                   value.last() < (1ULL << 63) &&
                                   value.last() >= value.first() &&
                                   value.last() - value.first() + 1 ==
                                       state->config.idBlockSize;
                std::lock_guard guardLock(state->mutex);
                state->idFetching = false;
                if (valid && value.first() > state->lastReserved &&
                    state->ids.size() + state->config.idBlockSize <=
                        state->config.idBlockSize * 2)
                {
                    for (auto id = value.first(); id <= value.last();
                         ++id)
                        state->ids.push_back(id);
                    state->lastReserved = value.last();
                }
                state->changed.notify_all();
            }
        );
        if (!result)
        {
            std::lock_guard guardLock(mutex);
            idFetching = false;
            changed.notify_all();
        }
    }

    void fail(const std::shared_ptr<ResultJob>& job)
    {
        try
        {
            const auto failed = name(job->id, true),
                       pending = name(job->id);
            struct stat info{};
            if (::fstatat(
                    directory,
                    failed.c_str(),
                    &info,
                    AT_SYMLINK_NOFOLLOW
                ) == 0)
            {
                if (decode(
                        readFile(
                            directory,
                            failed,
                            RpcClient::maxFrame
                        ),
                        true
                    )
                        ->payload != job->payload)
                    diskError();
                if (::unlinkat(directory, pending.c_str(), 0) != 0 &&
                    errno != ENOENT)
                    diskError();
            }
            else if (
                errno != ENOENT || ::renameat(
                                       directory,
                                       pending.c_str(),
                                       directory,
                                       failed.c_str()
                                   ) != 0
            )
                diskError();
            syncFd(directory);
        }
        catch (...)
        {
            std::lock_guard guardLock(mutex);
            healthy = false;
        }
        SaveCompletion completion;
        {
            std::lock_guard guardLock(mutex);
            job->phase = Phase::Failed;
            job->processing = false;
            completion = std::move(job->completion);
        }
        complete(std::move(completion), false);
    }
    void resultStep(const std::shared_ptr<ResultJob>& job)
    {
        if (job->operatorRequeue)
        {
            // A separate operator action opens one new finite
            // persisted budget.
            job->attempts = 0;
            job->nextDue = 0;
            writeRecord(*job);
            if (::unlinkat(
                    directory,
                    name(job->id, true).c_str(),
                    0
                ) != 0 &&
                errno != ENOENT)
                diskError();
            syncFd(directory);
            std::lock_guard guardLock(mutex);
            job->operatorRequeue = false;
            job->phase = Phase::Ready;
            job->processing = false;
            ++counts.operatorRequeues;
            return;
        }
        if (job->phase == Phase::Write)
        {
            writeRecord(*job);
            std::lock_guard guardLock(mutex);
            job->phase = Phase::Ready;
            job->processing = false;
            return;
        }
        if (job->phase == Phase::Response)
        {
            arena::storage::SaveResultRequest request;
            arena::storage::SaveResultReply reply;
            const bool committed =
                job->response.status() == arena::rpc::OK &&
                request.ParseFromString(job->payload) &&
                reply.ParseFromString(job->response.payload()) &&
                reply.has_match() &&
                sameMatch(reply.match(), request.match()) &&
                reply.has_settlement() &&
                reply.settlement().settlement_id() == job->id &&
                reply.settlement().match_id() == job->id &&
                reply.settlement().event_id().size() == 36;
            if (committed)
            {
                bool removed = true;
                try
                {
                    if (::unlinkat(
                            directory,
                            name(job->id).c_str(),
                            0
                        ) != 0)
                        diskError();
                    syncFd(directory);
                }
                catch (...)
                {
                    removed = false;
                }
                SaveCompletion completion;
                {
                    std::lock_guard guardLock(mutex);
                    completion = std::move(job->completion);
                    ++counts.resultsCommitted;
                    if (removed)
                    {
                        bytes -= job->payload.size() + recordOverhead;
                        results.erase(job->id);
                    }
                    else
                    {
                        ++counts.cleanupFailures;
                        healthy = false;
                        job->phase = Phase::Failed;
                        job->processing = false;
                    }
                }
                complete(std::move(completion), true);
                return;
            }
            const auto status = job->response.status();
            if (status == arena::rpc::INVALID_ARGUMENT ||
                status == arena::rpc::CONFLICT ||
                status == arena::rpc::UNAUTHENTICATED ||
                status == arena::rpc::NOT_FOUND ||
                job->attempts >= config.maxResultAttempts)
                return fail(job);
            const auto shift = std::min<std::uint32_t>(
                job->attempts - 1,
                16
            );
            const auto cap = std::min<std::uint64_t>(
                config.retryMax.count(),
                static_cast<std::uint64_t>(config.retryBase.count())
                    << shift
            );
            job->nextDue = wallMillis() +
                           std::uniform_int_distribution<
                               std::uint64_t>(0, cap)(generator);
            writeRecord(*job);
            std::lock_guard guardLock(mutex);
            job->phase = Phase::Ready;
            job->processing = false;
            return;
        }
        if (job->attempts >= config.maxResultAttempts)
            return fail(job);
        ++job->attempts;
        job->nextDue = 0;
        // Persist the attempt before sending, including when a crash
        // precedes write.
        writeRecord(*job);
        {
            std::lock_guard guardLock(mutex);
            if (stopping)
            {
                job->phase = Phase::Ready;
                job->processing = false;
                return;
            }
            job->phase = Phase::Inflight;
            job->processing = false;
            ++counts.resultAttempts;
        }
        arena::rpc::Request request;
        request.set_method("match.save_result");
        request.set_payload(job->payload);
        request.set_idempotency_key(
            "match-" + std::to_string(job->id)
        );
        request.set_service_token(config.serviceToken);
        std::weak_ptr<State> weak = shared_from_this();
        const auto selected = route();
        auto accepted = selected
                            ? client.call(
                                  std::move(request),
                                  {config.rpcTimeout,
                                   {},
                                   selected->endpoint},
                                  [weak, job](auto response)
                                  {
                                      const auto state = weak.lock();
                                      if (!state)
                                          return;
                                      std::lock_guard guardLock(
                                          state->mutex
                                      );
                                      job->response = std::move(
                                          response
                                      );
                                      job->phase = Phase::Response;
                                      state->changed.notify_all();
                                  }
                              )
                            : std::expected<
                                  arena::internal_rpc::Call,
                                  arena::rpc::Status>(std::unexpected(
                                  arena::rpc::UNAVAILABLE
                              ));
        if (!accepted)
        {
            std::lock_guard guardLock(mutex);
            job->response.set_status(accepted.error());
            job->phase = Phase::Response;
            changed.notify_all();
        }
    }

    void run()
    {
        try
        {
            recover();
            if (config.runtime || config.probeHealth)
                boost::asio::post(
                    io,
                    [weak = weak_from_this()]
                    {
                        if (auto self = weak.lock())
                            self->probe();
                    }
                );
        }
        catch (...)
        {
            std::lock_guard guardLock(mutex);
            healthy = false;
            recovered = true;
            changed.notify_all();
        }
        bool diskTurn = true;
        for (;;)
        {
            std::function<void()> action;
            std::shared_ptr<ResultJob> job;
            {
                std::unique_lock guardLock(mutex);
                if (!work.empty() && !diskTurn)
                {
                    action = std::move(work.front());
                    work.pop_front();
                    diskTurn = true;
                }
                else
                {
                    for (auto& [id, candidate] : results)
                    {
                        (void)id;
                        if (candidate->processing)
                            continue;
                        if (candidate->operatorRequeue ||
                            candidate->phase == Phase::Write ||
                            candidate->phase == Phase::Response ||
                            (!stopping &&
                             candidate->phase == Phase::Ready &&
                             candidate->nextDue <= wallMillis()))
                        {
                            job = candidate;
                            job->processing = true;
                            diskTurn = false;
                            break;
                        }
                    }
                    if (!job && !work.empty())
                    {
                        action = std::move(work.front());
                        work.pop_front();
                        diskTurn = true;
                    }
                    else if (!job)
                    {
                        if (stopping && client.inflight() == 0 &&
                            auth.empty() && lookups.empty())
                            break;
                        changed.wait_for(guardLock, Milliseconds{10});
                        continue;
                    }
                }
            }
            if (action)
            {
                try
                {
                    action();
                }
                catch (...)
                {
                }
            }
            if (job)
                try
                {
                    resultStep(job);
                }
                catch (...)
                {
                    {
                        std::lock_guard guardLock(mutex);
                        healthy = false;
                    }
                    fail(job);
                }
        }
        std::vector<SaveCompletion> pending;
        {
            std::lock_guard guardLock(mutex);
            for (auto& [id, job] : results)
            {
                (void)id;
                if (job->completion)
                    pending.push_back(std::move(job->completion));
            }
        }
        for (auto& callback : pending)
            complete(std::move(callback), false);
        guard.reset();
    }
};
} // namespace service::gateway

namespace service::gateway
{
PersistentStorage::PersistentStorage(PersistentStorageConfig config)
    : m_state(std::make_shared<State>(std::move(config)))
{
    m_state->start();
}
PersistentStorage::~PersistentStorage() noexcept
{
    stop();
}

bool PersistentStorage::prime(std::chrono::milliseconds timeout)
{
    if (timeout < Milliseconds{1} || timeout > Milliseconds{30000})
        return false;
    auto state = m_state;
    state->requestIds();
    std::unique_lock lock(state->mutex);
    state->changed.wait_for(
        lock,
        timeout,
        [&]
        {
            return !state->accepting ||
                   (state->recovered &&
                    (!state->healthy || !state->ids.empty()));
        }
    );
    return state->accepting && state->recovered && state->healthy &&
           !state->ids.empty();
}
std::optional<std::uint64_t> PersistentStorage::takeTicketId()
{
    auto state = m_state;
    std::optional<std::uint64_t> value;
    bool prefetch = false;
    {
        std::lock_guard lock(state->mutex);
        if (!state->accepting || !state->recovered || !state->healthy)
            return {};
        if (!state->ids.empty())
        {
            value = state->ids.front();
            state->ids.pop_front();
        }
        prefetch = state->ids.size() <= state->config.idBlockSize / 2;
    }
    if (prefetch)
        state->requestIds();
    return value;
}
std::optional<std::uint32_t>
PersistentStorage::rating(PlayerId id) const
{
    std::lock_guard lock(m_state->mutex);
    const auto value = m_state->ratings.find(id.value());
    if (value == m_state->ratings.end())
        return {};
    return value->second;
}

std::expected<PersistentStorage::Cancellation, AuthError>
PersistentStorage::authenticate(
    std::string account,
    std::string password,
    Clock::time_point deadline,
    std::string correlation,
    AuthCompletion completion
)
{
    if (account.empty() || account.size() > 128 || password.empty() ||
        password.size() > 1024 ||
        !std::all_of(
            account.begin(),
            account.end(),
            [](unsigned char c) { return c >= 33 && c <= 126; }
        ) ||
        !completion || !correlationValid(correlation))
        return std::unexpected(AuthError::InvalidCredentials);
    auto state = m_state;
    const auto selected = state->route();
    if (!selected)
        return std::unexpected(AuthError::Unavailable);
    auto job = std::make_shared<State::AuthJob>();
    job->account = std::move(account);
    job->password = std::move(password);
    job->deadline = deadline;
    job->completion = std::move(completion);
    {
        std::lock_guard lock(state->mutex);
        if (!state->accepting || Clock::now() >= deadline ||
            state->auth.size() + state->lookups.size() >=
                state->config.maxAuth)
        {
            ++state->counts.rejected;
            OPENSSL_cleanse(
                job->password.data(),
                job->password.size()
            );
            return std::unexpected(AuthError::Unavailable);
        }
        job->id = ++state->sequence;
        state->auth.emplace(job->id, job);
    }
    arena::storage::FindAccountRequest payload;
    payload.set_account_name(job->account);
    arena::rpc::Request request;
    request.set_method("account.find");
    request.set_payload(payload.SerializeAsString());
    request.set_service_token(state->config.serviceToken);
    arena::internal_rpc::Options options{
        state->config.rpcTimeout,
        arena::internal_rpc::Context{
            deadline,
            std::move(correlation)
        },
        selected->endpoint
    };
    std::weak_ptr<State> weak = state;
    auto accepted = state->client.call(
        std::move(request),
        std::move(options),
        [weak, job](auto response)
        {
            if (auto owner = weak.lock())
                owner->authResponse(job, std::move(response));
        }
    );
    if (!accepted)
    {
        std::lock_guard lock(state->mutex);
        state->auth.erase(job->id);
        OPENSSL_cleanse(job->password.data(), job->password.size());
        return std::unexpected(AuthError::Unavailable);
    }
    {
        std::lock_guard lock(state->mutex);
        job->call = *accepted;
    }
    return Cancellation{[weak,
                         token = std::weak_ptr<State::AuthJob>(job)]
                        {
                            const auto state = weak.lock();
                            const auto job = token.lock();
                            if (!state || !job)
                                return;
                            job->cancelled = true;
                            arena::internal_rpc::Call call;
                            {
                                std::lock_guard lock(state->mutex);
                                call = job->call;
                            }
                            call.cancel();
                        }};
}

bool PersistentStorage::save(
    arena::storage::MatchRecord record, SaveCompletion completion
)
{
    if (!completion || !validMatch(record))
        return false;
    arena::storage::SaveResultRequest request;
    *request.mutable_match() = std::move(record);
    if (request.ByteSizeLong() > RpcClient::maxFrame - 1024)
        return false;
    auto job = std::make_shared<State::ResultJob>();
    job->id = request.match().match_id();
    job->payload = request.SerializeAsString();
    job->completion = std::move(completion);
    const auto required = job->payload.size() + recordOverhead;
    auto state = m_state;
    {
        std::lock_guard lock(state->mutex);
        if (!state->accepting || !state->recovered || !state->healthy)
        {
            ++state->counts.rejected;
            return false;
        }
        const auto found = state->results.find(job->id);
        if (found != state->results.end())
        {
            if (found->second->payload != job->payload ||
                found->second->completion)
            {
                ++state->counts.rejected;
                return false;
            }
            if (found->second->phase == State::Phase::Failed)
            {
                if (state->work.size() >=
                    state->config.maxWorkPending)
                    return false;
                state->work.push_back(
                    [callback = std::move(job->completion)]() mutable
                    { complete(std::move(callback), false); }
                );
            }
            else
                found->second->completion = std::move(
                    job->completion
                );
        }
        else
        {
            if (state->results.size() >=
                    state->config.maxSpoolFiles ||
                required > state->config.maxSpoolBytes - state->bytes)
            {
                ++state->counts.rejected;
                return false;
            }
            state->bytes += required;
            state->results.emplace(job->id, std::move(job));
        }
        state->changed.notify_all();
    }
    return true;
}
bool PersistentStorage::save(
    const service::lobby::ManagedRoom& allocation,
    const service::lobby::RoomSettlement& value,
    SaveCompletion completion
)
{
    if (allocation.matchId != value.matchId ||
        allocation.roomId != value.roomId ||
        value.resultId != value.matchId || value.players.size() != 2)
        return false;
    arena::storage::MatchRecord record;
    record.set_match_id(value.matchId);
    record.set_room_id(value.roomId.value());
    record.set_state(
        value.reason == service::lobby::SettlementReason::Completed
            ? "completed"
            : "aborted"
    );
    record.set_result_version(1);
    record.set_winner_player_id(value.winnerPlayerId);
    record.set_final_tick(value.finalTick);
    record.set_final_checksum(value.finalChecksum);
    record.set_finish_reason(
        value.reason == service::lobby::SettlementReason::Completed
            ? "completed"
        : value.reason ==
                service::lobby::SettlementReason::ReadyTimeout
            ? "ready_timeout"
            : "member_unavailable"
    );
    record.set_created_unix_ms(value.createdUnixMs);
    record.set_started_unix_ms(value.startedUnixMs);
    record.set_finished_unix_ms(value.finishedUnixMs);
    std::uint32_t slot = 0;
    for (const auto& member : allocation.members)
    {
        const auto player = std::find_if(
            value.players.begin(),
            value.players.end(),
            [&](const auto& p)
            {
                return p.member.playerId == member.playerId &&
                       p.member.sessionId == member.sessionId &&
                       p.member.epoch == member.roomEpoch;
            }
        );
        if (player == value.players.end())
            return false;
        auto* row = record.add_participants();
        row->set_player_id(member.playerId.value());
        row->set_session_id(member.sessionId.value());
        row->set_membership_epoch(member.roomEpoch);
        row->set_initial_mmr(member.initialMmr);
        row->set_slot(++slot);
        row->set_last_input_seq(player->sequence);
        row->set_final_x(player->x);
        row->set_final_y(player->y);
        row->set_last_dx(player->lastDx);
        row->set_last_dy(player->lastDy);
    }
    return save(std::move(record), std::move(completion));
}
bool PersistentStorage::requeueFailed(std::uint64_t id)
{
    std::lock_guard lock(m_state->mutex);
    const auto found = m_state->results.find(id);
    if (!m_state->accepting || !m_state->healthy ||
        found == m_state->results.end() ||
        found->second->phase != State::Phase::Failed ||
        found->second->processing || found->second->operatorRequeue)
        return false;
    found->second->operatorRequeue = true;
    m_state->changed.notify_all();
    return true;
}

std::expected<PersistentStorage::Cancellation, ErrorCode>
PersistentStorage::settlement(
    PlayerId player,
    std::uint64_t id,
    Clock::time_point deadline,
    std::string correlation,
    LookupCompletion completion
)
{
    if (!player.value() || !id || id >= (1ULL << 63) || !completion ||
        !correlationValid(correlation))
        return std::unexpected(ErrorCode::ERR_INVALID_REQUEST);
    auto state = m_state;
    const auto selected = state->route();
    if (!selected)
        return std::unexpected(ErrorCode::ERR_UNAVAILABLE);
    auto job = std::make_shared<State::Lookup>();
    job->endpoint = selected->endpoint;
    job->player = player;
    job->matchId = id;
    job->deadline = deadline;
    job->correlation = std::move(correlation);
    job->completion = std::move(completion);
    {
        std::lock_guard lock(state->mutex);
        if (!state->accepting || Clock::now() >= deadline ||
            state->auth.size() + state->lookups.size() >=
                state->config.maxAuth)
            return std::unexpected(ErrorCode::ERR_UNAVAILABLE);
        job->id = ++state->sequence;
        state->lookups.emplace(job->id, job);
    }
    std::weak_ptr<State> weak = state;
    auto finish = [weak, job](LookupResult result) mutable
    {
        const auto owner = weak.lock();
        if (!owner)
            return;
        LookupCompletion callback;
        {
            std::lock_guard lock(owner->mutex);
            const auto found = owner->lookups.find(job->id);
            if (found == owner->lookups.end())
                return;
            callback = std::move(job->completion);
            owner->lookups.erase(found);
        }
        owner->changed.notify_all();
        complete(std::move(callback), std::move(result));
    };
    arena::storage::MatchGetRequest payload;
    payload.set_match_id(id);
    arena::rpc::Request request;
    request.set_method("match.get");
    request.set_payload(payload.SerializeAsString());
    request.set_service_token(state->config.serviceToken);
    auto accepted = state->client.call(
        std::move(request),
        {state->config.rpcTimeout,
         arena::internal_rpc::Context{deadline, job->correlation},
         job->endpoint},
        [weak, job, finish](auto response) mutable
        {
            const auto owner = weak.lock();
            if (!owner)
                return;
            arena::storage::MatchGetReply reply;
            if (job->cancelled || Clock::now() >= job->deadline ||
                response.status() != arena::rpc::OK ||
                !reply.ParseFromString(response.payload()) ||
                !reply.has_match() || !validMatch(reply.match()) ||
                reply.match().match_id() != job->matchId)
                return finish(
                    std::unexpected(ErrorCode::ERR_UNAVAILABLE)
                );
            const auto& record = reply.match();
            if (std::none_of(
                    record.participants().begin(),
                    record.participants().end(),
                    [&](const auto& p)
                    { return p.player_id() == job->player.value(); }
                ))
                return finish(
                    std::unexpected(ErrorCode::ERR_UNAUTHENTICATED)
                );
            job->match = record;
            {
                std::lock_guard lock(owner->mutex);
                job->followup = true;
            }
            arena::storage::SettlementGetRequest payload;
            payload.set_settlement_id(job->matchId);
            arena::rpc::Request request;
            request.set_method("settlement.get");
            request.set_payload(payload.SerializeAsString());
            request.set_service_token(owner->config.serviceToken);
            auto second = owner->client.call(
                std::move(request),
                {owner->config.rpcTimeout,
                 arena::internal_rpc::Context{
                     job->deadline,
                     job->correlation
                 },
                 job->endpoint},
                [job, finish](auto response) mutable
                {
                    arena::storage::SettlementGetReply reply;
                    if (job->cancelled ||
                        Clock::now() >= job->deadline ||
                        response.status() != arena::rpc::OK ||
                        !reply.ParseFromString(response.payload()) ||
                        !reply.has_settlement() ||
                        reply.settlement().match_id() !=
                            job->matchId ||
                        reply.settlement().settlement_id() !=
                            job->matchId ||
                        reply.settlement().event_id().size() != 36)
                        return finish(
                            std::unexpected(
                                ErrorCode::ERR_UNAVAILABLE
                            )
                        );
                    service::lobby::RoomSettlement value{
                        job->matchId,
                        job->matchId,
                        arena::domain::RoomId{job->match.room_id()},
                        job->match.final_tick(),
                        job->match.final_checksum(),
                        service::lobby::SettlementReason::Completed,
                        job->match.winner_player_id(),
                        {}
                    };
                    value.resultId = value.matchId = job->matchId;
                    value.roomId = arena::domain::RoomId{
                        job->match.room_id()
                    };
                    value.finalTick = job->match.final_tick();
                    value.finalChecksum = job->match.final_checksum();
                    value.winnerPlayerId = job->match
                                               .winner_player_id();
                    value.reason =
                        job->match.finish_reason() == "completed"
                            ? service::lobby::SettlementReason::
                                  Completed
                        : job->match.finish_reason() ==
                                "ready_timeout"
                            ? service::lobby::SettlementReason::
                                  ReadyTimeout
                            : service::lobby::SettlementReason::
                                  MemberUnavailable;
                    value.createdUnixMs = job->match
                                              .created_unix_ms();
                    value.startedUnixMs = job->match
                                              .started_unix_ms();
                    value.finishedUnixMs = job->match
                                               .finished_unix_ms();
                    for (const auto& p : job->match.participants())
                        value.players.push_back(
                            service::room::PlayerState{
                                {PlayerId{p.player_id()},
                                 arena::domain::SessionId{
                                     p.session_id()
                                 },
                                 p.membership_epoch()},
                                p.final_x(),
                                p.final_y(),
                                p.last_input_seq(),
                                p.last_dx(),
                                p.last_dy()
                            }
                        );
                    finish(std::move(value));
                }
            );
            if (!second)
                return finish(
                    std::unexpected(ErrorCode::ERR_UNAVAILABLE)
                );
            {
                std::lock_guard lock(owner->mutex);
                job->call = *second;
            }
            if (job->cancelled)
                second->cancel();
        }
    );
    if (!accepted)
    {
        std::lock_guard lock(state->mutex);
        state->lookups.erase(job->id);
        return std::unexpected(ErrorCode::ERR_UNAVAILABLE);
    }
    {
        std::lock_guard lock(state->mutex);
        if (!job->followup)
            job->call = *accepted;
    }
    return Cancellation{[weak,
                         token = std::weak_ptr<State::Lookup>(job)]
                        {
                            const auto owner = weak.lock();
                            const auto job = token.lock();
                            if (!owner || !job)
                                return;
                            job->cancelled = true;
                            arena::internal_rpc::Call call;
                            {
                                std::lock_guard lock(owner->mutex);
                                call = job->call;
                            }
                            call.cancel();
                        }};
}

bool PersistentStorage::ready() const
{
    return metrics().dependencyReady;
}

PersistentStorageMetrics PersistentStorage::metrics() const
{
    const auto active = m_state->client.inflight();
    const auto snapshot = m_state->config.runtime
                              ? m_state->config.runtime->snapshot()
                              : nullptr;
    const auto selected = m_state->route(snapshot);
    std::lock_guard lock(m_state->mutex);
    auto value = m_state->counts;
    value.rpcActive = active;
    value.routeVersion = snapshot ? snapshot->version() : 0;
    value.probeVersion = m_state->healthVersion;
    value.dependencyReady = m_state->accepting &&
                            !m_state->stopping && m_state->healthy &&
                            m_state->recovered && selected &&
                            m_state->remoteReady &&
                            m_state->healthRoute &&
                            selected->nodeId ==
                                m_state->healthRoute->nodeId &&
                            selected->endpoint ==
                                m_state->healthRoute->endpoint &&
                            Clock::now() - m_state->healthObserved <=
                                Milliseconds{1000};
    value.authActive = m_state->auth.size() + m_state->lookups.size();
    value.workPending = m_state->work.size();
    value.idsAvailable = m_state->ids.size();
    value.spoolFiles = m_state->results.size();
    value.spoolBytes = m_state->bytes;
    value.failedResults = std::count_if(
        m_state->results.begin(),
        m_state->results.end(),
        [](const auto& item)
        { return item.second->phase == State::Phase::Failed; }
    );
    value.accepting = m_state->accepting;
    value.recovered = m_state->recovered;
    value.spoolHealthy = m_state->healthy;
    return value;
}
void PersistentStorage::stop() noexcept
{
    auto state = m_state;
    if (!state)
        return;
    {
        std::lock_guard lock(state->mutex);
        state->accepting = false;
        state->stopping = true;
        for (auto& [id, job] : state->auth)
        {
            (void)id;
            job->cancelled = true;
        }
        for (auto& [id, job] : state->lookups)
        {
            (void)id;
            job->cancelled = true;
        }
        state->changed.notify_all();
    }
    try
    {
        if (state->config.runtime || state->config.probeHealth)
            boost::asio::post(
                state->io,
                [weak = state->weak_from_this()]
                {
                    if (auto self = weak.lock())
                    {
                        boost::system::error_code ignored;
                        self->healthTimer.cancel(ignored);
                    }
                }
            );
        state->client.stop();
    }
    catch (...)
    {
    }
    // Callback-initiated destruction requests shutdown; the last
    // worker-owned State reference joins the other thread and
    // detaches only itself.
    if (state->ioThread.get_id() == std::this_thread::get_id() ||
        state->workThread.get_id() == std::this_thread::get_id())
        return;
    std::lock_guard stoppingLock(state->stopMutex);
    if (state->workThread.joinable())
        state->workThread.join();
    if (state->ioThread.joinable())
        state->ioThread.join();
}
} // namespace service::gateway
