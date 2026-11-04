/** @file
 * @brief Implements bounded Gateway-to-Lobby RPC and passive presence
 * tracking.
 */
#include "net/LobbyClient.h"

#include <algorithm>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include "runtime/RuntimeConfig.h"
#include "security/SecureErase.h"

namespace service::gateway
{
namespace
{
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Status = arena::rpc::Status;
using Binding = service::lobby::SessionBinding;
std::int64_t unixSeconds()
{
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()
    )
        .count();
}
bool same(const Binding& a, const Binding& b)
{
    return a.playerId == b.playerId &&
           a.connectionId == b.connectionId && a.epoch == b.epoch;
}
void closeSafely(std::function<void()> close) noexcept
{
    try
    {
        if (close)
            close();
    }
    catch (...)
    {
    }
}
void scrub(arena::protocol::Envelope& message) noexcept
{
    if (message.has_login_request())
        arena::security::eraseSecret(
            *message.mutable_login_request()->mutable_credential()
        );
    if (message.has_login_response())
        arena::security::eraseSecret(
            *message.mutable_login_response()->mutable_session_token()
        );
}
struct InputCleanup
{
    arena::lobby::ExecuteRequest& input;
    ~InputCleanup()
    {
        if (input.has_message())
            scrub(*input.mutable_message());
    }
};
struct ResponseCleanup
{
    arena::rpc::Response& response;
    ~ResponseCleanup()
    {
        if (!response.payload().empty())
            arena::security::eraseSecret(*response.mutable_payload());
    }
};
unsigned expectedReply(const arena::protocol::Envelope& message)
{
    const auto payload = static_cast<unsigned>(
        message.payload_case()
    );
    if (message.msg_id() && message.msg_id() != payload)
        return 0;
    switch (payload)
    {
    case arena::protocol::MSG_LOGIN_REQUEST:
    case arena::protocol::MSG_SESSION_STATE_REQUEST:
    case arena::protocol::MSG_ROOM_JOIN_REQUEST:
    case arena::protocol::MSG_ROOM_LEAVE_REQUEST:
    case arena::protocol::MSG_MATCH_JOIN_REQUEST:
    case arena::protocol::MSG_MATCH_CANCEL_REQUEST:
    case arena::protocol::MSG_MATCH_STATUS_REQUEST:
    case arena::protocol::MSG_ROOM_INPUT_REQUEST:
    case arena::protocol::MSG_ROOM_SNAPSHOT_REQUEST:
    case arena::protocol::MSG_ROOM_READY_REQUEST:
    case arena::protocol::MSG_ROOM_STATUS_REQUEST:
    case arena::protocol::MSG_SETTLEMENT_REQUEST:
        return payload + 1;
    default:
        return 0;
    }
}
struct Expected
{
    std::uint64_t requestId, player, epoch;
    unsigned message;
};
bool validReply(
    const arena::lobby::ExecuteReply& reply, const Expected& expected
)
{
    if (!reply.has_message() || !reply.committed_revision())
        return false;
    const auto& message = reply.message();
    const auto payload = static_cast<unsigned>(
        message.payload_case()
    );
    if (message.request_id() != expected.requestId ||
        (message.msg_id() && message.msg_id() != payload))
        return false;

    if (reply.has_session())
    {
        const auto& session = reply.session();
        if (!session.player_id() || !session.epoch() ||
            !session.session_id() ||
            session.expires_at_unix() <= unixSeconds())
            return false;
        if (expected.message != arena::protocol::MSG_LOGIN_RESPONSE &&
            (session.player_id() != expected.player ||
             session.epoch() != expected.epoch))
            return false;
    }
    if (message.has_error())
        return message.error().code() &&
               arena::protocol::ErrorCode_IsValid(
                   static_cast<int>(message.error().code())
               );
    if (payload != expected.message)
        return false;
    if (message.has_login_response())
    {
        const auto& login = message.login_response();
        return reply.has_session() && login.accepted() &&
               !login.session_token().empty() &&
               login.session_token().size() <= 4096 &&
               login.player_id() == reply.session().player_id() &&
               login.session_epoch() == reply.session().epoch() &&
               login.session_id() == reply.session().session_id() &&
               (!login.has_error() || !login.error().code());
    }
    return true;
}
} // namespace

struct LobbyClient::State : std::enable_shared_from_this<State>
{
    struct Tracked
    {
        Binding binding;
        std::int64_t expiresAt;
        std::shared_ptr<std::atomic<bool>> valid;
        std::function<void()> close;
    };
    using Flags = std::
        map<std::uint64_t, std::shared_ptr<std::atomic<bool>>>;
    Config config;
    boost::asio::strand<boost::asio::any_io_executor> strand;
    arena::internal_rpc::Client business, control, probeClient;
    boost::asio::steady_timer timer;
    mutable std::mutex mutex;
    std::map<std::uint64_t, Tracked> connections;
    std::atomic<bool> stopping{false}, timerPending{false},
        dependencyReady{false};
    std::atomic<std::uint64_t> probeVersion{0};
    std::atomic<std::size_t> probeActive{0};
    std::atomic<std::size_t> businessActive{0}, controlActive{0},
        posts{0};
    std::uint64_t cursor{0}, heartbeats{0}, revoked{0};

    State(boost::asio::any_io_executor executor, Config configured)
        : config(std::move(configured))
        , strand(boost::asio::make_strand(std::move(executor)))
        , business(strand, config.endpoint, config.maxInflight)
        , control(strand, config.endpoint, 1)
        , probeClient(strand, config.endpoint, 1)
        , timer(strand)
    {
        if (!config.endpoint.address().is_loopback() ||
            !config.endpoint.port() ||
            config.serviceToken.size() < 32 ||
            config.serviceToken.size() > 256 ||
            config.gatewayInstance.size() != 32 ||
            !std::all_of(
                config.gatewayInstance.begin(),
                config.gatewayInstance.end(),
                [](char c)
                {
                    return (c >= '0' && c <= '9') ||
                           (c >= 'a' && c <= 'f');
                }
            ) ||
            config.maxInflight < 1 || config.maxInflight > 1024 ||
            config.maxConnections < 1 ||
            config.maxConnections > 100000 || config.timeout < 1ms ||
            config.timeout > 1000ms)
            throw std::invalid_argument(
                "invalid Lobby client configuration"
            );
    }
    ~State()
    {
        arena::security::eraseSecret(config.serviceToken);
    }
    struct Route
    {
        boost::asio::ip::tcp::endpoint endpoint;
        std::uint64_t version;
        std::string nodeId;
        bool operator==(const Route&) const = default;
    };
    std::optional<Route> route() const
    {
        if (!config.runtime)
            return Route{config.endpoint, 0, "startup"};
        const auto snapshot = config.runtime->snapshot();
        const auto node = snapshot->resolve("lobby");
        if (!node)
            return std::nullopt;
        return Route{
            {boost::asio::ip::make_address(node->host), node->port},
            snapshot->version(),
            node->id
        };
    }
    void probe()
    {
        if (stopping.load() || probeActive.load())
            return;
        const auto selected = route();
        if (!selected)
        {
            dependencyReady = false;
            return;
        }
        arena::rpc::Request request;
        request.set_method("health.ready");
        request.set_protocol_version(
            arena::internal_rpc::protocolVersion
        );
        request.set_caller_service("gateway");
        request.set_target_service("lobby");
        request.set_service_token(config.serviceToken);
        const auto budget = std::min(config.timeout, 300ms);
        arena::internal_rpc::Options options;
        options.timeout = budget;
        options.endpoint = selected->endpoint;
        options.context = Context{
            Clock::now() + budget,
            "gateway_lobby_ready",
            {"gateway"}
        };
        ++probeActive;
        try
        {
            auto accepted = probeClient.call(
                std::move(request),
                std::move(options),
                [self = shared_from_this(),
                 selected = *selected](arena::rpc::Response response)
                {
                    struct Done
                    {
                        std::atomic<std::size_t>& active;
                        ~Done()
                        {
                            --active;
                        }
                    } done{self->probeActive};
                    ResponseCleanup cleanup{response};
                    // Qualify the entire reply before publishing
                    // readiness. Readers on Gateway reactors must not
                    // see a temporary false on success.
                    bool qualified = false;
                    try
                    {
                        if (!self->stopping.load())
                        {
                            const auto current = self->route();
                            qualified = response.status() ==
                                            arena::rpc::OK &&
                                        response.protocol_version() ==
                                            arena::internal_rpc::
                                                protocolVersion &&
                                        response.payload().empty() &&
                                        current &&
                                        *current == selected;
                        }
                    }
                    catch (...)
                    {
                        // Resolver/allocation failures must revoke
                        // the prior result.
                        qualified = false;
                    }
                    qualified = qualified && !self->stopping.load();
                    if (qualified)
                        self->probeVersion.store(
                            selected.version,
                            std::memory_order_release
                        );
                    self->dependencyReady.store(
                        qualified,
                        std::memory_order_release
                    );
                }
            );
            if (!accepted)
            {
                --probeActive;
                dependencyReady = false;
            }
        }
        catch (...)
        {
            --probeActive;
            dependencyReady = false;
        }
    }
    bool gateway(std::string& supplied) const
    {
        if (supplied.empty())
            supplied = config.gatewayInstance;
        return supplied == config.gatewayInstance;
    }
    template <
        class Request,
        class Reply,
        class Validate,
        class Completion>
    std::expected<Call, Status> send(
        const Request& input,
        Context context,
        bool heartbeat,
        Validate validate,
        Completion completion
    )
    {
        if (stopping.load())
            return std::unexpected(arena::rpc::UNAVAILABLE);
        const auto endpoint = route();
        if (!endpoint)
            return std::unexpected(arena::rpc::UNAVAILABLE);
        arena::rpc::Request request;
        if constexpr (
            std::is_same_v<Request, arena::lobby::ExecuteRequest>
        )
            request.set_method("lobby.execute");
        else if constexpr (
            std::is_same_v<Request, arena::lobby::DetachRequest>
        )
            request.set_method("lobby.detach");
        else
            request.set_method("lobby.heartbeat");
        request.set_protocol_version(
            arena::internal_rpc::protocolVersion
        );
        request.set_caller_service("gateway");
        request.set_target_service("lobby");
        request.set_service_token(config.serviceToken);
        if (!input.SerializeToString(request.mutable_payload()))
            return std::unexpected(arena::rpc::INVALID_ARGUMENT);
        arena::internal_rpc::Options options;
        options.timeout = config.timeout;
        options.context = std::move(context);
        options.endpoint = endpoint->endpoint;
        auto& count = heartbeat ? controlActive : businessActive;
        struct Admission
        {
            std::atomic<std::size_t>& count;
            bool transferred{false};
            explicit Admission(std::atomic<std::size_t>& value)
                : count(value)
            {
                ++count;
            }
            ~Admission()
            {
                if (!transferred)
                    --count;
            }
        } admission{count};
        auto accepted =
            (heartbeat ? control : business)
                .call(
                    std::move(request),
                    std::move(options),
                    [self = shared_from_this(),
                     heartbeat,
                     validate = std::move(validate),
                     completion = std::move(completion)](
                        arena::rpc::Response response
                    ) mutable
                    {
                        struct Done
                        {
                            std::atomic<std::size_t>& count;
                            ~Done()
                            {
                                --count;
                            }
                        } done{
                            heartbeat ? self->controlActive
                                      : self->businessActive
                        };
                        ResponseCleanup cleanup{response};
                        if (response.status() != arena::rpc::OK)
                            return completion(
                                std::unexpected(response.status())
                            );
                        Reply reply;
                        if (!reply.ParseFromString(
                                response.payload()
                            ) ||
                            !validate(reply))
                        {
                            if constexpr (
                                std::is_same_v<
                                    Reply,
                                    arena::lobby::ExecuteReply>
                            )
                                if (reply.has_message())
                                    scrub(*reply.mutable_message());
                            return completion(
                                std::unexpected(arena::rpc::INTERNAL)
                            );
                        }
                        if constexpr (
                            std::is_same_v<
                                Reply,
                                arena::lobby::ExecuteReply>
                        )
                        {
                            struct Cleanup
                            {
                                Reply& value;
                                ~Cleanup()
                                {
                                    if (value.has_message())
                                        scrub(
                                            *value.mutable_message()
                                        );
                                }
                            } owned{reply};
                            completion(std::move(reply));
                        }
                        else
                            completion(std::move(reply));
                    }
                );
        admission.transferred = bool(accepted);
        return accepted;
    }
    std::expected<Call, Status> execute(
        arena::lobby::ExecuteRequest input,
        Context context,
        ExecuteCompletion completion
    )
    {
        InputCleanup cleanup{input};
        if (!completion)
            return std::unexpected(arena::rpc::INVALID_ARGUMENT);
        if (!gateway(*input.mutable_gateway_instance()) ||
            !input.connection_id() || !input.has_message() ||
            !input.message().request_id())
            return std::unexpected(arena::rpc::INVALID_ARGUMENT);
        const auto expected = expectedReply(input.message());
        if (!expected)
            return std::unexpected(arena::rpc::INVALID_ARGUMENT);
        if (expected != arena::protocol::MSG_LOGIN_RESPONSE &&
            (!input.player_id() || !input.session_epoch()))
            return std::unexpected(arena::rpc::UNAUTHENTICATED);
        const Expected match{
            input.message().request_id(),
            input.player_id(),
            input.session_epoch(),
            expected
        };
        return send<
            arena::lobby::ExecuteRequest,
            arena::lobby::ExecuteReply>(
            input,
            std::move(context),
            false,
            [match](const auto& reply)
            { return validReply(reply, match); },
            std::move(completion)
        );
    }
    std::expected<Call, Status> detach(
        arena::lobby::DetachRequest input,
        Context context,
        DetachCompletion completion
    )
    {
        if (!completion)
            return std::unexpected(arena::rpc::INVALID_ARGUMENT);
        if (!gateway(*input.mutable_gateway_instance()) ||
            !input.connection_id() || !input.player_id() ||
            !input.session_epoch())
            return std::unexpected(arena::rpc::INVALID_ARGUMENT);
        return send<
            arena::lobby::DetachRequest,
            arena::lobby::DetachReply>(
            input,
            std::move(context),
            false,
            [](const auto& reply)
            { return reply.committed_revision() != 0; },
            std::move(completion)
        );
    }
    void revoke(
        std::uint64_t id,
        const std::shared_ptr<std::atomic<bool>>& expected
    )
    {
        std::function<void()> close;
        {
            std::lock_guard lock(mutex);
            const auto found = connections.find(id);
            if (found == connections.end() ||
                found->second.valid != expected ||
                !expected->exchange(false))
                return;
            ++revoked;
            close = std::move(found->second.close);
        }
        closeSafely(std::move(close));
    }
    std::expected<Call, Status> heartbeat(
        arena::lobby::HeartbeatRequest input,
        Context context,
        HeartbeatCompletion completion
    )
    {
        if (!completion)
            return std::unexpected(arena::rpc::INVALID_ARGUMENT);
        if (!gateway(*input.mutable_gateway_instance()) ||
            input.connection_ids_size() > 512)
            return std::unexpected(arena::rpc::INVALID_ARGUMENT);
        std::set<std::uint64_t> ids;
        for (const auto id : input.connection_ids())
            if (!id || !ids.insert(id).second)
                return std::unexpected(arena::rpc::INVALID_ARGUMENT);
        Flags flags;
        {
            std::lock_guard lock(mutex);
            for (const auto id : ids)
                if (const auto found = connections.find(id);
                    found != connections.end())
                    flags.emplace(id, found->second.valid);
        }
        return send<
            arena::lobby::HeartbeatRequest,
            arena::lobby::HeartbeatReply>(
            input,
            std::move(context),
            true,
            [ids = std::move(ids)](const auto& reply)
            {
                if (!reply.committed_revision() ||
                    reply.revoked_connection_ids_size() > 512)
                    return false;
                std::set<std::uint64_t> seen;
                for (const auto id : reply.revoked_connection_ids())
                    if (!ids.contains(id) || !seen.insert(id).second)
                        return false;
                return true;
            },
            [self = shared_from_this(),
             flags = std::move(flags),
             completion = std::move(completion)](
                HeartbeatResult result
            ) mutable
            {
                if (result)
                {
                    {
                        std::lock_guard lock(self->mutex);
                        ++self->heartbeats;
                    }
                    for (const auto id :
                         result->revoked_connection_ids())
                        if (const auto flag = flags.find(id);
                            flag != flags.end())
                            self->revoke(id, flag->second);
                }
                completion(std::move(result));
            }
        );
    }
    void arm()
    {
        if (stopping.load())
            return;
        timer.expires_after(250ms);
        timerPending = true;
        timer.async_wait(
            [weak = weak_from_this()](boost::system::error_code error)
            {
                const auto self = weak.lock();
                if (!self)
                    return;
                self->timerPending = false;
                if (error || self->stopping.load())
                    return;
                self->probe();
                arena::lobby::HeartbeatRequest request;
                {
                    std::lock_guard lock(self->mutex);
                    auto current = self->connections.upper_bound(
                        self->cursor
                    );
                    const auto size = self->connections.size();
                    for (std::size_t visited = 0;
                         visited < size &&
                         request.connection_ids_size() < 512;
                         ++visited)
                    {
                        if (current == self->connections.end())
                            current = self->connections.begin();
                        self->cursor = current->first;
                        if (current->second.valid->load())
                            request.add_connection_ids(
                                current->first
                            );
                        ++current;
                    }
                }
                if (request.connection_ids_size())
                    self->heartbeat(
                        std::move(request),
                        Context{
                            Clock::now() + self->config.timeout,
                            "gateway_presence",
                            {"gateway"}
                        },
                        [](auto) {}
                    );
                self->arm();
            }
        );
    }
    void start()
    {
        ++posts;
        try
        {
            boost::asio::post(
                strand,
                [self = shared_from_this()]
                {
                    --self->posts;
                    self->probe();
                    self->arm();
                }
            );
        }
        catch (...)
        {
            --posts;
            throw;
        }
    }
    void stop() noexcept
    {
        if (stopping.exchange(true))
            return;
        std::map<std::uint64_t, Tracked> retired;
        {
            std::lock_guard lock(mutex);
            retired.swap(connections);
            for (auto& [id, entry] : retired)
            {
                (void)id;
                entry.valid->store(false);
            }
        }
        dependencyReady = false;
        business.stop();
        control.stop();
        probeClient.stop();
        ++posts;
        try
        {
            boost::asio::post(
                strand,
                [self = shared_from_this()]
                {
                    --self->posts;
                    boost::system::error_code ignored;
                    self->timer.cancel(ignored);
                }
            );
        }
        catch (...)
        {
            --posts;
        }
        for (auto& [id, entry] : retired)
        {
            (void)id;
            closeSafely(std::move(entry.close));
        }
    }
};

LobbyClient::LobbyClient(
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
LobbyClient::~LobbyClient() noexcept
{
    stop();
}
std::expected<LobbyClient::Call, LobbyClient::Status>
LobbyClient::execute(
    arena::lobby::ExecuteRequest request,
    Context context,
    ExecuteCompletion completion
)
{
    return m_state->execute(
        std::move(request),
        std::move(context),
        std::move(completion)
    );
}
std::expected<LobbyClient::Call, LobbyClient::Status>
LobbyClient::detach(
    arena::lobby::DetachRequest request,
    Context context,
    DetachCompletion completion
)
{
    return m_state->detach(
        std::move(request),
        std::move(context),
        std::move(completion)
    );
}
std::expected<LobbyClient::Call, LobbyClient::Status>
LobbyClient::heartbeat(
    arena::lobby::HeartbeatRequest request,
    Context context,
    HeartbeatCompletion completion
)
{
    return m_state->heartbeat(
        std::move(request),
        std::move(context),
        std::move(completion)
    );
}
std::expected<std::shared_ptr<std::atomic<bool>>, LobbyClient::Status>
LobbyClient::track(
    Binding binding,
    std::int64_t expiresAt,
    std::function<void()> close
)
{
    if (!binding.connectionId || !binding.epoch ||
        expiresAt <= unixSeconds() || !close)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    auto state = m_state;
    std::lock_guard lock(state->mutex);
    if (state->stopping.load())
        return std::unexpected(arena::rpc::UNAVAILABLE);
    if (const auto found = state->connections.find(
            binding.connectionId
        );
        found != state->connections.end())
    {
        if (!same(found->second.binding, binding) ||
            found->second.expiresAt != expiresAt)
            return std::unexpected(arena::rpc::CONFLICT);
        return found->second.valid;
    }
    if (state->connections.size() >= state->config.maxConnections)
        return std::unexpected(arena::rpc::OVERLOADED);
    auto valid = std::make_shared<std::atomic<bool>>(true);
    state->connections.emplace(
        binding.connectionId,
        State::Tracked{binding, expiresAt, valid, std::move(close)}
    );
    return valid;
}
void LobbyClient::untrack(Binding binding, bool sendDetach) noexcept
{
    auto state = m_state;
    {
        std::lock_guard lock(state->mutex);
        const auto found = state->connections.find(
            binding.connectionId
        );
        if (found == state->connections.end() ||
            !same(found->second.binding, binding))
            return;
        found->second.valid->store(false);
        state->connections.erase(found);
    }
    if (sendDetach && !state->stopping.load())
        try
        {
            arena::lobby::DetachRequest request;
            request.set_connection_id(binding.connectionId);
            request.set_player_id(binding.playerId.value());
            request.set_session_epoch(binding.epoch);
            state->detach(
                std::move(request),
                Context{
                    Clock::now() + state->config.timeout,
                    "detach_" + std::to_string(binding.connectionId),
                    {"gateway"}
                },
                [](auto) {}
            );
        }
        catch (...)
        {
        }
}
void LobbyClient::invalidate(Binding binding) noexcept
{
    try
    {
        std::shared_ptr<std::atomic<bool>> flag;
        {
            std::lock_guard lock(m_state->mutex);
            const auto found = m_state->connections.find(
                binding.connectionId
            );
            if (found == m_state->connections.end() ||
                !same(found->second.binding, binding))
                return;
            flag = found->second.valid;
        }
        m_state->revoke(binding.connectionId, flag);
    }
    catch (...)
    {
    }
}
void LobbyClient::stop() noexcept
{
    m_state->stop();
}
LobbyClient::Stats LobbyClient::stats() const noexcept
{
    auto state = m_state;
    Stats result;
    {
        std::lock_guard lock(state->mutex);
        result.tracked = state->connections.size();
        result.heartbeats = state->heartbeats;
        result.revoked = state->revoked;
    }
    result.businessInflight = state->businessActive.load();
    result.heartbeatInflight = state->controlActive.load();
    result.probeInflight = state->probeActive.load();
    const auto version = state->config.runtime
                             ? state->config.runtime->snapshot()
                                   ->version()
                             : 0;
    result.dependencyReady = !state->stopping.load() &&
                             state->dependencyReady.load(
                                 std::memory_order_acquire
                             ) &&
                             state->probeVersion.load(
                                 std::memory_order_acquire
                             ) == version;
    result.ownerPosts = state->posts.load();
    result.accepting = !state->stopping.load();
    result.stopped = !result.accepting && !result.businessInflight &&
                     !result.heartbeatInflight &&
                     !result.probeInflight && !result.ownerPosts &&
                     !state->timerPending.load();
    return result;
}
} // namespace service::gateway
