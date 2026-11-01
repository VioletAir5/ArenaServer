/** @file
 * @brief Keeps remote authentication behind one typed, nonblocking
 * Lobby adapter.
 */
#include "lobby/AuthClient.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "security/SecureErase.h"

namespace service::lobby
{
namespace
{
using namespace std::chrono_literals;
using Status = arena::rpc::Status;
constexpr std::size_t maxPayload = 8192;
constexpr std::int64_t maxJsonInteger = 9007199254740991;

/** @brief Covers copies that remain locally owned on rejected and
 * moved paths. */
struct RequestCleanup
{
    /// Candidate contains either a password or a bearer token.
    arena::auth::AuthenticateRequest& value;
    ~RequestCleanup()
    {
        arena::security::eraseSecret(*value.mutable_credential());
    }
};

struct ReplyCleanup
{
    /// Untransferred signer output must not retain an unused token
    /// copy.
    arena::auth::AuthenticateReply& value;
    ~ReplyCleanup()
    {
        arena::security::eraseSecret(*value.mutable_session_token());
    }
};

/** @brief Rejects invalid bounds before creating any executor work.
 */
AuthClient::Config validated(AuthClient::Config config)
{
    const auto address = config.endpoint.address();
    if (!config.endpoint.port() ||
        !(address == boost::asio::ip::address_v4::loopback() ||
          address == boost::asio::ip::address_v6::loopback()) ||
        config.token.size() < 32 || config.token.size() > 256 ||
        config.maxInflight < 1 || config.maxInflight > 4096 ||
        config.timeout < 1ms || config.timeout > 1000ms)
    {
        arena::security::eraseSecret(config.token);
        throw std::invalid_argument(
            "invalid Lobby Auth client configuration"
        );
    }
    return config;
}

/** @brief Checks credential shape while leaving actual verification
 * to Auth. */
bool validRequest(const arena::auth::AuthenticateRequest& value)
{
    if (!value.proposed_epoch() || value.credential().empty() ||
        value.ByteSizeLong() > maxPayload)
        return false;
    if (value.credential_kind() ==
        arena::auth::AuthenticateRequest::JWT)
        return value.account().empty() &&
               value.credential().size() <= 4096;
    return value.credential_kind() ==
               arena::auth::AuthenticateRequest::PASSWORD &&
           !value.account().empty() &&
           value.account().size() <= 128 &&
           value.credential().size() <= 1024 &&
           std::all_of(
               value.account().begin(),
               value.account().end(),
               [](unsigned char c) { return c >= 32 && c <= 126; }
           );
}

/** @brief Bounds an opaque token without importing verification or
 * signing policy. */
bool tokenShape(std::string_view token)
{
    if (token.empty() || token.size() > 4096)
        return false;
    std::size_t dots = 0, segment = 0;
    for (unsigned char value : token)
    {
        if (value == '.')
        {
            if (!segment || ++dots > 2)
                return false;
            segment = 0;
        }
        else
        {
            if (!((value >= 'A' && value <= 'Z') ||
                  (value >= 'a' && value <= 'z') ||
                  (value >= '0' && value <= '9') || value == '-' ||
                  value == '_'))
                return false;
            ++segment;
        }
    }
    return dots == 2 && segment != 0;
}

/** @brief Checks a signer candidate, not proof that its prior session
 * is current. */
bool validReply(
    const arena::auth::AuthenticateReply& value,
    std::uint64_t proposed,
    bool resumed
)
{
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::system_clock::now()
                             .time_since_epoch()
    )
                         .count();
    if (now < 0 || now > maxJsonInteger - 86400 ||
        !value.player_id() || value.epoch() != proposed ||
        value.expires_at_unix() <= now ||
        value.expires_at_unix() > now + 86400 ||
        !tokenShape(value.session_token()) ||
        value.resumed() != resumed)
        return false;
    if (!resumed)
        return value.previous_epoch() == 0 &&
               value.previous_expires_at_unix() == 0;
    // A shorter replacement lifetime is legal. Lobby separately
    // compares these old claims to its exact retained authority
    // record before installing it.
    return value.previous_epoch() > 0 &&
           value.previous_epoch() < proposed &&
           value.previous_expires_at_unix() > 0 &&
           value.previous_expires_at_unix() <= maxJsonInteger;
}
} // namespace

AuthClient::AuthClient(
    boost::asio::any_io_executor executor, Config config
)
    : m_config(validated(std::move(config)))
    , m_client(
          std::move(executor), m_config.endpoint, m_config.maxInflight
      )
{
}
AuthClient::~AuthClient() noexcept
{
    try
    {
        stop();
    }
    catch (...)
    {
    }
    arena::security::eraseSecret(m_config.token);
}

std::expected<arena::internal_rpc::Call, Status>
AuthClient::authenticate(
    arena::auth::AuthenticateRequest request,
    arena::internal_rpc::Context context,
    Completion completion
)
{
    RequestCleanup cleanup{request};
    if (!completion || !validRequest(request))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    if (context.deadline <= std::chrono::steady_clock::now())
        return std::unexpected(arena::rpc::DEADLINE_EXCEEDED);
    auto endpoint = m_config.endpoint;
    if (m_config.runtime)
    {
        const auto snapshot = m_config.runtime->snapshot();
        const auto selected = snapshot->resolve("auth");
        if (!selected)
            return std::unexpected(arena::rpc::UNAVAILABLE);
        endpoint = {
            boost::asio::ip::make_address(selected->host),
            selected->port
        };
    }
    arena::rpc::Request wire;
    wire.set_method("auth.authenticate");
    wire.set_caller_service("lobby");
    wire.set_target_service("auth");
    wire.set_service_token(m_config.token);
    if (!request.SerializeToString(wire.mutable_payload()))
    {
        arena::security::eraseSecret(*wire.mutable_service_token());
        arena::security::eraseSecret(*wire.mutable_payload());
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    }
    const auto proposed = request.proposed_epoch();
    const bool resumed = request.credential_kind() ==
                         arena::auth::AuthenticateRequest::JWT;
    arena::internal_rpc::Options options;
    options.timeout = m_config.timeout;
    options.context = std::move(context);
    options.endpoint = endpoint;
    return m_client.call(
        std::move(wire),
        std::move(options),
        [proposed, resumed, complete = std::move(completion)](
            arena::rpc::Response response
        ) mutable
        {
            arena::auth::AuthenticateReply reply;
            ReplyCleanup cleanup{reply};
            if (response.status() != arena::rpc::OK)
            {
                arena::security::eraseSecret(
                    *response.mutable_payload()
                );
                complete(std::unexpected(response.status()));
                return;
            }
            const bool parsed = response.payload().size() <=
                                    maxPayload &&
                                reply.ParseFromString(
                                    response.payload()
                                );
            arena::security::eraseSecret(*response.mutable_payload());
            if (!parsed || !validReply(reply, proposed, resumed))
            {
                complete(std::unexpected(arena::rpc::INTERNAL));
                return;
            }
            reply.DiscardUnknownFields();
            complete(Result(std::move(reply)));
        }
    );
}
void AuthClient::stop()
{
    m_client.stop();
}
std::size_t AuthClient::inflight() const
{
    return m_client.inflight();
}
} // namespace service::lobby
