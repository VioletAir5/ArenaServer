/** @file
 * @brief Implements route registration and stable application error
 * descriptions.
 */
#include "net/MessageRegistry.h"

#include <string>

namespace service::gateway
{
ErrorCode normalizeErrorCode(ErrorCode code) noexcept
{
    return code != arena::protocol::ERR_OK &&
                   arena::protocol::ErrorCode_IsValid(
                       static_cast<int>(code)
                   )
               ? code
               : arena::protocol::ERR_INTERNAL;
}

std::string_view errorMessage(ErrorCode code) noexcept
{
    switch (code)
    {
    case arena::protocol::ERR_UNKNOWN_MESSAGE:
        return "unknown message";
    case arena::protocol::ERR_INVALID_REQUEST:
        return "invalid request";
    case arena::protocol::ERR_UNAUTHENTICATED:
        return "authentication required";
    case arena::protocol::ERR_TIMEOUT:
        return "request timed out";
    case arena::protocol::ERR_TOO_MANY_REQUESTS:
        return "too many requests";
    case arena::protocol::ERR_DUPLICATE_REQUEST:
        return "duplicate request id";
    case arena::protocol::ERR_BUSINESS_FAILURE:
        return "business failure";
    case arena::protocol::ERR_UNAVAILABLE:
        return "service unavailable";
    default:
        return "internal error";
    }
}

BusinessError::BusinessError(ErrorCode code)
    : std::runtime_error(
          std::string(errorMessage(normalizeErrorCode(code)))
      )
    , m_code(normalizeErrorCode(code))
{
}

void MessageRegistry::registerHandler(
    MsgId id, MessageHandler handler, MessageRouteOptions options
)
{
    if (m_sealed)
        throw std::logic_error("message registry is sealed");
    if (id <= arena::protocol::MSG_UNSPECIFIED ||
        id == arena::protocol::MSG_LOGIN_RESPONSE ||
        id == arena::protocol::MSG_PONG ||
        id == arena::protocol::MSG_ERROR || !handler ||
        id == arena::protocol::MSG_SESSION_STATE_RESPONSE ||
        id == arena::protocol::MSG_ROOM_JOIN_RESPONSE ||
        id == arena::protocol::MSG_ROOM_LEAVE_RESPONSE ||
        id == arena::protocol::MSG_MATCH_JOIN_RESPONSE ||
        id == arena::protocol::MSG_MATCH_CANCEL_RESPONSE ||
        id == arena::protocol::MSG_MATCH_STATUS_RESPONSE ||
        options.timeout <= std::chrono::milliseconds::zero() ||
        options.timeout > std::chrono::hours(1))
        throw std::invalid_argument("invalid message route");
    if (!m_routes
             .emplace(
                 static_cast<std::uint32_t>(id),
                 Route{std::move(handler), options}
             )
             .second)
        throw std::invalid_argument("duplicate message route");
}

const MessageRegistry::Route*
MessageRegistry::find(std::uint32_t id) const
{
    if (!m_sealed)
        throw std::logic_error("message registry is not sealed");
    const auto found = m_routes.find(id);
    return found != m_routes.end() ? &found->second : nullptr;
}
} // namespace service::gateway
