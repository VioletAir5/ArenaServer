/** @file
 * @brief Stores authenticated session identity.
 */
#pragma once
#include <cstdint>

#include "net/Socket.h"

namespace service::gateway
{
using SessionID = std::uint64_t;
/** @brief Stores authenticated session identity. */
class Session
{

private:
    SessionID _id{};
    Socket _socket;

public:
    /** @brief Constructs the session. */
    Session() = delete;
    /** @brief Constructs the session. */
    Session(SessionID, Socket&&);

    /** @brief Copy construction is disabled. */
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    /** @brief Constructs the session. */
    Session(Session&&) noexcept = default;
    Session& operator=(Session&&) noexcept = default;
    /** @brief Releases owned resources. */
    ~Session() = default;

    SessionID id() const noexcept
    {
        return _id;
    }

    Socket& socket() noexcept
    {
        return _socket;
    }

    const Socket& socket() const noexcept
    {
        return _socket;
    }
};
} // namespace service::gateway
