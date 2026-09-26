#pragma once
#include <cstdint>

#include "net/Socket.h"

namespace service
{
using SessionID = std::uint64_t;
class Session
{

private:
    SessionID _id{};
    Socket _socket;

public:
    Session() = delete;
    Session(SessionID, Socket&&);

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    Session(Session&&) noexcept = default;
    Session& operator=(Session&&) noexcept = default;
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
} // namespace service
