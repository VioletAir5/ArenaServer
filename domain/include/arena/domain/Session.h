#pragma once

#include <array>

#include "arena/domain/Entity.h"

namespace arena::domain
{
namespace detail
{
struct SessionLifecycle
{
    enum class State : std::uint8_t { Online, Detached, Expired, Revoked };
    enum class Event : std::uint8_t { TransportLost, ResumeAuthorized, LeaseExpired, Revoke };
    static constexpr State initial = State::Online;
    static constexpr OwnerService ownerService = OwnerService::Gateway;
    static constexpr std::string_view ownerExecutor = "SessionRegistry shard executor";
    static constexpr std::array states{State::Online, State::Detached, State::Expired, State::Revoked};
    static constexpr std::array events{Event::TransportLost, Event::ResumeAuthorized, Event::LeaseExpired, Event::Revoke};
    using Transition = service::state::Rule<State, Event>;
    static constexpr std::array rules{
        Transition{State::Online, Event::TransportLost, State::Detached},
        Transition{State::Detached, Event::ResumeAuthorized, State::Online},
        Transition{State::Online, Event::LeaseExpired, State::Expired},
        Transition{State::Detached, Event::LeaseExpired, State::Expired},
        Transition{State::Online, Event::Revoke, State::Revoked},
        Transition{State::Detached, Event::Revoke, State::Revoked}
    };
};
} // namespace detail

// Lifecycle model only: trusted owner events, no network or persistence side effects.
class Session final : public Entity<detail::SessionLifecycle, SessionId>
{
    using Base = Entity<detail::SessionLifecycle, SessionId>;
public:
    Session(SessionId id, PlayerId playerId)
        : Base(id), m_playerId(playerId)
    {
    }
    PlayerId playerId() const noexcept { return m_playerId; }

private:
    const PlayerId m_playerId;
};
} // namespace arena::domain
