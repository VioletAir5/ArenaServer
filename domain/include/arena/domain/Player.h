#pragma once

#include <array>

#include "arena/domain/Entity.h"

namespace arena::domain
{
namespace detail
{
struct PlayerLifecycle
{
    enum class State : std::uint8_t
    {
        Active,
        Suspended,
        Retired
    };
    enum class Event : std::uint8_t
    {
        Suspend,
        Reinstate,
        Retire
    };
    static constexpr State initial = State::Active;
    static constexpr OwnerService ownerService = OwnerService::Lobby;
    static constexpr std::string_view
        ownerExecutor = "PlayerDirectory shard executor";
    static constexpr std::array states{
        State::Active, State::Suspended, State::Retired
    };
    static constexpr std::array events{
        Event::Suspend, Event::Reinstate, Event::Retire
    };
    using Transition = service::state::Rule<State, Event>;
    static constexpr std::array rules{
        Transition{State::Active, Event::Suspend, State::Suspended},
        Transition{State::Suspended, Event::Reinstate, State::Active},
        Transition{State::Active, Event::Retire, State::Retired},
        Transition{State::Suspended, Event::Retire, State::Retired}
    };
};
} // namespace detail

// Lifecycle model only: trusted owner events, no network or
// persistence side effects.
class Player final : public Entity<detail::PlayerLifecycle, PlayerId>
{
    using Base = Entity<detail::PlayerLifecycle, PlayerId>;

public:
    explicit Player(PlayerId id)
        : Base(id)
    {
    }
};
} // namespace arena::domain
