#pragma once

#include <array>

#include "arena/domain/Entity.h"

namespace arena::domain
{
namespace detail
{
struct RoomLifecycle
{
    enum class State : std::uint8_t { Forming, Ready, Running, Closing, Closed };
    enum class Event : std::uint8_t { MembersReady, ReadinessLost, StartBattle, BattleEnded, RequestClose, CloseCompleted };
    static constexpr State initial = State::Forming;
    static constexpr OwnerService ownerService = OwnerService::Room;
    static constexpr std::string_view ownerExecutor = "RoomActor owner / tick executor";
    static constexpr std::array states{State::Forming, State::Ready, State::Running, State::Closing, State::Closed};
    static constexpr std::array events{Event::MembersReady, Event::ReadinessLost, Event::StartBattle, Event::BattleEnded, Event::RequestClose, Event::CloseCompleted};
    using Transition = service::state::Rule<State, Event>;
    static constexpr std::array rules{
        Transition{State::Forming, Event::MembersReady, State::Ready},
        Transition{State::Ready, Event::ReadinessLost, State::Forming},
        Transition{State::Ready, Event::StartBattle, State::Running},
        Transition{State::Running, Event::BattleEnded, State::Forming},
        Transition{State::Forming, Event::RequestClose, State::Closing},
        Transition{State::Ready, Event::RequestClose, State::Closing},
        Transition{State::Running, Event::RequestClose, State::Closing},
        Transition{State::Closing, Event::CloseCompleted, State::Closed}
    };
};
} // namespace detail

// Lifecycle model only: trusted owner events, no network or persistence side effects.
class Room final : public Entity<detail::RoomLifecycle, RoomId>
{
    using Base = Entity<detail::RoomLifecycle, RoomId>;
public:
    explicit Room(RoomId id) : Base(id) {}
};
} // namespace arena::domain
