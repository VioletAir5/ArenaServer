#pragma once

#include <array>

#include "arena/domain/Entity.h"

namespace arena::domain
{
namespace detail
{
struct BattleLifecycle
{
    enum class State : std::uint8_t { Prepared, Running, Finished, Aborted };
    enum class Event : std::uint8_t { Start, Finish, Abort };
    static constexpr State initial = State::Prepared;
    static constexpr OwnerService ownerService = OwnerService::Room;
    static constexpr std::string_view ownerExecutor = "Owning RoomActor / same tick executor";
    static constexpr std::array states{State::Prepared, State::Running, State::Finished, State::Aborted};
    static constexpr std::array events{Event::Start, Event::Finish, Event::Abort};
    using Transition = service::state::Rule<State, Event>;
    static constexpr std::array rules{
        Transition{State::Prepared, Event::Start, State::Running},
        Transition{State::Running, Event::Finish, State::Finished},
        Transition{State::Prepared, Event::Abort, State::Aborted},
        Transition{State::Running, Event::Abort, State::Aborted}
    };
};
} // namespace detail

// Lifecycle model only: trusted owner events, no network or persistence side effects.
class Battle final : public Entity<detail::BattleLifecycle, BattleId>
{
    using Base = Entity<detail::BattleLifecycle, BattleId>;
public:
    Battle(BattleId id, RoomId roomId)
        : Base(id), m_roomId(roomId)
    {
    }
    RoomId roomId() const noexcept { return m_roomId; }

private:
    const RoomId m_roomId;
};
} // namespace arena::domain
