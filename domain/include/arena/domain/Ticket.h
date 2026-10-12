#pragma once

#include <array>

#include "arena/domain/Entity.h"

namespace arena::domain
{
namespace detail
{
struct TicketLifecycle
{
    enum class State : std::uint8_t { Queued, Reserved, Assigned, Cancelled, Expired, Failed };
    enum class Event : std::uint8_t { Reserve, ReleaseReservation, Assign, Cancel, DeadlineExpired, AllocationFailed };
    static constexpr State initial = State::Queued;
    static constexpr OwnerService ownerService = OwnerService::Match;
    static constexpr std::string_view ownerExecutor = "MatchQueue shard executor";
    static constexpr std::array states{State::Queued, State::Reserved, State::Assigned, State::Cancelled, State::Expired, State::Failed};
    static constexpr std::array events{Event::Reserve, Event::ReleaseReservation, Event::Assign, Event::Cancel, Event::DeadlineExpired, Event::AllocationFailed};
    using Transition = service::state::Rule<State, Event>;
    static constexpr std::array rules{
        Transition{State::Queued, Event::Reserve, State::Reserved},
        Transition{State::Reserved, Event::ReleaseReservation, State::Queued},
        Transition{State::Reserved, Event::Assign, State::Assigned},
        Transition{State::Queued, Event::Cancel, State::Cancelled},
        Transition{State::Reserved, Event::Cancel, State::Cancelled},
        Transition{State::Queued, Event::DeadlineExpired, State::Expired},
        Transition{State::Reserved, Event::DeadlineExpired, State::Expired},
        Transition{State::Reserved, Event::AllocationFailed, State::Failed}
    };
};
} // namespace detail

// Lifecycle model only: trusted owner events, no network or persistence side effects.
class Ticket final : public Entity<detail::TicketLifecycle, TicketId>
{
    using Base = Entity<detail::TicketLifecycle, TicketId>;
public:
    Ticket(TicketId id, PlayerId playerId)
        : Base(id), m_playerId(playerId)
    {
    }
    PlayerId playerId() const noexcept { return m_playerId; }

private:
    const PlayerId m_playerId;
};
} // namespace arena::domain
