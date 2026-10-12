#pragma once

#include <array>

#include "arena/domain/Entity.h"

namespace arena::domain
{
namespace detail
{
struct SettlementLifecycle
{
    enum class State : std::uint8_t { Pending, Applying, RetryWait, Committed, Failed };
    enum class Event : std::uint8_t { BeginApply, CommitConfirmed, RetryableFailure, RetryDue, PermanentFailure };
    static constexpr State initial = State::Pending;
    static constexpr OwnerService ownerService = OwnerService::Storage;
    static constexpr std::string_view ownerExecutor = "SettlementCoordinator shard executor";
    static constexpr std::array states{State::Pending, State::Applying, State::RetryWait, State::Committed, State::Failed};
    static constexpr std::array events{Event::BeginApply, Event::CommitConfirmed, Event::RetryableFailure, Event::RetryDue, Event::PermanentFailure};
    using Transition = service::state::Rule<State, Event>;
    static constexpr std::array rules{
        Transition{State::Pending, Event::BeginApply, State::Applying},
        Transition{State::Applying, Event::CommitConfirmed, State::Committed},
        Transition{State::Applying, Event::RetryableFailure, State::RetryWait},
        Transition{State::RetryWait, Event::RetryDue, State::Applying},
        Transition{State::Applying, Event::PermanentFailure, State::Failed},
        Transition{State::RetryWait, Event::PermanentFailure, State::Failed}
    };
};
} // namespace detail

// Lifecycle model only: trusted owner events, no network or persistence side effects.
class Settlement final : public Entity<detail::SettlementLifecycle, SettlementId>
{
    using Base = Entity<detail::SettlementLifecycle, SettlementId>;
public:
    Settlement(SettlementId id, BattleId battleId)
        : Base(id), m_battleId(battleId)
    {
    }
    BattleId battleId() const noexcept { return m_battleId; }

private:
    const BattleId m_battleId;
};
} // namespace arena::domain
