#pragma once

#include <string_view>

#include "arena/domain/Identity.h"
#include "state/OwnedStateMachine.h"

namespace arena::domain
{
enum class OwnerService { Lobby, Gateway, Match, Room, Storage };
using TransitionError = service::state::TransitionError;

template<typename Definition, typename Id>
class Entity
{
public:
    using State = typename Definition::State;
    using Event = typename Definition::Event;
    using Result = typename service::state::OwnedStateMachine<Definition>::Result;
    static constexpr auto states = Definition::states;
    static constexpr auto events = Definition::events;
    static constexpr auto initialState = Definition::initial;
    static constexpr OwnerService ownerService = Definition::ownerService;
    static constexpr std::string_view ownerExecutor = Definition::ownerExecutor;

    Entity(const Entity&) = delete;
    Entity& operator=(const Entity&) = delete;
    Entity(Entity&&) = delete;
    Entity& operator=(Entity&&) = delete;

    Id id() const noexcept { return m_id; }
    std::thread::id ownerThread() const noexcept { return m_lifecycle.ownerThread(); }
    Result snapshot() const noexcept { return m_lifecycle.snapshot(); }
    Result apply(Event event, std::uint64_t expectedRevision) noexcept
    {
        return m_lifecycle.apply(event, expectedRevision);
    }

protected:
    explicit Entity(Id id) : m_id(id) {}
    // No polymorphic ownership: final domain objects are destroyed as themselves.
    ~Entity() = default;

private:
    const Id m_id;
    service::state::OwnedStateMachine<Definition> m_lifecycle;
};
} // namespace arena::domain
