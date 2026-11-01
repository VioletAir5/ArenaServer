#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <stdexcept>
#include <thread>
#include <type_traits>

namespace service::state
{
enum class TransitionError
{
    WrongThread,
    InvalidEvent,
    StaleRevision,
    IllegalTransition,
    RevisionExhausted
};

template <typename State> struct Snapshot
{
    State state;
    std::uint64_t revision;
    bool operator==(const Snapshot&) const = default;
};

template <typename State, typename Event> struct Rule
{
    State from;
    Event event;
    State to;
};

namespace detail
{
template <typename Values, typename Value>
constexpr bool contains(const Values& values, Value value)
{
    for (const auto candidate : values)
        if (candidate == value)
            return true;
    return false;
}

// A definition is a closed, deterministic graph; reject ambiguous
// rules and undeclared states/events at compile time.
template <typename Definition> consteval bool validDefinition()
{
    if (!contains(Definition::states, Definition::initial))
        return false;
    for (std::size_t i = 0; i < Definition::states.size(); ++i)
        for (std::size_t j = i + 1; j < Definition::states.size();
             ++j)
            if (Definition::states[i] == Definition::states[j])
                return false;
    for (std::size_t i = 0; i < Definition::events.size(); ++i)
        for (std::size_t j = i + 1; j < Definition::events.size();
             ++j)
            if (Definition::events[i] == Definition::events[j])
                return false;
    for (std::size_t i = 0; i < Definition::rules.size(); ++i)
    {
        const auto rule = Definition::rules[i];
        if (!contains(Definition::states, rule.from) ||
            !contains(Definition::states, rule.to) ||
            !contains(Definition::events, rule.event))
            return false;
        for (std::size_t j = i + 1; j < Definition::rules.size(); ++j)
            if (Definition::rules[j].from == rule.from &&
                Definition::rules[j].event == rule.event)
                return false;
    }
    return true;
}
/** @brief Tests exact transition-count reachability without replaying
 * events. */
template <typename Definition>
bool reachableAtRevision(
    typename Definition::State target, std::uint64_t revision
) noexcept
{
    constexpr auto size = Definition::states.size();
    using Reachable = std::array<bool, size>;
    using Matrix = std::array<Reachable, size>;
    const auto index = [](typename Definition::State state)
    {
        for (std::size_t i = 0; i < size; ++i)
            if (Definition::states[i] == state)
                return i;
        return size;
    };
    const auto destination = index(target);
    if (destination == size)
        return false;
    Matrix edges{};
    for (const auto rule : Definition::rules)
        edges[index(rule.from)][index(rule.to)] = true;
    Reachable reached{};
    reached[index(Definition::initial)] = true;
    while (revision)
    {
        if (revision & 1)
        {
            Reachable next{};
            for (std::size_t from = 0; from < size; ++from)
                for (std::size_t to = 0; to < size; ++to)
                    next[to] = next[to] ||
                               (reached[from] && edges[from][to]);
            reached = next;
        }
        revision >>= 1;
        if (!revision)
            break;
        Matrix squared{};
        for (std::size_t from = 0; from < size; ++from)
            for (std::size_t via = 0; via < size; ++via)
                for (std::size_t to = 0; to < size; ++to)
                    squared[from][to] = squared[from][to] ||
                                        (edges[from][via] &&
                                         edges[via][to]);
        edges = squared;
    }
    return reached[destination];
}
} // namespace detail

// Construct and mutate on one executor thread. Foreign access rejects
// before reading mutable state. Copy snapshots, never share mutable
// entities.
template <typename Definition> class OwnedStateMachine final
{
public:
    using State = typename Definition::State;
    using Event = typename Definition::Event;
    using Result = std::expected<Snapshot<State>, TransitionError>;

    static_assert(std::is_enum_v<State> && std::is_enum_v<Event>);
    static_assert(detail::validDefinition<Definition>());

    // Infrastructure callers may supply a starting revision. Fresh
    // domain entities start at zero; recovery uses the validated
    // snapshot constructor.
    explicit OwnedStateMachine(std::uint64_t revision = 0)
        : m_revision(revision)
    {
    }
    /** @brief Rehydrates one validated state on its new construction
     * owner. */
    explicit OwnedStateMachine(Snapshot<State> restored)
        : m_state(restored.state)
        , m_revision(restored.revision)
    {
        if (!detail::reachableAtRevision<Definition>(
                restored.state,
                restored.revision
            ))
            throw std::invalid_argument(
                "invalid restored state machine snapshot"
            );
    }
    OwnedStateMachine(const OwnedStateMachine&) = delete;
    OwnedStateMachine& operator=(const OwnedStateMachine&) = delete;
    OwnedStateMachine(OwnedStateMachine&&) = delete;
    OwnedStateMachine& operator=(OwnedStateMachine&&) = delete;

    std::thread::id ownerThread() const noexcept
    {
        return m_owner;
    }

    Result snapshot() const noexcept
    {
        if (std::this_thread::get_id() != m_owner)
            return std::unexpected(TransitionError::WrongThread);
        return Snapshot<State>{m_state, m_revision};
    }

    Result apply(Event event, std::uint64_t expectedRevision) noexcept
    {
        if (std::this_thread::get_id() != m_owner)
            return std::unexpected(TransitionError::WrongThread);
        if (!detail::contains(Definition::events, event))
            return std::unexpected(TransitionError::InvalidEvent);
        if (expectedRevision != m_revision)
            return std::unexpected(TransitionError::StaleRevision);
        for (const auto rule : Definition::rules)
        {
            if (rule.from != m_state || rule.event != event)
                continue;
            if (m_revision ==
                std::numeric_limits<std::uint64_t>::max())
                return std::unexpected(
                    TransitionError::RevisionExhausted
                );
            m_state = rule.to;
            ++m_revision;
            return Snapshot<State>{m_state, m_revision};
        }
        return std::unexpected(TransitionError::IllegalTransition);
    }

private:
    const std::thread::id m_owner{std::this_thread::get_id()};
    State m_state{Definition::initial};
    std::uint64_t m_revision;
};
} // namespace service::state
