#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arena/domain/Battle.h"
#include "arena/domain/Player.h"
#include "arena/domain/Room.h"
#include "arena/domain/Session.h"
#include "arena/domain/Settlement.h"
#include "arena/domain/Ticket.h"

using namespace arena::domain;

namespace
{
// Matrices are the independent domain specification: rows=states,
// columns=events. -1 rejects; nonnegative cells name the destination
// state's index. Tests never read the production rule table to
// compute an expected transition.
template <typename T> struct Spec;
template <> struct Spec<Player>
{
    static constexpr std::string_view name = "Player";
    static constexpr auto owner = OwnerService::Lobby;
    static constexpr std::array states{
        Player::State::Active,
        Player::State::Suspended,
        Player::State::Retired
    };
    static constexpr std::array events{
        Player::Event::Suspend,
        Player::Event::Reinstate,
        Player::Event::Retire
    };
    static constexpr std::array matrix{
        std::array{1, -1, 2},
        std::array{-1, 0, 2},
        std::array{-1, -1, -1}
    };
    static auto make(std::uint64_t id = 1)
    {
        return std::make_unique<Player>(PlayerId{id});
    }
};
template <> struct Spec<Session>
{
    static constexpr std::string_view name = "Session";
    static constexpr auto owner = OwnerService::Gateway;
    static constexpr std::array states{
        Session::State::Online,
        Session::State::Detached,
        Session::State::Expired,
        Session::State::Revoked
    };
    static constexpr std::array events{
        Session::Event::TransportLost,
        Session::Event::ResumeAuthorized,
        Session::Event::LeaseExpired,
        Session::Event::Revoke
    };
    static constexpr std::array matrix{
        std::array{1, -1, 2, 3},
        std::array{-1, 0, 2, 3},
        std::array{-1, -1, -1, -1},
        std::array{-1, -1, -1, -1}
    };
    static auto make(std::uint64_t id = 1)
    {
        return std::make_unique<Session>(
            SessionId{id},
            PlayerId{100}
        );
    }
};
template <> struct Spec<Ticket>
{
    static constexpr std::string_view name = "Ticket";
    static constexpr auto owner = OwnerService::Match;
    static constexpr std::array states{
        Ticket::State::Queued,
        Ticket::State::Reserved,
        Ticket::State::Assigned,
        Ticket::State::Cancelled,
        Ticket::State::Expired,
        Ticket::State::Failed
    };
    static constexpr std::array events{
        Ticket::Event::Reserve,
        Ticket::Event::ReleaseReservation,
        Ticket::Event::Assign,
        Ticket::Event::Cancel,
        Ticket::Event::DeadlineExpired,
        Ticket::Event::AllocationFailed
    };
    static constexpr std::array matrix{
        std::array{1, -1, -1, 3, 4, -1},
        std::array{-1, 0, 2, 3, 4, 5},
        std::array{-1, -1, -1, -1, -1, -1},
        std::array{-1, -1, -1, -1, -1, -1},
        std::array{-1, -1, -1, -1, -1, -1},
        std::array{-1, -1, -1, -1, -1, -1}
    };
    static auto make(std::uint64_t id = 1)
    {
        return std::make_unique<Ticket>(TicketId{id}, PlayerId{100});
    }
};
template <> struct Spec<Room>
{
    static constexpr std::string_view name = "Room";
    static constexpr auto owner = OwnerService::Room;
    static constexpr std::array states{
        Room::State::Forming,
        Room::State::Ready,
        Room::State::Running,
        Room::State::Closing,
        Room::State::Closed
    };
    static constexpr std::array events{
        Room::Event::MembersReady,
        Room::Event::ReadinessLost,
        Room::Event::StartBattle,
        Room::Event::BattleEnded,
        Room::Event::RequestClose,
        Room::Event::CloseCompleted
    };
    static constexpr std::array matrix{
        std::array{1, -1, -1, -1, 3, -1},
        std::array{-1, 0, 2, -1, 3, -1},
        std::array{-1, -1, -1, 0, 3, -1},
        std::array{-1, -1, -1, -1, -1, 4},
        std::array{-1, -1, -1, -1, -1, -1}
    };
    static auto make(std::uint64_t id = 1)
    {
        return std::make_unique<Room>(RoomId{id});
    }
};
template <> struct Spec<Battle>
{
    static constexpr std::string_view name = "Battle";
    static constexpr auto owner = OwnerService::Room;
    static constexpr std::array states{
        Battle::State::Prepared,
        Battle::State::Running,
        Battle::State::Finished,
        Battle::State::Aborted
    };
    static constexpr std::array events{
        Battle::Event::Start,
        Battle::Event::Finish,
        Battle::Event::Abort
    };
    static constexpr std::array matrix{
        std::array{1, -1, 3},
        std::array{-1, 2, 3},
        std::array{-1, -1, -1},
        std::array{-1, -1, -1}
    };
    static auto make(std::uint64_t id = 1)
    {
        return std::make_unique<Battle>(BattleId{id}, RoomId{100});
    }
};
template <> struct Spec<Settlement>
{
    static constexpr std::string_view name = "Settlement";
    static constexpr auto owner = OwnerService::Storage;
    static constexpr std::array states{
        Settlement::State::Pending,
        Settlement::State::Applying,
        Settlement::State::RetryWait,
        Settlement::State::Committed,
        Settlement::State::Failed
    };
    static constexpr std::array events{
        Settlement::Event::BeginApply,
        Settlement::Event::CommitConfirmed,
        Settlement::Event::RetryableFailure,
        Settlement::Event::RetryDue,
        Settlement::Event::PermanentFailure
    };
    static constexpr std::array matrix{
        std::array{1, -1, -1, -1, -1},
        std::array{-1, 3, 2, -1, 4},
        std::array{-1, -1, -1, 1, 4},
        std::array{-1, -1, -1, -1, -1},
        std::array{-1, -1, -1, -1, -1}
    };
    static auto make(std::uint64_t id = 1)
    {
        return std::make_unique<Settlement>(
            SettlementId{id},
            BattleId{100}
        );
    }
};

// Traverse only the independent specification to reach each state
// through the public Interface; there is no test-only setter for
// arbitrary states.
template <typename T> bool reach(T& entity, std::size_t target)
{
    const auto& matrix = Spec<T>::matrix;
    std::vector<int> previous(matrix.size(), -1);
    std::vector<std::size_t> via(matrix.size());
    std::queue<std::size_t> queue;
    previous[0] = 0;
    queue.push(0);
    while (!queue.empty())
    {
        const auto from = queue.front();
        queue.pop();
        for (std::size_t event = 0; event < Spec<T>::events.size();
             ++event)
        {
            const auto to = matrix[from][event];
            if (to < 0 ||
                previous[static_cast<std::size_t>(to)] != -1)
                continue;
            previous[static_cast<std::size_t>(to)] = static_cast<int>(
                from
            );
            via[static_cast<std::size_t>(to)] = event;
            queue.push(static_cast<std::size_t>(to));
        }
    }
    if (previous[target] == -1)
        return false;
    std::vector<std::size_t> path;
    for (auto at = target; at != 0;
         at = static_cast<std::size_t>(previous[at]))
        path.push_back(via[at]);
    std::reverse(path.begin(), path.end());
    for (const auto event : path)
    {
        const auto before = entity.snapshot();
        if (!before ||
            !entity.apply(Spec<T>::events[event], before->revision))
            return false;
    }
    const auto final = entity.snapshot();
    return final && final->state == Spec<T>::states[target];
}

template <typename T>
class DomainLifecycleTest : public ::testing::Test
{
};
using Entities = ::testing::
    Types<Player, Session, Ticket, Room, Battle, Settlement>;
struct EntityNames
{
    template <typename T> static std::string GetName(int)
    {
        return std::string(Spec<T>::name);
    }
};
TYPED_TEST_SUITE(DomainLifecycleTest, Entities, EntityNames);

TYPED_TEST(DomainLifecycleTest, StartsWithStableIdentityAndFixedOwner)
{
    using T = TypeParam;
    static_assert(!std::is_copy_constructible_v<T>);
    static_assert(!std::is_move_constructible_v<T>);
    static_assert(!std::is_copy_assignable_v<T>);
    auto entity = Spec<T>::make(42);
    EXPECT_EQ(T::states, Spec<T>::states);
    EXPECT_EQ(T::events, Spec<T>::events);
    EXPECT_EQ(T::ownerService, Spec<T>::owner);
    EXPECT_FALSE(T::ownerExecutor.empty());
    EXPECT_EQ(entity->id().value(), 42u);
    EXPECT_EQ(entity->ownerThread(), std::this_thread::get_id());
    const auto initial = entity->snapshot();
    ASSERT_TRUE(initial);
    EXPECT_EQ(initial->state, Spec<T>::states[0]);
    EXPECT_EQ(initial->revision, 0u);
}

TYPED_TEST(
    DomainLifecycleTest, AcceptsExactlyTheIndependentStateEventMatrix
)
{
    using T = TypeParam;
    for (std::size_t from = 0; from < Spec<T>::states.size(); ++from)
        for (std::size_t event = 0; event < Spec<T>::events.size();
             ++event)
        {
            SCOPED_TRACE(
                "state=" + std::to_string(from) +
                " event=" + std::to_string(event)
            );
            auto entity = Spec<T>::make();
            ASSERT_TRUE(reach(*entity, from));
            const auto before = entity->snapshot();
            ASSERT_TRUE(before);
            const auto result = entity->apply(
                Spec<T>::events[event],
                before->revision
            );
            const auto expected = Spec<T>::matrix[from][event];
            if (expected < 0)
            {
                ASSERT_FALSE(result);
                EXPECT_EQ(
                    result.error(),
                    TransitionError::IllegalTransition
                );
                EXPECT_EQ(entity->snapshot(), before);
            }
            else
            {
                ASSERT_TRUE(result);
                EXPECT_EQ(
                    result->state,
                    Spec<
                        T>::states[static_cast<std::size_t>(expected)]
                );
                EXPECT_EQ(result->revision, before->revision + 1);
                EXPECT_EQ(entity->snapshot(), result);
            }
        }
}

TYPED_TEST(
    DomainLifecycleTest, RejectsEveryUndeclaredByteEventInEveryState
)
{
    using T = TypeParam;
    for (std::size_t from = 0; from < Spec<T>::states.size(); ++from)
    {
        auto entity = Spec<T>::make();
        ASSERT_TRUE(reach(*entity, from));
        const auto before = entity->snapshot();
        ASSERT_TRUE(before);
        for (unsigned value = 0; value <= 255; ++value)
        {
            const auto event = static_cast<typename T::Event>(value);
            if (std::find(
                    Spec<T>::events.begin(),
                    Spec<T>::events.end(),
                    event
                ) != Spec<T>::events.end())
                continue;
            SCOPED_TRACE(
                "state=" + std::to_string(from) +
                " invalid event=" + std::to_string(value)
            );
            const auto result = entity->apply(
                event,
                before->revision
            );
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), TransitionError::InvalidEvent);
            EXPECT_EQ(entity->snapshot(), before);
        }
    }
}

TYPED_TEST(
    DomainLifecycleTest,
    RejectsStaleRevisionInEveryStateWithoutMutation
)
{
    using T = TypeParam;
    for (std::size_t from = 0; from < Spec<T>::states.size(); ++from)
    {
        auto entity = Spec<T>::make();
        ASSERT_TRUE(reach(*entity, from));
        const auto before = entity->snapshot();
        ASSERT_TRUE(before);
        const auto result = entity->apply(
            Spec<T>::events[0],
            before->revision + 1
        );
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), TransitionError::StaleRevision);
        EXPECT_EQ(entity->snapshot(), before);
    }
}

TYPED_TEST(DomainLifecycleTest, DoesNotReplayAnySuccessfulCommand)
{
    using T = TypeParam;
    for (std::size_t from = 0; from < Spec<T>::states.size(); ++from)
        for (std::size_t event = 0; event < Spec<T>::events.size();
             ++event)
        {
            if (Spec<T>::matrix[from][event] < 0)
                continue;
            auto entity = Spec<T>::make();
            ASSERT_TRUE(reach(*entity, from));
            const auto before = entity->snapshot();
            ASSERT_TRUE(before);
            const auto first = entity->apply(
                Spec<T>::events[event],
                before->revision
            );
            ASSERT_TRUE(first);
            const auto replay = entity->apply(
                Spec<T>::events[event],
                before->revision
            );
            ASSERT_FALSE(replay);
            EXPECT_EQ(replay.error(), TransitionError::StaleRevision);
            EXPECT_EQ(entity->snapshot(), first);
        }
}

TYPED_TEST(
    DomainLifecycleTest, RejectsForeignReadsAndWritesInEveryState
)
{
    using T = TypeParam;
    for (std::size_t from = 0; from < Spec<T>::states.size(); ++from)
    {
        auto entity = Spec<T>::make();
        ASSERT_TRUE(reach(*entity, from));
        const auto before = entity->snapshot();
        ASSERT_TRUE(before);
        typename T::Result read = std::unexpected(
            TransitionError::InvalidEvent
        );
        typename T::Result write = std::unexpected(
            TransitionError::InvalidEvent
        );
        std::uint64_t immutableId = 0;
        std::thread foreign(
            [&]
            {
                immutableId = entity->id().value();
                read = entity->snapshot();
                write = entity->apply(
                    Spec<T>::events[0],
                    before->revision
                );
            }
        );
        foreign.join();
        ASSERT_FALSE(read);
        ASSERT_FALSE(write);
        EXPECT_EQ(read.error(), TransitionError::WrongThread);
        EXPECT_EQ(write.error(), TransitionError::WrongThread);
        EXPECT_EQ(immutableId, entity->id().value());
        EXPECT_EQ(entity->snapshot(), before);
        EXPECT_EQ(entity->ownerThread(), std::this_thread::get_id());
    }
}

TYPED_TEST(DomainLifecycleTest, DoesNotMutateAnotherEntity)
{
    using T = TypeParam;
    auto first = Spec<T>::make(1);
    auto second = Spec<T>::make(2);
    const auto secondBefore = second->snapshot();
    ASSERT_TRUE(first->apply(Spec<T>::events[0], 0));
    EXPECT_EQ(second->snapshot(), secondBefore);
    EXPECT_NE(first->id(), second->id());
}

TEST(DomainIdentityTest, RejectsZeroAndMixedIdentities)
{
    static_assert(!std::is_constructible_v<PlayerId, SessionId>);
    static_assert(
        !std::is_constructible_v<Session, PlayerId, PlayerId>
    );
    static_assert(
        !std::is_constructible_v<Ticket, SessionId, PlayerId>
    );
    static_assert(
        !std::is_constructible_v<Battle, BattleId, PlayerId>
    );
    static_assert(
        !std::is_constructible_v<Settlement, SettlementId, RoomId>
    );
    EXPECT_THROW((PlayerId{0}), std::invalid_argument);
    EXPECT_THROW((SessionId{0}), std::invalid_argument);
    EXPECT_THROW((TicketId{0}), std::invalid_argument);
    EXPECT_THROW((RoomId{0}), std::invalid_argument);
    EXPECT_THROW((BattleId{0}), std::invalid_argument);
    EXPECT_THROW((SettlementId{0}), std::invalid_argument);
}

TEST(
    DomainIdentityTest,
    KeepsReferencesImmutableAndDoesNotShareLifecycle
)
{
    Session session(SessionId{1}, PlayerId{42});
    Ticket ticket(TicketId{2}, PlayerId{42});
    Battle battle(BattleId{3}, RoomId{4});
    Settlement settlement(SettlementId{5}, BattleId{3});
    EXPECT_EQ(session.playerId(), PlayerId{42});
    EXPECT_EQ(ticket.playerId(), PlayerId{42});
    EXPECT_EQ(battle.roomId(), RoomId{4});
    EXPECT_EQ(settlement.battleId(), BattleId{3});
    ASSERT_TRUE(session.apply(Session::Event::TransportLost, 0));
    EXPECT_EQ(ticket.snapshot()->state, Ticket::State::Queued);
    EXPECT_EQ(battle.snapshot()->state, Battle::State::Prepared);
    EXPECT_EQ(
        settlement.snapshot()->state,
        Settlement::State::Pending
    );
}

TEST(DomainScenarioTest, DelayedDisconnectCannotDetachAResumedSession)
{
    Session session(SessionId{1}, PlayerId{42});
    ASSERT_TRUE(session.apply(Session::Event::TransportLost, 0));
    ASSERT_TRUE(session.apply(Session::Event::ResumeAuthorized, 1));
    const auto stale = session.apply(
        Session::Event::TransportLost,
        0
    );
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), TransitionError::StaleRevision);
    EXPECT_EQ(session.snapshot()->state, Session::State::Online);
    EXPECT_EQ(session.snapshot()->revision, 2u);
}

TEST(DomainScenarioTest, ExpiredOrRevokedSessionsCannotResume)
{
    for (const auto event :
         {Session::Event::LeaseExpired, Session::Event::Revoke})
    {
        Session session(SessionId{1}, PlayerId{42});
        ASSERT_TRUE(session.apply(Session::Event::TransportLost, 0));
        ASSERT_TRUE(session.apply(event, 1));
        const auto result = session.apply(
            Session::Event::ResumeAuthorized,
            2
        );
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), TransitionError::IllegalTransition);
    }
}

TEST(DomainScenarioTest, CancelledTicketRejectsLateAssignment)
{
    Ticket ticket(TicketId{1}, PlayerId{42});
    ASSERT_TRUE(ticket.apply(Ticket::Event::Reserve, 0));
    ASSERT_TRUE(ticket.apply(Ticket::Event::Cancel, 1));
    const auto stale = ticket.apply(Ticket::Event::Assign, 1);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), TransitionError::StaleRevision);
    const auto fresh = ticket.apply(Ticket::Event::Assign, 2);
    ASSERT_FALSE(fresh);
    EXPECT_EQ(fresh.error(), TransitionError::IllegalTransition);
    EXPECT_EQ(ticket.snapshot()->state, Ticket::State::Cancelled);
}

TEST(
    DomainScenarioTest,
    RoomCanHostNewBattlesWithoutRestartingFinishedBattle
)
{
    Room room(RoomId{1});
    Battle first(BattleId{2}, room.id());
    ASSERT_TRUE(room.apply(Room::Event::MembersReady, 0));
    ASSERT_TRUE(room.apply(Room::Event::StartBattle, 1));
    ASSERT_TRUE(first.apply(Battle::Event::Start, 0));
    ASSERT_TRUE(first.apply(Battle::Event::Finish, 1));
    ASSERT_TRUE(room.apply(Room::Event::BattleEnded, 2));
    EXPECT_EQ(room.snapshot()->state, Room::State::Forming);
    ASSERT_TRUE(room.apply(Room::Event::MembersReady, 3));
    ASSERT_TRUE(room.apply(Room::Event::StartBattle, 4));
    Battle second(BattleId{3}, room.id());
    EXPECT_NE(first.id(), second.id());
    ASSERT_TRUE(second.apply(Battle::Event::Start, 0));
    const auto restart = first.apply(Battle::Event::Start, 2);
    ASSERT_FALSE(restart);
    EXPECT_EQ(restart.error(), TransitionError::IllegalTransition);
}

TEST(
    DomainScenarioTest,
    SettlementRetriesSameIdentityAndCommitsOnlyOnce
)
{
    Settlement settlement(SettlementId{9}, BattleId{3});
    const auto id = settlement.id();
    ASSERT_TRUE(settlement.apply(Settlement::Event::BeginApply, 0));
    ASSERT_TRUE(
        settlement.apply(Settlement::Event::RetryableFailure, 1)
    );
    ASSERT_TRUE(settlement.apply(Settlement::Event::RetryDue, 2));
    ASSERT_TRUE(
        settlement.apply(Settlement::Event::CommitConfirmed, 3)
    );
    EXPECT_EQ(settlement.id(), id);
    const auto duplicate = settlement.apply(
        Settlement::Event::CommitConfirmed,
        4
    );
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error(), TransitionError::IllegalTransition);
    EXPECT_EQ(
        settlement.snapshot()->state,
        Settlement::State::Committed
    );
    EXPECT_EQ(settlement.snapshot()->revision, 4u);
}

struct Toggle
{
    enum class State
    {
        A,
        B
    };
    enum class Event
    {
        Toggle
    };
    static constexpr State initial = State::A;
    static constexpr std::array states{State::A, State::B};
    static constexpr std::array events{Event::Toggle};
    static constexpr std::array rules{
        service::state::Rule{State::A, Event::Toggle, State::B},
        service::state::Rule{State::B, Event::Toggle, State::A}
    };
};

TEST(OwnedStateMachineTest, RevisionNeverWraps)
{
    service::state::OwnedStateMachine<Toggle> machine(
        std::numeric_limits<std::uint64_t>::max() - 1
    );
    const auto first = machine.apply(
        Toggle::Event::Toggle,
        std::numeric_limits<std::uint64_t>::max() - 1
    );
    ASSERT_TRUE(first);
    EXPECT_EQ(first->state, Toggle::State::B);
    EXPECT_EQ(
        first->revision,
        std::numeric_limits<std::uint64_t>::max()
    );
    const auto exhausted = machine.apply(
        Toggle::Event::Toggle,
        first->revision
    );
    ASSERT_FALSE(exhausted);
    EXPECT_EQ(exhausted.error(), TransitionError::RevisionExhausted);
    EXPECT_EQ(machine.snapshot(), first);
}
} // namespace
