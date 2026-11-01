#include <array>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

#include "arena/domain/Battle.h"
#include "arena/domain/Room.h"
#include "arena/domain/Ticket.h"

namespace
{
using arena::domain::Battle;
using arena::domain::Room;
using arena::domain::Ticket;
using service::state::Snapshot;

struct RecoveryGraph
{
    enum class State
    {
        First,
        Second,
        Terminal
    };
    enum class Event
    {
        Forward,
        Back,
        Finish,
        Stay
    };
    static constexpr State initial = State::First;
    static constexpr std::array states{
        State::First, State::Second, State::Terminal
    };
    static constexpr std::array events{
        Event::Forward, Event::Back, Event::Finish, Event::Stay
    };
    using Rule = service::state::Rule<State, Event>;
    static constexpr std::array rules{
        Rule{State::First, Event::Forward, State::Second},
        Rule{State::Second, Event::Back, State::First},
        Rule{State::Second, Event::Finish, State::Terminal},
        Rule{State::Terminal, Event::Stay, State::Terminal}
    };
};

TEST(StateMachineRecoveryTest, RequiresAnExactReachableRevision)
{
    using State = RecoveryGraph::State;
    using Machine = service::state::OwnedStateMachine<RecoveryGraph>;
    std::array<bool, 3> reachable{true, false, false};
    for (std::uint64_t revision = 0; revision != 40; ++revision)
    {
        for (std::size_t state = 0; state != reachable.size();
             ++state)
        {
            const auto image = Snapshot<State>{
                static_cast<State>(state),
                revision
            };
            if (reachable[state])
            {
                Machine restored(image);
                ASSERT_TRUE(restored.snapshot());
                EXPECT_EQ(*restored.snapshot(), image);
            }
            else
                EXPECT_THROW((Machine{image}), std::invalid_argument);
        }
        // Independent graph oracle: First -> Second; Second ->
        // First/Terminal; Terminal -> Terminal. The production rule
        // table is not consulted.
        reachable = {
            reachable[1],
            reachable[0],
            reachable[1] || reachable[2]
        };
    }
    EXPECT_THROW(
        (Machine{Snapshot<State>{static_cast<State>(99), 0}}),
        std::invalid_argument
    );
}

TEST(
    StateMachineRecoveryTest, HandlesHugeRevisionAndReservationCycles
)
{
    constexpr auto
        maximum = std::numeric_limits<std::uint64_t>::max();
    Ticket queued(
        arena::domain::TicketId{1},
        arena::domain::PlayerId{2},
        Snapshot<Ticket::State>{Ticket::State::Queued, maximum - 1}
    );
    ASSERT_TRUE(queued.snapshot());
    EXPECT_EQ(queued.snapshot()->revision, maximum - 1);
    ASSERT_TRUE(queued.apply(Ticket::Event::Reserve, maximum - 1));
    EXPECT_EQ(queued.snapshot()->state, Ticket::State::Reserved);
    EXPECT_EQ(
        queued.apply(Ticket::Event::Assign, maximum).error(),
        service::state::TransitionError::RevisionExhausted
    );
    Ticket reserved(
        arena::domain::TicketId{3},
        arena::domain::PlayerId{4},
        Snapshot<Ticket::State>{Ticket::State::Reserved, maximum}
    );
    EXPECT_EQ(reserved.snapshot()->revision, maximum);
    EXPECT_THROW(
        (Ticket{
            arena::domain::TicketId{5},
            arena::domain::PlayerId{6},
            Snapshot<Ticket::State>{Ticket::State::Assigned, maximum}
        }),
        std::invalid_argument
    );
    Ticket cancelled(
        arena::domain::TicketId{7},
        arena::domain::PlayerId{8},
        Snapshot<Ticket::State>{Ticket::State::Cancelled, maximum}
    );
    EXPECT_EQ(cancelled.snapshot()->state, Ticket::State::Cancelled);
}

TEST(
    StateMachineRecoveryTest,
    RestoresRoomAndBattleWithoutResettingTheirFences
)
{
    Room playing(
        arena::domain::RoomId{1},
        Snapshot<Room::State>{Room::State::Running, 2}
    );
    EXPECT_EQ(playing.snapshot()->revision, 2U);
    ASSERT_TRUE(playing.apply(Room::Event::RequestClose, 2));
    EXPECT_EQ(playing.snapshot()->state, Room::State::Closing);
    EXPECT_THROW(
        (Room{
            arena::domain::RoomId{2},
            Snapshot<Room::State>{Room::State::Running, 3}
        }),
        std::invalid_argument
    );
    Room closed(
        arena::domain::RoomId{3},
        Snapshot<Room::State>{Room::State::Closed, UINT64_MAX}
    );
    EXPECT_EQ(closed.snapshot()->revision, UINT64_MAX);
    EXPECT_THROW(
        (Room{
            arena::domain::RoomId{4},
            Snapshot<Room::State>{Room::State::Closed, 1}
        }),
        std::invalid_argument
    );
    Battle battle(
        arena::domain::BattleId{5},
        arena::domain::RoomId{1},
        Snapshot<Battle::State>{Battle::State::Running, 1}
    );
    ASSERT_TRUE(battle.apply(Battle::Event::Finish, 1));
    EXPECT_EQ(battle.snapshot()->state, Battle::State::Finished);
    EXPECT_THROW(
        (Battle{
            arena::domain::BattleId{6},
            arena::domain::RoomId{1},
            Snapshot<Battle::State>{Battle::State::Finished, 3}
        }),
        std::invalid_argument
    );
}

TEST(
    StateMachineRecoveryTest,
    RehydrationCapturesTheNewOwnerAndRejectsForeignAccess
)
{
    const Snapshot<Ticket::State> image{Ticket::State::Queued, 2};
    Ticket owner(
        arena::domain::TicketId{1},
        arena::domain::PlayerId{2},
        image
    );
    bool foreignRejected = false, newOwnerWorked = false;
    std::thread other(
        [&]
        {
            foreignRejected =
                owner.snapshot().error() ==
                    service::state::TransitionError::WrongThread &&
                owner.apply(Ticket::Event::Reserve, 2).error() ==
                    service::state::TransitionError::WrongThread;
            Ticket restored(
                arena::domain::TicketId{1},
                arena::domain::PlayerId{2},
                image
            );
            const auto applied = restored.apply(
                Ticket::Event::Reserve,
                2
            );
            newOwnerWorked = applied &&
                             applied->state ==
                                 Ticket::State::Reserved &&
                             applied->revision == 3;
        }
    );
    other.join();
    EXPECT_TRUE(foreignRejected);
    EXPECT_TRUE(newOwnerWorked);
    EXPECT_EQ(*owner.snapshot(), image);
}
} // namespace
