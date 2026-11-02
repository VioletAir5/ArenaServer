#include <atomic>
#include <limits>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "lobby/OnlineRegistry.h"

namespace
{
using arena::domain::PlayerId;
using service::auth::AuthError;
using service::auth::IssuedToken;
using service::auth::JwtTokens;
using service::auth::TokenClaims;

class VerifiedSessionTest : public testing::Test
{
protected:
    std::atomic<std::int64_t> wall{1000};
    JwtTokens tokens{
        [this]
        {
            service::auth::JwtConfig config;
            config.key = "0123456789abcdef0123456789abcdef";
            config.now = [this] { return wall.load(); };
            return config;
        }()
    };
    std::shared_ptr<service::lobby::OnlineRegistry>
        online = std::make_shared<service::lobby::OnlineRegistry>();

    IssuedToken issue(std::uint64_t player, std::uint64_t epoch)
    {
        auto signedToken = tokens.issue(PlayerId{player}, epoch);
        if (!signedToken)
            throw std::runtime_error("fixture token signing failed");
        return std::move(*signedToken);
    }

    auto install(
        IssuedToken candidate,
        std::uint64_t connection,
        std::optional<TokenClaims> resume = {},
        std::function<void()> kick = [] {}
    )
    {
        return online->installVerified(
            std::move(candidate),
            connection,
            resume,
            tokens.clock(),
            std::move(kick)
        );
    }
};

TEST_F(
    VerifiedSessionTest,
    RemoteSignedCandidatePreservesSessionAndRoomOnResume
)
{
    const auto firstEpoch = online->epochWatermark() + 1;
    auto candidate = issue(7, firstEpoch);
    const auto prior = candidate.claims;
    auto first = install(std::move(candidate), 11);
    ASSERT_TRUE(first);
    ASSERT_TRUE(tokens.verify(first->token));
    ASSERT_TRUE(online->joinRoom(
        first->lease->binding,
        arena::domain::RoomId{41}
    ));
    auto second = install(issue(7, firstEpoch + 1), 12, prior);
    ASSERT_TRUE(second);
    EXPECT_TRUE(second->resumed);
    EXPECT_EQ(second->snapshot.sessionId, first->snapshot.sessionId);
    EXPECT_EQ(second->snapshot.roomId, arena::domain::RoomId{41});
    EXPECT_EQ(second->snapshot.roomEpoch, 1u);
    EXPECT_FALSE(first->lease->current());
    EXPECT_TRUE(second->lease->current());
    EXPECT_EQ(
        online->state(second->lease->binding)->members.size(),
        1u
    );
}

TEST_F(
    VerifiedSessionTest,
    OlderCompletionCannotReplaceNewerPlayerBinding
)
{
    const auto base = online->epochWatermark();
    auto kicked = std::make_shared<std::atomic<unsigned>>(0);
    auto current = install(
        issue(7, base + 3),
        12,
        {},
        [kicked] { ++*kicked; }
    );
    ASSERT_TRUE(current);
    for (auto epoch : {base + 1, base + 2, base + 3})
    {
        auto stale = install(issue(7, epoch), 13);
        ASSERT_FALSE(stale);
        EXPECT_EQ(stale.error(), AuthError::Superseded);
    }
    EXPECT_EQ(kicked->load(), 0u);
    EXPECT_TRUE(current->lease->current());
    EXPECT_EQ(online->snapshot(PlayerId{7})->connectionId, 12u);
    EXPECT_EQ(online->epochWatermark(), base + 3);
}

TEST_F(
    VerifiedSessionTest,
    DifferentPlayersMayFinishAuthenticationOutOfOrder
)
{
    const auto base = online->epochWatermark();
    auto later = install(issue(7, base + 2), 11);
    auto earlier = install(issue(8, base + 1), 12);
    ASSERT_TRUE(later);
    ASSERT_TRUE(earlier);
    EXPECT_TRUE(later->lease->current());
    EXPECT_TRUE(earlier->lease->current());
    EXPECT_NE(later->snapshot.sessionId, earlier->snapshot.sessionId);
    EXPECT_EQ(online->epochWatermark(), base + 2);
}

TEST_F(
    VerifiedSessionTest,
    BadCandidateOrResumePreservesPreviousLeaseAndRoster
)
{
    const auto base = online->epochWatermark();
    auto kicked = std::make_shared<std::atomic<unsigned>>(0);
    auto first = install(
        issue(7, base + 1),
        11,
        {},
        [kicked] { ++*kicked; }
    );
    ASSERT_TRUE(first);
    ASSERT_TRUE(online->joinRoom(
        first->lease->binding,
        arena::domain::RoomId{41}
    ));
    auto expired = issue(7, base + 2);
    expired.claims.expiresAt = wall.load();
    auto rejected = install(std::move(expired), 12);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), AuthError::InvalidCredentials);
    auto invalid = issue(7, base + 2);
    invalid.encoded.clear();
    EXPECT_FALSE(install(std::move(invalid), 12));
    for (auto prior :
         {TokenClaims{PlayerId{7}, base, 1300},
          TokenClaims{PlayerId{8}, base + 1, 1300},
          TokenClaims{PlayerId{7}, base + 1, 1301}})
    {
        auto result = install(issue(7, base + 2), 12, prior);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), AuthError::Superseded);
    }
    EXPECT_TRUE(first->lease->current());
    EXPECT_EQ(kicked->load(), 0u);
    EXPECT_EQ(
        online->snapshot(PlayerId{7})->roomId,
        arena::domain::RoomId{41}
    );
    EXPECT_EQ(online->epochWatermark(), base + 1);
}

TEST_F(
    VerifiedSessionTest,
    PasswordReplacementGetsNewSessionAndRetiresOldRoom
)
{
    const auto base = online->epochWatermark();
    auto first = install(issue(7, base + 1), 11);
    ASSERT_TRUE(first);
    ASSERT_TRUE(online->joinRoom(
        first->lease->binding,
        arena::domain::RoomId{41}
    ));
    auto second = install(issue(7, base + 2), 12);
    ASSERT_TRUE(second);
    EXPECT_FALSE(second->resumed);
    EXPECT_NE(second->snapshot.sessionId, first->snapshot.sessionId);
    EXPECT_FALSE(second->snapshot.roomId);
    EXPECT_EQ(online->roomCount(), 0u);
    EXPECT_FALSE(first->lease->current());
}

TEST_F(
    VerifiedSessionTest, MaximumEpochDoesNotWrapAndBlocksLocalIssuer
)
{
    auto last = install(
        issue(7, std::numeric_limits<std::uint64_t>::max()),
        11
    );
    ASSERT_TRUE(last);
    auto result = online->install(PlayerId{8}, 12, {}, tokens, [] {});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), AuthError::Unavailable);
    EXPECT_EQ(
        online->epochWatermark(),
        std::numeric_limits<std::uint64_t>::max()
    );
    EXPECT_TRUE(last->lease->current());
}

TEST_F(
    VerifiedSessionTest,
    RacingRemoteCandidatesConvergeOnLargestPlayerEpoch
)
{
    const auto base = online->epochWatermark();
    std::vector<std::thread> threads;
    std::atomic<unsigned> invalidErrors{0};
    for (unsigned i = 1; i <= 32; ++i)
        threads.emplace_back(
            [&, i]
            {
                auto result = install(issue(7, base + i), i);
                if (!result &&
                    result.error() != AuthError::Superseded)
                    ++invalidErrors;
            }
        );
    for (auto& thread : threads)
        thread.join();
    EXPECT_EQ(invalidErrors.load(), 0u);
    ASSERT_TRUE(online->snapshot(PlayerId{7}));
    EXPECT_EQ(online->snapshot(PlayerId{7})->epoch, base + 32);
    EXPECT_EQ(online->activeCount(), 1u);
}
} // namespace
