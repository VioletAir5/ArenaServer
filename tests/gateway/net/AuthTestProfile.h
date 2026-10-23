#pragma once

#include <iostream>

#include "net/LoginHandler.h"
#include "net/MatchmakingService.h"
#include "net/OnlineRegistry.h"
#include "net/PreLoginHandshake.h"
#include "net/RoomService.h"

namespace service::gateway
{
// Strict test doubles only; never selected by the application.
class TestPasswordAccounts final
    : public service::auth::PasswordAccounts
{
public:
    explicit TestPasswordAccounts(bool presencePlayers = false)
        : m_presencePlayers(presencePlayers)
    {
    }
    std::expected<
        service::auth::PasswordAccount,
        service::auth::AuthError>
    find(std::string_view account) const override
    {
        const auto player = account == "alice" ? 42U
                            : m_presencePlayers && account == "bob"
                                ? 43U
                            : m_presencePlayers && account == "carl"
                                ? 44U
                                : 0U;
        if (!player)
            return std::unexpected(
                service::auth::AuthError::InvalidCredentials
            );
        return service::auth::PasswordAccount{
            arena::domain::PlayerId{player},
            "test-only-digest"
        };
    }

private:
    bool m_presencePlayers;
};
class TestPasswordDigest final : public service::auth::PasswordDigest
{
public:
    std::expected<std::string, service::auth::AuthError>
    hash(std::string_view) const override
    {
        return std::unexpected(service::auth::AuthError::Unavailable);
    }
    std::expected<bool, service::auth::AuthError> verify(
        std::string_view password, std::string_view digest
    ) const override
    {
        return password == "test-password" &&
               digest == "test-only-digest";
    }
};

inline ConnectionHandlerFactory makeAuthTestFactory(
    std::shared_ptr<OnlineRegistry> registry = {},
    bool presence = false,
    std::shared_ptr<MatchmakingService> matcher = {},
    std::shared_ptr<RoomService> rooms = {}
)
{
    auto passwords = std::make_shared<
        service::auth::LoginAuthenticator>(
        std::make_shared<TestPasswordAccounts>(presence),
        std::make_shared<TestPasswordDigest>()
    );
    service::auth::JwtConfig config;
    config
        .key = "0123456789abcdef0123456789abcdef"; // Test-only, never
                                                   // a default key.
    if (!presence)
        config.now = [] { return 1700000000LL; };
    if (!registry)
        registry = std::make_shared<OnlineRegistry>();
    auto routes = makeLoginMessageRegistry(
        std::move(passwords),
        std::make_shared<service::auth::JwtTokens>(std::move(config)),
        std::move(registry),
        std::move(matcher),
        std::move(rooms)
    );
    return [routes](ConnectionContext::Ptr context)
    {
        return makePreLoginHandshakeHandler(
            std::move(context),
            routes
        );
    };
}
inline void printOnlineTestCounts(const OnlineRegistry& registry)
{
    std::cerr << "ONLINE_COUNTS active=" << registry.activeCount()
              << " retained=" << registry.retainedCount()
              << " rooms=" << registry.roomCount() << std::endl;
}
inline void printMatchTestCounts(const MatchmakingService& matcher)
{
    const auto m = matcher.metrics();
    std::cerr << "MATCH_COUNTS joined=" << m.joined
              << " replayed=" << m.replayed
              << " cancelled=" << m.cancelled
              << " expired=" << m.expired
              << " matched_pairs=" << m.matchedPairs
              << " rejected=" << m.rejected
              << " released_pairs=" << m.releasedPairs
              << " queued=" << m.queued
              << " assigned=" << m.assignedPlayers
              << " retained=" << m.retained
              << " wait_ms_sum=" << m.totalMatchedWaitMs
              << " mmr_gap_sum=" << m.totalMmrGap;
    const auto histogram =
        [](std::string_view name, const auto& counts)
    {
        std::cerr << ' ' << name << '=';
        bool first = true;
        for (const auto value : counts)
        {
            if (!first)
                std::cerr << ',';
            std::cerr << value;
            first = false;
        }
    };
    histogram("matched_wait", m.matchedWait);
    histogram("mmr_gap", m.mmrGap);
    histogram("abandoned_wait", m.abandonedWait);
    std::cerr << std::endl;
}
inline void printRoomTestCounts(const RoomService& rooms)
{
    const auto metrics = rooms.metrics();
    std::cerr << "ROOM_COUNTS actors=" << metrics.actors
              << " ticks=" << metrics.ticks
              << " applied=" << metrics.applied
              << " rejected=" << metrics.rejected
              << " duplicates=" << metrics.duplicates
              << " skipped_wall_slots=" << metrics.skippedWallSlots
              << std::endl;
}
} // namespace service::gateway
