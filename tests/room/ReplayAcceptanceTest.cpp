#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <map>
#include <random>
#include <sstream>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <gtest/gtest.h>

#include "net/ConnectionSchedulers.h"
#include "net/RoomService.h"

#include "room/ReplayFile.h"
#include "room/ReplayRecorder.h"

using namespace service::room;
namespace
{
std::uint64_t independentChecksum(
    const Snapshot& snapshot, const ActorConfig& config
)
{
    std::vector<std::pair<std::uint64_t, unsigned>> words{
        {1, 8},
        {snapshot.roomId.value(), 8},
        {snapshot.incarnation, 8},
        {snapshot.tick, 8},
        {static_cast<std::uint32_t>(config.positionLimit), 4},
        {snapshot.players.size(), 8}
    };
    for (const auto& p : snapshot.players)
        words.insert(
            words.end(),
            {{p.member.playerId.value(), 8},
             {p.member.sessionId.value(), 8},
             {p.member.epoch, 8},
             {static_cast<std::uint32_t>(p.x), 4},
             {static_cast<std::uint32_t>(p.y), 4},
             {p.sequence, 8},
             {static_cast<std::uint32_t>(p.lastDx), 4},
             {static_cast<std::uint32_t>(p.lastDy), 4}}
        );
    std::uint64_t hash = 14695981039346656037ULL;
    for (auto [value, bytes] : words)
        for (unsigned i = 0; i < bytes; ++i)
        {
            hash = (hash ^ (value & 255U)) * 1099511628211ULL;
            value >>= 8;
        }
    return hash;
}

struct ArtifactDirectory
{
    std::filesystem::path path;
    bool temporary;
    ArtifactDirectory()
    {
        const auto requested = std::getenv(
            "ARENA_REPLAY_ACCEPTANCE_DIR"
        );
        temporary = !requested;
        path = requested ? std::filesystem::path(requested)
                         : std::filesystem::temp_directory_path() /
                               ("arena-replay-acceptance-" +
                                std::to_string(::getpid()));
        std::filesystem::create_directories(path);
    }
    ~ArtifactDirectory()
    {
        if (temporary)
        {
            std::error_code ignored;
            std::filesystem::remove_all(path, ignored);
        }
    }
};

TEST(
    ReplayAcceptanceTest,
    HundredSeedMatchesPersistEveryLiveTickBeyondRollingHistory
)
{
    ArtifactDirectory output;
    for (std::uint32_t seed = 0; seed < 100; ++seed)
    {
        SCOPED_TRACE(testing::Message() << "seed=" << seed);
        ActorConfig config;
        config.maxMembers = 8;
        config.maxInputsPerTick = 64;
        config.historyTicks = 8;
        config.positionLimit = 16;
        RoomActor actor(RoomId{7000 + seed}, 1000 + seed, config);
        const auto checkpoint = actor.snapshot();
        std::ostringstream stem;
        stem << "seed-" << std::setw(3) << std::setfill('0') << seed;
        auto writer = ReplayWriter::create(
            output.path / (stem.str() + ".arena"),
            config,
            checkpoint
        );
        ASSERT_TRUE(writer);
        std::ofstream manifest(
            output.path / (stem.str() + ".checksums")
        );
        ASSERT_TRUE(manifest);
        manifest << "checkpoint " << checkpoint.tick << ' '
                 << checkpoint.checksum << '\n';
        std::vector<std::uint64_t> live;
        std::mt19937 random(0xB4771EU + seed);
        std::map<std::uint64_t, std::uint64_t> epochs{
            {1, 1},
            {2, 1},
            {3, 1},
            {4, 1}
        };
        std::map<std::uint64_t, std::uint64_t> sessions{
            {1, 101},
            {2, 102},
            {3, 103},
            {4, 104}
        };
        for (std::uint64_t tick = 1; tick <= 256; ++tick)
        {
            SCOPED_TRACE(
                testing::Message() << "absolute_tick=" << tick
            );
            const auto before = actor.snapshot();
            std::vector<Member> roster;
            for (std::uint64_t p = 1; p <= 4; ++p)
            {
                if (random() % 37 == 0)
                    ++epochs[p];
                if (random() % 73 == 0)
                    sessions[p] += 100;
                if (random() % 11 != 0)
                    roster.push_back(
                        {PlayerId{p},
                         SessionId{sessions[p]},
                         epochs[p]}
                    );
            }
            std::vector<Input> inputs;
            if (tick % 7 != 0)
            {
                for (const auto& member : roster)
                {
                    const auto previous = std::find_if(
                        before.players.begin(),
                        before.players.end(),
                        [&](const auto& p)
                        { return p.member == member; }
                    );
                    const auto sequence = previous ==
                                                  before.players.end()
                                              ? 0
                                              : previous->sequence;
                    const auto next = sequence + 1;
                    const auto dx = static_cast<std::int32_t>(
                                        random() % 3
                                    ) -
                                    1;
                    const auto dy = static_cast<std::int32_t>(
                                        random() % 3
                                    ) -
                                    1;
                    switch (random() % 6)
                    {
                    case 0:
                        inputs.push_back({member, next, dx, dy});
                        break;
                    case 1:
                        inputs.push_back({member, next + 2, dx, dy});
                        break;
                    case 2:
                        inputs.push_back({member, next, dx, dy});
                        inputs.push_back(inputs.back());
                        break;
                    case 3:
                        inputs.push_back({member, next, 1, dy});
                        inputs.push_back({member, next, -1, dy});
                        break;
                    case 4:
                        inputs.push_back(
                            {member,
                             sequence ? sequence - 1 : 0,
                             dx,
                             dy}
                        );
                        break;
                    default:
                        inputs.push_back({member, next + 1, 0, 1});
                        inputs.push_back({member, next, 1, 0});
                        break;
                    }
                    if (tick % 13 == 0)
                        inputs.push_back({member, next + 7, 2, 0});
                }
            }
            std::shuffle(roster.begin(), roster.end(), random);
            std::shuffle(inputs.begin(), inputs.end(), random);
            auto result = actor.step(
                std::move(roster),
                std::move(inputs)
            );
            ASSERT_EQ(result.snapshot.tick, tick);
            ASSERT_EQ(
                result.snapshot.checksum,
                independentChecksum(result.snapshot, config)
            );
            live.push_back(result.snapshot.checksum);
            manifest << tick << ' ' << result.snapshot.checksum
                     << '\n';
            ASSERT_TRUE((*writer)->append(result.record));
        }
        ASSERT_TRUE((*writer)->finish());
        manifest.close();
        ASSERT_TRUE(manifest);
        ASSERT_LE(actor.replayLog().ticks.size(), 8U);
        auto loaded = readReplayFile(
            output.path / (stem.str() + ".arena")
        );
        ASSERT_TRUE(loaded);
        EXPECT_EQ(loaded->checkpoint, checkpoint);
        ASSERT_EQ(loaded->ticks.size(), 256U);
        auto replayed = RoomActor::replay(*loaded);
        ASSERT_TRUE(replayed);
        EXPECT_EQ(replayed->checksums, live);
        EXPECT_EQ(replayed->snapshot, actor.snapshot());
    }
}

TEST(
    ReplayFailureTest,
    ReportsFirstAbsoluteMismatchBeforeLaterMalformedRecord
)
{
    ActorConfig config;
    config.historyTicks = 1;
    RoomActor actor(RoomId{7}, 9, config);
    actor.step({}, {});
    actor.step({}, {});
    ReplayLog log{config, actor.snapshot(), {}};
    for (int i = 0; i < 4; ++i)
        log.ticks.push_back(actor.step({}, {}).record);
    log.ticks[1].checksum ^= 1;
    log.ticks[3].tick = 999;
    auto failure = RoomActor::replay(log);
    ASSERT_FALSE(failure);
    EXPECT_EQ(failure.error().code, ReplayError::ChecksumMismatch);
    EXPECT_EQ(failure.error().tick, 4U);
    ASSERT_TRUE(failure.error().expectedChecksum);
    ASSERT_TRUE(failure.error().actualChecksum);
    EXPECT_NE(
        failure.error().expectedChecksum,
        failure.error().actualChecksum
    );
    EXPECT_FALSE(failure.error().reason.empty());
}

TEST(
    ReplayFailureTest,
    CheckpointAndStructuralFailuresHaveUnambiguousLocation
)
{
    RoomActor actor(RoomId{7}, 9);
    actor.step({}, {});
    actor.step({}, {});
    auto checkpoint = actor.snapshot();
    ReplayLog log{{}, checkpoint, {}};
    log.checkpoint.checksum ^= 1;
    auto badHash = RoomActor::replay(log);
    ASSERT_FALSE(badHash);
    EXPECT_EQ(badHash.error().code, ReplayError::ChecksumMismatch);
    EXPECT_EQ(badHash.error().tick, 2U);
    log.checkpoint = checkpoint;
    log.ticks.push_back(actor.step({}, {}).record);
    log.ticks[0].tick = 99;
    auto gap = RoomActor::replay(log);
    ASSERT_FALSE(gap);
    EXPECT_EQ(gap.error().code, ReplayError::InvalidLog);
    EXPECT_EQ(gap.error().tick, 3U);
    EXPECT_FALSE(gap.error().expectedChecksum);
    EXPECT_FALSE(gap.error().actualChecksum);
    log.ticks.clear();
    log.config.positionLimit = 0;
    auto badConfig = RoomActor::replay(log);
    ASSERT_FALSE(badConfig);
    EXPECT_EQ(badConfig.error().tick, 2U);
}

TEST(ReplayAcceptanceTest, DiagnosticArchiveStartsAtAbsoluteTickThree)
{
    ArtifactDirectory output;
    ActorConfig config;
    config.historyTicks = 2;
    config.positionLimit = 32;
    RoomActor actor(RoomId{999}, 1999, config);
    const Member member{PlayerId{1}, SessionId{101}, 1};
    for (std::uint64_t sequence = 1; sequence <= 3; ++sequence)
        actor.step({member}, {{member, sequence, 1, 0}});
    const auto checkpoint = actor.snapshot();
    auto writer = ReplayWriter::create(
        output.path / "diagnostic.arena",
        config,
        checkpoint
    );
    ASSERT_TRUE(writer);
    std::ofstream manifest(output.path / "diagnostic.checksums");
    ASSERT_TRUE(manifest);
    manifest << "checkpoint " << checkpoint.tick << ' '
             << checkpoint.checksum << '\n';
    std::uint64_t sequence = 3;
    for (std::uint64_t tick = 4; tick <= 12; ++tick)
    {
        std::vector<Input> inputs;
        if (tick % 2 == 0)
            inputs.push_back(
                {member, ++sequence, tick == 6 ? -1 : 1, 0}
            );
        if (tick == 7)
            inputs.push_back({member, 0, 0, 0});
        auto result = actor.step({member}, std::move(inputs));
        EXPECT_EQ(
            result.snapshot.checksum,
            independentChecksum(result.snapshot, config)
        );
        ASSERT_TRUE((*writer)->append(result.record));
        manifest << tick << ' ' << result.snapshot.checksum << '\n';
    }
    EXPECT_TRUE((*writer)->finish());
}

TEST(ReplayAcceptanceTest, RoomOwnerArchivePersistsEveryObservedTick)
{
    namespace gw = service::gateway;
    using namespace std::chrono_literals;
    ArtifactDirectory output;
    ReplayRecorderConfig recorderConfig;
    recorderConfig.directory = output.path / "owner-recordings";
    auto recorder = std::make_shared<ReplayRecorder>(recorderConfig);
    auto online = std::make_shared<gw::OnlineRegistry>();
    service::auth::JwtConfig jwtConfig;
    jwtConfig.key = "0123456789abcdef0123456789abcdef";
    jwtConfig.now = [] { return std::int64_t{1700000000}; };
    service::auth::JwtTokens jwt(std::move(jwtConfig));
    auto grant = online->install(PlayerId{42}, 7, {}, jwt, [] {});
    ASSERT_TRUE(grant);
    ASSERT_TRUE(online->joinRoom(grant->lease->binding, RoomId{77}));
    boost::asio::io_context context;
    boost::asio::steady_timer watchdog(context);
    gw::RoomServiceConfig config;
    config.replays = recorder;
    config.actor.historyTicks = 2;
    gw::RoomService rooms(online, config);
    rooms.start(gw::connectionScheduler(context.get_executor()));
    std::vector<std::pair<std::uint64_t, std::uint64_t>> live;
    std::uint64_t incarnation = 0, membership = 0, sequence = 0;
    bool timedOut = false;
    const auto finish = [&]
    {
        rooms.stop();
        boost::system::error_code ignored;
        watchdog.cancel(ignored);
    };
    std::function<void(gw::RoomService::InputResult)> receive;
    receive = [&](auto result)
    {
        EXPECT_TRUE(result);
        if (!result)
        {
            finish();
            return;
        }
        EXPECT_EQ(
            result->snapshot.checksum,
            independentChecksum(result->snapshot, config.actor)
        );
        live.emplace_back(
            result->snapshot.tick,
            result->snapshot.checksum
        );
        if (sequence == 3)
        {
            finish();
            return;
        }
        const bool accepted = rooms.input(
            grant->lease->binding,
            grant->snapshot.sessionId,
            RoomId{77},
            incarnation,
            membership,
            ++sequence,
            1,
            0,
            receive
        );
        EXPECT_TRUE(accepted);
        if (!accepted)
            finish();
    };
    ASSERT_TRUE(rooms.snapshot(
        grant->lease->binding,
        grant->snapshot.sessionId,
        RoomId{77},
        [&](auto result)
        {
            EXPECT_TRUE(result);
            if (!result)
            {
                finish();
                return;
            }
            EXPECT_EQ(
                result->checksum,
                independentChecksum(*result, config.actor)
            );
            live.emplace_back(result->tick, result->checksum);
            incarnation = result->incarnation;
            membership = result->players.at(0).member.epoch;
            const bool accepted = rooms.input(
                grant->lease->binding,
                grant->snapshot.sessionId,
                RoomId{77},
                incarnation,
                membership,
                ++sequence,
                1,
                0,
                receive
            );
            EXPECT_TRUE(accepted);
            if (!accepted)
                finish();
        }
    ));
    watchdog.expires_after(2s);
    watchdog.async_wait(
        [&](auto error)
        {
            if (!error)
            {
                timedOut = true;
                rooms.stop();
            }
        }
    );
    context.run();
    EXPECT_FALSE(timedOut);
    ASSERT_EQ(live.size(), 4U);
    recorder->stop();
    EXPECT_EQ(recorder->metrics().completed, 1U);
    EXPECT_EQ(recorder->metrics().failed, 0U);
    std::vector<std::filesystem::path> archives;
    for (const auto& item :
         std::filesystem::directory_iterator(recorder->directory()))
        if (item.path().extension() == ".areplay")
            archives.push_back(item.path());
    ASSERT_EQ(archives.size(), 1U);
    auto stored = readReplayFile(archives.front());
    ASSERT_TRUE(stored);
    ASSERT_EQ(stored->ticks.size(), live.size());
    auto replay = RoomActor::replay(*stored);
    ASSERT_TRUE(replay);
    for (std::size_t index = 0; index < live.size(); ++index)
    {
        EXPECT_EQ(stored->ticks[index].tick, live[index].first);
        EXPECT_EQ(replay->checksums[index], live[index].second);
    }
    std::filesystem::copy_file(
        archives.front(),
        output.path / "owner.arena",
        std::filesystem::copy_options::overwrite_existing
    );
    std::ofstream manifest(output.path / "owner.checksums");
    ASSERT_TRUE(manifest);
    manifest << "checkpoint " << stored->checkpoint.tick << ' '
             << stored->checkpoint.checksum << '\n';
    for (const auto& [tick, checksum] : live)
        manifest << tick << ' ' << checksum << '\n';
}
} // namespace
