#include "room/ReplayRecorder.h"

#include <unistd.h>

#include <filesystem>

#include <gtest/gtest.h>

using namespace service::room;
namespace
{
struct ReplayRecorderTest : testing::Test
{
    std::filesystem::path root;
    void SetUp() override
    {
        static unsigned count = 0;
        root = std::filesystem::temp_directory_path() /
               ("arena-recorder-test-" + std::to_string(::getpid()) +
                "-" + std::to_string(++count));
        std::filesystem::create_directories(root);
    }
    void TearDown() override
    {
        std::error_code ignored;
        std::filesystem::remove_all(root, ignored);
    }
    ReplayRecorderConfig config() const
    {
        ReplayRecorderConfig c;
        c.directory = root;
        return c;
    }
    static std::vector<std::filesystem::path>
    completed(const ReplayRecorder& recorder)
    {
        std::vector<std::filesystem::path> paths;
        for (const auto& entry : std::filesystem::directory_iterator(
                 recorder.directory()
             ))
            if (entry.path().extension() == ".areplay")
                paths.push_back(entry.path());
        return paths;
    }
};

TEST_F(
    ReplayRecorderTest,
    FinishedStreamPersistsEveryTickBeyondHistoryAndJoinsCleanly
)
{
    ReplayRecorder recorder(config());
    ActorConfig policy;
    policy.historyTicks = 2;
    RoomActor actor(RoomId{7}, 9, policy);
    auto token = recorder.begin(policy, actor.snapshot());
    ASSERT_TRUE(token);
    const Member member{PlayerId{1}, SessionId{101}, 1};
    std::vector<std::uint64_t> live;
    for (std::uint64_t seq = 1; seq <= 240; ++seq)
    {
        auto result = actor.step(
            {member},
            {{member, seq, seq % 2 ? 1 : -1, 0}}
        );
        live.push_back(result.snapshot.checksum);
        ASSERT_TRUE(token->append(std::move(result.record)));
    }
    token->finish();
    token.reset();
    recorder.stop();
    recorder.stop();
    const auto metrics = recorder.metrics();
    EXPECT_EQ(metrics.completed, 1U);
    EXPECT_EQ(metrics.failed, 0U);
    EXPECT_EQ(metrics.active, 0U);
    EXPECT_EQ(metrics.queued, 0U);
    EXPECT_EQ(metrics.queuedBytes, 0U);
    const auto files = completed(recorder);
    ASSERT_EQ(files.size(), 1U);
    auto stored = readReplayFile(files[0]);
    ASSERT_TRUE(stored);
    ASSERT_EQ(stored->ticks.size(), 240U);
    auto replay = RoomActor::replay(*stored);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->checksums, live);
    EXPECT_EQ(replay->snapshot, actor.snapshot());
}

TEST_F(
    ReplayRecorderTest, AbandonedStreamCannotPublishACompletedArchive
)
{
    ReplayRecorder recorder(config());
    RoomActor actor(RoomId{7}, 9);
    auto token = recorder.begin({}, actor.snapshot());
    ASSERT_TRUE(token);
    ASSERT_TRUE(token->append(actor.step({}, {}).record));
    token.reset();
    recorder.stop();
    EXPECT_EQ(recorder.metrics().completed, 0U);
    EXPECT_GE(recorder.metrics().failed, 1U);
    EXPECT_TRUE(completed(recorder).empty());
}

TEST_F(
    ReplayRecorderTest,
    InsufficientByteBudgetFailsWithoutASuccessfulPartialArchive
)
{
    auto limits = config();
    limits.maxQueuedBytes = 1;
    ReplayRecorder recorder(limits);
    RoomActor actor(RoomId{7}, 9);
    auto token = recorder.begin({}, actor.snapshot());
    if (token)
    {
        EXPECT_FALSE(token->append(actor.step({}, {}).record));
        token->finish();
        token.reset();
    }
    recorder.stop();
    EXPECT_EQ(recorder.metrics().completed, 0U);
    EXPECT_GE(
        recorder.metrics().failed + recorder.metrics().rejected,
        1U
    );
    EXPECT_TRUE(completed(recorder).empty());
}

TEST_F(
    ReplayRecorderTest,
    ActiveRecordingLimitAndStoppedAdmissionAreExplicit
)
{
    auto limits = config();
    limits.maxActive = 1;
    ReplayRecorder recorder(limits);
    RoomActor a(RoomId{7}, 9), b(RoomId{8}, 10);
    auto first = recorder.begin({}, a.snapshot());
    ASSERT_TRUE(first);
    EXPECT_FALSE(recorder.begin({}, b.snapshot()));
    EXPECT_GE(recorder.metrics().rejected, 1U);
    first->finish();
    first.reset();
    recorder.stop();
    EXPECT_FALSE(recorder.begin({}, b.snapshot()));
    EXPECT_EQ(recorder.metrics().completed, 1U);
    EXPECT_EQ(completed(recorder).size(), 1U);
}

TEST_F(ReplayRecorderTest, StopCannotPublishAnUnsealedLiveToken)
{
    ReplayRecorder recorder(config());
    RoomActor actor(RoomId{7}, 9);
    auto token = recorder.begin({}, actor.snapshot());
    ASSERT_TRUE(token);
    ASSERT_TRUE(token->append(actor.step({}, {}).record));
    recorder.stop();
    token->finish();
    token.reset();
    EXPECT_EQ(recorder.metrics().completed, 0U);
    EXPECT_GE(recorder.metrics().failed, 1U);
    EXPECT_TRUE(completed(recorder).empty());
}
TEST_F(
    ReplayRecorderTest,
    ReservedVectorCapacityCannotBypassQueueByteBudget
)
{
    auto limits = config();
    limits.maxQueuedBytes = 1024;
    ReplayRecorder recorder(limits);
    RoomActor actor(RoomId{7}, 9);
    auto token = recorder.begin({}, actor.snapshot());
    ASSERT_TRUE(token);
    auto record = actor.step({}, {}).record;
    record.inputs.reserve(10000);
    EXPECT_FALSE(token->append(std::move(record)));
    token->finish();
    token.reset();
    recorder.stop();
    EXPECT_EQ(recorder.metrics().completed, 0U);
    EXPECT_TRUE(completed(recorder).empty());
}
} // namespace
