#include "room/ReplayFile.h"

#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include <gtest/gtest.h>

using namespace service::room;
namespace
{
struct ReplayFileTest : testing::Test
{
    std::filesystem::path directory;
    void SetUp() override
    {
        static unsigned serial = 0;
        directory = std::filesystem::temp_directory_path() /
                    ("arena-replay-file-" +
                     std::to_string(::getpid()) + "-" +
                     std::to_string(++serial));
        std::filesystem::create_directories(directory);
    }
    void TearDown() override
    {
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }
    std::filesystem::path
    path(std::string name = "sample.arena") const
    {
        return directory / name;
    }
    static ReplayLog log()
    {
        ActorConfig config;
        config.maxMembers = 4;
        config.maxInputsPerTick = 16;
        config.historyTicks = 2;
        config.positionLimit = 32;
        RoomActor actor(RoomId{7}, 9, config);
        ReplayLog result{config, actor.snapshot(), {}};
        const Member a{PlayerId{1}, SessionId{11}, 1},
            b{PlayerId{2}, SessionId{12}, 1};
        result.ticks.push_back(
            actor.step({b, a}, {{b, 0, 0, 0}, {a, 1, -1, 1}}).record
        );
        result.ticks.push_back(
            actor
                .step(
                    {a, b},
                    {{a, 1, -1, 1}, {b, 1, 1, -1}, {b, 1, 1, -1}}
                )
                .record
        );
        result.ticks.push_back(
            actor.step({{PlayerId{1}, SessionId{11}, 2}, b}, {})
                .record
        );
        return result;
    }
    static std::string bytes(const std::filesystem::path& file)
    {
        std::ifstream input(file, std::ios::binary);
        return {
            std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()
        };
    }
    static void
    save(const std::filesystem::path& file, const std::string& data)
    {
        std::ofstream output(file, std::ios::binary);
        output.write(
            data.data(),
            static_cast<std::streamsize>(data.size())
        );
    }
    static void
    put(std::string& data,
        std::size_t offset,
        std::uint64_t value,
        unsigned width)
    {
        for (unsigned i = 0; i < width; ++i)
        {
            data.at(offset + i) = static_cast<char>(value & 255U);
            value >>= 8;
        }
    }
    static void
    append(std::string& data, std::uint64_t value, unsigned width)
    {
        for (unsigned i = 0; i < width; ++i)
        {
            data.push_back(static_cast<char>(value & 255U));
            value >>= 8;
        }
    }
};

TEST_F(
    ReplayFileTest,
    RoundTripPreservesEveryCanonicalAttemptBeyondRollingHistory
)
{
    auto original = log();
    ASSERT_TRUE(writeReplayFile(path(), original));
    auto loaded = readReplayFile(path());
    ASSERT_TRUE(loaded);
    EXPECT_EQ(loaded->checkpoint, original.checkpoint);
    EXPECT_EQ(loaded->config.maxMembers, original.config.maxMembers);
    EXPECT_EQ(
        loaded->config.maxInputsPerTick,
        original.config.maxInputsPerTick
    );
    EXPECT_EQ(
        loaded->config.historyTicks,
        original.config.historyTicks
    );
    EXPECT_EQ(
        loaded->config.positionLimit,
        original.config.positionLimit
    );
    ASSERT_EQ(loaded->ticks.size(), 3U);
    for (std::size_t i = 0; i < 3; ++i)
    {
        EXPECT_EQ(loaded->ticks[i].tick, original.ticks[i].tick);
        EXPECT_EQ(
            loaded->ticks[i].checksum,
            original.ticks[i].checksum
        );
        EXPECT_EQ(
            loaded->ticks[i].members,
            original.ticks[i].members
        );
        EXPECT_EQ(loaded->ticks[i].inputs, original.ticks[i].inputs);
    }
    auto replay = RoomActor::replay(*loaded);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->checksums.size(), 3U);
}

TEST_F(
    ReplayFileTest,
    VersionOneEncodingMatchesExplicitLittleEndianGolden
)
{
    ActorConfig config;
    config.maxMembers = 2;
    config.maxInputsPerTick = 3;
    config.historyTicks = 4;
    config.positionLimit = 5;
    RoomActor actor(RoomId{7}, 9, config);
    ReplayLog empty{config, actor.snapshot(), {}};
    ASSERT_EQ(empty.checkpoint.checksum, 16557647251942399711ULL);
    ASSERT_TRUE(writeReplayFile(path(), empty));
    std::string expected("AREPLAY\0", 8);
    append(expected, 1, 4);
    append(expected, 1, 4);
    append(expected, 2, 8);
    append(expected, 3, 8);
    append(expected, 4, 8);
    append(expected, 5, 4);
    append(expected, 7, 8);
    append(expected, 9, 8);
    append(expected, 0, 8);
    append(expected, 16557647251942399711ULL, 8);
    append(expected, 0, 4);
    append(expected, 0x21444e45U, 4);
    append(expected, 16, 4);
    append(expected, 0, 8);
    append(expected, 0, 8);
    EXPECT_EQ(bytes(path()), expected);
    EXPECT_EQ(expected.size(), 104U);
}

TEST_F(
    ReplayFileTest,
    IncrementalFinishIsRequiredAndSuccessfulFinishIsIdempotent
)
{
    auto value = log();
    {
        auto writer = ReplayWriter::create(
            path(),
            value.config,
            value.checkpoint
        );
        ASSERT_TRUE(writer);
        ASSERT_TRUE((*writer)->append(value.ticks[0]));
    }
    auto incomplete = readReplayFile(path());
    ASSERT_FALSE(incomplete);
    EXPECT_EQ(incomplete.error().code, ReplayFileErrorCode::Invalid);
    auto writer = ReplayWriter::create(
        path(),
        value.config,
        value.checkpoint
    );
    ASSERT_TRUE(writer);
    for (const auto& tick : value.ticks)
        ASSERT_TRUE((*writer)->append(tick));
    ASSERT_TRUE((*writer)->finish());
    EXPECT_TRUE((*writer)->finish());
    EXPECT_EQ((*writer)->ticksWritten(), 3U);
    EXPECT_EQ(
        (*writer)->bytesWritten(),
        std::filesystem::file_size(path())
    );
    const auto complete = bytes(path());
    auto late = (*writer)->append(value.ticks.back());
    ASSERT_FALSE(late);
    EXPECT_EQ(late.error().code, ReplayFileErrorCode::Invalid);
    EXPECT_EQ(bytes(path()), complete);
    EXPECT_TRUE(readReplayFile(path()));
}

TEST_F(
    ReplayFileTest,
    NonCanonicalOrNonContiguousAppendMakesWriterTerminal
)
{
    auto value = log();
    auto writer = ReplayWriter::create(
        path(),
        value.config,
        value.checkpoint
    );
    ASSERT_TRUE(writer);
    auto bad = value.ticks[0];
    std::reverse(bad.members.begin(), bad.members.end());
    auto rejected = (*writer)->append(bad);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, ReplayFileErrorCode::Invalid);
    EXPECT_FALSE((*writer)->append(value.ticks[0]));
    EXPECT_FALSE((*writer)->finish());
    writer->reset();
    auto gapWriter = ReplayWriter::create(
        path("gap.arena"),
        value.config,
        value.checkpoint
    );
    ASSERT_TRUE(gapWriter);
    bad = value.ticks[0];
    bad.tick += 1;
    auto gap = (*gapWriter)->append(bad);
    ASSERT_FALSE(gap);
    EXPECT_EQ(gap.error().code, ReplayFileErrorCode::Invalid);
}

TEST_F(
    ReplayFileTest,
    StoredHashMismatchRemainsAReplayFailureRatherThanAFileFormatFailure
)
{
    auto value = log();
    value.ticks[1].checksum ^= 1;
    ASSERT_TRUE(writeReplayFile(path(), value));
    auto loaded = readReplayFile(path());
    ASSERT_TRUE(loaded);
    auto failure = RoomActor::replay(*loaded);
    ASSERT_FALSE(failure);
    EXPECT_EQ(failure.error().code, ReplayError::ChecksumMismatch);
    EXPECT_EQ(failure.error().tick, 2U);
    EXPECT_TRUE(failure.error().expectedChecksum);
    EXPECT_TRUE(failure.error().actualChecksum);
}

TEST_F(
    ReplayFileTest,
    RejectsTruncationMissingFooterTrailingDataAndUnknownVersions
)
{
    ASSERT_TRUE(writeReplayFile(path(), log()));
    const auto valid = bytes(path());
    for (const auto size : std::vector<
             std::size_t>{0, 7, 8, 79, 80, 87, valid.size() - 1})
    {
        SCOPED_TRACE(size);
        save(path("bad.arena"), valid.substr(0, size));
        auto result = readReplayFile(path("bad.arena"));
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, ReplayFileErrorCode::Invalid);
    }
    for (const auto offset : {8U, 12U})
    {
        auto bad = valid;
        put(bad, offset, 2, 4);
        save(path("bad.arena"), bad);
        auto result = readReplayFile(path("bad.arena"));
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, ReplayFileErrorCode::Invalid);
    }
    save(path("bad.arena"), valid + "x");
    EXPECT_FALSE(readReplayFile(path("bad.arena")));
    auto bad = valid;
    put(bad, bad.size() - 8, 999, 8);
    save(path("bad.arena"), bad);
    EXPECT_FALSE(readReplayFile(path("bad.arena")));
}

TEST_F(ReplayFileTest, UntrustedLengthsAreBoundedBeforeAllocation)
{
    ASSERT_TRUE(writeReplayFile(path(), log()));
    const auto valid = bytes(path());
    for (const auto offset : {76U})
    {
        auto bad = valid;
        put(bad, offset, UINT32_MAX, 4);
        save(path("huge.arena"), bad);
        auto result = readReplayFile(path("huge.arena"));
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, ReplayFileErrorCode::Limit);
    }
    auto hugeLength = valid;
    put(hugeLength, 84, UINT32_MAX, 4);
    save(path("length.arena"), hugeLength);
    auto truncated = readReplayFile(path("length.arena"));
    ASSERT_FALSE(truncated);
    EXPECT_EQ(truncated.error().code, ReplayFileErrorCode::Invalid);
    auto bad = valid;
    put(bad, 16, UINT64_MAX, 8);
    save(path("huge.arena"), bad);
    auto configLimit = readReplayFile(path("huge.arena"));
    ASSERT_FALSE(configLimit);
    EXPECT_EQ(configLimit.error().code, ReplayFileErrorCode::Limit);
    ReplayFileLimits small;
    small.maxFileBytes = valid.size() - 1;
    auto fileLimit = readReplayFile(path(), small);
    ASSERT_FALSE(fileLimit);
    EXPECT_EQ(fileLimit.error().code, ReplayFileErrorCode::Limit);
}

TEST_F(ReplayFileTest, WriterEnforcesTickByteAndConfigurationBounds)
{
    auto value = log();
    ReplayFileLimits limits;
    limits.maxTicks = 1;
    auto writer = ReplayWriter::create(
        path(),
        value.config,
        value.checkpoint,
        limits
    );
    ASSERT_TRUE(writer);
    ASSERT_TRUE((*writer)->append(value.ticks[0]));
    auto full = (*writer)->append(value.ticks[1]);
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error().code, ReplayFileErrorCode::Limit);
    EXPECT_FALSE((*writer)->finish());
    limits = {};
    limits.maxFileBytes = 80;
    auto bytesFull = ReplayWriter::create(
        path("bytes.arena"),
        value.config,
        value.checkpoint,
        limits
    );
    ASSERT_FALSE(bytesFull);
    EXPECT_EQ(bytesFull.error().code, ReplayFileErrorCode::Limit);
    value.config.positionLimit = 0;
    auto invalid = writeReplayFile(path("invalid.arena"), value);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, ReplayFileErrorCode::Invalid);
}

TEST_F(
    ReplayFileTest,
    OperatingSystemFailuresStayDistinctFromMalformedFiles
)
{
    auto missing = readReplayFile(path("missing.arena"));
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, ReplayFileErrorCode::Io);
    auto destination = writeReplayFile(directory, log());
    ASSERT_FALSE(destination);
    EXPECT_EQ(destination.error().code, ReplayFileErrorCode::Io);
    auto parentMissing = writeReplayFile(
        path("absent") / "file.arena",
        log()
    );
    ASSERT_FALSE(parentMissing);
    EXPECT_EQ(parentMissing.error().code, ReplayFileErrorCode::Io);
    auto full = writeReplayFile("/dev/full", log());
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error().code, ReplayFileErrorCode::Io);
}
} // namespace
