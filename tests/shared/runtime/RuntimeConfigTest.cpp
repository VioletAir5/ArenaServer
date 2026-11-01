#include "runtime/RuntimeConfig.h"

#include <unistd.h>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace arena::runtime
{
namespace
{
std::string document(std::uint64_t version, int port = 7105)
{
    return "{\"version\":\"" + std::to_string(version) +
           "\",\"desiredReady\":true,\"drainGraceMs\":5000,\"nodes\":"
           "["
           "{\"service\":\"storage\",\"nodeId\":\"node-a\",\"host\":"
           "\"127.0.0.1\",\"port\":" +
           std::to_string(port) + ",\"ready\":true}]}";
}

ConfigSnapshot::Ptr snapshot(std::uint64_t version, int port = 7105)
{
    auto parsed = ConfigSnapshot::parse(document(version, port));
    if (!parsed)
        throw std::runtime_error("invalid test snapshot");
    return *parsed;
}

TEST(
    RuntimeConfigTest,
    RejectsMissingRangePartialUnknownAndDuplicateFields
)
{
    ConfigStore store(snapshot(1));
    const auto old = store.snapshot();
    const std::vector<std::string> invalid{
        "",
        "{",
        "{}",
        document(0),
        document(2, 0),
        document(2, 65536),
        "{\"version\":\"2\",\"desiredReady\":true,\"drainGraceMs\":"
        "1}",
        "{\"version\":\"2\",\"desiredReady\":true,\"drainGraceMs\":1,"
        "\"nodes\":[]}",
        document(2).substr(0, document(2).size() - 1),
        "{\"version\":\"3\"," + document(2).substr(1),
        "{\"unexpected\":true," + document(2).substr(1),
        "{\"desired_ready\":false," + document(2).substr(1),
        std::string(ConfigSnapshot::maxBytes + 1, ' ')
    };
    for (const auto& candidate : invalid)
    {
        EXPECT_FALSE(ConfigSnapshot::parse(candidate))
            << candidate.substr(0, 100);
        EXPECT_EQ(store.snapshot(), old);
    }
    auto alias = document(2);
    alias.insert(alias.find("\"nodeId\""), "\"node_id\":\"node-b\",");
    EXPECT_FALSE(ConfigSnapshot::parse(alias));
    auto out_of_range = document(2);
    out_of_range.replace(out_of_range.find("5000"), 4, "10001");
    EXPECT_FALSE(ConfigSnapshot::parse(out_of_range));
    auto missing_ready = document(2);
    missing_ready.erase(missing_ready.find(",\"ready\":true"), 13);
    EXPECT_FALSE(ConfigSnapshot::parse(missing_ready));
}

TEST(RuntimeConfigTest, BooleanFieldsRequireJsonBooleanTokens)
{
    for (const std::string field :
         {"\"desiredReady\":true", "\"ready\":true"})
    {
        for (const char* invalid :
             {"\"true\"", "\"false\"", "1", "0", "null", "[]", "{}"})
        {
            auto candidate = document(2);
            candidate.replace(
                candidate.find(field),
                field.size(),
                field.substr(0, field.find(':') + 1) + invalid
            );
            EXPECT_FALSE(ConfigSnapshot::parse(candidate))
                << candidate;
        }
    }
}

TEST(
    RuntimeConfigTest,
    RetainsOldSnapshotAndRestoresContentAtNewVersion
)
{
    ConfigStore store(snapshot(1, 7105));
    const auto original = store.snapshot();
    ASSERT_TRUE(store.publish(snapshot(2, 7106)));
    EXPECT_EQ(original->resolve("storage")->port, 7105);
    EXPECT_EQ(store.snapshot()->resolve("storage")->port, 7106);
    EXPECT_FALSE(store.publish(original));
    EXPECT_EQ(store.snapshot()->version(), 2);
    ASSERT_TRUE(store.publish(snapshot(3, 7105)));
    EXPECT_EQ(store.snapshot()->version(), 3);
    EXPECT_EQ(store.snapshot()->resolve("storage")->port, 7105);
}

TEST(
    RuntimeConfigTest,
    CanonicalDiscoverySkipsUnreadyNodesAndRejectsDuplicateIdentity
)
{
    auto text = document(1);
    const auto node = text.substr(
        text.find("[{") + 1,
        text.size() - text.find("[{") - 3
    );
    auto other = node;
    other.replace(other.find("node-a"), 6, "node-z");
    const auto position = text.find("}]}");
    text.insert(position + 1, "," + other);
    auto parsed = ConfigSnapshot::parse(text);
    ASSERT_TRUE(parsed);
    EXPECT_EQ((*parsed)->resolve("storage")->id, "node-a");
    EXPECT_FALSE((*parsed)->resolve("missing"));
    auto unready = text;
    unready.replace(
        unready.find("\"ready\":true"),
        12,
        "\"ready\":false"
    );
    auto next = ConfigSnapshot::parse(unready);
    ASSERT_TRUE(next);
    EXPECT_EQ((*next)->resolve("storage")->id, "node-z");
    text.replace(text.find("node-z"), 6, "node-a");
    EXPECT_FALSE(ConfigSnapshot::parse(text));
}

TEST(
    RuntimeConfigTest,
    TenThousandConcurrentReadsAcrossOneThousandPublications
)
{
    ConfigStore store(snapshot(1, 10001));
    std::barrier round(9);
    std::atomic<int> inconsistent{0}, reads{0};
    std::vector<std::thread> readers;
    for (int index = 0; index < 8; ++index)
        readers.emplace_back(
            [&]
            {
                for (int phase = 0; phase < 1000; ++phase)
                {
                    round.arrive_and_wait();
                    for (int sample = 0;
                         sample < (phase < 250 ? 2 : 1);
                         ++sample)
                    {
                        const auto value = store.snapshot();
                        const auto node = value->resolve("storage");
                        if (!node ||
                            node->port !=
                                10000 + value->version() % 1000)
                            ++inconsistent;
                        ++reads;
                    }
                    round.arrive_and_wait();
                }
            }
        );
    for (std::uint64_t version = 2; version <= 1001; ++version)
    {
        const auto candidate = snapshot(
            version,
            10000 + version % 1000
        );
        round.arrive_and_wait();
        EXPECT_TRUE(store.publish(candidate));
        round.arrive_and_wait();
    }
    for (auto& reader : readers)
        reader.join();
    EXPECT_EQ(reads, 10000);
    EXPECT_EQ(inconsistent, 0);
    EXPECT_EQ(store.snapshot()->version(), 1001);
}

TEST(
    RuntimeConfigTest,
    RecursiveNotificationIsBoundedAndThrowingObserverCannotUndoPublication
)
{
    ConfigStore store(snapshot(1));
    int callbacks = 0;
    ASSERT_TRUE(store.publish(
        snapshot(2),
        [&](auto current)
        {
            ++callbacks;
            EXPECT_EQ(current, store.snapshot());
            auto nested = store.publish(snapshot(3));
            ASSERT_FALSE(nested);
            EXPECT_EQ(nested.error(), ConfigError::Busy);
        }
    ));
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(store.snapshot()->version(), 2);
    ASSERT_TRUE(store.publish(
        snapshot(3),
        [](auto) { throw std::runtime_error("observer"); }
    ));
    EXPECT_EQ(store.snapshot()->version(), 3);
    EXPECT_EQ(store.notificationFailures(), 1);
    EXPECT_TRUE(store.publish(snapshot(4)));
}

TEST(RuntimeConfigTest, RegularFileHalfWriteAndSymlinkBoundaries)
{
    std::array<char, 40> path{};
    const std::string pattern = "/tmp/arena-config-test-XXXXXX";
    std::copy(pattern.begin(), pattern.end(), path.begin());
    ASSERT_NE(::mkdtemp(path.data()), nullptr);
    const std::filesystem::path root(path.data());
    struct Cleanup
    {
        std::filesystem::path root;
        ~Cleanup()
        {
            std::error_code ignored;
            std::filesystem::remove_all(root, ignored);
        }
    } cleanup{root};
    const auto file = root / "runtime.json";
    {
        std::ofstream output(file);
        output << document(1);
    }
    ASSERT_TRUE(ConfigSnapshot::read(file));
    {
        std::ofstream output(file);
        output << document(2).substr(0, 30);
    }
    EXPECT_FALSE(ConfigSnapshot::read(file));
    {
        std::ofstream output(file);
        output << document(3);
    }
    ASSERT_TRUE(ConfigSnapshot::read(file));
    std::filesystem::create_symlink(file, root / "alias");
    EXPECT_FALSE(ConfigSnapshot::read(root / "alias"));
    EXPECT_FALSE(ConfigSnapshot::read(root));
    EXPECT_FALSE(ConfigSnapshot::read(root / "missing"));
}
} // namespace
} // namespace arena::runtime