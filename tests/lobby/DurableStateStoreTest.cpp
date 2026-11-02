#include "lobby/DurableStateStore.h"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

namespace
{
std::atomic<bool> failNextWalSync{false};
std::atomic<unsigned> injectedSyncFailures{0};
std::atomic<bool> failNextCheckpointSync{false};
std::atomic<bool> failNextDirectorySync{false};
std::atomic<int> checkpointPausePhase{0};
int checkpointSignal = -1;

bool checkpointDescriptor(int descriptor)
{
    std::array<char, 4096> path{};
    const auto length = ::readlink(
        ("/proc/self/fd/" + std::to_string(descriptor)).c_str(),
        path.data(),
        path.size()
    );
    return length > 0 && std::string_view(
                             path.data(),
                             static_cast<std::size_t>(length)
                         )
                             .ends_with("/.authority.compact");
}

void checkpointPause(int phase)
{
    if (checkpointPausePhase.load() != phase)
        return;
    const char signal = static_cast<char>('0' + phase);
    if (::write(checkpointSignal, &signal, 1) != 1)
        ::_exit(91);
    for (;;)
        ::pause();
}
} // namespace

// Only arena_lobby_state_tests links --wrap=fdatasync. The journal
// still writes real bytes; this injection removes exactly one
// durability acknowledgement.
extern "C" int __real_fdatasync(int descriptor);
extern "C" int __wrap_fdatasync(int descriptor)
{
    if (checkpointDescriptor(descriptor))
    {
        if (failNextCheckpointSync.exchange(false))
        {
            errno = EIO;
            return -1;
        }
        const auto result = __real_fdatasync(descriptor);
        if (result == 0)
            checkpointPause(1);
        return result;
    }
    if (failNextWalSync.load())
    {
        std::array<char, 4096> path{};
        const auto link = "/proc/self/fd/" +
                          std::to_string(descriptor);
        const auto length = ::readlink(
            link.c_str(),
            path.data(),
            path.size()
        );
        if (length > 0 &&
            std::string_view(
                path.data(),
                static_cast<std::size_t>(length)
            )
                .ends_with("/authority.wal") &&
            failNextWalSync.exchange(false))
        {
            ++injectedSyncFailures;
            errno = EIO;
            return -1;
        }
    }
    return __real_fdatasync(descriptor);
}

extern "C" int __real_fsync(int descriptor);
extern "C" int __wrap_fsync(int descriptor)
{
    struct stat info{};
    if (failNextDirectorySync.load() &&
        ::fstat(descriptor, &info) == 0 && S_ISDIR(info.st_mode) &&
        failNextDirectorySync.exchange(false))
    {
        errno = EIO;
        return -1;
    }
    return __real_fsync(descriptor);
}

extern "C" int __real_renameat(int, const char*, int, const char*);
extern "C" int
__wrap_renameat(int a, const char* b, int c, const char* d)
{
    const auto result = __real_renameat(a, b, c, d);
    if (result == 0 && std::string_view(b) == ".authority.compact" &&
        std::string_view(d) == "authority.wal")
        checkpointPause(2);
    return result;
}

namespace service::lobby
{
namespace
{
using namespace std::chrono_literals;
using Store = DurableStateStore;

struct Directory
{
    Directory()
    {
        std::array<char, 64> path{};
        const std::string
            pattern = "/tmp/arena-authority-test-XXXXXX";
        std::copy(pattern.begin(), pattern.end(), path.begin());
        if (!::mkdtemp(path.data()))
            throw std::runtime_error(
                "temporary journal directory failed"
            );
        root = path.data();
    }
    ~Directory()
    {
        std::error_code ignored;
        std::filesystem::remove_all(root, ignored);
    }
    std::filesystem::path root;
};

std::size_t descriptors()
{
    return static_cast<std::size_t>(std::distance(
        std::filesystem::directory_iterator("/proc/self/fd"),
        std::filesystem::directory_iterator{}
    ));
}

template <class Predicate>
bool until(Predicate predicate, std::chrono::milliseconds budget = 3s)
{
    const auto end = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < end)
    {
        if (predicate())
            return true;
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

bool readByte(int descriptor, char& value)
{
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < deadline)
    {
        pollfd descriptorState{descriptor, POLLIN, 0};
        const auto ready = ::poll(&descriptorState, 1, 100);
        if (ready < 0 && errno == EINTR)
            continue;
        if (ready < 0)
            return false;
        if (ready && (descriptorState.revents & (POLLIN | POLLHUP)))
            return ::read(descriptor, &value, 1) == 1;
    }
    return false;
}

bool reapChild(pid_t child, int& status)
{
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline)
    {
        const auto waited = ::waitpid(child, &status, WNOHANG);
        if (waited == child)
            return true;
        if (waited < 0 && errno != EINTR)
            return false;
        std::this_thread::sleep_for(1ms);
    }
    ::kill(child, SIGKILL);
    while (::waitpid(child, &status, 0) < 0 && errno == EINTR)
    {
    }
    return false;
}

struct Harness
{
    explicit Harness(Store::Config config)
        : store(
              std::make_unique<Store>(
                  io.get_executor(), std::move(config)
              )
          )
        , runner([this] { io.run(); })
    {
    }
    ~Harness()
    {
        store->stop();
        until([&] { return store->stats().closed; });
        io.stop();
        if (runner.joinable())
            runner.join();
        store.reset();
    }
    Store::Result commit(
        std::uint64_t revision, std::vector<Store::Mutation> mutations
    )
    {
        auto result = std::make_shared<std::promise<Store::Result>>();
        auto future = result->get_future();
        if (!store->commit(
                revision,
                std::move(mutations),
                [result](auto value) { result->set_value(value); }
            ))
            throw std::runtime_error(
                "journal test admission rejected"
            );
        if (future.wait_for(3s) != std::future_status::ready)
            throw std::runtime_error(
                "journal owner completion timed out"
            );
        return future.get();
    }
    boost::asio::io_context io;
    std::unique_ptr<Store> store;
    std::thread runner;
};

TEST(
    DurableStateStoreTest,
    DirectorySyncLossAfterRenameQuarantinesAndRecoversCheckpoint
)
{
    Directory directory;
    Store::Config config{directory.root};
    config.maxBytes = 384;
    config.maxRecordBytes = 128;
    const auto baseline = descriptors();
    {
        Harness owner(config);
        for (std::uint64_t revision = 0; revision < 3; ++revision)
            ASSERT_TRUE(owner.commit(
                revision,
                {{"data", std::string(80, '0' + revision)}}
            ));
        failNextDirectorySync = true;
        const auto failed = owner.commit(
            3,
            {{"data", std::string(80, '3')}}
        );
        ASSERT_FALSE(failed);
        EXPECT_FALSE(failNextDirectorySync.load());
        EXPECT_EQ(failed.error(), Store::Error::OutcomeUnknown);
        EXPECT_EQ(owner.store->snapshot()->revision, 3);
        EXPECT_EQ(
            owner.store->snapshot()->values.at("data"),
            std::string(80, '2')
        );
        EXPECT_TRUE(owner.store->stats().poisoned);
        EXPECT_FALSE(owner.store->commit(
            3,
            {{"data", "rejected"}},
            [](auto) {}
        ));
        owner.store->stop();
        ASSERT_TRUE(until([&]
                          { return owner.store->stats().closed; }));
        EXPECT_EQ(owner.store->stats().pending, 0);
        EXPECT_EQ(owner.store->stats().pendingBytes, 0);
    }
    {
        std::ifstream input(
            directory.root / "authority.wal",
            std::ios::binary
        );
        std::string magic(8, '\0');
        input.read(magic.data(), 8);
        EXPECT_EQ(magic, "ARLOBBY2");
    }
    {
        Harness restored(config);
        EXPECT_EQ(restored.store->snapshot()->revision, 3);
        EXPECT_EQ(
            restored.store->snapshot()->values.at("data"),
            std::string(80, '2')
        );
        ASSERT_TRUE(
            restored.commit(3, {{"data", std::string(80, '3')}})
        );
    }
    {
        Harness restored(config);
        EXPECT_EQ(restored.store->snapshot()->revision, 4);
        EXPECT_EQ(
            restored.store->snapshot()->values.at("data"),
            std::string(80, '3')
        );
    }
    EXPECT_EQ(descriptors(), baseline);
}

TEST(
    DurableStateStoreTest,
    CheckpointSyncLossLeavesAcknowledgedPrefixAndReusesStaleTemp
)
{
    Directory directory;
    Store::Config config{directory.root};
    config.maxBytes = 384;
    config.maxRecordBytes = 128;
    const auto baseline = descriptors();
    {
        Harness owner(config);
        for (std::uint64_t revision = 0; revision < 3; ++revision)
            ASSERT_TRUE(owner.commit(
                revision,
                {{"data", std::string(80, '0' + revision)}}
            ));
        failNextCheckpointSync = true;
        const auto failed = owner.commit(
            3,
            {{"data", std::string(80, '3')}}
        );
        ASSERT_FALSE(failed);
        EXPECT_FALSE(failNextCheckpointSync.load());
        EXPECT_EQ(failed.error(), Store::Error::OutcomeUnknown);
        EXPECT_TRUE(owner.store->stats().poisoned);
        EXPECT_EQ(owner.store->snapshot()->revision, 3);
        EXPECT_EQ(
            owner.store->snapshot()->values.at("data"),
            std::string(80, '2')
        );
    }
    EXPECT_TRUE(
        std::filesystem::exists(directory.root / ".authority.compact")
    );
    {
        Harness restored(config);
        EXPECT_EQ(restored.store->snapshot()->revision, 3);
        ASSERT_TRUE(
            restored.commit(3, {{"data", std::string(80, '3')}})
        );
        EXPECT_EQ(restored.store->stats().compactions, 1);
    }
    {
        Harness restored(config);
        EXPECT_EQ(restored.store->snapshot()->revision, 4);
        EXPECT_EQ(
            restored.store->snapshot()->values.at("data"),
            std::string(80, '3')
        );
    }
    EXPECT_EQ(descriptors(), baseline);
}

TEST(
    DurableStateStoreTest,
    KillBeforeAndAfterRenameRecoversSamePrefixAndNewWriter
)
{
    for (const int phase : {1, 2})
    {
        Directory directory;
        Store::Config config{directory.root};
        config.maxBytes = 384;
        config.maxRecordBytes = 128;
        {
            Harness owner(config);
            for (std::uint64_t revision = 0; revision < 3; ++revision)
                ASSERT_TRUE(owner.commit(
                    revision,
                    {{"data", std::string(80, '0' + revision)}}
                ));
        }
        const auto baseline = descriptors();
        std::array<int, 2> channel{};
        ASSERT_EQ(::pipe(channel.data()), 0);
        const auto child = ::fork();
        ASSERT_GE(child, 0);
        if (child == 0)
        {
            ::close(channel[0]);
            checkpointSignal = channel[1];
            checkpointPausePhase = phase;
            try
            {
                Harness owner(config);
                owner.commit(3, {{"data", std::string(80, '3')}});
            }
            catch (...)
            {
                ::_exit(92);
            }
            ::_exit(93);
        }
        ::close(channel[1]);
        char observed = 0;
        const bool reached = readByte(channel[0], observed);
        ::close(channel[0]);
        const auto killed = ::kill(child, SIGKILL);
        int status = 0;
        const bool reaped = reapChild(child, status);
        ASSERT_TRUE(reached) << phase;
        EXPECT_EQ(observed, '0' + phase);
        EXPECT_EQ(killed, 0);
        ASSERT_TRUE(reaped);
        EXPECT_TRUE(WIFSIGNALED(status));
        EXPECT_EQ(WTERMSIG(status), SIGKILL);
        {
            Harness restored(config);
            EXPECT_EQ(restored.store->snapshot()->revision, 3);
            EXPECT_EQ(
                restored.store->snapshot()->values.at("data"),
                std::string(80, '2')
            );
            ASSERT_TRUE(
                restored.commit(3, {{"data", std::string(80, '3')}})
            );
        }
        {
            Harness restored(config);
            EXPECT_EQ(restored.store->snapshot()->revision, 4);
            EXPECT_EQ(
                restored.store->snapshot()->values.at("data"),
                std::string(80, '3')
            );
        }
        EXPECT_EQ(descriptors(), baseline);
    }
}

TEST(
    DurableStateStoreTest,
    CompactedEmptyStateRetainsRevisionAndRejectsIncompleteCheckpoint
)
{
    Directory directory;
    Store::Config config{directory.root};
    config.maxBytes = 384;
    config.maxRecordBytes = 128;
    {
        Harness owner(config);
        for (std::uint64_t revision = 0; revision < 3; ++revision)
            ASSERT_TRUE(owner.commit(
                revision,
                {{"data", std::string(80, '0')}}
            ));
        ASSERT_TRUE(owner.commit(3, {{"data", std::nullopt}}));
        ASSERT_TRUE(
            owner.commit(4, {{"again", std::string(80, 'x')}})
        );
        EXPECT_GT(owner.store->stats().compactions, 0);
    }
    const auto journal = directory.root / "authority.wal";
    {
        Harness restored(config);
        EXPECT_EQ(restored.store->snapshot()->revision, 5);
        EXPECT_FALSE(
            restored.store->snapshot()->values.contains("data")
        );
        EXPECT_EQ(
            restored.store->snapshot()->values.at("again"),
            std::string(80, 'x')
        );
        for (std::uint64_t revision = 5; revision < 12; ++revision)
            ASSERT_TRUE(restored.commit(
                revision,
                {{"again", std::string(80, 'y')}}
            ));
    }
    std::ifstream input(journal, std::ios::binary);
    const std::string bytes(
        (std::istreambuf_iterator<char>(input)),
        {}
    );
    input.close();
    ASSERT_EQ(bytes.substr(0, 8), "ARLOBBY2");
    const auto baseline = descriptors();
    // Header declares a mandatory checkpoint frame. This partial body
    // must fail closed instead of returning an empty state at the
    // base revision.
    std::filesystem::resize_file(journal, 30);
    boost::asio::io_context io;
    EXPECT_THROW(
        Store(io.get_executor(), config),
        std::runtime_error
    );
    EXPECT_EQ(std::filesystem::file_size(journal), 30);
    EXPECT_EQ(descriptors(), baseline);
}

TEST(
    DurableStateStoreTest,
    RewritesCompactBoundedMultiFrameStateAndPreserveCas
)
{
    Directory directory;
    const auto baseline = descriptors();
    Store::Config config{directory.root};
    config.maxBytes = 1024;
    config.maxRecordBytes = 128;
    std::shared_ptr<const Store::Snapshot> old;
    {
        Harness owner(config);
        ASSERT_TRUE(
            owner.commit(0, {{"left", std::string(80, 'a')}})
        );
        ASSERT_TRUE(
            owner.commit(1, {{"right", std::string(80, 'b')}})
        );
        ASSERT_TRUE(
            owner.commit(2, {{"stable", std::string(80, 'c')}})
        );
        old = owner.store->snapshot();
        for (std::uint64_t revision = 3; revision < 1000; ++revision)
        {
            const auto result = owner.commit(
                revision,
                {{"left", std::to_string(revision)}}
            );
            ASSERT_TRUE(result) << revision;
            EXPECT_EQ(*result, revision + 1);
            EXPECT_LE(
                owner.store->stats().fileBytes,
                config.maxBytes
            );
        }
        EXPECT_GT(owner.store->stats().compactions, 0);
        const auto stale = owner.commit(2, {{"left", "stale"}});
        ASSERT_FALSE(stale);
        EXPECT_EQ(stale.error(), Store::Error::Conflict);
        EXPECT_EQ(old->values.at("left"), std::string(80, 'a'));
    }
    {
        Harness restored(config);
        EXPECT_EQ(restored.store->snapshot()->revision, 1000);
        EXPECT_EQ(
            restored.store->snapshot()->values.at("left"),
            "999"
        );
        EXPECT_EQ(
            restored.store->snapshot()->values.at("right"),
            std::string(80, 'b')
        );
        ASSERT_TRUE(restored.commit(1000, {{"right", std::nullopt}}));
    }
    EXPECT_EQ(descriptors(), baseline);
}

TEST(
    DurableStateStoreTest,
    AtomicBatchesRetainSnapshotsAndReopenCompleteState
)
{
    Directory directory;
    const auto baseline = descriptors();
    std::shared_ptr<const Store::Snapshot> old;
    {
        Harness owner({directory.root});
        ASSERT_EQ(owner.store->snapshot()->revision, 0);
        const auto first = owner.commit(
            0,
            {{"session/42", "epoch-1"}, {"room/7", "42,43"}}
        );
        ASSERT_TRUE(first);
        EXPECT_EQ(*first, 1);
        old = owner.store->snapshot();
        const auto next = owner.commit(
            1,
            {{"session/42", "epoch-2"},
             {"room/7", std::nullopt},
             {"empty", ""}}
        );
        ASSERT_TRUE(next);
        EXPECT_EQ(*next, 2);
        EXPECT_EQ(old->values.at("session/42"), "epoch-1");
        EXPECT_TRUE(old->values.contains("room/7"));
        EXPECT_FALSE(
            owner.store->snapshot()->values.contains("room/7")
        );
    }
    {
        Harness restored({directory.root});
        const auto state = restored.store->snapshot();
        EXPECT_EQ(state->revision, 2);
        EXPECT_EQ(state->values.at("session/42"), "epoch-2");
        EXPECT_EQ(state->values.at("empty"), "");
        EXPECT_FALSE(state->values.contains("room/7"));
    }
    EXPECT_EQ(old->values.at("session/42"), "epoch-1");
    EXPECT_EQ(descriptors(), baseline);
}

TEST(
    DurableStateStoreTest, ConcurrentCompareAndSwapHasExactlyOneWinner
)
{
    Directory directory;
    Harness owner({directory.root});
    std::array<std::future<Store::Result>, 16> results;
    std::vector<std::thread> callers;
    std::atomic<unsigned> rejected{0};
    for (unsigned index = 0; index < results.size(); ++index)
    {
        auto
            promise = std::make_shared<std::promise<Store::Result>>();
        results[index] = promise->get_future();
        callers.emplace_back(
            [&, index, promise]
            {
                if (!owner.store->commit(
                        0,
                        {{"candidate", std::to_string(index)}},
                        [promise](auto value)
                        { promise->set_value(value); }
                    ))
                {
                    ++rejected;
                    promise->set_value(
                        std::unexpected(Store::Error::Unavailable)
                    );
                }
            }
        );
    }
    for (auto& caller : callers)
        caller.join();
    unsigned winners = 0, conflicts = 0;
    for (auto& future : results)
    {
        ASSERT_EQ(future.wait_for(3s), std::future_status::ready);
        const auto result = future.get();
        if (result)
        {
            ++winners;
            EXPECT_EQ(*result, 1);
        }
        else if (result.error() == Store::Error::Conflict)
            ++conflicts;
    }
    EXPECT_EQ(rejected, 0);
    EXPECT_EQ(winners, 1);
    EXPECT_EQ(conflicts, 15);
    EXPECT_EQ(owner.store->snapshot()->revision, 1);
    EXPECT_EQ(owner.store->snapshot()->values.size(), 1);
    EXPECT_TRUE(until([&]
                      { return owner.store->stats().pending == 0; }));
}

TEST(
    DurableStateStoreTest,
    UnpumpedOwnerKeepsPendingAdmissionBoundedAndStopDrains
)
{
    Directory directory;
    boost::asio::io_context io;
    Store::Config config{directory.root};
    config.maxPending = 2;
    config.maxPendingBytes = 4096;
    config.maxRecordBytes = 128;
    Store store(io.get_executor(), config);
    std::vector<std::uint64_t> delivered;
    const auto completion = [&](Store::Result value)
    {
        ASSERT_TRUE(value);
        delivered.push_back(*value);
    };
    ASSERT_TRUE(store.commit(0, {{"a", "1"}}, completion));
    ASSERT_TRUE(store.commit(1, {{"b", "2"}}, completion));
    ASSERT_TRUE(until([&] { return store.stats().committed == 2; }));
    EXPECT_EQ(store.stats().pending, 2);
    EXPECT_TRUE(delivered.empty());
    EXPECT_FALSE(store.commit(2, {{"c", "3"}}, completion));
    EXPECT_EQ(store.stats().pending, 2);
    const auto started = std::chrono::steady_clock::now();
    store.stop();
    store.stop();
    EXPECT_LT(std::chrono::steady_clock::now() - started, 100ms);
    ASSERT_TRUE(until([&] { return store.stats().writerStopped; }));
    EXPECT_FALSE(store.stats().closed);
    io.run();
    EXPECT_EQ(delivered, (std::vector<std::uint64_t>{1, 2}));
    EXPECT_TRUE(store.stats().closed);
    EXPECT_EQ(store.stats().pendingBytes, 0);
    EXPECT_FALSE(store.commit(2, {{"c", "3"}}, completion));
}

TEST(
    DurableStateStoreTest,
    InvalidCapacityAndStateLimitsNeverPartiallyWrite
)
{
    Directory directory;
    Store::Config config{directory.root};
    config.maxBytes = 128;
    config.maxRecordBytes = 64;
    config.maxPendingBytes = 4096;
    Harness owner(config);
    std::atomic<unsigned> unexpected{0};
    const auto callback = [&](auto) { ++unexpected; };
    EXPECT_FALSE(owner.store->commit(0, {}, callback));
    EXPECT_FALSE(owner.store->commit(0, {{"", "x"}}, callback));
    EXPECT_FALSE(
        owner.store->commit(0, {{"x", "1"}, {"x", "2"}}, callback)
    );
    EXPECT_FALSE(owner.store->commit(
        0,
        {{"x", std::string(100, 'x')}},
        callback
    ));
    std::string retained(1, 'x');
    retained.reserve(8192);
    std::vector<Store::Mutation> reserved;
    reserved.push_back({"x", std::move(retained)});
    EXPECT_FALSE(
        owner.store->commit(0, std::move(reserved), callback)
    );
    EXPECT_EQ(unexpected, 0);
    ASSERT_TRUE(owner.commit(0, {{"a", "12345678"}}));
    ASSERT_TRUE(owner.commit(1, {{"b", "12345678"}}));
    const auto bytes = std::filesystem::file_size(
        directory.root / "authority.wal"
    );
    const auto full = owner.commit(2, {{"c", "12345678"}});
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error(), Store::Error::Capacity);
    EXPECT_EQ(owner.store->snapshot()->revision, 2);
    EXPECT_FALSE(owner.store->snapshot()->values.contains("c"));
    EXPECT_EQ(
        std::filesystem::file_size(directory.root / "authority.wal"),
        bytes
    );
    EXPECT_FALSE(owner.store->stats().poisoned);
    Directory second;
    config = Store::Config{second.root};
    config.maxKeys = 1;
    Harness bounded(config);
    const auto rejected = bounded.commit(0, {{"a", "1"}, {"b", "2"}});
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), Store::Error::Capacity);
    EXPECT_EQ(bounded.store->snapshot()->revision, 0);
    EXPECT_TRUE(bounded.store->snapshot()->values.empty());
    EXPECT_EQ(
        std::filesystem::file_size(second.root / "authority.wal"),
        16
    );
}

TEST(DurableStateStoreTest, RecoveryTruncatesOnlyIncompleteFinalFrame)
{
    Directory directory;
    std::uintmax_t prefix;
    {
        Harness owner({directory.root});
        ASSERT_TRUE(owner.commit(0, {{"session", "valid-prefix"}}));
        prefix = std::filesystem::file_size(
            directory.root / "authority.wal"
        );
        ASSERT_TRUE(
            owner.commit(1, {{"session", "unacknowledged-tail"}})
        );
    }
    const auto journal = directory.root / "authority.wal";
    const auto complete = std::filesystem::file_size(journal);
    std::filesystem::resize_file(journal, complete - 3);
    {
        Harness owner({directory.root});
        EXPECT_EQ(owner.store->snapshot()->revision, 1);
        EXPECT_EQ(
            owner.store->snapshot()->values.at("session"),
            "valid-prefix"
        );
        EXPECT_EQ(std::filesystem::file_size(journal), prefix);
        ASSERT_TRUE(owner.commit(1, {{"session", "new-tail"}}));
    }
    {
        Harness owner({directory.root});
        EXPECT_EQ(owner.store->snapshot()->revision, 2);
        EXPECT_EQ(
            owner.store->snapshot()->values.at("session"),
            "new-tail"
        );
    }
}

TEST(
    DurableStateStoreTest,
    CompleteCorruptionAndUnsupportedHeaderFailClosedWithoutFdLeaks
)
{
    Directory directory;
    {
        Harness owner({directory.root});
        ASSERT_TRUE(owner.commit(0, {{"key", "payload"}}));
    }
    const auto journal = directory.root / "authority.wal";
    std::ifstream input(journal, std::ios::binary);
    const std::string original(
        (std::istreambuf_iterator<char>(input)),
        {}
    );
    input.close();
    const auto baseline = descriptors();
    for (const auto index : {original.size() - 1, std::size_t{11}})
    {
        auto corrupt = original;
        corrupt[index] ^= 1;
        std::ofstream output(
            journal,
            std::ios::binary | std::ios::trunc
        );
        output.write(
            corrupt.data(),
            static_cast<std::streamsize>(corrupt.size())
        );
        output.close();
        boost::asio::io_context io;
        EXPECT_THROW(
            Store(io.get_executor(), {directory.root}),
            std::runtime_error
        );
        EXPECT_EQ(
            std::filesystem::file_size(journal),
            corrupt.size()
        );
        EXPECT_EQ(descriptors(), baseline);
    }
}

TEST(
    DurableStateStoreTest,
    ExclusiveLockSymlinkAndSpecialFilesAreRejected
)
{
    Directory directory;
    {
        Harness owner({directory.root});
        boost::asio::io_context io;
        const auto baseline = descriptors();
        EXPECT_THROW(
            Store(io.get_executor(), {directory.root}),
            std::runtime_error
        );
        EXPECT_EQ(descriptors(), baseline);
    }
    Directory link;
    const auto outside = link.root / "outside";
    {
        std::ofstream file(outside);
        file << "unchanged";
    }
    std::filesystem::create_symlink(
        outside,
        link.root / "authority.wal"
    );
    {
        boost::asio::io_context io;
        EXPECT_THROW(
            Store(io.get_executor(), {link.root}),
            std::runtime_error
        );
    }
    std::ifstream external(outside);
    std::string bytes;
    external >> bytes;
    EXPECT_EQ(bytes, "unchanged");
    Directory fifo;
    ASSERT_EQ(
        ::mkfifo((fifo.root / "authority.wal").c_str(), 0600),
        0
    );
    boost::asio::io_context io;
    const auto started = std::chrono::steady_clock::now();
    EXPECT_THROW(
        Store(io.get_executor(), {fifo.root}),
        std::runtime_error
    );
    EXPECT_LT(std::chrono::steady_clock::now() - started, 1s);
}

TEST(
    DurableStateStoreTest,
    SyncFailureQuarantinesWriterAndReopenResolvesUnknownOutcome
)
{
    Directory directory;
    const auto before = injectedSyncFailures.load();
    {
        Harness owner({directory.root});
        failNextWalSync = true;
        const auto result = owner.commit(
            0,
            {{"receipt", "possibly-durable"}}
        );
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), Store::Error::OutcomeUnknown);
        EXPECT_EQ(owner.store->snapshot()->revision, 0);
        EXPECT_TRUE(owner.store->stats().poisoned);
        EXPECT_FALSE(
            owner.store->commit(0, {{"other", "x"}}, [](auto) {})
        );
        EXPECT_TRUE(until([&]
                          { return owner.store->stats().closed; }));
        EXPECT_EQ(owner.store->stats().ioFailures, 1);
    }
    EXPECT_EQ(injectedSyncFailures.load(), before + 1);
    {
        // This is a real reopen in the same kernel, not a simulated
        // power-loss claim.
        Harness recovered({directory.root});
        EXPECT_EQ(recovered.store->snapshot()->revision, 1);
        EXPECT_EQ(
            recovered.store->snapshot()->values.at("receipt"),
            "possibly-durable"
        );
        EXPECT_TRUE(recovered.commit(1, {{"resolved", "yes"}}));
    }
}

TEST(
    DurableStateStoreTest,
    CallbackFailureCannotUndoCommitAndClosedWaitsForDelivery
)
{
    Directory directory;
    Harness owner({directory.root});
    ASSERT_TRUE(owner.store->commit(
        0,
        {{"first", "saved"}},
        [](auto) { throw std::runtime_error("test callback"); }
    ));
    ASSERT_TRUE(until(
        [&] { return owner.store->stats().callbackFailures == 1; }
    ));
    EXPECT_EQ(owner.store->snapshot()->revision, 1);
    auto entered = std::make_shared<std::promise<void>>();
    auto enteredFuture = entered->get_future();
    auto release = std::make_shared<std::promise<void>>();
    auto released = release->get_future().share();
    ASSERT_TRUE(owner.store->commit(
        1,
        {{"second", "saved"}},
        [entered, released](auto result)
        {
            EXPECT_TRUE(result);
            entered->set_value();
            released.wait_for(3s);
        }
    ));
    ASSERT_EQ(enteredFuture.wait_for(3s), std::future_status::ready);
    owner.store->stop();
    ASSERT_TRUE(
        until([&] { return owner.store->stats().writerStopped; })
    );
    EXPECT_FALSE(owner.store->stats().closed);
    EXPECT_EQ(owner.store->stats().pending, 1);
    release->set_value();
    EXPECT_TRUE(until([&] { return owner.store->stats().closed; }));
    EXPECT_EQ(owner.store->snapshot()->revision, 2);
}

/** Runs before this test starts any parent thread. The child owns
 * every writer. */
TEST(
    DurableStateStoreTest,
    AbruptWriterDeathPreservesAcknowledgedPrefix
)
{
    Directory directory;
    std::array<int, 2> acknowledgements{}, commands{};
    ASSERT_EQ(::pipe2(acknowledgements.data(), O_CLOEXEC), 0);
    ASSERT_EQ(::pipe2(commands.data(), O_CLOEXEC), 0);
    const auto child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0)
    {
        ::close(acknowledgements[0]);
        ::close(commands[1]);
        try
        {
            Harness owner({directory.root});
            for (unsigned index = 0; index < 5; ++index)
            {
                if (!owner.commit(
                        index,
                        {{"key/" + std::to_string(index),
                          "acknowledged"}}
                    ))
                    ::_exit(11);
                const char ack = 'A';
                if (::write(acknowledgements[1], &ack, 1) != 1)
                    ::_exit(12);
                char proceed;
                if (::read(commands[0], &proceed, 1) != 1)
                    ::_exit(13);
            }
            if (!owner.store->commit(
                    5,
                    {{"unacknowledged", std::string(65536, 'x')}},
                    [](auto) {}
                ))
                ::_exit(14);
            const char admitted = 'P';
            ::write(acknowledgements[1], &admitted, 1);
            for (;;)
                ::pause();
        }
        catch (...)
        {
            ::_exit(15);
        }
    }
    ::close(acknowledgements[1]);
    ::close(commands[0]);
    bool passed = true;
    for (unsigned index = 0; index < 5; ++index)
    {
        char ack;
        if (!readByte(acknowledgements[0], ack) || ack != 'A')
        {
            passed = false;
            break;
        }
        const char proceed = 'C';
        if (::write(commands[1], &proceed, 1) != 1)
        {
            passed = false;
            break;
        }
    }
    char marker;
    if (passed)
        passed = readByte(acknowledgements[0], marker) &&
                 marker == 'P';
    EXPECT_EQ(::kill(child, SIGKILL), 0);
    int status{};
    EXPECT_TRUE(reapChild(child, status));
    ::close(acknowledgements[0]);
    ::close(commands[1]);
    ASSERT_TRUE(passed);
    ASSERT_TRUE(WIFSIGNALED(status));
    EXPECT_EQ(WTERMSIG(status), SIGKILL);
    Harness restored({directory.root});
    const auto state = restored.store->snapshot();
    EXPECT_GE(state->revision, 5);
    EXPECT_LE(state->revision, 6);
    for (unsigned index = 0; index < 5; ++index)
        EXPECT_EQ(
            state->values.at("key/" + std::to_string(index)),
            "acknowledged"
        );
}

TEST(
    DurableStateStoreTest,
    RealPartialWriteFailureReturnsUnknownAndRecoversPreviousPrefix
)
{
    Directory directory;
    const auto child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0)
    {
        ::signal(SIGXFSZ, SIG_IGN);
        try
        {
            Harness owner({directory.root});
            if (!owner.commit(0, {{"prefix", "preserved"}}))
                ::_exit(21);
            const auto limit = std::filesystem::file_size(
                                   directory.root / "authority.wal"
                               ) +
                               8;
            const rlimit ceiling{limit, limit};
            if (::setrlimit(RLIMIT_FSIZE, &ceiling) != 0)
                ::_exit(22);
            const auto failure = owner.commit(
                1,
                {{"next", std::string(1024, 'x')}}
            );
            if (failure ||
                failure.error() != Store::Error::OutcomeUnknown ||
                !owner.store->stats().poisoned)
                ::_exit(23);
            if (!until([&] { return owner.store->stats().closed; }))
                ::_exit(24);
            ::_exit(0);
        }
        catch (...)
        {
            ::_exit(25);
        }
    }
    int status{};
    ASSERT_TRUE(reapChild(child, status));
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    Harness owner({directory.root});
    EXPECT_EQ(owner.store->snapshot()->revision, 1);
    EXPECT_EQ(
        owner.store->snapshot()->values.at("prefix"),
        "preserved"
    );
    EXPECT_FALSE(owner.store->snapshot()->values.contains("next"));
}
} // namespace
} // namespace service::lobby