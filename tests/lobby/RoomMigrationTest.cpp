#include "lobby/RoomMigration.h"

#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

#include "lobby_state.pb.h"
#include "room/RoomRuntime.h"
#include "room/RoomWire.h"

namespace
{
std::atomic<bool> holdSync{false}, enteredSync{false},
    failSync{false};
std::mutex syncMutex;
std::condition_variable syncChanged;
std::atomic<unsigned> directoryId{0};
} // namespace
extern "C" int __real_fdatasync(int);
extern "C" int __wrap_fdatasync(int fd)
{
    if (holdSync.load())
    {
        enteredSync.store(true);
        std::unique_lock lock(syncMutex);
        syncChanged.wait(lock, [] { return !holdSync.load(); });
    }
    if (failSync.exchange(false))
    {
        errno = EIO;
        return -1;
    }
    return __real_fdatasync(fd);
}

namespace
{
using namespace service::lobby;
using Error = RoomDirectoryError;
using namespace std::chrono_literals;
class RoomMigrationTest : public ::testing::Test
{
protected:
    boost::asio::io_context io;
    AtomicJournal::Config config;
    RoomDirectoryLimits limits;
    std::unique_ptr<AtomicJournal> store;
    std::unique_ptr<RoomMigration> migration;
    std::shared_ptr<const RoomDirectory> committed;
    RoomIdentity key{7, 19};

    void wait(const std::function<bool()>& predicate)
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (!predicate() &&
               std::chrono::steady_clock::now() < deadline)
        {
            io.restart();
            io.poll();
            std::this_thread::sleep_for(1ms);
        }
        if (!predicate())
            throw std::runtime_error(
                "physical journal completion timed out"
            );
    }
    void SetUp() override
    {
        holdSync = false;
        enteredSync = false;
        failSync = false;
        config.directory = std::filesystem::temp_directory_path() /
                           ("arena-room-migration-" +
                            std::to_string(::getpid()) + "-" +
                            std::to_string(++directoryId));
        std::filesystem::create_directories(config.directory);
        migration = std::make_unique<RoomMigration>(
            limits,
            "same-boot"
        );
        open();
    }
    void open()
    {
        store = std::make_unique<AtomicJournal>(
            io.get_executor(),
            config
        );
        auto loaded = migration->load(*store);
        if (!loaded)
            throw std::runtime_error(
                "room directory recovery rejected"
            );
        committed = *loaded;
    }
    void close()
    {
        if (!store)
            return;
        holdSync = false;
        syncChanged.notify_all();
        store->stop();
        wait([&] { return store->stats().closed; });
        store.reset();
    }
    void reopen()
    {
        close();
        open();
    }
    void TearDown() override
    {
        holdSync = false;
        failSync = false;
        syncChanged.notify_all();
        close();
        std::filesystem::remove_all(config.directory);
    }
    RoomWorkerImage
    image(std::uint64_t tick = 0, std::uint64_t cursor = 0)
    {
        // The coordinator intentionally treats these bytes as opaque.
        // Real RoomActor/checkpoint validation belongs to the worker
        // RPC boundary.
        return {
            tick,
            1000 + tick,
            cursor,
            "worker-image/" + std::to_string(tick) + "/" +
                std::to_string(cursor),
            tick
        };
    }
    RoomOperation input(std::string id)
    {
        return {std::move(id), RoomIngressKind::Input, 0, "axis"};
    }
    RoomOperation tick(std::uint64_t n)
    {
        return {
            "tick/" + std::to_string(n),
            RoomIngressKind::Tick,
            n,
            {}
        };
    }
    RoomMigration::Candidate plan(
        std::vector<RoomDirectoryCommand> commands,
        std::int64_t now = 1
    )
    {
        auto planned = migration->plan(*committed, commands, now);
        if (!planned)
            throw std::runtime_error(
                "unexpected rejected room candidate " +
                std::to_string(static_cast<int>(planned.error()))
            );
        return std::move(*planned);
    }
    AtomicJournal::Result write(
        std::uint64_t revision,
        std::vector<AtomicJournal::Mutation> mutations
    )
    {
        auto reply = std::make_shared<
            std::optional<AtomicJournal::Result>>();
        if (!store->commit(
                revision,
                std::move(mutations),
                [reply](auto r) { *reply = r; }
            ))
            return std::unexpected(AtomicJournal::Error::Capacity);
        wait([&] { return reply->has_value(); });
        return **reply;
    }
    void commit(
        std::vector<RoomDirectoryCommand> commands,
        std::int64_t now = 1
    )
    {
        auto candidate = plan(std::move(commands), now);
        std::uint64_t acknowledged = candidate.expectedRevision();
        if (!candidate.mutations().empty())
        {
            const auto result = write(
                candidate.expectedRevision(),
                candidate.mutations()
            );
            if (!result)
                throw std::runtime_error("journal commit rejected");
            acknowledged = *result;
        }
        const auto published = migration->publish(
            std::move(candidate),
            *store,
            acknowledged
        );
        if (!published)
            throw std::runtime_error(
                "durable room publication rejected"
            );
        committed = *published;
    }
    const RoomRouteRecord& room()
    {
        return *committed->find(key);
    }
    void create()
    {
        commit({RoomCreate{key, "source", image()}});
    }
    RoomPrepare prepare()
    {
        return {
            key,
            room().ownerEpoch,
            room().migrationWatermark + 1,
            "target",
            9'000'000'001
        };
    }
    RoomApply apply(
        std::string owner,
        std::uint64_t epoch,
        std::uint64_t n,
        std::uint64_t cursor,
        std::vector<RoomAppliedReply> replies
    )
    {
        return {
            key,
            std::move(owner),
            epoch,
            image(n, cursor),
            std::move(replies),
            RoomReplayExtent{
                room().image.tick + 1,
                n,
                "opaque-ordered-replay/" + std::to_string(n)
            }
        };
    }
    std::string roomStorageKey()
    {
        return "room-directory/room/" + std::to_string(key.room) +
               "/" + std::to_string(key.incarnation);
    }
};

TEST_F(
    RoomMigrationTest,
    PublicationWaitsForPhysicalSyncAndComposesAuthorityValues
)
{
    auto candidate = plan({RoomCreate{key, "source", image()}});
    auto mutations = candidate.mutations();
    mutations.push_back({"authority/other", "same-batch"});
    auto result = std::make_shared<
        std::optional<AtomicJournal::Result>>();
    holdSync = true;
    ASSERT_TRUE(store->commit(
        candidate.expectedRevision(),
        mutations,
        [result](auto r) { *result = r; }
    ));
    wait([] { return enteredSync.load(); });
    EXPECT_EQ(committed->size(), 0u);
    EXPECT_EQ(store->snapshot()->revision, 0u);
    auto early = migration->publish(std::move(candidate), *store, 1);
    ASSERT_FALSE(early);
    EXPECT_EQ(early.error(), Error::NotDurable);
    EXPECT_FALSE(result->has_value());
    holdSync = false;
    syncChanged.notify_all();
    wait([&] { return result->has_value(); });
    ASSERT_TRUE(**result);
    auto published = migration->publish(
        std::move(candidate),
        *store,
        ***result
    );
    ASSERT_TRUE(published);
    const auto consumed = migration->publish(
        std::move(candidate),
        *store,
        ***result
    );
    ASSERT_FALSE(consumed);
    EXPECT_EQ(consumed.error(), Error::Invalid);
    EXPECT_EQ((*published)->find(key)->owner, "source");
    EXPECT_EQ(
        store->snapshot()->values.at("authority/other"),
        "same-batch"
    );
    EXPECT_EQ(committed->size(), 0u);
}

TEST_F(
    RoomMigrationTest,
    NormalHandoffKeepsCutoffBuffersIngressAndFencesOldWorker
)
{
    create();
    commit(
        {RoomAccept{key, 1, input("first")},
         RoomAccept{key, 1, tick(1)}}
    );
    const auto p = prepare();
    commit({p, RoomFreeze{key, p.migration}});
    EXPECT_EQ(room().handoff->cutoff, 2u);
    commit(
        {RoomAccept{key, 1, input("after-freeze")},
         RoomFreeze{key, p.migration}}
    );
    EXPECT_EQ(room().handoff->cutoff, 2u);
    commit({apply("source", 1, 1, 2, {{1, "accepted"}, {2, {}}})});
    const auto fingerprint = roomImageFingerprint(key, room().image);
    commit(
        {RoomFrozen{key, p.migration, fingerprint},
         RoomTransferred{key, p.migration, "target", 2, fingerprint},
         RoomCommit{key, p.migration}}
    );
    EXPECT_EQ(room().owner, "target");
    EXPECT_EQ(room().ownerEpoch, 2u);
    ASSERT_EQ(room().inbox.size(), 1u);
    EXPECT_EQ(room().inbox.front().sequence, 3u);
    auto stale = migration->plan(
        *committed,
        {RoomAccept{key, 1, input("stale")}},
        1
    );
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), Error::StaleEpoch);
    commit(
        {RoomAccept{key, 2, tick(2)},
         apply("target", 2, 2, 4, {{3, "accepted"}, {4, {}}})}
    );
    EXPECT_EQ(room().image.cursor, 4u);
    EXPECT_TRUE(room().inbox.empty());
    EXPECT_TRUE(room().receipts.at("first").applied);
    EXPECT_TRUE(room().receipts.at("after-freeze").applied);
    const auto saved = room();
    reopen();
    EXPECT_EQ(room(), saved);
}

TEST_F(
    RoomMigrationTest, EveryPreCommitAbortFencesBothProspectiveOwners
)
{
    create();
    for (int phase = 0; phase < 4; ++phase)
    {
        const auto p = prepare();
        const auto oldEpoch = room().ownerEpoch;
        commit({p});
        if (phase >= 1)
            commit({RoomFreeze{key, p.migration}});
        const auto hash = roomImageFingerprint(key, room().image);
        if (phase >= 2)
            commit({RoomFrozen{key, p.migration, hash}});
        if (phase >= 3)
            commit({RoomTransferred{
                key,
                p.migration,
                "target",
                oldEpoch + 1,
                hash
            }});
        commit({RoomAbort{key, p.migration}});
        EXPECT_EQ(room().ownerEpoch, oldEpoch + 2);
        EXPECT_EQ(room().owner, "source");
        for (const auto epoch : {oldEpoch, oldEpoch + 1})
        {
            auto stale = migration->plan(
                *committed,
                {RoomAccept{key, epoch, input("old")}},
                1
            );
            ASSERT_FALSE(stale);
            EXPECT_EQ(stale.error(), Error::StaleEpoch);
        }
        const auto revision = committed->revision();
        commit({RoomAbort{key, p.migration}});
        EXPECT_EQ(committed->revision(), revision);
        reopen();
    }
}

TEST_F(
    RoomMigrationTest,
    CommittedMigrationRepeatsExactlyAndCannotBeAborted
)
{
    create();
    const auto p = prepare();
    const auto hash = roomImageFingerprint(key, room().image);
    commit(
        {p,
         RoomFreeze{key, p.migration, true},
         RoomTransferred{key, p.migration, "target", 2, hash},
         RoomCommit{key, p.migration}}
    );
    const auto revision = committed->revision();
    commit(
        {p,
         RoomFrozen{key, p.migration, hash},
         RoomTransferred{key, p.migration, "target", 2, hash},
         RoomCommit{key, p.migration}}
    );
    EXPECT_EQ(committed->revision(), revision);
    auto changed = p;
    ++changed.deadline;
    auto conflict = migration->plan(*committed, {changed}, 1);
    ASSERT_FALSE(conflict);
    EXPECT_EQ(conflict.error(), Error::Conflict);
    auto abort = migration->plan(
        *committed,
        {RoomAbort{key, p.migration}},
        1
    );
    ASSERT_FALSE(abort);
    EXPECT_EQ(abort.error(), Error::Phase);
    auto badProof = migration->plan(
        *committed,
        {RoomFrozen{key, p.migration, "different"}},
        1
    );
    ASSERT_FALSE(badProof);
    EXPECT_EQ(badProof.error(), Error::Conflict);
}

TEST_F(
    RoomMigrationTest,
    SourceFailureRestoresLastCommittedImageAndKeepsEveryPendingInput
)
{
    create();
    commit(
        {RoomAccept{key, 1, input("move")},
         RoomAccept{key, 1, tick(1)}}
    );
    const auto p = prepare();
    commit({p, RoomFreeze{key, p.migration}});
    reopen();
    // No source proposal was acknowledged. Recovery cannot invent its
    // progress.
    commit({RoomFreeze{key, p.migration, true}});
    EXPECT_EQ(room().handoff->cutoff, 0u);
    EXPECT_EQ(room().image.tick, 0u);
    ASSERT_EQ(room().inbox.size(), 2u);
    const auto hash = roomImageFingerprint(key, room().image);
    commit(
        {RoomTransferred{key, p.migration, "target", 2, hash},
         RoomCommit{key, p.migration}}
    );
    commit({apply("target", 2, 1, 2, {{1, "applied"}, {2, {}}})});
    EXPECT_EQ(room().image.tick, 1u);
    EXPECT_EQ(room().receipts.at("move").response, "applied");
    const auto revision = committed->revision();
    commit({RoomAccept{key, 2, input("move")}});
    EXPECT_EQ(committed->revision(), revision);
    EXPECT_EQ(room().nextSequence, 3u);
}

TEST_F(
    RoomMigrationTest,
    RestartAtEveryPhasePreservesBudgetCursorAndImmutableImage
)
{
    create();
    const auto p = prepare();
    commit({p});
    const auto check = [&]
    {
        const auto saved = room();
        const auto revision = committed->revision();
        reopen();
        EXPECT_EQ(room(), saved);
        EXPECT_EQ(committed->revision(), revision);
        EXPECT_EQ(room().handoff->deadline, p.deadline);
    };
    check();
    commit({RoomFreeze{key, p.migration}});
    check();
    const auto hash = roomImageFingerprint(key, room().image);
    commit({RoomFrozen{key, p.migration, hash}});
    check();
    commit({RoomTransferred{key, p.migration, "target", 2, hash}});
    check();
    commit({RoomCommit{key, p.migration}});
    check();
}

TEST_F(
    RoomMigrationTest,
    PendingReceiptCannotExpireAndRetryNeverRenewsItsDeadline
)
{
    close();
    limits.receiptRetentionNanos = 10;
    migration = std::make_unique<RoomMigration>(limits, "same-boot");
    open();
    create();
    commit({RoomAccept{key, 1, input("one")}}, 1);
    const auto expires = room().receipts.at("one").expiresAt;
    commit(
        {RoomPruneReceipts{key}, RoomAccept{key, 1, input("one")}},
        100
    );
    EXPECT_EQ(room().receipts.size(), 1u);
    EXPECT_EQ(room().receipts.at("one").expiresAt, expires);
    RoomApply
        applied{key, "source", 1, image(0, 1), {{1, "done"}}, {}};
    commit({applied, RoomPruneReceipts{key}}, 100);
    EXPECT_TRUE(room().receipts.empty());
    EXPECT_EQ(room().image.cursor, 1u);
}

TEST_F(RoomMigrationTest, CapacityAndConflictingDedupAreAtomic)
{
    close();
    limits.maxInbox = 2;
    limits.maxReceipts = 2;
    migration = std::make_unique<RoomMigration>(limits, "same-boot");
    open();
    create();
    commit({RoomAccept{key, 1, input("one")}});
    const auto before = room();
    auto full = migration->plan(
        *committed,
        {RoomAccept{key, 1, input("two")},
         RoomAccept{key, 1, input("three")}},
        1
    );
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error(), Error::Capacity);
    EXPECT_EQ(room(), before);
    auto changed = input("one");
    changed.payload = "different";
    auto conflict = migration->plan(
        *committed,
        {RoomAccept{key, 1, changed}},
        1
    );
    ASSERT_FALSE(conflict);
    EXPECT_EQ(conflict.error(), Error::Conflict);
    EXPECT_EQ(store->snapshot()->revision, committed->revision());
}

TEST_F(
    RoomMigrationTest,
    SameRevisionFromAnotherCandidateCannotPublishThisCandidate
)
{
    auto first = plan({RoomCreate{key, "source", image()}});
    auto other = plan({RoomCreate{{8, 19}, "other", image()}});
    auto result = write(other.expectedRevision(), other.mutations());
    ASSERT_TRUE(result);
    auto wrong = migration
                     ->publish(std::move(first), *store, *result);
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error(), Error::NotDurable);
    auto correct = migration
                       ->publish(std::move(other), *store, *result);
    ASSERT_TRUE(correct);
    EXPECT_FALSE((*correct)->find(key));
}

TEST_F(
    RoomMigrationTest,
    OtherDirectoryWritesCannotHideBehindMatchingCandidateMutations
)
{
    commit(
        {RoomCreate{key, "source", image()},
         RoomCreate{{8, 19}, "source", image()}}
    );
    auto first = plan({RoomAccept{key, 1, input("first")}});
    auto second = plan({RoomAccept{{8, 19}, 1, input("second")}});
    auto mutations = first.mutations();
    mutations.insert(
        mutations.end(),
        second.mutations().begin(),
        second.mutations().end()
    );
    const auto result = write(
        first.expectedRevision(),
        std::move(mutations)
    );
    ASSERT_TRUE(result);
    auto wrong = migration
                     ->publish(std::move(first), *store, *result);
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error(), Error::NotDurable);
    auto actual = migration->load(*store);
    ASSERT_TRUE(actual);
    EXPECT_EQ((*actual)->find(key)->inbox.size(), 1u);
    EXPECT_EQ((*actual)->find({8, 19})->inbox.size(), 1u);
}

TEST_F(
    RoomMigrationTest, StoreCasConflictDoesNotPublishAnyCandidateState
)
{
    auto candidate = plan({RoomCreate{key, "source", image()}});
    auto result = write(1, candidate.mutations());
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), AtomicJournal::Error::Conflict);
    auto unpublished = migration
                           ->publish(std::move(candidate), *store, 1);
    ASSERT_FALSE(unpublished);
    EXPECT_EQ(committed->size(), 0u);
    EXPECT_EQ(store->snapshot()->revision, 0u);
}

TEST_F(
    RoomMigrationTest, UnknownSyncOutcomeQuarantinesUntilActualReopen
)
{
    create();
    const auto old = committed;
    auto candidate = plan({RoomAccept{key, 1, input("once")}});
    failSync = true;
    const auto result = write(
        candidate.expectedRevision(),
        candidate.mutations()
    );
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), AtomicJournal::Error::OutcomeUnknown);
    EXPECT_TRUE(store->stats().poisoned);
    EXPECT_FALSE(migration->publish(
        std::move(candidate),
        *store,
        candidate.expectedRevision() + 1
    ));
    EXPECT_FALSE(migration->load(*store));
    EXPECT_TRUE(old->find(key)->inbox.empty());
    reopen();
    ASSERT_EQ(room().inbox.size(), 1u);
    const auto revision = committed->revision();
    commit({RoomAccept{key, 1, input("once")}});
    EXPECT_EQ(committed->revision(), revision);
    EXPECT_EQ(room().nextSequence, 2u);
}

TEST_F(
    RoomMigrationTest, BootMismatchAndWellFormedCorruptionFailClosed
)
{
    create();
    RoomMigration other(limits, "different-boot");
    auto boot = other.load(*store);
    ASSERT_FALSE(boot);
    EXPECT_EQ(boot.error(), Error::BootMismatch);
    arena::lobby_state::DirectoryRoom record;
    ASSERT_TRUE(record.ParseFromString(
        store->snapshot()->values.at(roomStorageKey())
    ));
    record.set_next_sequence(0);
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{roomStorageKey(), record.SerializeAsString()}}
    ));
    const auto bad = migration->load(*store);
    ASSERT_FALSE(bad);
    EXPECT_EQ(bad.error(), Error::Invalid);
    EXPECT_TRUE(committed->find(key));
}

TEST_F(
    RoomMigrationTest,
    MissingReplayExtentAndEpochExhaustionAreRejected
)
{
    create();
    commit({RoomAccept{key, 1, tick(1)}});
    commit({apply("source", 1, 1, 1, {{1, {}}})});
    reopen();
    const auto replay = "room-directory/replay/7/19/1";
    ASSERT_TRUE(
        write(store->snapshot()->revision, {{replay, std::nullopt}})
    );
    EXPECT_FALSE(migration->load(*store));
    // Restore the exact extent; changing only ownership high water is
    // a valid recovery image that must not wrap when Prepare reserves
    // two epochs.
    arena::lobby_state::DirectoryReplay extent;
    extent.set_version(2);
    extent.set_room_id(7);
    extent.set_incarnation(19);
    extent.set_first_tick(1);
    extent.set_last_tick(1);
    extent.set_payload("opaque-ordered-replay/1");
    arena::lobby_state::DirectoryRoom record;
    ASSERT_TRUE(record.ParseFromString(
        store->snapshot()->values.at(roomStorageKey())
    ));
    record.set_owner_epoch(
        std::numeric_limits<std::uint64_t>::max() - 1
    );
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{replay, extent.SerializeAsString()},
         {roomStorageKey(), record.SerializeAsString()}}
    ));
    const auto restored = migration->load(*store);
    ASSERT_TRUE(restored);
    committed = *restored;
    auto exhausted = migration->plan(*committed, {prepare()}, 1);
    ASSERT_FALSE(exhausted);
    EXPECT_EQ(exhausted.error(), Error::Exhausted);
}
TEST(
    RoomMigrationConfigTest,
    FullWireNodeBudgetIncludesPendingReceiptsAndHandoff
)
{
    RoomDirectoryLimits bounds;
    bounds.maxInbox = 16384;
    bounds.maxReceipts = 32765;
    EXPECT_NO_THROW(RoomMigration(bounds, "boot"));
    ++bounds.maxReceipts;
    EXPECT_THROW(
        RoomMigration(bounds, "boot"),
        std::invalid_argument
    );
    bounds.maxInbox = bounds.maxReceipts = 25000;
    EXPECT_THROW(
        RoomMigration(bounds, "boot"),
        std::invalid_argument
    );
}

TEST_F(RoomMigrationTest, OnePublicRoomCannotHaveTwoLiveIncarnations)
{
    auto split = migration->plan(
        *committed,
        {RoomCreate{key, "source", image()},
         RoomCreate{{7, 20}, "other", image()}},
        1
    );
    ASSERT_FALSE(split);
    EXPECT_EQ(split.error(), Error::Conflict);
    EXPECT_EQ(committed->size(), 0u);
    create();
    auto second = migration->plan(
        *committed,
        {RoomCreate{{7, 20}, "other", image()}},
        1
    );
    ASSERT_FALSE(second);
    EXPECT_EQ(second.error(), Error::Conflict);
    arena::lobby_state::DirectoryRoom forged;
    ASSERT_TRUE(forged.ParseFromString(
        store->snapshot()->values.at(roomStorageKey())
    ));
    forged.set_incarnation(20);
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{"room-directory/room/7/20", forged.SerializeAsString()}}
    ));
    const auto invalid = migration->load(*store);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), Error::Invalid);
    EXPECT_EQ(committed->size(), 1u);
}

TEST_F(
    RoomMigrationTest,
    NoopSharesExactlyOneAuthorityCasAndConsumesOnlyAfterValidation
)
{
    create();
    const auto before = committed;
    auto candidate = plan({RoomCreate{key, "source", image()}});
    ASSERT_TRUE(candidate.mutations().empty());
    ASSERT_TRUE(write(
        candidate.expectedRevision(),
        {{"authority/metadata", "committed-with-retry"}}
    ));
    const auto revision = store->snapshot()->revision;
    const auto result = migration->publish(
        std::move(candidate),
        *store,
        revision
    );
    ASSERT_TRUE(result);
    EXPECT_EQ((*result)->revision(), revision);
    EXPECT_EQ(before->revision() + 1, revision);
    EXPECT_EQ(before->find(key)->image, (*result)->find(key)->image);
    auto skipped = migration->plan(
        *before,
        {RoomCreate{key, "source", image()}},
        1
    );
    ASSERT_TRUE(skipped);
    ASSERT_TRUE(
        write(revision, {{"authority/metadata", "one-more-revision"}})
    );
    const auto wrong = migration->publish(
        std::move(*skipped),
        *store,
        revision + 1
    );
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error(), Error::NotDurable);
}

TEST_F(
    RoomMigrationTest,
    ReplayCapacityCannotPersistAStateThatRecoveryRejects
)
{
    close();
    config.maxKeys = 100000;
    open();
    create();
    constexpr std::uint64_t count = 65536;
    for (std::uint64_t start = 1; start <= count; start += count / 2)
    {
        std::vector<AtomicJournal::Mutation> mutations;
        mutations.reserve(count / 2 + 1);
        for (auto n = start; n < start + count / 2; ++n)
        {
            arena::lobby_state::DirectoryReplay replay;
            replay.set_version(2);
            replay.set_room_id(7);
            replay.set_incarnation(19);
            replay.set_first_tick(n);
            replay.set_last_tick(n);
            replay.set_payload("opaque");
            mutations.push_back(
                {"room-directory/replay/7/19/" + std::to_string(n),
                 replay.SerializeAsString()}
            );
        }
        if (start > 1)
        {
            arena::lobby_state::DirectoryRoom record;
            ASSERT_TRUE(record.ParseFromString(
                store->snapshot()->values.at(roomStorageKey())
            ));
            record.set_next_sequence(count + 1);
            record.set_admitted_tick(count);
            record.mutable_image()->set_tick(count);
            record.mutable_image()->set_logical_tick(count);
            record.mutable_image()->set_cursor(count);
            record.mutable_image()->set_checksum(123);
            record.mutable_image()->set_checkpoint(
                "seeded-worker-image"
            );
            mutations.push_back(
                {roomStorageKey(), record.SerializeAsString()}
            );
        }
        ASSERT_TRUE(
            write(store->snapshot()->revision, std::move(mutations))
        );
    }
    auto restored = migration->load(*store);
    ASSERT_TRUE(restored);
    committed = *restored;
    commit({RoomAccept{key, 1, tick(count + 1)}});
    const auto revision = committed->revision();
    const auto over = migration->plan(
        *committed,
        {apply("source", 1, count + 1, count + 1, {{count + 1, {}}})},
        1
    );
    ASSERT_FALSE(over);
    EXPECT_EQ(over.error(), Error::Capacity);
    EXPECT_EQ(store->snapshot()->revision, revision);
    EXPECT_EQ(room().image.tick, count);
    ASSERT_EQ(room().inbox.size(), 1u);
    EXPECT_TRUE(migration->load(*store));
}

namespace runtime = service::room;
namespace roomWire = service::room::wire;

class RuntimeRoomMigrationTest : public RoomMigrationTest
{
protected:
    std::optional<runtime::RuntimeImage> live;

    static runtime::RuntimeAuthorization
    authorization(std::uint64_t player)
    {
        return {
            {runtime::PlayerId{player},
             runtime::SessionId{player + 10},
             player + 99},
            player + 20,
            player + 1000,
            true
        };
    }
    runtime::RuntimeCommand command(
        runtime::RuntimeCommandKind kind, std::uint64_t player = 1
    )
    {
        runtime::RuntimeCommand out;
        out.kind = kind;
        out.caller = authorization(player);
        out.incarnation = key.incarnation;
        out.matchId = 101;
        return out;
    }
    runtime::RuntimeCutoff cutoff()
    {
        return {
            {authorization(1), authorization(2)},
            {authorization(1), authorization(2)},
            {},
            static_cast<std::int64_t>(
                (live->logicalCutoff + 1) * 50'000'000
            ),
            1'700'000'000'000 + live->logicalCutoff * 50
        };
    }
    RoomWorkerImage workerImage(
        const runtime::RuntimeImage& source, std::uint64_t cursor
    )
    {
        const auto bytes = roomWire::encodeImage(source);
        if (!bytes)
            throw std::runtime_error(
                "runtime image encoding rejected"
            );
        return {
            source.actor.tick,
            source.actor.checksum,
            cursor,
            *bytes,
            source.logicalCutoff
        };
    }
    void initialize(std::uint64_t duration = 2)
    {
        key.room = (std::uint64_t{1} << 63) | 101;
        runtime::RuntimePolicy policy;
        policy.battleDurationTicks = duration;
        auto created = runtime::RoomRuntime::create(
            runtime::RoomId{key.room},
            key.incarnation,
            policy,
            runtime::RuntimeAllocation{
                101,
                {authorization(1).member, authorization(2).member},
                {1000, 2000}
            },
            1'700'000'000'000
        );
        if (!created)
            throw std::runtime_error(
                "managed runtime creation rejected"
            );
        live = std::move(*created);
        commit({RoomCreate{key, "source", workerImage(*live, 0)}});
    }
    runtime::RuntimeProposal advance(runtime::RuntimeCutoff cut)
    {
        const auto logical = live->logicalCutoff + 1;
        auto cursor = room().image.cursor;
        std::vector<RoomDirectoryCommand> accepted;
        std::vector<RoomAppliedReply> replies;
        for (std::size_t i = 0; i < cut.commands.size(); ++i)
        {
            const auto bytes = roomWire::encodeCommand(
                cut.commands[i]
            );
            if (!bytes)
                throw std::runtime_error(
                    "runtime command encoding rejected"
                );
            const auto
                kind = cut.commands[i].kind ==
                               runtime::RuntimeCommandKind::Input
                           ? RoomIngressKind::Input
                       : cut.commands[i].kind ==
                               runtime::RuntimeCommandKind::Ready
                           ? RoomIngressKind::Ready
                           : RoomIngressKind::Settlement;
            accepted.push_back(
                RoomAccept{
                    key,
                    room().ownerEpoch,
                    {"command/" + std::to_string(logical) + "/" +
                         std::to_string(i),
                     kind,
                     0,
                     *bytes}
                }
            );
            replies.push_back({++cursor, {}});
        }
        auto context = cut;
        context.commands.clear();
        const auto bytes = roomWire::encodeCutoff(context);
        if (!bytes)
            throw std::runtime_error(
                "runtime cutoff encoding rejected"
            );
        accepted.push_back(
            RoomAccept{
                key,
                room().ownerEpoch,
                {"tick/" + std::to_string(logical),
                 RoomIngressKind::Tick,
                 logical,
                 *bytes}
            }
        );
        replies.push_back({++cursor, {}});
        commit(std::move(accepted), cut.nowNanos);

        auto proposal = runtime::RoomRuntime::step(*live, cut);
        if (!proposal)
            throw std::runtime_error("real runtime cutoff rejected");
        for (std::size_t i = 0; i < proposal->results.size(); ++i)
            replies[i].response = proposal->results[i].error
                                      ? "error/" +
                                            std::to_string(
                                                static_cast<int>(
                                                    *proposal
                                                         ->results[i]
                                                         .error
                                                )
                                            )
                                      : "ok";
        std::optional<RoomReplayExtent> extent;
        if (proposal->replay)
        {
            const auto encoded = roomWire::encodeReplayLog(
                {live->policy.actor, live->actor, {*proposal->replay}}
            );
            if (!encoded)
                throw std::runtime_error(
                    "real replay encoding rejected"
                );
            const auto replayed = runtime::RoomActor::replay(
                {live->policy.actor, live->actor, {*proposal->replay}}
            );
            if (!replayed ||
                replayed->snapshot != proposal->image.actor)
                throw std::runtime_error(
                    "real cutoff replay diverged"
                );
            extent = RoomReplayExtent{
                proposal->replay->tick,
                proposal->replay->tick,
                *encoded
            };
        }
        commit(
            {RoomApply{
                key,
                room().owner,
                room().ownerEpoch,
                workerImage(proposal->image, cursor),
                std::move(replies),
                std::move(extent)
            }},
            cut.nowNanos
        );
        live = proposal->image;
        return std::move(*proposal);
    }
    void enterPlaying()
    {
        auto cut = cutoff();
        auto first = command(runtime::RuntimeCommandKind::Ready, 1);
        auto second = command(runtime::RuntimeCommandKind::Ready, 2);
        first.ready = second.ready = true;
        cut.commands = {first, second};
        advance(std::move(cut));
        if (live->managed->phase != runtime::RuntimePhase::Ready)
            throw std::runtime_error("ready phase skipped");
        advance(cutoff());
        if (live->managed->phase != runtime::RuntimePhase::Playing)
            throw std::runtime_error("playing phase missing");
    }
    void restoreRuntime()
    {
        reopen();
        const auto decoded = roomWire::decodeImage(
            room().image.checkpoint
        );
        if (!decoded || !runtime::RoomRuntime::validate(*decoded))
            throw std::runtime_error(
                "recovered worker image invalid"
            );
        EXPECT_EQ(decoded->logicalCutoff, room().image.logicalTick);
        EXPECT_EQ(decoded->actor.tick, room().image.tick);
        EXPECT_EQ(decoded->actor.checksum, room().image.checksum);
        live = *decoded;
    }
};

TEST_F(
    RuntimeRoomMigrationTest,
    SettlingFinishedAndExpiryConsumeCutoffsWithoutInventingReplay
)
{
    initialize();
    enterPlaying();
    auto cut = cutoff();
    auto move = command(runtime::RuntimeCommandKind::Input);
    move.sequence = 1;
    move.dx = 1;
    cut.commands = {move};
    const auto ended = advance(std::move(cut));
    ASSERT_EQ(live->managed->phase, runtime::RuntimePhase::Settling);
    ASSERT_TRUE(ended.replay);
    const auto frozen = live->actor;
    const auto physical = frozen.tick;
    const auto beforeQuery = room().image.cursor;

    cut = cutoff();
    cut.commands = {
        command(runtime::RuntimeCommandKind::Status),
        command(runtime::RuntimeCommandKind::Settlement)
    };
    const auto queried = advance(std::move(cut));
    EXPECT_FALSE(queried.replay);
    ASSERT_EQ(queried.results.size(), 2u);
    EXPECT_FALSE(queried.results[0].error);
    EXPECT_EQ(
        queried.results[1].error,
        runtime::RuntimeError::Unavailable
    );
    EXPECT_EQ(room().image.cursor, beforeQuery + 3);
    EXPECT_EQ(live->actor, frozen);
    EXPECT_EQ(room().admittedTick, live->logicalCutoff);
    restoreRuntime();

    cut = cutoff();
    runtime::RuntimeCommand ack;
    ack.kind = runtime::RuntimeCommandKind::SettlementAck;
    ack.matchId = 101;
    ack.committed = true;
    cut.commands = {ack};
    const auto finished = advance(std::move(cut));
    EXPECT_FALSE(finished.replay);
    EXPECT_TRUE(finished.releaseMembership);
    ASSERT_EQ(live->managed->phase, runtime::RuntimePhase::Finished);
    EXPECT_EQ(live->actor, frozen);

    cut = cutoff();
    cut.commands = {command(runtime::RuntimeCommandKind::Settlement)};
    const auto result = advance(std::move(cut));
    ASSERT_EQ(result.results.size(), 1u);
    EXPECT_FALSE(result.results[0].error);
    EXPECT_FALSE(result.replay);
    cut = cutoff();
    cut.nowNanos = *live->managed->expiresAtNanos;
    const auto expired = advance(std::move(cut));
    EXPECT_TRUE(expired.retire);
    EXPECT_FALSE(expired.replay);
    EXPECT_EQ(live->actor.tick, physical);
    EXPECT_GT(live->logicalCutoff, physical);
    const auto saved = room();
    restoreRuntime();
    EXPECT_EQ(room(), saved);
    for (const auto& [id, receipt] : room().receipts)
        EXPECT_TRUE(receipt.applied) << id;
    std::size_t extents = 0;
    for (const auto& [name, bytes] : store->snapshot()->values)
        if (name.starts_with("room-directory/replay/"))
        {
            arena::lobby_state::DirectoryReplay extent;
            ASSERT_TRUE(extent.ParseFromString(bytes));
            const auto segment = roomWire::decodeReplayLog(
                extent.payload()
            );
            ASSERT_TRUE(segment);
            ASSERT_EQ(segment->ticks.size(), 1u);
            const auto& record = segment->ticks.front();
            EXPECT_EQ(record.tick, ++extents);
            EXPECT_EQ(extent.first_tick(), record.tick);
            EXPECT_EQ(extent.last_tick(), record.tick);
            EXPECT_EQ(segment->checkpoint.tick + 1, record.tick);
        }
    EXPECT_EQ(extents, physical);
}

TEST_F(
    RuntimeRoomMigrationTest,
    UnavailableMemberAbortsWithoutPhysicsAndMigratesExactLogicalCutoff
)
{
    initialize(20);
    enterPlaying();
    const auto frozen = live->actor;
    auto cut = cutoff();
    cut.roster.pop_back();
    cut.commands = {command(runtime::RuntimeCommandKind::Status)};
    const auto aborted = advance(std::move(cut));
    EXPECT_FALSE(aborted.replay);
    ASSERT_EQ(live->managed->phase, runtime::RuntimePhase::Settling);
    ASSERT_TRUE(live->managed->result);
    EXPECT_EQ(
        live->managed->result->reason,
        runtime::RuntimeReason::MemberUnavailable
    );
    EXPECT_EQ(live->actor, frozen);
    EXPECT_EQ(live->logicalCutoff, frozen.tick + 1);
    restoreRuntime();
    const auto saved = room().image;
    const auto migration = prepare();
    const auto proof = roomImageFingerprint(key, saved);
    commit(
        {migration,
         RoomFreeze{key, migration.migration},
         RoomFrozen{key, migration.migration, proof},
         RoomTransferred{
             key,
             migration.migration,
             "target",
             2,
             proof
         },
         RoomCommit{key, migration.migration}}
    );
    EXPECT_EQ(room().image, saved);
    restoreRuntime();
    cut = cutoff();
    cut.commands = {command(runtime::RuntimeCommandKind::Status)};
    advance(std::move(cut));
    EXPECT_EQ(room().owner, "target");
    EXPECT_EQ(room().image.tick, frozen.tick);
    EXPECT_EQ(room().image.logicalTick, saved.logicalTick + 1);
    EXPECT_EQ(room().image.cursor, saved.cursor + 2);
}

TEST_F(
    RoomMigrationTest,
    LogicalCutoffRollbackJumpAndFabricatedPhysicsFailAtomically
)
{
    create();
    commit({RoomAccept{key, 1, tick(1)}});
    const auto before = room();
    auto frozen = image(0, 1);
    frozen.logicalTick = 1;
    for (const auto logical : {std::uint64_t{0}, std::uint64_t{2}})
    {
        auto wrong = frozen;
        wrong.logicalTick = logical;
        const auto result = migration->plan(
            *committed,
            {RoomApply{key, "source", 1, wrong, {{1, {}}}, {}}},
            1
        );
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), Error::Invalid);
        EXPECT_EQ(room(), before);
    }
    auto advanced = frozen;
    advanced.tick = 2;
    EXPECT_FALSE(migration->plan(
        *committed,
        {RoomApply{
            key,
            "source",
            1,
            advanced,
            {{1, {}}},
            RoomReplayExtent{1, 2, "fake"}
        }},
        1
    ));
    advanced.tick = 1;
    EXPECT_FALSE(migration->plan(
        *committed,
        {RoomApply{key, "source", 1, advanced, {{1, {}}}, {}}},
        1
    ));
    EXPECT_FALSE(migration->plan(
        *committed,
        {RoomApply{
            key,
            "source",
            1,
            frozen,
            {{1, {}}},
            RoomReplayExtent{1, 1, "fake"}
        }},
        1
    ));
    commit({RoomApply{key, "source", 1, frozen, {{1, {}}}, {}}});
    const auto saved = room();
    reopen();
    EXPECT_EQ(room(), saved);
    EXPECT_EQ(room().image.logicalTick, 1u);
    EXPECT_EQ(room().image.tick, 0u);
    EXPECT_TRUE(room().inbox.empty());
}

TEST_F(
    RoomMigrationTest,
    FingerprintUsesSharedV2IdentityIncludingLogicalCutoff
)
{
    const auto original = image();
    auto changed = original;
    ++changed.logicalTick;
    const auto proof = roomImageFingerprint(key, original);
    ASSERT_EQ(proof.size(), 64u);
    const auto shared = roomWire::imageFingerprint(
        {key.room,
         key.incarnation,
         original.tick,
         original.checksum,
         original.cursor,
         original.logicalTick},
        original.checkpoint
    );
    ASSERT_TRUE(shared);
    EXPECT_EQ(proof, *shared);
    EXPECT_NE(proof, roomImageFingerprint(key, changed));
    auto bounds = limits;
    bounds.maxImageBytes = roomWire::kMaxImageBytes + 1;
    EXPECT_THROW(
        RoomMigration(bounds, "boot"),
        std::invalid_argument
    );
}

TEST_F(
    RoomMigrationTest,
    DirectoryVersionOneFailsClosedWithoutChangingCommittedBytes
)
{
    create();
    commit({RoomAccept{key, 1, tick(1)}});
    commit({apply("source", 1, 1, 1, {{1, {}}})});
    const auto saved = room();
    const auto savedRoom = store->snapshot()->values.at(
        roomStorageKey()
    );
    const auto savedMeta = store->snapshot()->values.at(
        "room-directory/meta"
    );
    arena::lobby_state::DirectoryMeta meta;
    ASSERT_TRUE(meta.ParseFromString(savedMeta));
    meta.set_version(1);
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{"room-directory/meta", meta.SerializeAsString()}}
    ));
    EXPECT_FALSE(migration->load(*store));
    EXPECT_EQ(room(), saved);
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{"room-directory/meta", savedMeta}}
    ));
    arena::lobby_state::DirectoryRoom record;
    ASSERT_TRUE(record.ParseFromString(savedRoom));
    record.set_version(1);
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{roomStorageKey(), record.SerializeAsString()}}
    ));
    EXPECT_FALSE(migration->load(*store));
    EXPECT_EQ(
        store->snapshot()->values.at(roomStorageKey()),
        record.SerializeAsString()
    );
    EXPECT_EQ(room(), saved);
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{roomStorageKey(), savedRoom}}
    ));
    const auto replayKey = "room-directory/replay/7/19/1";
    arena::lobby_state::DirectoryReplay replay;
    ASSERT_TRUE(replay.ParseFromString(
        store->snapshot()->values.at(replayKey)
    ));
    replay.set_version(1);
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{replayKey, replay.SerializeAsString()}}
    ));
    EXPECT_FALSE(migration->load(*store));
    EXPECT_EQ(
        store->snapshot()->values.at(replayKey),
        replay.SerializeAsString()
    );
    EXPECT_EQ(room(), saved);
}

TEST_F(
    RoomMigrationTest,
    EmptyCandidateExposesOnlyBorrowedValuesAndComposesCoreOnlyCas
)
{
    auto candidate = plan({});
    EXPECT_EQ(candidate.expectedRevision(), 0u);
    EXPECT_TRUE(candidate.mutations().empty());
    EXPECT_TRUE(candidate.planned().ids().empty());
    ASSERT_TRUE(write(0, {{"authority/core", "core-only"}}));
    auto published = migration
                         ->publish(std::move(candidate), *store, 1);
    ASSERT_TRUE(published);
    EXPECT_EQ((*published)->revision(), 1u);
    EXPECT_TRUE((*published)->ids().empty());
    EXPECT_EQ(committed->revision(), 0u);
}

TEST_F(
    RoomMigrationTest,
    RetirementDrainsAcceptedIngressThenFencesEveryWriter
)
{
    create();
    commit({RoomAccept{key, 1, input("before-close")}});
    const auto initial = room().image;
    commit({RoomRetire{
        key,
        "source",
        1,
        initial.cursor,
        roomImageFingerprint(key, initial)
    }});
    EXPECT_EQ(room().lifecycle, RoomRouteLifecycle::Closing);
    EXPECT_FALSE(migration->plan(
        *committed,
        {RoomAccept{key, 1, input("late")}},
        1
    ));
    EXPECT_FALSE(migration->plan(*committed, {prepare()}, 1));
    commit({RoomAccept{key, 1, tick(1)}});
    auto drained = image(0, 2);
    drained.logicalTick = 1;
    commit({RoomApply{
        key,
        "source",
        1,
        drained,
        {{1, "closed"}, {2, {}}},
        {}
    }});
    EXPECT_TRUE(room().inbox.empty());
    EXPECT_EQ(room().lifecycle, RoomRouteLifecycle::Closing);
    EXPECT_FALSE(migration->plan(
        *committed,
        {RoomRetire{
            key,
            "source",
            1,
            initial.cursor,
            roomImageFingerprint(key, initial)
        }},
        1
    ));
    const auto proof = roomImageFingerprint(key, drained);
    commit({RoomRetire{key, "source", 1, drained.cursor, proof}});
    const auto saved = room();
    EXPECT_EQ(saved.lifecycle, RoomRouteLifecycle::Retired);
    EXPECT_EQ(committed->ids(), std::vector<RoomIdentity>{key});
    for (const auto& command : std::vector<RoomDirectoryCommand>{
             RoomAccept{key, 1, tick(2)},
             RoomApply{
                 key,
                 "source",
                 1,
                 drained,
                 {{1, "closed"}, {2, {}}},
                 {}
             },
             prepare()
         })
    {
        const auto rejected = migration
                                  ->plan(*committed, {command}, 1);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error(), Error::Phase);
    }
    const auto revision = committed->revision();
    commit({RoomRetire{key, "source", 1, drained.cursor, proof}});
    EXPECT_EQ(committed->revision(), revision);
    reopen();
    EXPECT_EQ(room(), saved);
    commit(
        {RoomPruneReceipts{key}},
        limits.receiptRetentionNanos + 2
    );
    EXPECT_EQ(room().lifecycle, RoomRouteLifecycle::Retired);
    EXPECT_TRUE(room().receipts.empty());
}

TEST_F(
    RoomMigrationTest,
    RetiredLifetimePermitsNewIncarnationButStillConsumesBoundedStorage
)
{
    close();
    limits.maxRooms = 2;
    migration = std::make_unique<RoomMigration>(limits, "same-boot");
    open();
    create();
    commit({RoomRetire{
        key,
        "source",
        1,
        0,
        roomImageFingerprint(key, room().image)
    }});
    const RoomIdentity replacement{key.room, key.incarnation + 1};
    commit({RoomCreate{replacement, "replacement", image()}});
    EXPECT_EQ(committed->size(), 2u);
    EXPECT_EQ(
        committed->ids(),
        (std::vector<RoomIdentity>{key, replacement})
    );
    EXPECT_FALSE(migration->plan(
        *committed,
        {RoomCreate{key, "source", image()}},
        1
    ));
    const auto activeConflict = migration->plan(
        *committed,
        {RoomCreate{
            {key.room, key.incarnation + 2},
            "third",
            image()
        }},
        1
    );
    ASSERT_FALSE(activeConflict);
    EXPECT_EQ(activeConflict.error(), Error::Conflict);
    const auto capacity = migration->plan(
        *committed,
        {RoomCreate{{8, 30}, "other", image()}},
        1
    );
    ASSERT_FALSE(capacity);
    EXPECT_EQ(capacity.error(), Error::Capacity);
    reopen();
    EXPECT_EQ(room().lifecycle, RoomRouteLifecycle::Retired);
    EXPECT_EQ(
        committed->find(replacement)->lifecycle,
        RoomRouteLifecycle::Active
    );
    arena::lobby_state::DirectoryRoom forged;
    ASSERT_TRUE(forged.ParseFromString(
        store->snapshot()->values.at(roomStorageKey())
    ));
    forged.set_lifecycle(99);
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{roomStorageKey(), forged.SerializeAsString()}}
    ));
    EXPECT_FALSE(migration->load(*store));
}

TEST_F(
    RoomMigrationTest,
    RetirementChecksExactAuthorityAndRejectsUnresolvedHandoffAtomically
)
{
    create();
    const auto proof = roomImageFingerprint(key, room().image);
    const auto original = room();
    for (const auto& command : std::vector<RoomDirectoryCommand>{
             RoomRetire{key, "other", 1, 0, proof},
             RoomRetire{key, "source", 2, 0, proof},
             RoomRetire{key, "source", 1, 1, proof},
             RoomRetire{key, "source", 1, 0, "wrong"}
         })
    {
        EXPECT_FALSE(migration->plan(*committed, {command}, 1));
        EXPECT_EQ(room(), original);
    }
    const auto handoff = prepare();
    commit({handoff});
    const auto rejected = migration->plan(
        *committed,
        {RoomRetire{key, "source", 1, 0, proof}},
        1
    );
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), Error::Phase);
    const auto durable = store->snapshot()->values.at(
        roomStorageKey()
    );
    arena::lobby_state::DirectoryRoom impossible;
    ASSERT_TRUE(impossible.ParseFromString(durable));
    impossible.set_lifecycle(2);
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{roomStorageKey(), impossible.SerializeAsString()}}
    ));
    EXPECT_FALSE(migration->load(*store));
    ASSERT_TRUE(write(
        store->snapshot()->revision,
        {{roomStorageKey(), durable}}
    ));
    const auto recovered = migration->load(*store);
    ASSERT_TRUE(recovered);
    committed = *recovered;
    commit(
        {RoomAbort{key, handoff.migration},
         RoomRetire{key, "source", 3, 0, proof}}
    );
    EXPECT_EQ(room().lifecycle, RoomRouteLifecycle::Retired);
    const auto revision = committed->revision();
    commit(
        {RoomFreeze{key, handoff.migration},
         RoomAbort{key, handoff.migration}}
    );
    EXPECT_EQ(committed->revision(), revision);
    EXPECT_EQ(room().lifecycle, RoomRouteLifecycle::Retired);
}
TEST_F(
    RoomMigrationTest,
    OwnedSameValueAndMissingDeletionPreserveResultNamespaceContract
)
{
    commit(
        {RoomCreate{key, "source", image()},
         RoomCreate{{8, 20}, "other", image()}}
    );
    auto candidate = plan({RoomAccept{key, 1, input("accepted")}});
    auto writes = candidate.mutations();
    const std::string other = "room-directory/room/8/20";
    writes.push_back({other, store->snapshot()->values.at(other)});
    writes.push_back({"room-directory/replay/absent", std::nullopt});
    writes.push_back({"authority/core", "unowned-change"});
    auto ack = write(candidate.expectedRevision(), std::move(writes));
    ASSERT_TRUE(ack);
    auto result = migration
                      ->publish(std::move(candidate), *store, *ack);
    ASSERT_TRUE(result);
    EXPECT_EQ((*result)->find(key)->inbox.size(), 1);
    EXPECT_TRUE(migration->load(*store));
}
TEST_F(
    RoomMigrationTest,
    UnexpectedOwnedAdditionCannotHideBehindValidCandidate
)
{
    create();
    auto candidate = plan({RoomAccept{key, 1, input("accepted")}});
    auto writes = candidate.mutations();
    writes.push_back({"room-directory/unplanned", "unexpected"});
    const auto ack = write(
        candidate.expectedRevision(),
        std::move(writes)
    );
    ASSERT_TRUE(ack);
    auto result = migration
                      ->publish(std::move(candidate), *store, *ack);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), Error::NotDurable);
    EXPECT_TRUE(room().inbox.empty());
}
TEST_F(
    RoomMigrationTest,
    UnexpectedOwnedDeletionCannotHideBehindValidCandidate
)
{
    commit(
        {RoomCreate{key, "source", image()},
         RoomCreate{{8, 20}, "other", image()}}
    );
    auto candidate = plan({RoomAccept{key, 1, input("accepted")}});
    auto writes = candidate.mutations();
    writes.push_back({"room-directory/room/8/20", std::nullopt});
    const auto ack = write(
        candidate.expectedRevision(),
        std::move(writes)
    );
    ASSERT_TRUE(ack);
    auto result = migration
                      ->publish(std::move(candidate), *store, *ack);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), Error::NotDurable);
    EXPECT_NE(committed->find({8, 20}), nullptr);
}
TEST_F(
    RoomMigrationTest,
    SameBytesAndRevisionFromAnotherJournalDoNotProveThisBase
)
{
    create();
    const auto base = store->snapshot();
    auto candidate = plan({RoomAccept{key, 1, input("accepted")}});
    auto otherConfig = config;
    otherConfig.directory = config.directory / "other";
    std::filesystem::create_directories(otherConfig.directory);
    AtomicJournal other(io.get_executor(), otherConfig);
    const auto persist =
        [&](std::vector<AtomicJournal::Mutation> writes)
    {
        auto reply = std::make_shared<
            std::optional<AtomicJournal::Result>>();
        if (!other.commit(
                other.snapshot()->revision,
                std::move(writes),
                [reply](auto result) { *reply = result; }
            ))
            throw std::runtime_error(
                "other journal admission failed"
            );
        wait([&] { return reply->has_value(); });
        if (!**reply)
            throw std::runtime_error("other journal commit failed");
        return ***reply;
    };
    std::vector<AtomicJournal::Mutation> seed;
    for (const auto& [name, value] : base->values)
        seed.push_back({name, value});
    EXPECT_EQ(persist(std::move(seed)), base->revision);
    EXPECT_EQ(other.snapshot()->values, base->values);
    const auto ack = persist(candidate.mutations());
    EXPECT_EQ(ack, candidate.expectedRevision() + 1);
    auto result = migration
                      ->publish(std::move(candidate), other, ack);
    EXPECT_FALSE(result);
    if (!result)
    {
        EXPECT_EQ(result.error(), Error::NotDurable);
    }
    other.stop();
    wait([&] { return other.stats().closed; });
}
TEST_F(
    RoomMigrationTest,
    ReopenFencesOldCandidateButRecoveredBaseSupportsFreshAuthorityCas
)
{
    create();
    const auto revision = committed->revision();
    auto stale = plan({});
    reopen();
    EXPECT_EQ(committed->revision(), revision);
    auto rejected = migration
                        ->publish(std::move(stale), *store, revision);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), Error::NotDurable);
    auto fresh = plan({});
    auto same = migration
                    ->publish(std::move(fresh), *store, revision);
    ASSERT_TRUE(same);
    committed = *same;
    auto next = plan({});
    const auto ack = write(
        revision,
        {{"authority/core", "first-after-recovery"}}
    );
    ASSERT_TRUE(ack);
    EXPECT_TRUE(migration->publish(std::move(next), *store, *ack));
}
TEST_F(
    RoomMigrationTest,
    ReplayCountIncludesLastPlannedExtentAndRetiredHistoryAfterReopen
)
{
    close();
    config.maxKeys = 100000;
    open();
    create();
    constexpr std::uint64_t seeded = 65535;
    for (std::uint64_t start = 1; start <= seeded; start += 32768)
    {
        std::vector<AtomicJournal::Mutation> writes;
        for (auto n = start; n <= seeded && n < start + 32768; ++n)
        {
            arena::lobby_state::DirectoryReplay replay;
            replay.set_version(2);
            replay.set_room_id(key.room);
            replay.set_incarnation(key.incarnation);
            replay.set_first_tick(n);
            replay.set_last_tick(n);
            replay.set_payload("opaque");
            writes.push_back(
                {"room-directory/replay/7/19/" + std::to_string(n),
                 replay.SerializeAsString()}
            );
        }
        if (start > 1)
        {
            arena::lobby_state::DirectoryRoom record;
            ASSERT_TRUE(record.ParseFromString(
                store->snapshot()->values.at(roomStorageKey())
            ));
            record.set_next_sequence(seeded + 1);
            record.set_admitted_tick(seeded);
            record.mutable_image()->set_tick(seeded);
            record.mutable_image()->set_logical_tick(seeded);
            record.mutable_image()->set_cursor(seeded);
            record.mutable_image()->set_checksum(123);
            record.mutable_image()->set_checkpoint(
                "seeded-worker-image"
            );
            writes.push_back(
                {roomStorageKey(), record.SerializeAsString()}
            );
        }
        ASSERT_TRUE(
            write(store->snapshot()->revision, std::move(writes))
        );
    }
    auto loaded = migration->load(*store);
    ASSERT_TRUE(loaded);
    committed = *loaded;
    commit({RoomAccept{key, 1, tick(seeded + 1)}});
    commit({apply(
        "source",
        1,
        seeded + 1,
        seeded + 1,
        {{seeded + 1, {}}}
    )});
    const auto proof = roomImageFingerprint(key, room().image);
    commit(
        {RoomRetire{key, "source", 1, room().image.cursor, proof}}
    );
    const auto retired = key;
    reopen();
    ASSERT_EQ(
        committed->find(retired)->lifecycle,
        RoomRouteLifecycle::Retired
    );
    key = {8, 20};
    create();
    commit({RoomAccept{key, 1, tick(1)}});
    const auto revision = committed->revision();
    const auto overflow = migration->plan(
        *committed,
        {apply("source", 1, 1, 1, {{1, {}}})},
        1
    );
    ASSERT_FALSE(overflow);
    EXPECT_EQ(overflow.error(), Error::Capacity);
    EXPECT_EQ(store->snapshot()->revision, revision);
    EXPECT_EQ(room().image.tick, 0);
    EXPECT_EQ(room().inbox.size(), 1);
    EXPECT_TRUE(migration->load(*store));
}
} // namespace
