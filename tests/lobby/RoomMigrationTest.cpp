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
    DurableStateStore::Config config;
    RoomDirectoryLimits limits;
    std::unique_ptr<DurableStateStore> store;
    std::unique_ptr<RoomMigration> migration;
    std::shared_ptr<const RoomDirectory> committed;
    const RoomIdentity key{7, 19};

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
        store = std::make_unique<DurableStateStore>(
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
                std::to_string(cursor)
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
    DurableStateStore::Result write(
        std::uint64_t revision,
        std::vector<DurableStateStore::Mutation> mutations
    )
    {
        auto reply = std::make_shared<
            std::optional<DurableStateStore::Result>>();
        if (!store->commit(
                revision,
                std::move(mutations),
                [reply](auto r) { *reply = r; }
            ))
            return std::unexpected(
                DurableStateStore::Error::Capacity
            );
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
        return "room-directory/room/7/19";
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
        std::optional<DurableStateStore::Result>>();
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
    EXPECT_EQ(result.error(), DurableStateStore::Error::Conflict);
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
    EXPECT_EQ(
        result.error(),
        DurableStateStore::Error::OutcomeUnknown
    );
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
    extent.set_version(1);
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
        std::vector<DurableStateStore::Mutation> mutations;
        mutations.reserve(count / 2 + 1);
        for (auto n = start; n < start + count / 2; ++n)
        {
            arena::lobby_state::DirectoryReplay replay;
            replay.set_version(1);
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
} // namespace