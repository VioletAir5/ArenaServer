#include "gateway_test/PersistentStorage.h"

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <mutex>
#include <thread>

#include <boost/asio/io_context.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <gtest/gtest.h>

#include "rpc/Client.h"

namespace service::gateway
{
namespace
{
using namespace std::chrono_literals;
using Tcp = boost::asio::ip::tcp;
using Clock = std::chrono::steady_clock;

template <class Predicate>
bool until(
    Predicate predicate, std::chrono::milliseconds timeout = 3s
)
{
    const auto end = Clock::now() + timeout;
    while (Clock::now() < end)
    {
        if (predicate())
            return true;
        std::this_thread::sleep_for(2ms);
    }
    return predicate();
}
struct Peer
{
    boost::asio::io_context context;
    Tcp::acceptor listener{
        context, {boost::asio::ip::address_v4::loopback(), 0}
    };
    std::thread thread;
    std::atomic<bool> stopped{false}, holdAccounts{false},
        holdResults{false}, dropOnce{false};
    std::atomic<int> resultStatus{arena::rpc::OK}, accounts{0},
        saves{0}, mutations{0};
    std::atomic<bool> sawSpool{false}, healthProfileValid{true};
    std::atomic<int> healthStatus{arena::rpc::OK}, healthCalls{0};
    std::filesystem::path spool;
    std::string digest;
    std::uint64_t nextId{10000};
    std::mutex mutex;
    std::map<std::uint64_t, arena::storage::MatchRecord> matches;

    explicit Peer(std::filesystem::path directory)
        : spool(std::move(directory))
    {
        service::auth::Pbkdf2PasswordDigest kdf;
        digest = *kdf.hash("correct");
        listener.non_blocking(true);
        thread = std::thread([this] { run(); });
    }
    ~Peer()
    {
        stopped = true;
        holdAccounts = false;
        holdResults = false;
        if (thread.joinable())
            thread.join();
    }
    Tcp::endpoint endpoint() const
    {
        return listener.local_endpoint();
    }
    void run()
    {
        while (!stopped)
        {
            Tcp::socket socket(context);
            boost::system::error_code error;
            listener.accept(socket, error);
            if (error)
            {
                std::this_thread::sleep_for(1ms);
                continue;
            }
            timeval timeout{1, 0};
            ::setsockopt(
                socket.native_handle(),
                SOL_SOCKET,
                SO_RCVTIMEO,
                &timeout,
                sizeof(timeout)
            );
            std::array<unsigned char, 4> header{};
            boost::asio::read(
                socket,
                boost::asio::buffer(header),
                error
            );
            if (error)
                continue;
            std::uint32_t size = 0;
            for (auto byte : header)
                size = (size << 8) | byte;
            if (!size || size > arena::internal_rpc::Client::maxFrame)
                continue;
            std::string bytes(size, '\0');
            boost::asio::read(
                socket,
                boost::asio::buffer(bytes),
                error
            );
            arena::rpc::Request request;
            if (error || !request.ParseFromString(bytes))
                continue;
            arena::rpc::Response reply;
            reply.set_request_id(request.request_id());
            reply.set_correlation_id(request.correlation_id());
            reply.set_protocol_version(request.protocol_version());
            reply.set_status(arena::rpc::OK);
            bool drop = false;
            if (request.method() == "health.ready")
            {
                if (request.protocol_version() !=
                        arena::internal_rpc::protocolVersion ||
                    request.caller_service() != "gateway" ||
                    request.target_service() != "storage" ||
                    request.service_path_size() != 1 ||
                    request.service_path(0) != "gateway" ||
                    !request.request_id() ||
                    request.correlation_id().empty() ||
                    !request.deadline_unix_ms())
                    healthProfileValid = false;
                ++healthCalls;
                reply.set_status(
                    static_cast<arena::rpc::Status>(
                        healthStatus.load()
                    )
                );
            }
            else if (request.method() == "match.allocate_ids")
            {
                arena::storage::AllocateIdsRequest value;
                value.ParseFromString(request.payload());
                arena::storage::AllocateIdsReply result;
                result.set_first(nextId);
                nextId += value.count();
                result.set_last(nextId - 1);
                reply.set_payload(result.SerializeAsString());
            }
            else if (request.method() == "account.find")
            {
                ++accounts;
                while (holdAccounts && !stopped)
                    std::this_thread::sleep_for(1ms);
                arena::storage::FindAccountRequest value;
                value.ParseFromString(request.payload());
                arena::storage::FindAccountReply result;
                result.mutable_account()->set_account_id(1);
                result.mutable_account()->set_account_name(
                    value.account_name()
                );
                result.mutable_account()->set_state(
                    value.account_name() == "disabled" ? "disabled"
                                                       : "active"
                );
                result.mutable_account()->set_password_digest(digest);
                auto* player = result.mutable_player();
                player->set_player_id(42);
                player->set_account_id(1);
                player->set_state("active");
                player->set_display_name("Alice");
                player->set_mmr(1470);
                reply.set_payload(result.SerializeAsString());
            }
            else if (request.method() == "match.save_result")
            {
                ++saves;
                arena::storage::SaveResultRequest value;
                value.ParseFromString(request.payload());
                sawSpool = std::filesystem::exists(
                    spool /
                    (std::to_string(value.match().match_id()) +
                     ".pending")
                );
                while (holdResults && !stopped)
                    std::this_thread::sleep_for(1ms);
                reply.set_status(
                    static_cast<arena::rpc::Status>(
                        resultStatus.load()
                    )
                );
                if (reply.status() == arena::rpc::OK)
                {
                    bool replayed = false;
                    {
                        std::lock_guard lock(mutex);
                        auto [found, inserted] = matches.emplace(
                            value.match().match_id(),
                            value.match()
                        );
                        if (!inserted &&
                            found->second.SerializeAsString() !=
                                value.match().SerializeAsString())
                            reply.set_status(arena::rpc::CONFLICT);
                        replayed = !inserted;
                        if (inserted)
                            ++mutations;
                    }
                    arena::storage::SaveResultReply result;
                    *result.mutable_match() = value.match();
                    auto* message = result.mutable_match();
                    message->GetReflection()
                        ->MutableUnknownFields(message)
                        ->AddVarint(99, 7);
                    auto* member = message->mutable_participants(0);
                    member->GetReflection()
                        ->MutableUnknownFields(member)
                        ->AddVarint(99, 9);
                    result.set_replayed(replayed);
                    result.mutable_settlement()->set_match_id(
                        value.match().match_id()
                    );
                    result.mutable_settlement()->set_settlement_id(
                        value.match().match_id()
                    );
                    result.mutable_settlement()->set_event_id(
                        "00000000-0000-0000-0000-000000000001"
                    );
                    result.mutable_settlement()->set_replayed(
                        replayed
                    );
                    reply.set_payload(result.SerializeAsString());
                    drop = dropOnce.exchange(false);
                }
            }
            else if (request.method() == "match.get")
            {
                arena::storage::MatchGetRequest value;
                value.ParseFromString(request.payload());
                arena::storage::MatchGetReply result;
                std::lock_guard lock(mutex);
                const auto found = matches.find(value.match_id());
                if (found == matches.end())
                    reply.set_status(arena::rpc::NOT_FOUND);
                else
                    *result.mutable_match() = found->second;
                reply.set_payload(result.SerializeAsString());
            }
            else if (request.method() == "settlement.get")
            {
                arena::storage::SettlementGetRequest value;
                value.ParseFromString(request.payload());
                arena::storage::SettlementGetReply result;
                result.mutable_settlement()->set_match_id(
                    value.settlement_id()
                );
                result.mutable_settlement()->set_settlement_id(
                    value.settlement_id()
                );
                result.mutable_settlement()->set_event_id(
                    "00000000-0000-0000-0000-000000000001"
                );
                reply.set_payload(result.SerializeAsString());
            }
            else
                reply.set_status(arena::rpc::NOT_FOUND);
            if (drop)
                continue;
            bytes = reply.SerializeAsString();
            for (unsigned i = 0; i < 4; ++i)
                header[i] = static_cast<unsigned char>(
                    bytes.size() >> (24 - 8 * i)
                );
            boost::asio::write(
                socket,
                boost::asio::buffer(header),
                error
            );
            if (!error)
                boost::asio::write(
                    socket,
                    boost::asio::buffer(bytes),
                    error
                );
        }
    }
};

class PersistentStorageTest : public ::testing::Test
{
protected:
    std::filesystem::path directory;
    void SetUp() override
    {
        std::string pattern = "/tmp/arena-native-storage-XXXXXX";
        auto* made = ::mkdtemp(pattern.data());
        ASSERT_NE(made, nullptr);
        directory = made;
    }
    void TearDown() override
    {
        std::filesystem::remove_all(directory);
    }
    PersistentStorageConfig config(const Peer& peer) const
    {
        PersistentStorageConfig value;
        value.endpoint = peer.endpoint();
        value.spoolDirectory = directory;
        value.serviceToken = std::string(40, 'x');
        value.idBlockSize = 8;
        value.rpcTimeout = 150ms;
        value.retryBase = 20ms;
        value.retryMax = 40ms;
        return value;
    }
    arena::storage::MatchRecord result(std::uint64_t id) const
    {
        arena::storage::MatchRecord value;
        value.set_match_id(id);
        value.set_room_id((1ULL << 63) | id);
        value.set_state("completed");
        value.set_finish_reason("completed");
        value.set_result_version(1);
        value.set_final_tick(40);
        value.set_final_checksum(UINT64_MAX);
        for (unsigned i = 0; i < 2; ++i)
        {
            auto* row = value.add_participants();
            row->set_player_id(42 + i);
            row->set_session_id(501 + i);
            row->set_slot(i + 1);
            row->set_membership_epoch(7 + i);
            row->set_initial_mmr(1440 + 40 * i);
            row->set_last_input_seq(10 + i);
            row->set_final_x(-4);
            row->set_final_y(8);
            row->set_last_dx(-1);
            row->set_last_dy(1);
        }
        return value;
    }
};

TEST_F(PersistentStorageTest, HealthProbeUsesExplicitStorageV1Profile)
{
    Peer peer(directory);
    peer.healthStatus = arena::rpc::UNAVAILABLE;
    auto settings = config(peer);
    settings.probeHealth = true;
    PersistentStorage storage(settings);
    ASSERT_TRUE(until([&] { return peer.healthCalls.load() > 0; }));
    EXPECT_FALSE(storage.ready());
    EXPECT_TRUE(peer.healthProfileValid);
    peer.healthStatus = arena::rpc::OK;
    ASSERT_TRUE(until([&] { return storage.ready(); }));
    EXPECT_TRUE(peer.healthProfileValid);
    storage.stop();
    EXPECT_FALSE(storage.ready());
}
TEST_F(
    PersistentStorageTest,
    CanonicalAuthenticationAndBoundedCancellation
)
{
    Peer peer(directory);
    auto settings = config(peer);
    settings.maxAuth = 2;
    PersistentStorage storage(settings);
    ASSERT_TRUE(storage.prime(2s));
    std::promise<PersistentStorage::AuthResult> first;
    auto accepted = storage.authenticate(
        "alice",
        "correct",
        Clock::now() + 2s,
        "auth-test",
        [&](auto value) { first.set_value(std::move(value)); }
    );
    ASSERT_TRUE(accepted);
    auto future = first.get_future();
    ASSERT_EQ(future.wait_for(3s), std::future_status::ready);
    auto verified = future.get();
    ASSERT_TRUE(verified);
    EXPECT_EQ(verified->value(), 42);
    EXPECT_EQ(storage.rating(arena::domain::PlayerId{42}), 1470);
    std::promise<PersistentStorage::AuthResult> disabled;
    ASSERT_TRUE(storage.authenticate(
        "disabled",
        "correct",
        Clock::now() + 2s,
        "disabled-test",
        [&](auto value) { disabled.set_value(std::move(value)); }
    ));
    auto blocked = disabled.get_future();
    ASSERT_EQ(blocked.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(
        blocked.get().error(),
        service::auth::AuthError::InvalidCredentials
    );

    peer.holdAccounts = true;
    std::atomic<int> callbacks{0};
    auto a = storage.authenticate(
        "alice",
        "correct",
        Clock::now() + 2s,
        "cancel-a",
        [&](auto) { ++callbacks; }
    );
    auto b = storage.authenticate(
        "alice",
        "correct",
        Clock::now() + 2s,
        "cancel-b",
        [&](auto) { ++callbacks; }
    );
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    auto rejected = storage.authenticate(
        "alice",
        "correct",
        Clock::now() + 2s,
        "overload",
        [&](auto)
        { ADD_FAILURE() << "rejected admission invoked callback"; }
    );
    EXPECT_FALSE(rejected);
    (*a)();
    (*a)();
    (*b)();
    peer.holdAccounts = false;
    ASSERT_TRUE(until(
        [&]
        {
            return callbacks == 2 &&
                   storage.metrics().authActive == 0;
        }
    ));
    storage.stop();
    EXPECT_FALSE(storage.metrics().accepting);
}

TEST_F(
    PersistentStorageTest,
    DurableSpoolPrecedesRpcAndUnknownCommitResolvesSameIdentity
)
{
    Peer peer(directory);
    peer.dropOnce = true;
    PersistentStorage storage(config(peer));
    ASSERT_TRUE(storage.prime(2s));
    const auto id = storage.takeTicketId();
    ASSERT_TRUE(id);
    std::promise<bool> acknowledged;
    ASSERT_TRUE(storage.save(
        result(*id),
        [&](bool value) { acknowledged.set_value(value); }
    ));
    auto answer = acknowledged.get_future();
    ASSERT_EQ(answer.wait_for(3s), std::future_status::ready);
    EXPECT_TRUE(answer.get());
    EXPECT_TRUE(peer.sawSpool);
    EXPECT_EQ(peer.mutations, 1);
    EXPECT_EQ(peer.saves, 2);
    EXPECT_EQ(storage.metrics().resultsCommitted, 1);
    EXPECT_EQ(storage.metrics().spoolFiles, 0);
    std::promise<PersistentStorage::LookupResult> lookup;
    ASSERT_TRUE(storage.settlement(
        arena::domain::PlayerId{42},
        *id,
        Clock::now() + 2s,
        "lookup",
        [&](auto value) { lookup.set_value(std::move(value)); }
    ));
    auto found = lookup.get_future();
    ASSERT_EQ(found.wait_for(2s), std::future_status::ready);
    const auto value = found.get();
    ASSERT_TRUE(value);
    EXPECT_EQ(value->players.at(0).lastDx, -1);
    EXPECT_EQ(value->players.at(0).lastDy, 1);
    std::promise<PersistentStorage::LookupResult> unauthorized;
    ASSERT_TRUE(storage.settlement(
        arena::domain::PlayerId{44},
        *id,
        Clock::now() + 2s,
        "not-member",
        [&](auto value) { unauthorized.set_value(std::move(value)); }
    ));
    auto denied = unauthorized.get_future();
    ASSERT_EQ(denied.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(denied.get().error(), ErrorCode::ERR_UNAUTHENTICATED);
}

TEST_F(
    PersistentStorageTest,
    RestartRetainsAttemptBudgetAndFailedJobNeedsOperatorAction
)
{
    Peer peer(directory);
    peer.resultStatus = arena::rpc::UNAVAILABLE;
    peer.holdResults = true;
    auto settings = config(peer);
    settings.maxResultAttempts = 3;
    settings.retryBase = 100ms;
    settings.retryMax = 100ms;
    const auto record = result(19000);
    {
        PersistentStorage storage(settings);
        ASSERT_TRUE(storage.prime(2s));
        ASSERT_TRUE(storage.save(record, [](bool) {}));
        ASSERT_TRUE(until([&] { return peer.saves == 1; }));
        storage.stop();
        EXPECT_TRUE(
            std::filesystem::exists(directory / "19000.pending")
        );
    }
    peer.holdResults = false;
    {
        PersistentStorage storage(settings);
        ASSERT_TRUE(storage.prime(2s));
        std::promise<bool> failure;
        ASSERT_TRUE(storage.save(
            record,
            [&](bool value) { failure.set_value(value); }
        ));
        auto answer = failure.get_future();
        ASSERT_EQ(answer.wait_for(3s), std::future_status::ready);
        EXPECT_FALSE(answer.get());
        EXPECT_EQ(peer.saves, 3);
        EXPECT_TRUE(
            std::filesystem::exists(directory / "19000.failed")
        );
        std::this_thread::sleep_for(180ms);
        EXPECT_EQ(peer.saves, 3);
    }
    {
        PersistentStorage storage(settings);
        ASSERT_TRUE(storage.prime(2s));
        std::this_thread::sleep_for(180ms);
        EXPECT_EQ(peer.saves, 3);
        peer.resultStatus = arena::rpc::OK;
        ASSERT_TRUE(storage.requeueFailed(19000));
        ASSERT_TRUE(until(
            [&] { return storage.metrics().resultsCommitted == 1; }
        ));
        EXPECT_EQ(peer.saves, 4);
        EXPECT_EQ(peer.mutations, 1);
        EXPECT_EQ(storage.metrics().operatorRequeues, 1);
    }
}

TEST_F(
    PersistentStorageTest,
    FiniteSpoolRejectsConflictingPayloadAndExclusiveOwner
)
{
    Peer peer(directory);
    peer.holdResults = true;
    auto settings = config(peer);
    settings.maxSpoolFiles = 1;
    PersistentStorage storage(settings);
    ASSERT_TRUE(storage.prime(2s));
    EXPECT_THROW(
        PersistentStorage duplicate(settings),
        std::runtime_error
    );
    auto record = result(20000);
    ASSERT_TRUE(storage.save(record, [](bool) {}));
    auto changed = record;
    changed.set_final_checksum(9);
    EXPECT_FALSE(storage.save(changed, [](bool) {}));
    EXPECT_FALSE(storage.save(result(20001), [](bool) {}));
    peer.holdResults = false;
    ASSERT_TRUE(
        until([&] { return storage.metrics().resultsCommitted == 1; })
    );
}

TEST_F(PersistentStorageTest, CorruptSpoolFailsClosedBeforeReplay)
{
    Peer peer(directory);
    peer.resultStatus = arena::rpc::CONFLICT;
    auto settings = config(peer);
    {
        PersistentStorage storage(settings);
        ASSERT_TRUE(storage.prime(2s));
        std::promise<bool> completion;
        ASSERT_TRUE(storage.save(
            result(21000),
            [&](bool ok) { completion.set_value(ok); }
        ));
        auto done = completion.get_future();
        ASSERT_EQ(done.wait_for(2s), std::future_status::ready);
        EXPECT_FALSE(done.get());
    }
    {
        std::fstream file(
            directory / "21000.failed",
            std::ios::in | std::ios::out | std::ios::binary
        );
        ASSERT_TRUE(file.good());
        file.seekp(24);
        file.put('X');
        file.flush();
    }
    const auto prior = peer.saves.load();
    PersistentStorage storage(settings);
    EXPECT_FALSE(storage.prime(2s));
    EXPECT_FALSE(storage.metrics().spoolHealthy);
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(peer.saves, prior);
    EXPECT_FALSE(storage.takeTicketId());
}
} // namespace
} // namespace service::gateway
