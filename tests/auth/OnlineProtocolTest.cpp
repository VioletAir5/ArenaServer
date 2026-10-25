#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "net/ConnectionHost.h"
#include "net/LoginHandler.h"
#include "net/PacketBuffer.h"
#include "net/PreLoginHandshake.h"
#include "net/RoomService.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

#include "auth/LoginAuthenticator.h"

namespace
{
using namespace service::gateway;
namespace auth = service::auth;
namespace wire = arena::protocol;

class Accounts final : public auth::PasswordAccounts
{
public:
    std::expected<auth::PasswordAccount, auth::AuthError>
    find(std::string_view account) const override
    {
        std::uint64_t id = 0;
        if (account == "alice")
            id = 42;
        if (account == "bob")
            id = 43;
        if (account == "charlie")
            id = 44;
        if (!id)
            return std::unexpected(
                auth::AuthError::InvalidCredentials
            );
        return auth::PasswordAccount{
            arena::domain::PlayerId{id},
            "test-digest"
        };
    }
};

class Digest final : public auth::PasswordDigest
{
public:
    std::expected<std::string, auth::AuthError>
    hash(std::string_view) const override
    {
        return std::unexpected(auth::AuthError::Unavailable);
    }
    std::expected<bool, auth::AuthError> verify(
        std::string_view password, std::string_view digest
    ) const override
    {
        return password == "test-password" && digest == "test-digest";
    }
};

struct Transport final : ITransport
{
    explicit Transport(std::uint64_t id)
        : identity(id)
    {
    }
    std::uint64_t identity;
    std::string output;
    bool open{true};
    std::uint64_t id() const noexcept override
    {
        return identity;
    }
    void send(std::span<const std::byte> bytes) override
    {
        output.append(
            reinterpret_cast<const char*>(bytes.data()),
            bytes.size()
        );
    }
    void shutdown() override
    {
    }
    void close() override
    {
        open = false;
    }
    bool connected() const noexcept override
    {
        return open;
    }
    void pauseRead() override
    {
    }
    void resumeRead() override
    {
    }
    std::size_t pendingBytes() const noexcept override
    {
        return 0;
    }
};

struct Client
{
    std::shared_ptr<Transport> transport;
    ConnectionContext::Ptr context;
    std::vector<std::function<void()>> posts;
    std::shared_ptr<ConnectionHost> host;

    Client(
        std::uint64_t id,
        std::shared_ptr<const MessageRegistry> routes
    )
        : transport(std::make_shared<Transport>(id))
        , context(
              std::make_shared<ConnectionContext>(
                  id,
                  InetAddress("127.0.0.1", 1),
                  ConnectionClock::now()
              )
          )
    {
        host = makeConnectionHost(
            transport,
            context,
            {[this](auto task) { posts.push_back(std::move(task)); },
             [](auto, auto) { return ConnectionTimer{}; }},
            [routes](auto metadata)
            {
                return makePreLoginHandshakeHandler(
                    std::move(metadata),
                    routes
                );
            }
        );
        host->start();
    }

    void send(const wire::Envelope& request)
    {
        const auto payload = service::protocol::EnvelopeCodec::encode(
            request
        );
        ASSERT_TRUE(payload);
        const auto frame = service::protocol::PacketCodec::encode(
            1,
            7,
            std::as_bytes(std::span(*payload))
        );
        ASSERT_TRUE(frame);
        host->received(
            std::as_bytes(
                std::span(frame->peek(), frame->readableBytes())
            )
        );
    }

    wire::Envelope reply()
    {
        wire::Envelope result;
        unsigned replies = 0;
        service::protocol::PacketCodec codec(kMaxPacketSize);
        codec.feed(
            std::as_bytes(std::span(transport->output)),
            [&](const auto& header, std::string_view payload)
            {
                EXPECT_EQ(header.m_type, 2);
                EXPECT_EQ(header.m_requestId, 7);
                EXPECT_TRUE(
                    service::protocol::EnvelopeCodec::decode(
                        std::as_bytes(std::span(payload)),
                        result
                    )
                );
                ++replies;
                return true;
            }
        );
        EXPECT_EQ(replies, 1);
        transport->output.clear();
        return result;
    }

    void disconnect()
    {
        transport->close();
        host->transportClosed();
    }
};

wire::Envelope password(std::string_view account, bool legacy = false)
{
    wire::Envelope request;
    if (!legacy)
    {
        request.set_request_id(99);
        request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    }
    request.mutable_login_request()->set_account(
        std::string(account)
    );
    request.mutable_login_request()->set_credential("test-password");
    return request;
}

wire::Envelope resume(std::string_view token)
{
    wire::Envelope request;
    request.set_request_id(100);
    request.set_msg_id(wire::MSG_LOGIN_REQUEST);
    request.mutable_login_request()->set_credential_kind(
        wire::CREDENTIAL_JWT
    );
    request.mutable_login_request()->set_credential(
        std::string(token)
    );
    return request;
}

wire::Envelope stateRequest(bool legacy = false)
{
    wire::Envelope request;
    if (!legacy)
    {
        request.set_request_id(101);
        request.set_msg_id(wire::MSG_SESSION_STATE_REQUEST);
    }
    request.mutable_session_state_request();
    return request;
}

wire::Envelope joinRequest(std::uint64_t room, bool legacy = false)
{
    wire::Envelope request;
    if (!legacy)
    {
        request.set_request_id(102);
        request.set_msg_id(wire::MSG_ROOM_JOIN_REQUEST);
    }
    request.mutable_room_join_request()->set_room_id(room);
    return request;
}

wire::Envelope leaveRequest()
{
    wire::Envelope request;
    request.set_request_id(103);
    request.set_msg_id(wire::MSG_ROOM_LEAVE_REQUEST);
    request.mutable_room_leave_request();
    return request;
}

std::vector<wire::Envelope> lifecycleRequests()
{
    wire::Envelope ready;
    ready.set_request_id(104);
    ready.set_msg_id(wire::MSG_ROOM_READY_REQUEST);
    ready.mutable_room_ready_request()->set_ready(true);
    ready.mutable_room_ready_request()->set_actor_incarnation(1);
    ready.mutable_room_ready_request()->set_membership_epoch(1);
    wire::Envelope status;
    status.set_request_id(105);
    status.set_msg_id(wire::MSG_ROOM_STATUS_REQUEST);
    status.mutable_room_status_request();
    wire::Envelope settlement;
    settlement.set_request_id(106);
    settlement.set_msg_id(wire::MSG_SETTLEMENT_REQUEST);
    settlement.mutable_settlement_request()->set_match_id(7);
    return {ready, status, settlement};
}

const wire::RoomMember*
member(const wire::SessionStateResponse& state, std::uint64_t player)
{
    for (const auto& entry : state.members())
        if (entry.player_id() == player)
            return &entry;
    return nullptr;
}

struct OnlineProtocolTest : testing::Test
{
    ConnectionClock::time_point now{};
    std::shared_ptr<OnlineRegistry> registry;
    std::shared_ptr<const MessageRegistry> routes;

    void SetUp() override
    {
        OnlineRegistryConfig config;
        config.grace = std::chrono::milliseconds(100);
        config.maxPlayers = 10;
        config.maxRooms = 2;
        config.maxRoomMembers = 2;
        config.now = [this] { return now; };
        registry = std::make_shared<OnlineRegistry>(
            std::move(config)
        );
        auth::JwtConfig jwt;
        jwt.key = "0123456789abcdef0123456789abcdef";
        jwt.now = [] { return 1700000000LL; };
        routes = makeLoginMessageRegistry(
            std::make_shared<auth::LoginAuthenticator>(
                std::make_shared<Accounts>(),
                std::make_shared<Digest>()
            ),
            std::make_shared<auth::JwtTokens>(std::move(jwt)),
            registry
        );
    }

    wire::LoginResponse
    login(Client& client, std::string_view account)
    {
        client.send(password(account));
        const auto response = client.reply();
        EXPECT_EQ(response.msg_id(), wire::MSG_LOGIN_RESPONSE);
        EXPECT_EQ(response.request_id(), 99);
        EXPECT_TRUE(response.login_response().accepted());
        return response.login_response();
    }

    wire::SessionStateResponse state(Client& client)
    {
        client.send(stateRequest());
        const auto response = client.reply();
        EXPECT_EQ(
            response.msg_id(),
            wire::MSG_SESSION_STATE_RESPONSE
        );
        EXPECT_TRUE(response.has_session_state_response());
        return response.session_state_response();
    }

    wire::SessionStateResponse
    join(Client& client, std::uint64_t room)
    {
        client.send(joinRequest(room));
        const auto response = client.reply();
        EXPECT_EQ(response.msg_id(), wire::MSG_ROOM_JOIN_RESPONSE);
        EXPECT_TRUE(response.room_join_response().has_state());
        return response.room_join_response().state();
    }
};

TEST_F(
    OnlineProtocolTest,
    AuthenticatedRoutesExposeStableLogicalIdentityAndRoster
)
{
    Client alice(1, routes);
    const auto initial = login(alice, "alice");
    EXPECT_NE(initial.session_id(), 0);
    EXPECT_EQ(initial.room_id(), 0);
    EXPECT_FALSE(initial.resumed());
    auto snapshot = state(alice);
    EXPECT_EQ(snapshot.session_id(), initial.session_id());
    EXPECT_EQ(snapshot.player_id(), 42);
    EXPECT_EQ(snapshot.presence(), wire::PRESENCE_ONLINE);
    EXPECT_EQ(snapshot.room_id(), 0);
    EXPECT_EQ(snapshot.members_size(), 0);
    snapshot = join(alice, 7);
    EXPECT_EQ(snapshot.session_id(), initial.session_id());
    EXPECT_EQ(snapshot.room_id(), 7);
    ASSERT_EQ(snapshot.members_size(), 1);
    EXPECT_EQ(snapshot.members(0).session_id(), initial.session_id());
    EXPECT_EQ(join(alice, 7).members_size(), 1);
    alice.send(leaveRequest());
    auto response = alice.reply();
    EXPECT_EQ(response.msg_id(), wire::MSG_ROOM_LEAVE_RESPONSE);
    EXPECT_EQ(response.room_leave_response().state().room_id(), 0);
    EXPECT_EQ(
        response.room_leave_response().state().members_size(),
        0
    );
    EXPECT_EQ(registry->roomCount(), 0);
    alice.send(leaveRequest());
    EXPECT_EQ(alice.reply().msg_id(), wire::MSG_ROOM_LEAVE_RESPONSE);
}

TEST_F(
    OnlineProtocolTest,
    LegacyPayloadInferenceAndOuterCorrelationRemainSupported
)
{
    Client alice(1, routes);
    alice.send(password("alice", true));
    auto response = alice.reply();
    EXPECT_EQ(response.msg_id(), 0);
    EXPECT_EQ(response.request_id(), 7);
    ASSERT_NE(response.login_response().session_id(), 0);
    alice.send(joinRequest(7, true));
    response = alice.reply();
    EXPECT_EQ(response.msg_id(), 0);
    EXPECT_EQ(response.request_id(), 7);
    EXPECT_EQ(response.room_join_response().state().room_id(), 7);
    alice.send(stateRequest(true));
    response = alice.reply();
    EXPECT_EQ(response.msg_id(), 0);
    EXPECT_EQ(response.request_id(), 7);
    EXPECT_EQ(response.session_state_response().room_id(), 7);
}

TEST_F(
    OnlineProtocolTest,
    PublicConnectionsCannotObserveOrMutateMembership
)
{
    Client anonymous(1, routes);
    for (const auto& request :
         {stateRequest(), joinRequest(7), leaveRequest()})
    {
        anonymous.send(request);
        EXPECT_EQ(
            anonymous.reply().error().code(),
            wire::ERR_UNAUTHENTICATED
        );
    }
    EXPECT_TRUE(anonymous.context->session->markAuthenticated());
    anonymous.send(stateRequest());
    EXPECT_EQ(
        anonymous.reply().error().code(),
        wire::ERR_UNAUTHENTICATED
    );
    EXPECT_EQ(registry->retainedCount(), 0);
    EXPECT_EQ(registry->roomCount(), 0);
}

TEST_F(
    OnlineProtocolTest,
    MismatchedKnownPayloadsNeverReachMembershipTransactions
)
{
    Client alice(1, routes);
    login(alice, "alice");
    auto request = stateRequest();
    request.set_msg_id(wire::MSG_ROOM_JOIN_REQUEST);
    alice.send(request);
    EXPECT_EQ(
        alice.reply().error().code(),
        wire::ERR_INVALID_REQUEST
    );
    request = joinRequest(7);
    request.set_msg_id(wire::MSG_SESSION_STATE_REQUEST);
    alice.send(request);
    EXPECT_EQ(
        alice.reply().error().code(),
        wire::ERR_INVALID_REQUEST
    );
    request.set_msg_id(wire::MSG_ROOM_LEAVE_REQUEST);
    alice.send(request);
    EXPECT_EQ(
        alice.reply().error().code(),
        wire::ERR_INVALID_REQUEST
    );
    EXPECT_EQ(registry->roomCount(), 0);
}

TEST_F(
    OnlineProtocolTest,
    JwtRecoveryPreservesSessionRoomAndChangesObservedPresence
)
{
    Client alice(1, routes), bob(2, routes);
    const auto original = login(alice, "alice");
    login(bob, "bob");
    join(alice, 7);
    join(bob, 7);
    alice.disconnect();
    auto roster = state(bob);
    ASSERT_NE(member(roster, 42), nullptr);
    EXPECT_EQ(
        member(roster, 42)->presence(),
        wire::PRESENCE_DETACHED
    );
    Client recovered(3, routes);
    recovered.send(resume(original.session_token()));
    const auto response = recovered.reply();
    ASSERT_TRUE(response.login_response().accepted());
    const auto renewed = response.login_response();
    EXPECT_TRUE(renewed.resumed());
    EXPECT_EQ(renewed.session_id(), original.session_id());
    EXPECT_EQ(renewed.room_id(), 7);
    EXPECT_GT(renewed.session_epoch(), original.session_epoch());
    alice.disconnect();
    roster = state(bob);
    ASSERT_NE(member(roster, 42), nullptr);
    EXPECT_EQ(member(roster, 42)->presence(), wire::PRESENCE_ONLINE);
    EXPECT_EQ(
        member(roster, 42)->session_id(),
        original.session_id()
    );
    EXPECT_EQ(state(recovered).room_id(), 7);
    Client replay(4, routes);
    replay.send(resume(original.session_token()));
    EXPECT_EQ(
        replay.reply().error().code(),
        wire::ERR_UNAUTHENTICATED
    );
    EXPECT_EQ(state(recovered).session_id(), original.session_id());
}

TEST_F(
    OnlineProtocolTest,
    PasswordReplacementStartsFreshSessionAndRemovesOldRoomSlot
)
{
    Client old(1, routes), observer(2, routes);
    const auto first = login(old, "alice");
    login(observer, "bob");
    join(old, 7);
    join(observer, 7);
    Client replacement(3, routes);
    const auto next = login(replacement, "alice");
    EXPECT_NE(next.session_id(), first.session_id());
    EXPECT_EQ(next.room_id(), 0);
    EXPECT_FALSE(next.resumed());
    const auto roster = state(observer);
    EXPECT_EQ(roster.members_size(), 1);
    EXPECT_EQ(member(roster, 42), nullptr);
    std::optional<MessageReply> result;
    routes->find(wire::MSG_ROOM_JOIN_REQUEST)
        ->handler(
            std::make_shared<const RequestMessage>(RequestMessage{
                1,
                123,
                wire::MSG_ROOM_JOIN_REQUEST,
                joinRequest(7),
                old.context
            }),
            [&](MessageReply reply) { result = std::move(reply); }
        );
    ASSERT_TRUE(result);
    ASSERT_FALSE(*result);
    EXPECT_EQ(result->error(), wire::ERR_UNAUTHENTICATED);
    old.disconnect();
    EXPECT_EQ(state(replacement).session_id(), next.session_id());
    EXPECT_EQ(
        registry->roomMembers(arena::domain::RoomId{7}).size(),
        1
    );
}

TEST_F(
    OnlineProtocolTest,
    GraceBoundaryRejectsJwtAndSweepRemovesEveryExpiredMembership
)
{
    Client alice(1, routes), observer(2, routes);
    const auto token = login(alice, "alice").session_token();
    login(observer, "bob");
    join(alice, 7);
    join(observer, 7);
    alice.disconnect();
    now += std::chrono::milliseconds(100);
    Client tooLate(3, routes);
    tooLate.send(resume(token));
    EXPECT_EQ(
        tooLate.reply().error().code(),
        wire::ERR_UNAUTHENTICATED
    );
    registry->sweep();
    const auto roster = state(observer);
    EXPECT_EQ(roster.members_size(), 1);
    EXPECT_EQ(member(roster, 42), nullptr);
    EXPECT_EQ(registry->retainedCount(), 1);
    observer.send(leaveRequest());
    EXPECT_EQ(
        observer.reply().msg_id(),
        wire::MSG_ROOM_LEAVE_RESPONSE
    );
    EXPECT_EQ(registry->roomCount(), 0);
}

TEST_F(
    OnlineProtocolTest,
    InvalidAndFullRoomRejectionsPreserveCurrentBinding
)
{
    Client alice(1, routes), bob(2, routes), charlie(3, routes);
    login(alice, "alice");
    login(bob, "bob");
    login(charlie, "charlie");
    join(alice, 7);
    join(bob, 7);
    join(charlie, 8);
    charlie.send(joinRequest(7));
    EXPECT_EQ(
        charlie.reply().error().code(),
        wire::ERR_BUSINESS_FAILURE
    );
    EXPECT_EQ(state(charlie).room_id(), 8);
    charlie.send(joinRequest(0));
    EXPECT_EQ(
        charlie.reply().error().code(),
        wire::ERR_INVALID_REQUEST
    );
    EXPECT_EQ(state(charlie).room_id(), 8);
    EXPECT_EQ(state(alice).members_size(), 2);
}

TEST_F(
    OnlineProtocolTest, RoomCapacityRejectionDoesNotCreateMembership
)
{
    Client alice(1, routes), bob(2, routes), charlie(3, routes);
    login(alice, "alice");
    login(bob, "bob");
    login(charlie, "charlie");
    join(alice, 7);
    join(bob, 8);
    charlie.send(joinRequest(9));
    EXPECT_EQ(charlie.reply().error().code(), wire::ERR_UNAVAILABLE);
    EXPECT_EQ(state(charlie).room_id(), 0);
    EXPECT_EQ(registry->roomCount(), 2);
}

TEST(
    OnlineProtocolDirectionTest,
    BuiltInNewRoutesRejectWrongBusinessResponseTypes
)
{
    for (const auto& request :
         {stateRequest(), joinRequest(7), leaveRequest()})
    {
        auto registry = std::make_shared<MessageRegistry>();
        registry->registerHandler(
            static_cast<MsgId>(request.msg_id()),
            [](auto, auto complete)
            {
                wire::Envelope reply;
                reply.mutable_pong();
                complete(std::move(reply));
            },
            {false}
        );
        registry->seal();
        Client client(1, registry);
        client.send(request);
        EXPECT_EQ(client.reply().error().code(), wire::ERR_INTERNAL);
    }
}

TEST(
    OnlineProtocolDirectionTest,
    NewResponseIdsCannotBeRegisteredAsRequests
)
{
    MessageRegistry registry;
    for (const auto id :
         {wire::MSG_SESSION_STATE_RESPONSE,
          wire::MSG_ROOM_JOIN_RESPONSE,
          wire::MSG_ROOM_LEAVE_RESPONSE,
          wire::MSG_ROOM_READY_RESPONSE,
          wire::MSG_ROOM_STATUS_RESPONSE,
          wire::MSG_SETTLEMENT_RESPONSE})
        EXPECT_THROW(
            registry.registerHandler(id, [](auto, auto) {}),
            std::invalid_argument
        );
}

TEST(
    OnlineProtocolDirectionTest,
    LifecycleRoutesRejectWrongBusinessResponseTypes
)
{
    for (const auto& request : lifecycleRequests())
    {
        auto registry = std::make_shared<MessageRegistry>();
        registry->registerHandler(
            static_cast<MsgId>(request.msg_id()),
            [](auto, auto complete)
            {
                wire::Envelope reply;
                reply.mutable_pong();
                complete(std::move(reply));
            },
            {false}
        );
        registry->seal();
        Client client(1, registry);
        client.send(request);
        EXPECT_EQ(client.reply().error().code(), wire::ERR_INTERNAL);
    }
}

TEST(
    OnlineProtocolDirectionTest,
    LifecycleRoutesRejectMismatchedRequestPayloads
)
{
    auto registry = std::make_shared<MessageRegistry>();
    bool called = false;
    for (const auto& request : lifecycleRequests())
        registry->registerHandler(
            static_cast<MsgId>(request.msg_id()),
            [&called](auto, auto) { called = true; },
            {false}
        );
    registry->seal();
    Client client(1, registry);
    for (auto request : lifecycleRequests())
    {
        request.mutable_ping();
        client.send(request);
        EXPECT_EQ(
            client.reply().error().code(),
            wire::ERR_INVALID_REQUEST
        );
    }
    EXPECT_FALSE(called);
}

TEST_F(
    OnlineProtocolTest,
    ProductionLifecycleRoutesRequireCurrentAuthentication
)
{
    auth::JwtConfig jwt;
    jwt.key = "0123456789abcdef0123456789abcdef";
    jwt.now = [] { return 1700000000LL; };
    auto rooms = std::make_shared<RoomService>(registry);
    auto production = makeLoginMessageRegistry(
        std::make_shared<auth::LoginAuthenticator>(
            std::make_shared<Accounts>(),
            std::make_shared<Digest>()
        ),
        std::make_shared<auth::JwtTokens>(std::move(jwt)),
        registry,
        {},
        rooms
    );
    for (const auto& request : lifecycleRequests())
    {
        Client client(1, production);
        client.send(request);
        EXPECT_EQ(
            client.reply().error().code(),
            wire::ERR_UNAUTHENTICATED
        );
    }
}
} // namespace
