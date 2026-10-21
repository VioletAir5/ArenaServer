#include <atomic>
#include <barrier>
#include <memory>
#include <span>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <openssl/evp.h>

#include "net/ConnectionRuntime.h"
#include "net/LoginHandler.h"
#include "net/PacketBuffer.h"
#include "net/PreLoginHandshake.h"
#include "protocol/EnvelopeCodec.hpp"
#include "protocol/PacketCodec.hpp"

#include "auth/JwtTokens.h"
#include "auth/LoginAuthenticator.h"

namespace
{
using namespace service::gateway;
using namespace service::auth;
using arena::domain::PlayerId;
namespace wire = arena::protocol;
constexpr std::int64_t clockValue = 1700000000;
constexpr std::string_view
    testKey = "0123456789abcdef0123456789abcdef"; // Test-only key.

JwtConfig config()
{
    JwtConfig value;
    value.key = testKey;
    value.now = [] { return clockValue; };
    return value;
}

// Independent wire encoder: OpenSSL standard base64, then URL
// translation.
std::string base64(std::string_view value)
{
    std::string result(4 * ((value.size() + 2) / 3) + 1, '\0');
    const int count = EVP_EncodeBlock(
        reinterpret_cast<unsigned char*>(result.data()),
        reinterpret_cast<const unsigned char*>(value.data()),
        static_cast<int>(value.size())
    );
    result.resize(static_cast<std::size_t>(count));
    for (auto& c : result)
        if (c == '+')
            c = '-';
        else if (c == '/')
            c = '_';
    while (!result.empty() && result.back() == '=')
        result.pop_back();
    return result;
}

std::string sign(
    std::string payload,
    std::string header = R"({"alg":"HS256","typ":"JWT"})"
)
{
    auto input = base64(header) + "." + base64(payload);
    unsigned char bytes[32];
    std::size_t size = 0;
    if (!EVP_Q_mac(
            nullptr,
            "HMAC",
            nullptr,
            "SHA256",
            nullptr,
            testKey.data(),
            testKey.size(),
            reinterpret_cast<const unsigned char*>(input.data()),
            input.size(),
            bytes,
            sizeof(bytes),
            &size
        ))
        throw std::runtime_error("test MAC failed");
    return input + "." +
           base64(
               std::string_view(
                   reinterpret_cast<const char*>(bytes),
                   size
               )
           );
}

std::string payload()
{
    return R"({"iss":"arena-auth","aud":"arena-gateway","sub":"42","iat":1700000000,"nbf":1700000000,"exp":1700000060,"epoch":"1"})";
}

TEST(
    JwtTokensTest,
    AcceptsIndependentSignedProfileAndRoundTripsLargeEpoch
)
{
    JwtTokens tokens(config());
    const auto verified = tokens.verify(sign(payload()));
    ASSERT_TRUE(verified);
    EXPECT_EQ(verified->playerId, PlayerId{42});
    EXPECT_EQ(verified->epoch, 1u);
    const auto issued = tokens.issue(PlayerId{42}, UINT64_MAX);
    ASSERT_TRUE(issued);
    const auto again = tokens.verify(issued->encoded);
    ASSERT_TRUE(again);
    EXPECT_EQ(again->epoch, UINT64_MAX);
}

TEST(JwtTokensTest, RejectsTamperingWrongKeyAndUnsignedAlgorithms)
{
    JwtTokens tokens(config());
    auto token = sign(payload());
    token[token.find('.') + 1] = token[token.find('.') + 1] == 'a'
                                     ? 'b'
                                     : 'a';
    EXPECT_FALSE(tokens.verify(token));
    auto other = config();
    other.key = std::string(32, 'x');
    EXPECT_FALSE(JwtTokens(other).verify(sign(payload())));
    for (const auto alg : {"none", "HS384", "RS256"})
        EXPECT_FALSE(tokens.verify(sign(
            payload(),
            std::string(R"({"alg":")") + alg + R"(","typ":"JWT"})"
        )));
}

TEST(JwtTokensTest, RequiresExactIssuerAudienceTypeAndAllClaims)
{
    JwtTokens tokens(config());
    for (
        const auto wrong : {
            R"({"iss":"other","aud":"arena-gateway","sub":"42","iat":1700000000,"nbf":1700000000,"exp":1700000060,"epoch":"1"})",
            R"({"iss":"arena-auth","aud":"other","sub":"42","iat":1700000000,"nbf":1700000000,"exp":1700000060,"epoch":"1"})",
            R"({"iss":"arena-auth","aud":"arena-gateway","sub":"42","iat":1700000000,"nbf":1700000000,"exp":1700000060})",
            R"({"iss":"arena-auth","aud":["arena-gateway"],"sub":"42","iat":1700000000,"nbf":1700000000,"exp":1700000060,"epoch":"1"})"
        })
        EXPECT_FALSE(tokens.verify(sign(wrong)));
    EXPECT_FALSE(tokens.verify(
        sign(payload(), R"({"alg":"HS256","typ":"OTHER"})")
    ));
    EXPECT_FALSE(tokens.verify(sign(
        payload(),
        R"({"alg":"HS256","typ":"JWT","kid":"untrusted"})"
    )));
}

TEST(
    JwtTokensTest,
    RejectsDuplicateClaimsEscapedKeysAndWrongScalarTypes
)
{
    JwtTokens tokens(config());
    for (
        const auto bad : {
            R"({"iss":"other","iss":"arena-auth","aud":"arena-gateway","sub":"42","iat":1700000000,"nbf":1700000000,"exp":1700000060,"epoch":"1"})",
            R"({"iss":"arena-auth","aud":"arena-gateway","sub":"42","iat":1700000000,"nbf":1700000000,"exp":1700000060,"\u0065poch":"1","epoch":"2"})",
            R"({"iss":"arena-auth","aud":"arena-gateway","sub":"42","iat":1700000000,"nbf":1700000000,"exp":"1700000060","epoch":"1"})",
            R"({"iss":"arena-auth","aud":"arena-gateway","sub":42,"iat":1700000000,"nbf":1700000000,"exp":1700000060,"epoch":"1"})",
            R"({"iss":"arena-auth","aud":"arena-gateway","sub":"42","iat":1700000000,"nbf":1700000000,"exp":1700000060,"epoch":1})",
            R"({"iss":"arena-auth","aud":"arena-gateway","sub":"42","iat":1700000000,"nbf":1700000000,"exp":1700000060.5,"epoch":"1"})"
        })
        EXPECT_FALSE(tokens.verify(sign(bad)));
    EXPECT_FALSE(tokens.verify(
        sign(payload(), R"({"alg":"none","alg":"HS256","typ":"JWT"})")
    ));
}

TEST(JwtTokensTest, EnforcesTimeBoundariesAndMaximumLifetime)
{
    auto cfg = config();
    auto token = sign(payload());
    for (const auto time : {1699999999LL, 1700000060LL, 1700000061LL})
    {
        cfg.now = [time] { return time; };
        EXPECT_FALSE(JwtTokens(cfg).verify(token));
    }
    cfg.now = [] { return 1700000059LL; };
    EXPECT_TRUE(JwtTokens(cfg).verify(token));
    cfg = config();
    cfg.lifetime = std::chrono::seconds(30);
    EXPECT_FALSE(JwtTokens(cfg).verify(token));
}

TEST(
    JwtTokensTest, RejectsMalformedOversizedDeepAndNoncanonicalTokens
)
{
    JwtTokens tokens(config());
    for (const auto& token :
         {std::string{},
          std::string("a.b.c"),
          std::string(4097, 'x'),
          sign(payload()) + ".",
          sign(payload()) + "=",
          sign("{}"),
          sign(R"({"a":{"b":{"c":1}}})"),
          sign("[1,2,3]")})
        EXPECT_FALSE(tokens.verify(token));
    auto token = sign(payload());
    token.back() = token.back() == 'A'
                       ? 'B'
                       : 'A'; // Alter signature/unused bits.
    EXPECT_FALSE(tokens.verify(token));
}

TEST(JwtTokensTest, RejectsZeroNoncanonicalAndOverflowIdentities)
{
    JwtTokens tokens(config());
    for (const auto bad :
         {"0", "042", "-1", "+42", "18446744073709551616"})
    {
        auto text = payload();
        text.replace(
            text.find("\"42\""),
            4,
            std::string("\"") + bad + "\""
        );
        EXPECT_FALSE(tokens.verify(sign(text)));
    }
    EXPECT_FALSE(tokens.issue(PlayerId{42}, 0));
}

TEST(JwtTokensTest, InvalidConfigurationFailsBeforeLogin)
{
    auto cfg = config();
    cfg.key = "short";
    EXPECT_THROW(JwtTokens{cfg}, std::invalid_argument);
    cfg = config();
    cfg.lifetime = std::chrono::seconds(0);
    EXPECT_THROW(JwtTokens{cfg}, std::invalid_argument);
    cfg = config();
    cfg.issuer.clear();
    EXPECT_THROW(JwtTokens{cfg}, std::invalid_argument);
}

// These adapters are test doubles, deliberately not password hashing
// algorithms.
struct Accounts final : PasswordAccounts
{
    std::expected<PasswordAccount, AuthError>
    find(std::string_view account) const override
    {
        if (account != "alice")
            return std::unexpected(AuthError::InvalidCredentials);
        return PasswordAccount{PlayerId{42}, "test-only-digest"};
    }
};
struct Digest final : PasswordDigest
{
    std::expected<std::string, AuthError>
    hash(std::string_view) const override
    {
        return std::unexpected(AuthError::Unavailable);
    }
    std::expected<bool, AuthError> verify(
        std::string_view password, std::string_view digest
    ) const override
    {
        return password == "test-password" &&
               digest == "test-only-digest";
    }
};
auto passwords()
{
    return std::make_shared<LoginAuthenticator>(
        std::make_shared<Accounts>(),
        std::make_shared<Digest>()
    );
}

TEST(PasswordDigestTest, UnimplementedAdaptersNeverAuthenticate)
{
    UnavailablePasswordDigest digest;
    EXPECT_EQ(
        digest.hash("anything").error(),
        AuthError::Unavailable
    );
    EXPECT_EQ(
        digest.verify("anything", "anything").error(),
        AuthError::Unavailable
    );
    LoginAuthenticator auth(
        std::make_shared<UnavailablePasswordAccounts>(),
        std::make_shared<UnavailablePasswordDigest>()
    );
    EXPECT_EQ(
        auth.authenticate("alice", "test-password").error(),
        AuthError::Unavailable
    );
}

TEST(
    PasswordDigestTest,
    ValidatesBoundsAndDistinguishesBadPasswordFromBackendFailure
)
{
    const auto auth = passwords();
    EXPECT_TRUE(auth->authenticate("alice", "test-password"));
    EXPECT_EQ(
        auth->authenticate("alice", "wrong").error(),
        AuthError::InvalidCredentials
    );
    EXPECT_EQ(
        auth->authenticate("unknown", "test-password").error(),
        AuthError::InvalidCredentials
    );
    EXPECT_FALSE(auth->authenticate(std::string(129, 'a'), "pw"));
    EXPECT_FALSE(auth->authenticate("alice", std::string(1025, 'a')));
    EXPECT_FALSE(auth->authenticate("alice", ""));
}

TEST(
    AccountSessionsTest,
    OneHundredConcurrentPasswordLoginsLeaveExactlyOneActive
)
{
    const auto auth = passwords();
    JwtTokens tokens(config());
    auto directory = std::make_shared<AccountSessions>();
    std::atomic<int> kicks{0};
    std::barrier start(100);
    std::vector<std::expected<SessionGrant, AuthError>> grants(100);
    std::vector<std::thread> workers;
    for (std::size_t i = 0; i < grants.size(); ++i)
        workers.emplace_back(
            [&, i]
            {
                start.arrive_and_wait();
                const auto player = auth->authenticate(
                    "alice",
                    "test-password"
                );
                if (!player)
                    grants[i] = std::unexpected(player.error());
                else
                    grants[i] = directory->install(
                        *player,
                        i + 1,
                        {},
                        tokens,
                        [&] { ++kicks; }
                    );
            }
        );
    for (auto& worker : workers)
        worker.join();
    EXPECT_EQ(directory->activeCount(), 1u);
    EXPECT_EQ(kicks.load(), 99);
    int active = 0;
    std::uint64_t winnerEpoch = 0;
    for (const auto& grant : grants)
    {
        ASSERT_TRUE(grant);
        if (grant->lease->current())
        {
            ++active;
            winnerEpoch = grant->lease->binding.epoch;
        }
        else
        {
            grant->lease->release(); // Old disconnect must not erase
                                     // the winner.
            auto claims = tokens.verify(grant->token);
            ASSERT_TRUE(claims);
            auto oldLogin = directory->install(
                PlayerId{42},
                1000,
                *claims,
                tokens,
                [] {}
            );
            ASSERT_FALSE(oldLogin);
            EXPECT_EQ(oldLogin.error(), AuthError::Superseded);
        }
    }
    EXPECT_EQ(active, 1);
    EXPECT_EQ(directory->activeCount(), 1u);
    EXPECT_EQ(directory->currentEpoch(PlayerId{42}), winnerEpoch);
}

TEST(
    AccountSessionsTest, ConditionalReleaseChecksEpochAndConnectionId
)
{
    JwtTokens tokens(config());
    auto directory = std::make_shared<AccountSessions>();
    const auto first = directory->install(
        PlayerId{42},
        1,
        {},
        tokens,
        [] {}
    );
    ASSERT_TRUE(first);
    const auto second = directory->install(
        PlayerId{42},
        2,
        {},
        tokens,
        [] {}
    );
    ASSERT_TRUE(second);
    EXPECT_FALSE(first->lease->current());
    directory->detach(first->lease->binding);
    directory->detach(
        {PlayerId{42}, 1, second->lease->binding.epoch}
    );
    EXPECT_EQ(directory->activeCount(), 1u);
    EXPECT_TRUE(second->lease->current());
    second->lease->release();
    EXPECT_EQ(directory->activeCount(), 0u);
}

TEST(AccountSessionsTest, DetachedSessionResumesOnceAndRotatesEpoch)
{
    JwtTokens tokens(config());
    auto directory = std::make_shared<AccountSessions>();
    const auto first = directory->install(
        PlayerId{42},
        1,
        {},
        tokens,
        [] {}
    );
    ASSERT_TRUE(first);
    first->lease->release();
    auto prior = tokens.verify(first->token);
    ASSERT_TRUE(prior);
    const auto second = directory->install(
        PlayerId{42},
        2,
        *prior,
        tokens,
        [] {}
    );
    ASSERT_TRUE(second);
    EXPECT_GT(
        second->lease->binding.epoch,
        first->lease->binding.epoch
    );
    EXPECT_FALSE(
        directory->install(PlayerId{42}, 3, *prior, tokens, [] {})
    );
    auto freshDirectory = std::make_shared<AccountSessions>();
    EXPECT_FALSE(
        freshDirectory
            ->install(PlayerId{42}, 3, *prior, tokens, [] {})
    );
}

TEST(
    AccountSessionsTest,
    AccountCapacityIsBoundedAndFailedInstallPreservesCurrentSession
)
{
    JwtTokens tokens(config());
    auto directory = std::make_shared<AccountSessions>(1);
    const auto first = directory->install(
        PlayerId{42},
        1,
        {},
        tokens,
        [] {}
    );
    ASSERT_TRUE(first);
    auto overflow = directory
                        ->install(PlayerId{43}, 2, {}, tokens, [] {});
    ASSERT_FALSE(overflow);
    EXPECT_EQ(overflow.error(), AuthError::Unavailable);
    EXPECT_TRUE(first->lease->current());
    auto claims = *tokens.verify(first->token);
    ++claims.epoch;
    EXPECT_FALSE(
        directory->install(PlayerId{42}, 2, claims, tokens, [] {})
    );
    EXPECT_TRUE(first->lease->current());
}

struct TestTransport final : IConnection
{
    explicit TestTransport(std::uint64_t value)
        : identity(value)
    {
    }
    std::uint64_t identity;
    std::string output;
    bool open = true;
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
    std::shared_ptr<TestTransport> transport;
    ConnectionContext::Ptr context;
    std::vector<std::function<void()>> posts;
    std::shared_ptr<ConnectionRuntime> runtime;
    Client(
        std::uint64_t id,
        std::shared_ptr<const MessageRegistry> routes
    )
        : transport(std::make_shared<TestTransport>(id))
        , context(
              std::make_shared<ConnectionContext>(
                  id,
                  InetAddress("127.0.0.1", 1),
                  ConnectionClock::now(),
                  std::make_shared<RejectionCounters>()
              )
          )
    {
        runtime = makeConnectionRuntime(
            transport,
            context,
            {[this](auto action)
             { posts.push_back(std::move(action)); },
             [](auto, auto) { return ConnectionTimer{}; }},
            [routes](auto ctx)
            { return makePreLoginHandshakeHandler(ctx, routes); }
        );
        runtime->start();
    }
    void send(wire::Envelope value)
    {
        const auto bytes = service::protocol::EnvelopeCodec::encode(
            value
        );
        ASSERT_TRUE(bytes);
        const auto frame = service::protocol::PacketCodec::encode(
            1,
            7,
            std::as_bytes(std::span(*bytes))
        );
        ASSERT_TRUE(frame);
        runtime->received(
            std::as_bytes(
                std::span(frame->peek(), frame->readableBytes())
            )
        );
    }
    wire::Envelope reply()
    {
        wire::Envelope value;
        int count = 0;
        service::protocol::PacketCodec codec(kMaxPacketSize);
        codec.feed(
            std::as_bytes(std::span(transport->output)),
            [&](auto&, auto bytes)
            {
                EXPECT_TRUE(
                    service::protocol::EnvelopeCodec::decode(
                        std::as_bytes(std::span(bytes)),
                        value
                    )
                );
                ++count;
                return true;
            }
        );
        EXPECT_EQ(count, 1);
        transport->output.clear();
        return value;
    }
    void drain()
    {
        while (!posts.empty())
        {
            auto tasks = std::move(posts);
            posts.clear();
            for (auto& task : tasks)
                task();
        }
    }
};
wire::Envelope login(std::uint64_t id = 1)
{
    wire::Envelope value;
    value.set_request_id(id);
    value.set_msg_id(wire::MSG_LOGIN_REQUEST);
    value.mutable_login_request()->set_account("alice");
    value.mutable_login_request()->set_credential("test-password");
    return value;
}
wire::Envelope ping()
{
    wire::Envelope value;
    value.set_request_id(2);
    value.set_msg_id(wire::MSG_PING);
    value.mutable_ping()->set_nonce(9);
    return value;
}

TEST(
    LoginHandlerTest,
    PasswordJwtResumeAndOldDisconnectUseTheSameDirectory
)
{
    auto directory = std::make_shared<AccountSessions>();
    auto tokens = std::make_shared<JwtTokens>(config());
    const auto routes = makeLoginMessageRegistry(
        passwords(),
        tokens,
        directory
    );
    Client first(1, routes);
    first.send(login());
    const auto original = first.reply();
    ASSERT_TRUE(original.login_response().accepted());
    EXPECT_EQ(original.login_response().player_id(), 42u);
    Client second(2, routes);
    wire::Envelope resume;
    resume.set_request_id(3);
    resume.mutable_login_request()->set_credential_kind(
        wire::CREDENTIAL_JWT
    );
    resume.mutable_login_request()->set_credential(
        original.login_response().session_token()
    );
    second.send(resume);
    const auto renewed = second.reply();
    ASSERT_TRUE(renewed.login_response().accepted());
    EXPECT_GT(
        renewed.login_response().session_epoch(),
        original.login_response().session_epoch()
    );
    first.drain();
    first.runtime->transportClosed();
    EXPECT_EQ(directory->activeCount(), 1u);
    second.send(ping());
    EXPECT_EQ(second.reply().pong().nonce(), 9u);
}

TEST(
    LoginHandlerTest, OldPingAndRepeatedLoginFailBeforeKickQueueDrains
)
{
    auto directory = std::make_shared<AccountSessions>();
    auto routes = makeLoginMessageRegistry(
        passwords(),
        std::make_shared<JwtTokens>(config()),
        directory
    );
    Client first(1, routes);
    Client second(2, routes);
    first.send(login());
    ASSERT_TRUE(first.reply().login_response().accepted());
    second.send(login());
    ASSERT_TRUE(second.reply().login_response().accepted());
    ASSERT_FALSE(first.posts.empty()); // Kick is still deferred.
    EXPECT_EQ(
        first.context->session->loginState(),
        ConnectionSession::LoginState::Expired
    );
    first.send(ping());
    first.send(login(4));
    EXPECT_TRUE(first.transport->output.empty());
    first.drain();
    first.runtime->transportClosed();
    EXPECT_EQ(directory->activeCount(), 1u);
}

TEST(
    LoginHandlerTest,
    BadPasswordOrMalformedJwtNeverReplacesAValidSession
)
{
    auto directory = std::make_shared<AccountSessions>();
    auto routes = makeLoginMessageRegistry(
        passwords(),
        std::make_shared<JwtTokens>(config()),
        directory
    );
    Client first(1, routes);
    first.send(login());
    ASSERT_TRUE(first.reply().login_response().accepted());
    Client second(2, routes);
    auto bad = login();
    bad.mutable_login_request()->set_credential("wrong");
    second.send(bad);
    EXPECT_EQ(
        second.reply().error().code(),
        wire::ERR_UNAUTHENTICATED
    );
    bad.mutable_login_request()->clear_account();
    bad.mutable_login_request()->set_credential_kind(
        wire::CREDENTIAL_JWT
    );
    second.send(bad);
    EXPECT_EQ(
        second.reply().error().code(),
        wire::ERR_UNAUTHENTICATED
    );
    EXPECT_EQ(directory->activeCount(), 1u);
    first.send(ping());
    EXPECT_EQ(first.reply().pong().nonce(), 9u);
}

TEST(
    LoginHandlerTest, SupersededAsynchronousCompletionCannotSendAReply
)
{
    auto directory = std::make_shared<AccountSessions>();
    auto tokens = std::make_shared<JwtTokens>(config());
    auto routes = std::make_shared<MessageRegistry>();
    RequestCompletion delayed;
    int calls = 0;
    routes->registerHandler(
        wire::MSG_PING,
        [&](auto, auto complete)
        {
            ++calls;
            delayed = std::move(complete);
        }
    );
    routes->seal();
    Client first(1, routes);
    auto grant = directory->install(
        PlayerId{42},
        1,
        {},
        *tokens,
        first.context->session->closeRequest()
    );
    ASSERT_TRUE(grant);
    ASSERT_TRUE(first.context->session->bind(grant->lease));
    first.send(ping());
    ASSERT_EQ(calls, 1);
    auto newer = directory
                     ->install(PlayerId{42}, 2, {}, *tokens, [] {});
    ASSERT_TRUE(newer);
    wire::Envelope reply;
    reply.mutable_pong()->set_nonce(9);
    delayed(reply);
    EXPECT_TRUE(first.transport->output.empty());
    first.send(ping());
    EXPECT_EQ(calls, 1);
}

TEST(LoginHandlerTest, SavedKickCannotPostAfterRuntimeTeardown)
{
    auto routes = makeLoginMessageRegistry(
        passwords(),
        std::make_shared<JwtTokens>(config()),
        std::make_shared<AccountSessions>()
    );
    std::function<void()> kick;
    {
        Client client(1, routes);
        kick = client.context->session->closeRequest();
        client.runtime->transportClosed();
        client.runtime.reset();
        EXPECT_TRUE(client.posts.empty());
    }
    EXPECT_NO_THROW(kick()); // Endpoint retained; scheduler/Client
                             // have already been destroyed.
}

TEST(
    LoginHandlerTest,
    ReplacementBeforeOwnerBindStillInvalidatesTheOldConnection
)
{
    auto directory = std::make_shared<AccountSessions>();
    auto tokens = std::make_shared<JwtTokens>(config());
    auto routes = makeLoginMessageRegistry(
        passwords(),
        tokens,
        directory
    );
    Client first(1, routes);
    auto old = directory->install(
        PlayerId{42},
        1,
        {},
        *tokens,
        first.context->session->closeRequest()
    );
    ASSERT_TRUE(old);
    auto current = directory
                       ->install(PlayerId{42}, 2, {}, *tokens, [] {});
    ASSERT_TRUE(current);
    ASSERT_FALSE(first.context->session->bind(old->lease));
    EXPECT_EQ(
        first.context->session->loginState(),
        ConnectionSession::LoginState::Expired
    );
    first.send(login());
    first.send(ping());
    EXPECT_TRUE(first.transport->output.empty());
    EXPECT_EQ(directory->activeCount(), 1u);
    EXPECT_TRUE(current->lease->current());
}
} // namespace
