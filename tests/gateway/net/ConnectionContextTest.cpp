#include <atomic>
#include <chrono>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "net/ConnectionHost.h"
#include "net/MessageRegistry.h"
#include "net/OnlineRegistry.h"

#include "ProtocolTestHarness.h"

using namespace service::gateway;

TEST(
    ConnectionContextTest,
    PreservesAdmissionMetadataAndIsolatesSessions
)
{
    const auto accepted = ConnectionContext::Clock::now() -
                          std::chrono::seconds(2);
    const auto first = std::make_shared<ConnectionContext>(
        42,
        InetAddress("127.0.0.2", 3456),
        accepted
    );
    const auto second = std::make_shared<ConnectionContext>(
        43,
        InetAddress("127.0.0.2", 3457),
        accepted
    );
    EXPECT_EQ(first->connectionId, 42u);
    EXPECT_EQ(first->peer.ip(), "127.0.0.2");
    EXPECT_EQ(first->peer.port(), 3456);
    EXPECT_EQ(first->acceptedAt, accepted);
    EXPECT_NE(first->session, second->session);
    EXPECT_TRUE(first->session->markAuthenticated());
    EXPECT_EQ(
        second->session->loginState(),
        ConnectionSession::LoginState::Pending
    );
    EXPECT_THROW(
        (ConnectionContext(
            0,
            InetAddress("127.0.0.1", 1234),
            accepted
        )),
        std::invalid_argument
    );
}

TEST(
    ConnectionContextTest,
    LoginTransitionsAreTerminalAndMutuallyExclusive
)
{
    ConnectionSession authenticated;
    EXPECT_EQ(
        authenticated.loginState(),
        ConnectionSession::LoginState::Pending
    );
    EXPECT_TRUE(authenticated.markAuthenticated());
    EXPECT_FALSE(authenticated.markAuthenticated());
    EXPECT_FALSE(authenticated.expireLogin());
    EXPECT_EQ(
        authenticated.loginState(),
        ConnectionSession::LoginState::Authenticated
    );
    ConnectionSession expired;
    EXPECT_TRUE(expired.expireLogin());
    EXPECT_FALSE(expired.expireLogin());
    EXPECT_FALSE(expired.markAuthenticated());
    EXPECT_EQ(
        expired.loginState(),
        ConnectionSession::LoginState::Expired
    );
}

TEST(ConnectionContextTest, SessionAccessRequiresItsOwner)
{
    ConnectionSession session;
    int rejected = 0;
    std::thread foreign(
        [&]
        {
            try
            {
                (void)session.loginState();
            }
            catch (const std::logic_error&)
            {
                ++rejected;
            }
            try
            {
                (void)session.markAuthenticated();
            }
            catch (const std::logic_error&)
            {
                ++rejected;
            }
            try
            {
                (void)session.expireLogin();
            }
            catch (const std::logic_error&)
            {
                ++rejected;
            }
        }
    );
    foreign.join();
    EXPECT_EQ(rejected, 3);
    EXPECT_EQ(
        session.loginState(),
        ConnectionSession::LoginState::Pending
    );
}

TEST(
    ConnectionContextTest,
    HostRetainsContextThroughCloseWhenFactoryDoesNotCaptureIt
)
{
    auto context = std::make_shared<ConnectionContext>(
        1,
        InetAddress("127.0.0.1", 1234),
        ConnectionContext::Clock::now()
    );
    std::weak_ptr<const ConnectionContext> observed = context;
    bool invoked = false, aliveDuringClose = false;
    int closed = 0;
    const auto owner = std::this_thread::get_id();
    ConnectionHandlerFactory factory =
        [&](ConnectionContext::Ptr info)
    {
        invoked = true;
        EXPECT_EQ(std::this_thread::get_id(), owner);
        EXPECT_EQ(info.get(), context.get());
        ConnectionHandler handler([](ConnectionChannel&,
                                     std::span<const std::byte>) {});
        handler.onClosed = [&]
        {
            ++closed;
            const auto retained = observed.lock();
            aliveDuringClose = bool(retained);
            if (retained)
            {
                EXPECT_EQ(retained->connectionId, 1U);
            }
        };
        return handler;
    };
    auto transport = std::make_shared<test::MemoryTransport>();
    auto host = makeConnectionHost(
        transport,
        context,
        {[](auto) {}, [](auto, auto) { return ConnectionTimer{}; }},
        factory
    );
    EXPECT_TRUE(invoked);
    host->start();
    context.reset();
    EXPECT_FALSE(observed.expired());
    host->transportClosed();
    EXPECT_EQ(closed, 1);
    EXPECT_TRUE(aliveDuringClose);
    EXPECT_FALSE(observed.expired());
    host.reset();
    EXPECT_EQ(closed, 1);
    EXPECT_TRUE(observed.expired());
}

TEST(
    ConnectionContextTest,
    RejectsMissingContextAndInvalidFactoryResult
)
{
    auto transport = std::make_shared<test::MemoryTransport>();
    ConnectionScheduler scheduler{
        [](auto) {},
        [](auto, auto) { return ConnectionTimer{}; }
    };
    bool invoked = false;
    ConnectionHandlerFactory counting = [&](ConnectionContext::Ptr)
    {
        invoked = true;
        return ConnectionHandler([](auto&, auto) {});
    };
    EXPECT_THROW(
        makeConnectionHost(transport, {}, scheduler, counting),
        std::invalid_argument
    );
    EXPECT_FALSE(invoked);
    auto context = std::make_shared<ConnectionContext>(
        1,
        InetAddress("127.0.0.1", 1234),
        ConnectionContext::Clock::now()
    );
    ConnectionHandlerFactory invalid = [](ConnectionContext::Ptr)
    { return ConnectionMessageCallback{}; };
    EXPECT_THROW(
        makeConnectionHost(transport, context, scheduler, invalid),
        std::invalid_argument
    );
    ConnectionHandlerFactory throwing =
        [](ConnectionContext::Ptr) -> ConnectionMessageCallback
    { throw std::runtime_error("factory failed"); };
    EXPECT_THROW(
        makeConnectionHost(transport, context, scheduler, throwing),
        std::runtime_error
    );
    ConnectionHandlerFactory nonstandard =
        [](ConnectionContext::Ptr) -> ConnectionHandler { throw 42; };
    EXPECT_THROW(
        makeConnectionHost(
            transport,
            context,
            scheduler,
            nonstandard
        ),
        int
    );
    auto host = makeConnectionHost(transport, context, scheduler, {});
    host->start();
    const std::string message = "default echo";
    host->received(std::as_bytes(std::span(message)));
    EXPECT_EQ(transport->output, message);
}

TEST(
    ConnectionContextTest,
    FeatureFactoryFailureClosesWithLiveContextOnce
)
{
    auto context = std::make_shared<ConnectionContext>(
        1,
        InetAddress("127.0.0.1", 1234),
        ConnectionContext::Clock::now()
    );
    std::weak_ptr<const ConnectionContext> observed = context;
    int closed = 0;
    bool aliveDuringClose = false;
    ConnectionHandlerFactory handlers = [&](ConnectionContext::Ptr)
    {
        ConnectionHandler handler([](auto&, auto) {});
        handler.onClosed = [&]
        {
            ++closed;
            aliveDuringClose = !observed.expired();
        };
        return handler;
    };
    ConnectionFeatureFactory fail = [](auto&, const auto&)
    { throw std::runtime_error("feature factory"); };
    EXPECT_THROW(
        makeConnectionHost(
            std::make_shared<test::MemoryTransport>(),
            std::move(context),
            {[](auto) {},
             [](auto, auto) { return ConnectionTimer{}; }},
            handlers,
            fail
        ),
        std::runtime_error
    );
    EXPECT_EQ(closed, 1);
    EXPECT_TRUE(aliveDuringClose);
    EXPECT_TRUE(observed.expired());
}

TEST(ConnectionContextTest, ForeignDisconnectRejectsBeforeMutation)
{
    ConnectionSession session;
    ASSERT_TRUE(session.markAuthenticated());
    std::thread foreign(
        [&] { EXPECT_THROW(session.disconnect(), std::logic_error); }
    );
    foreign.join();
    EXPECT_EQ(
        session.loginState(),
        ConnectionSession::LoginState::Authenticated
    );
    session.disconnect();
    EXPECT_EQ(
        session.loginState(),
        ConnectionSession::LoginState::Expired
    );
}

TEST(
    ConnectionContextTest,
    PassiveContextMayBeReleasedLastOnForeignThread
)
{
    auto context = std::make_shared<ConnectionContext>(
        99,
        InetAddress("127.0.0.1", 1234),
        ConnectionContext::Clock::now()
    );
    ASSERT_TRUE(context->session->markAuthenticated());
    std::weak_ptr<const ConnectionContext> observed = context;
    std::weak_ptr<ConnectionSession> session = context->session;
    std::thread foreign(
        [retained = std::move(context)]() mutable
        {
            // This is final passive metadata release, not an active
            // mutation API.
            retained.reset();
        }
    );
    foreign.join();
    EXPECT_TRUE(observed.expired());
    EXPECT_TRUE(session.expired());
}

TEST(
    ConnectionContextTest,
    RetainedRequestFinalReleaseDetachesSessionWithoutOwnerIo
)
{
    service::auth::JwtConfig config;
    config.key = "0123456789abcdef0123456789abcdef"; // Test-only key.
    config.now = [] { return std::int64_t{1700000000}; };
    service::auth::JwtTokens tokens(std::move(config));
    auto accounts = std::make_shared<OnlineRegistry>();
    auto context = std::make_shared<ConnectionContext>(
        100,
        InetAddress("127.0.0.1", 1234),
        ConnectionContext::Clock::now()
    );
    std::atomic<int> closePosts{0};
    context->session->setCloseRequest([&] { ++closePosts; });
    auto staleClose = context->session->closeRequest();
    auto grant = accounts->install(
        arena::domain::PlayerId{42},
        context->connectionId,
        std::nullopt,
        tokens,
        staleClose
    );
    ASSERT_TRUE(grant);
    ASSERT_TRUE(context->session->bind(grant->lease));
    EXPECT_EQ(accounts->activeCount(), 1U);
    std::weak_ptr<const ConnectionContext> observed = context;
    std::weak_ptr<ConnectionSession> session = context->session;
    auto request = std::make_shared<const RequestMessage>(
        RequestMessage{
            context->connectionId,
            1,
            arena::protocol::MSG_PING,
            {},
            context
        }
    );
    context.reset();
    std::thread business([retained = std::move(request)]() mutable
                         { retained.reset(); });
    business.join();
    EXPECT_TRUE(observed.expired());
    EXPECT_TRUE(session.expired());
    EXPECT_FALSE(grant->lease->current());
    EXPECT_EQ(accounts->activeCount(), 0U);
    staleClose();
    EXPECT_EQ(closePosts.load(), 0);
}

TEST(
    ConnectionContextTest,
    BindingIsOwnerCheckedAndOnlyReportsCurrentLease
)
{
    service::auth::JwtConfig config;
    config.key = "0123456789abcdef0123456789abcdef";
    config.now = [] { return std::int64_t{1700000000}; };
    service::auth::JwtTokens tokens(std::move(config));
    auto registry = std::make_shared<OnlineRegistry>();
    ConnectionSession session;
    EXPECT_FALSE(session.binding());
    auto grant = registry->install(
        arena::domain::PlayerId{42},
        100,
        std::nullopt,
        tokens,
        [] {}
    );
    ASSERT_TRUE(grant);
    ASSERT_TRUE(session.bind(grant->lease));
    const auto current = session.binding();
    ASSERT_TRUE(current);
    EXPECT_EQ(current->playerId, arena::domain::PlayerId{42});
    EXPECT_EQ(current->connectionId, 100U);
    EXPECT_EQ(current->epoch, grant->lease->binding.epoch);
    std::thread foreign(
        [&] { EXPECT_THROW(session.binding(), std::logic_error); }
    );
    foreign.join();
    ASSERT_TRUE(registry->install(
        arena::domain::PlayerId{42},
        101,
        std::nullopt,
        tokens,
        [] {}
    ));
    EXPECT_FALSE(session.binding());
    session.disconnect();
    EXPECT_FALSE(session.binding());
}
