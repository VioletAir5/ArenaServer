#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

#include "net/ConnectionHandler.h"

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
    CallbackCopiesRetainIgnoredContextUntilFinalRelease
)
{
    auto context = std::make_shared<ConnectionContext>(
        1,
        InetAddress("127.0.0.1", 1234),
        ConnectionContext::Clock::now()
    );
    std::weak_ptr<const ConnectionContext> observed = context;
    bool invoked = false;
    const auto owner = std::this_thread::get_id();
    ConnectionHandlerFactory factory =
        [&](ConnectionContext::Ptr info)
    {
        invoked = true;
        EXPECT_EQ(std::this_thread::get_id(), owner);
        EXPECT_EQ(info.get(), context.get());
        return [](IConnection&, std::span<const std::byte>) {};
    };
    auto callback = makeConnectionHandler(factory, context);
    auto copy = callback;
    EXPECT_TRUE(invoked);
    context.reset();
    EXPECT_FALSE(observed.expired());
    callback = {};
    EXPECT_FALSE(observed.expired());
    copy = {};
    EXPECT_TRUE(observed.expired());
}

TEST(
    ConnectionContextTest,
    RejectsMissingContextAndInvalidFactoryResult
)
{
    EXPECT_THROW(
        makeConnectionHandler({}, {}),
        std::invalid_argument
    );
    auto context = std::make_shared<ConnectionContext>(
        1,
        InetAddress("127.0.0.1", 1234),
        ConnectionContext::Clock::now()
    );
    ConnectionHandlerFactory invalid = [](ConnectionContext::Ptr)
    { return ConnectionMessageCallback{}; };
    EXPECT_THROW(
        makeConnectionHandler(invalid, context),
        std::invalid_argument
    );
    ConnectionHandlerFactory throwing =
        [](ConnectionContext::Ptr) -> ConnectionMessageCallback
    { throw std::runtime_error("factory failed"); };
    EXPECT_THROW(
        makeConnectionHandler(throwing, context),
        std::runtime_error
    );
    EXPECT_TRUE(makeConnectionHandler({}, context));
}
