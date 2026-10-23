#include "net/ConnectionHost.h"

#include <deque>
#include <thread>
#include <type_traits>

#include <gtest/gtest.h>

#include "net/BackpressureFeature.h"
using namespace service::gateway;
static_assert(!std::is_base_of_v<ITransport, ConnectionHost>);
static_assert(std::is_base_of_v<ConnectionChannel, ConnectionHost>);
namespace
{
struct Transport : ITransport
{
    bool open = true, paused = false;
    int closes = 0, resumes = 0, sends = 0, shutdowns = 0;
    std::function<void()> shutdownAction;
    std::uint64_t id() const noexcept override
    {
        return 7;
    }
    void send(std::span<const std::byte>) override
    {
        ++sends;
    }
    void shutdown() override
    {
        ++shutdowns;
        if (shutdownAction)
            shutdownAction();
    }
    void close() override
    {
        open = false;
        ++closes;
    }
    bool connected() const noexcept override
    {
        return open;
    }
    void pauseRead() override
    {
        paused = true;
    }
    void resumeRead() override
    {
        paused = false;
        ++resumes;
    }
    std::size_t pendingBytes() const noexcept override
    {
        return 0;
    }
};
struct ConnectionHostTest : ::testing::Test
{
    std::shared_ptr<Transport>
        transport = std::make_shared<Transport>();
    std::deque<std::function<void()>> posted;
    std::vector<std::function<void()>> timers;
    std::shared_ptr<ConnectionHost>
        runtime = std::make_shared<ConnectionHost>(
            transport,
            std::make_shared<ConnectionContext>(
                7, InetAddress("127.0.0.1", 1), ConnectionClock::now()
            ),
            ConnectionScheduler{
                [this](auto action)
                { posted.push_back(std::move(action)); },
                [this](auto, auto action)
                {
                    auto live = std::make_shared<bool>(true);
                    timers.push_back(
                        [live, action]
                        {
                            if (*live)
                                action();
                        }
                    );
                    return ConnectionTimer([live] { *live = false; });
                }
            },
            [](auto&, auto) {}
        );
    void drain()
    {
        while (!posted.empty())
        {
            auto f = std::move(posted.front());
            posted.pop_front();
            f();
        }
    }
};
struct Count : IConnectionFeature
{
    int closed = 0;
    void onClosed() noexcept override
    {
        ++closed;
    }
};
TEST_F(ConnectionHostTest, PauseReasonsDoNotResumeEachOther)
{
    runtime->start();
    runtime->setReadPaused(ReadPauseReason::Backpressure, true);
    runtime->setReadPaused(ReadPauseReason::Application, true);
    runtime->setReadPaused(ReadPauseReason::Backpressure, false);
    EXPECT_TRUE(transport->paused);
    runtime->setReadPaused(ReadPauseReason::Application, false);
    EXPECT_FALSE(transport->paused);
    EXPECT_EQ(transport->resumes, 1);
}
TEST_F(ConnectionHostTest, DeferredCloseNotifiesEveryPolicyOnce)
{
    auto& first = runtime->features().add<Count>();
    auto& second = runtime->features().add<Count>();
    runtime->start();
    runtime->close();
    EXPECT_FALSE(runtime->connected());
    EXPECT_EQ(transport->closes, 0);
    EXPECT_TRUE(transport->paused);
    runtime->close();
    drain();
    runtime->transportClosed();
    runtime->transportClosed();
    EXPECT_EQ(transport->closes, 1);
    EXPECT_EQ(first.closed, 1);
    EXPECT_EQ(second.closed, 1);
}
TEST_F(ConnectionHostTest, RegistrationFreezesAtStart)
{
    runtime->start();
    EXPECT_THROW(runtime->features().add<Count>(), std::logic_error);
}
TEST_F(
    ConnectionHostTest, CancelledTimerAndClosedHostNeverInvokeFeature
)
{
    int fired = 0;
    runtime->start();
    auto timer = runtime->scheduleAt(
        ConnectionClock::now(),
        [&](auto&) { ++fired; }
    );
    timer.reset();
    timers[0]();
    auto other = runtime->scheduleAt(
        ConnectionClock::now(),
        [&](auto&) { ++fired; }
    );
    runtime->transportClosed();
    timers[1]();
    EXPECT_EQ(fired, 0);
}
TEST_F(ConnectionHostTest, ExceptionsCloseAndOutputReentryIsCoalesced)
{
    struct Reentrant : IConnectionFeature
    {
        ConnectionHost* runtime;
        int calls = 0;
        explicit Reentrant(ConnectionHost* value)
            : runtime(value)
        {
        }
        void onOutputBufferChanged(
            ConnectionChannel&, std::size_t, std::size_t
        ) override
        {
            if (++calls == 1)
                runtime->outputChanged(2);
            else
                throw std::runtime_error("feature");
        }
    };
    auto& feature = runtime->features().add<Reentrant>(runtime.get());
    runtime->start();
    runtime->outputChanged(1);
    EXPECT_EQ(feature.calls, 2);
    EXPECT_FALSE(runtime->connected());
    drain();
    EXPECT_EQ(transport->closes, 1);
}
TEST_F(ConnectionHostTest, CloseNotificationWaitsUntilFeatureReturns)
{
    struct Closing : IConnectionFeature
    {
        ConnectionHost* runtime;
        bool inside = false, notifiedInside = false;
        explicit Closing(ConnectionHost* value)
            : runtime(value)
        {
        }
        void onOutputBufferChanged(
            ConnectionChannel&, std::size_t, std::size_t
        ) override
        {
            inside = true;
            runtime->transportClosed();
            inside = false;
        }
        void onClosed() noexcept override
        {
            notifiedInside = inside;
        }
    };
    auto& feature = runtime->features().add<Closing>(runtime.get());
    runtime->start();
    runtime->outputChanged(1);
    EXPECT_FALSE(feature.notifiedInside);
}
TEST_F(ConnectionHostTest, BackpressureHysteresisCancelsOldTimer)
{
    runtime->features().add<BackpressureFeature>(2, 4);
    runtime->start();
    runtime->outputChanged(6);
    ASSERT_TRUE(transport->paused);
    runtime->outputChanged(3);
    EXPECT_TRUE(transport->paused);
    runtime->setReadPaused(ReadPauseReason::Application, true);
    runtime->outputChanged(2);
    EXPECT_TRUE(transport->paused);
    timers[0]();
    EXPECT_TRUE(runtime->connected());
    runtime->setReadPaused(ReadPauseReason::Application, false);
    EXPECT_FALSE(transport->paused);
    runtime->outputChanged(5);
    timers[1]();
    drain();
    EXPECT_FALSE(runtime->connected());
    EXPECT_EQ(transport->closes, 1);
}
TEST_F(ConnectionHostTest, CloseCancelsBackpressureTimer)
{
    runtime->features().add<BackpressureFeature>(2, 4);
    runtime->start();
    runtime->outputChanged(6);
    runtime->transportClosed();
    timers[0]();
    drain();
    EXPECT_EQ(transport->closes, 0);
}
TEST_F(
    ConnectionHostTest,
    TimerChannelCloseKeepsHostAliveUntilCallbackReturns
)
{
    struct ObserveClose : IConnectionFeature
    {
        ObserveClose(int& count, bool& active, bool& nested)
            : closed(count)
            , inside(active)
            , notifiedInside(nested)
        {
        }
        void onClosed() noexcept override
        {
            ++closed;
            notifiedInside = inside;
        }
        int& closed;
        bool& inside;
        bool& notifiedInside;
    };
    int closed = 0, fired = 0;
    bool inside = false, notifiedInside = false;
    runtime->features()
        .add<ObserveClose>(closed, inside, notifiedInside);
    runtime->start();
    std::weak_ptr<ConnectionHost> observed = runtime;
    auto timer = runtime->scheduleAt(
        ConnectionClock::now(),
        [&](ConnectionChannel& channel)
        {
            inside = true;
            ++fired;
            EXPECT_EQ(channel.context().connectionId, 7U);
            channel.close();
            runtime.reset();
            EXPECT_FALSE(observed.expired());
            EXPECT_EQ(channel.id(), 7U);
            auto post = channel.poster();
            EXPECT_FALSE(post([](ConnectionChannel&) {}));
            EXPECT_EQ(closed, 0);
            inside = false;
        }
    );
    timers.front()();
    EXPECT_TRUE(observed.expired());
    EXPECT_EQ(closed, 1);
    EXPECT_FALSE(notifiedInside);
    drain();
    timers.front()();
    EXPECT_EQ(fired, 1);
    EXPECT_EQ(closed, 1);
}
TEST(BackpressureFeatureTest, RejectsInvalidConfiguration)
{
    EXPECT_THROW(BackpressureFeature(4, 4), std::invalid_argument);
    EXPECT_THROW(
        BackpressureFeature(2, 1024 * 1024),
        std::invalid_argument
    );
    EXPECT_THROW(
        BackpressureFeature(2, 4, ConnectionClock::duration::zero()),
        std::invalid_argument
    );
}
TEST_F(ConnectionHostTest, ForeignCompletionsAreBoundedAndRunOnOwner)
{
    runtime->start();
    auto post = runtime->poster();
    const auto owner = std::this_thread::get_id();
    std::size_t accepted = 0;
    int completed = 0;
    std::thread producer(
        [&]
        {
            for (std::size_t i = 0;
                 i < ConnectionHost::kMaxPostedActions + 1;
                 ++i)
                accepted += post(
                    [&](ConnectionChannel& channel)
                    {
                        EXPECT_EQ(std::this_thread::get_id(), owner);
                        EXPECT_EQ(channel.id(), 7U);
                        ++completed;
                    }
                );
        }
    );
    producer.join();
    EXPECT_EQ(accepted, ConnectionHost::kMaxPostedActions);
    EXPECT_EQ(completed, 0);
    drain();
    EXPECT_EQ(completed, static_cast<int>(accepted));
    std::thread next(
        [&] { EXPECT_TRUE(post([&](auto&) { ++completed; })); }
    );
    next.join();
    drain();
    EXPECT_EQ(completed, static_cast<int>(accepted) + 1);
}
TEST_F(ConnectionHostTest, ClosedMailboxDropsQueuedAndLateCompletion)
{
    runtime->start();
    auto post = runtime->poster();
    int completed = 0;
    std::thread producer(
        [&] { EXPECT_TRUE(post([&](auto&) { ++completed; })); }
    );
    producer.join();
    runtime->transportClosed();
    runtime.reset();
    std::thread late(
        [&] { EXPECT_FALSE(post([&](auto&) { ++completed; })); }
    );
    late.join();
    drain();
    EXPECT_EQ(completed, 0);
}
TEST_F(
    ConnectionHostTest, OwnerCompletionRunsInlineAndExceptionsClose
)
{
    auto post = runtime->poster();
    EXPECT_FALSE(post([](auto&) {}));
    runtime->start();
    EXPECT_FALSE(post({}));
    int completed = 0;
    EXPECT_TRUE(post([&](auto&) { ++completed; }));
    EXPECT_EQ(completed, 1);
    EXPECT_TRUE(post([](auto&)
                     { throw std::runtime_error("reply"); }));
    EXPECT_FALSE(runtime->connected());
    EXPECT_FALSE(post([](auto&) {}));
    drain();
    EXPECT_EQ(transport->closes, 1);
}
TEST_F(ConnectionHostTest, ForeignSendMustUseTheOwnerMailbox)
{
    runtime->start();
    std::thread foreign(
        [&] { EXPECT_THROW(runtime->send({}), std::logic_error); }
    );
    foreign.join();
    EXPECT_TRUE(runtime->connected());
}
TEST_F(ConnectionHostTest, ForeignChannelControlsRejectBeforeMutation)
{
    runtime->start();
    ConnectionChannel& channel = *runtime;
    std::thread foreign(
        [&]
        {
            EXPECT_THROW(channel.close(), std::logic_error);
            EXPECT_THROW(channel.shutdown(), std::logic_error);
            EXPECT_THROW(
                channel.setReadPaused(
                    ReadPauseReason::Application,
                    true
                ),
                std::logic_error
            );
            EXPECT_THROW(
                channel.scheduleAt(
                    ConnectionClock::now(),
                    [](ConnectionChannel&) {}
                ),
                std::logic_error
            );
        }
    );
    foreign.join();
    EXPECT_TRUE(channel.connected());
    EXPECT_TRUE(channel.acceptingInput());
    EXPECT_FALSE(transport->paused);
    EXPECT_TRUE(posted.empty());
    EXPECT_TRUE(timers.empty());
    EXPECT_EQ(transport->shutdowns, 0);
}
TEST(
    ConnectionHostLifecycleTest,
    ProtocolHooksRunOnceAndUnwindBeforeClose
)
{
    auto transport = std::make_shared<Transport>();
    int started = 0, closed = 0;
    bool inside = false, closedInside = false;
    std::shared_ptr<ConnectionHost> host;
    ConnectionHandler handler;
    handler.onStarted = [&](auto&) { ++started; };
    handler.onMessage = [&](auto&, auto)
    {
        inside = true;
        host->transportClosed();
        EXPECT_EQ(closed, 0);
        inside = false;
    };
    handler.onClosed = [&]
    {
        ++closed;
        closedInside = inside;
    };
    host = std::make_shared<ConnectionHost>(
        transport,
        std::make_shared<ConnectionContext>(
            7,
            InetAddress("127.0.0.1", 1),
            ConnectionClock::now()
        ),
        ConnectionScheduler{
            [](auto) {},
            [](auto, auto) { return ConnectionTimer{}; }
        },
        std::move(handler)
    );
    host->start();
    host->start();
    EXPECT_EQ(started, 1);
    host->received({});
    EXPECT_EQ(closed, 1);
    EXPECT_FALSE(closedInside);
    host->transportClosed();
    host.reset();
    EXPECT_EQ(closed, 1);
}
TEST(
    ConnectionHostMailboxTest,
    InlineForeignSchedulerIsRejectedWithoutReentry
)
{
    auto transport = std::make_shared<Transport>();
    auto host = std::make_shared<ConnectionHost>(
        transport,
        std::make_shared<ConnectionContext>(
            7,
            InetAddress("127.0.0.1", 1),
            ConnectionClock::now()
        ),
        ConnectionScheduler{
            [](auto action) { action(); },
            [](auto, auto) { return ConnectionTimer{}; }
        },
        [](auto&, auto) {}
    );
    host->start();
    auto post = host->poster();
    int completed = 0;
    std::thread foreign(
        [&] { EXPECT_FALSE(post([&](auto&) { ++completed; })); }
    );
    foreign.join();
    EXPECT_EQ(completed, 0);
    EXPECT_TRUE(host->connected());
}
TEST(ConnectionAuthLifecycleTest, CloseHandleMayDisconnectInline)
{
    auto context = std::make_shared<ConnectionContext>(
        7,
        InetAddress("127.0.0.1", 1),
        ConnectionClock::now()
    );
    int closed = 0;
    context->session->setCloseRequest(
        [&]
        {
            ++closed;
            context->session->disconnect();
        }
    );
    auto close = context->session->closeRequest();
    close();
    close();
    EXPECT_EQ(closed, 1);
    EXPECT_EQ(
        context->session->loginState(),
        ConnectionSession::LoginState::Expired
    );
}

struct ConnectionHostDrainTest : ::testing::Test
{
    std::shared_ptr<Transport>
        transport = std::make_shared<Transport>();
    std::deque<std::function<void()>> posted;
    std::shared_ptr<ConnectionHost> host;

    void create(ConnectionHandler handler)
    {
        host = makeConnectionHost(
            transport,
            std::make_shared<ConnectionContext>(
                7,
                InetAddress("127.0.0.1", 1),
                ConnectionClock::now()
            ),
            ConnectionScheduler{
                [this](auto action)
                { posted.push_back(std::move(action)); },
                [](auto, auto) { return ConnectionTimer{}; }
            },
            [handler = std::move(handler)](auto) { return handler; },
            [](auto&, const auto&) {}
        );
    }
    void drainOwner()
    {
        while (!posted.empty())
        {
            auto action = std::move(posted.front());
            posted.pop_front();
            action();
        }
    }
};
TEST_F(
    ConnectionHostDrainTest, WaitsForPendingRepliesBeforeWriteShutdown
)
{
    int messages = 0, draining = 0;
    std::function<void()> drained;
    ConnectionHandler handler([&](auto&, auto) { ++messages; });
    handler.onDraining = [&](std::function<void()> done)
    {
        ++draining;
        drained = std::move(done);
    };
    create(std::move(handler));
    host->start();
    host->received({});
    host->setReadPaused(ReadPauseReason::Application, true);
    ASSERT_TRUE(transport->paused);
    host->shutdown();
    EXPECT_EQ(draining, 1);
    EXPECT_FALSE(host->acceptingInput());
    EXPECT_TRUE(host->connected());
    EXPECT_FALSE(transport->paused);
    EXPECT_EQ(transport->shutdowns, 0);
    host->received({});
    EXPECT_EQ(messages, 1);
    host->setReadPaused(ReadPauseReason::Application, true);
    EXPECT_FALSE(transport->paused);
    auto post = host->poster();
    std::thread completion(
        [post]
        {
            EXPECT_TRUE(post([](ConnectionChannel& owner)
                             { owner.send({}); }));
        }
    );
    completion.join();
    drainOwner();
    EXPECT_EQ(transport->sends, 1);
    EXPECT_EQ(transport->shutdowns, 0);
    ASSERT_TRUE(drained);
    drained();
    drained();
    host->shutdown();
    EXPECT_EQ(draining, 1);
    EXPECT_EQ(transport->shutdowns, 1);
    host->send({});
    EXPECT_EQ(transport->sends, 1);
}
TEST_F(
    ConnectionHostDrainTest,
    ForcedCloseMakesRetainedDrainCompletionHarmless
)
{
    std::function<void()> drained;
    int closed = 0;
    ConnectionHandler handler([](auto&, auto) {});
    handler.onDraining = [&](std::function<void()> done)
    { drained = std::move(done); };
    handler.onClosed = [&] { ++closed; };
    create(std::move(handler));
    host->start();
    host->shutdown();
    host->close();
    drainOwner();
    host->transportClosed();
    drained();
    EXPECT_EQ(transport->shutdowns, 0);
    EXPECT_EQ(closed, 1);
    std::weak_ptr<ConnectionHost> observed = host;
    host.reset();
    EXPECT_TRUE(observed.expired());
    drained();
    EXPECT_EQ(closed, 1);
}
TEST_F(
    ConnectionHostDrainTest, SynchronousDrainCloseWaitsForHookToUnwind
)
{
    bool inside = false, closedInside = false;
    int closed = 0;
    ConnectionHandler handler([](auto&, auto) {});
    handler.onDraining = [&](std::function<void()> done)
    {
        inside = true;
        done();
        EXPECT_EQ(closed, 0);
        inside = false;
    };
    handler.onClosed = [&]
    {
        ++closed;
        closedInside = inside;
    };
    create(std::move(handler));
    transport->shutdownAction = [&] { host->transportClosed(); };
    host->start();
    host->shutdown();
    EXPECT_EQ(closed, 1);
    EXPECT_FALSE(closedInside);
    EXPECT_EQ(transport->shutdowns, 1);
}
TEST_F(
    ConnectionHostDrainTest,
    DrainDuringFeatureStartupSkipsProtocolActivation
)
{
    struct DrainOnStart : IConnectionFeature
    {
        void onConnected(ConnectionChannel& channel) override
        {
            channel.shutdown();
        }
    };
    int started = 0;
    ConnectionHandler handler([](auto&, auto) {});
    handler.onStarted = [&](auto&) { ++started; };
    create(std::move(handler));
    host->features().add<DrainOnStart>();
    host->start();
    EXPECT_EQ(started, 0);
    EXPECT_FALSE(host->acceptingInput());
    EXPECT_EQ(transport->shutdowns, 1);
}
TEST_F(
    ConnectionHostDrainTest, ShutdownBeforeStartNeverActivatesProtocol
)
{
    int started = 0;
    ConnectionHandler handler([](auto&, auto) {});
    handler.onStarted = [&](auto&) { ++started; };
    create(std::move(handler));
    host->shutdown();
    host->start();
    EXPECT_EQ(started, 0);
    EXPECT_FALSE(host->acceptingInput());
    EXPECT_EQ(transport->shutdowns, 1);
}
TEST_F(
    ConnectionHostDrainTest,
    ForeignDrainCompletionRejectsBeforeWeakHostAcquisition
)
{
    std::function<void()> drained;
    ConnectionHandler handler([](auto&, auto) {});
    handler.onDraining = [&](std::function<void()> done)
    { drained = std::move(done); };
    create(std::move(handler));
    host->start();
    host->shutdown();
    std::thread foreign(
        [&] { EXPECT_THROW(drained(), std::logic_error); }
    );
    foreign.join();
    EXPECT_EQ(transport->shutdowns, 0);
    drained();
    EXPECT_EQ(transport->shutdowns, 1);
}
TEST_F(ConnectionHostDrainTest, ThrowingDrainHookClosesConnection)
{
    ConnectionHandler handler([](auto&, auto) {});
    handler.onDraining = [](auto)
    { throw std::runtime_error("drain hook"); };
    create(std::move(handler));
    host->start();
    host->shutdown();
    EXPECT_FALSE(host->connected());
    drainOwner();
    EXPECT_EQ(transport->closes, 1);
    EXPECT_EQ(transport->shutdowns, 0);
}
} // namespace
