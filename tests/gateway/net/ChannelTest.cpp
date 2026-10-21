#include "net/Channel.h"

#include <sys/epoll.h>

#include <memory>
#include <stdexcept>

#include <gtest/gtest.h>

using service::gateway::Channel;

TEST(ChannelTest, StoresDescriptorAndInterestMasks)
{
    Channel channel(42);
    channel.setEvents(EPOLLIN | EPOLLRDHUP);
    channel.setRevents(EPOLLIN);

    EXPECT_EQ(channel.fd(), 42);
    EXPECT_EQ(
        channel.events(),
        static_cast<uint32_t>(EPOLLIN | EPOLLRDHUP)
    );
    EXPECT_EQ(channel.revents(), static_cast<uint32_t>(EPOLLIN));
}

TEST(ChannelTest, DispatchesOnlyMatchingCallbacks)
{
    Channel channel(42);
    int reads = 0;
    int writes = 0;
    int closes = 0;
    int errors = 0;

    channel.setReadCallback([&] { ++reads; });
    channel.setWriteCallback([&] { ++writes; });
    channel.setCloseCallback([&] { ++closes; });
    channel.setErrorCallback([&] { ++errors; });

    channel.setRevents(EPOLLIN);
    channel.handleEvent();

    EXPECT_EQ(reads, 1);
    EXPECT_EQ(writes, 0);
    EXPECT_EQ(closes, 0);
    EXPECT_EQ(errors, 0);

    channel.setRevents(EPOLLOUT | EPOLLRDHUP | EPOLLERR);
    channel.handleEvent();

    EXPECT_EQ(reads, 1);
    EXPECT_EQ(writes, 1);
    EXPECT_EQ(closes, 1);
    EXPECT_EQ(errors, 1);
}

TEST(ChannelTest, HupDispatchesErrorCallback)
{
    Channel channel(42);
    int errors = 0;
    channel.setErrorCallback([&] { ++errors; });
    channel.setRevents(EPOLLHUP);

    channel.handleEvent();

    EXPECT_EQ(errors, 1);
}

TEST(ChannelTest, MissingCallbacksAreSafe)
{
    Channel channel(42);
    channel.setRevents(EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLRDHUP);

    EXPECT_NO_THROW(channel.handleEvent());
}

TEST(ChannelTest, InterestHelpersPreserveMasksAndNotifyOnlyOnChange)
{
    Channel channel(42);
    channel.enableReading();
    EXPECT_EQ(channel.events(), static_cast<uint32_t>(EPOLLIN));

    int updates = 0;
    channel.setEventLoopCallback([&] { ++updates; });

    channel.enablePeerClose();
    channel.enableEdgeTriggered();
    channel.enableWriting();
    channel.enableWriting();

    EXPECT_EQ(updates, 3);
    EXPECT_TRUE(channel.isWriting());
    EXPECT_EQ(
        channel.events(),
        static_cast<uint32_t>(
            EPOLLIN | EPOLLRDHUP | EPOLLET | EPOLLOUT
        )
    );

    channel.disableWriting();
    EXPECT_FALSE(channel.isWriting());
    EXPECT_EQ(updates, 4);
    EXPECT_NE(channel.events() & EPOLLIN, 0U);
    EXPECT_NE(channel.events() & EPOLLRDHUP, 0U);
}

TEST(ChannelTest, FailedUpdateRestoresInterestMask)
{
    Channel channel(42);
    channel.enableReading();
    channel.setEventLoopCallback(
        [] { throw std::runtime_error("update failed"); }
    );

    EXPECT_THROW(channel.enableWriting(), std::runtime_error);
    EXPECT_EQ(channel.events(), static_cast<uint32_t>(EPOLLIN));
    EXPECT_FALSE(channel.isWriting());
}

TEST(ChannelTest, TiedOwnerSurvivesAllCallbacksInOneEvent)
{
    std::shared_ptr<Channel> channel = std::make_shared<Channel>(42);
    std::weak_ptr<Channel> weak = channel;
    channel->tie(channel);

    int reads = 0;
    channel->setErrorCallback([&]() { channel.reset(); });
    channel->setReadCallback(
        [&]()
        {
            EXPECT_FALSE(weak.expired());
            ++reads;
        }
    );
    channel->setRevents(EPOLLERR | EPOLLIN);
    channel->handleEvent();

    EXPECT_EQ(reads, 1);
    EXPECT_TRUE(weak.expired());
}

TEST(ChannelTest, ExpiredTieSuppressesEventCallbacks)
{
    Channel channel(42);
    auto owner = std::make_shared<int>(1);
    channel.tie(owner);
    owner.reset();

    int callbacks = 0;
    channel.setReadCallback([&]() { ++callbacks; });
    channel.setRevents(EPOLLIN);
    channel.handleEvent();

    EXPECT_EQ(callbacks, 0);
}
