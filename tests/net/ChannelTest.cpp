#include "net/Channel.h"

#include <sys/epoll.h>

#include <gtest/gtest.h>

using service::Channel;

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
