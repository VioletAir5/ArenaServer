#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <system_error>
#include <vector>

#include <gtest/gtest.h>

#include "net/Channel.h"
#include "net/EpollPoller.h"
#include "os/FileDescriptor.h"

using service::FileDescriptor;
using service::gateway::Channel;
using service::gateway::EpollPoller;

namespace
{
FileDescriptor makeEventFd()
{
    int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0)
    {
        throw std::system_error(
            errno,
            std::generic_category(),
            "eventfd"
        );
    }
    return FileDescriptor(fd);
}
} // namespace

TEST(EpollPollerPerformanceTest, ReadyEventThroughput)
{
    constexpr int channelCount = 128;
    constexpr int rounds = 10000;
    EpollPoller poller;
    std::vector<FileDescriptor> fds;
    std::vector<std::unique_ptr<Channel>> channels;
    fds.reserve(channelCount);
    channels.reserve(channelCount);

    for (int i = 0; i < channelCount; ++i)
    {
        fds.push_back(makeEventFd());
        auto channel = std::make_unique<Channel>(fds.back().get());
        channel->setEvents(EPOLLIN);
        poller.addChannel(channel.get());
        channels.push_back(std::move(channel));
    }

    std::vector<Channel*> active;
    const auto start = std::chrono::steady_clock::now();
    for (int round = 0; round < rounds; ++round)
    {
        for (const auto& fd : fds)
        {
            uint64_t one = 1;
            ASSERT_EQ(
                ::write(fd.get(), &one, sizeof(one)),
                sizeof(one)
            );
        }

        int received = 0;
        while (received < channelCount)
        {
            poller.poll(0, active);
            ASSERT_FALSE(active.empty());
            for (Channel* channel : active)
            {
                ASSERT_NE(channel->revents() & EPOLLIN, 0U);
                uint64_t value = 0;
                ASSERT_EQ(
                    ::read(channel->fd(), &value, sizeof(value)),
                    sizeof(value)
                );
                EXPECT_EQ(value, 1U);
                ++received;
            }
        }
        ASSERT_EQ(received, channelCount);
    }
    const auto elapsed = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - start
    )
                             .count();

    const auto totalEvents = static_cast<std::uint64_t>(
                                 channelCount
                             ) *
                             rounds;
    std::cout << "\n[EpollPoller ready-event throughput]\n"
              << "Channels: " << channelCount << "\n"
              << "Events: " << totalEvents << "\n"
              << "Elapsed: " << elapsed << " s\n"
              << "Throughput: "
              << static_cast<std::uint64_t>(totalEvents / elapsed)
              << " events/s\n";
}

TEST(EpollPollerPerformanceTest, RegistrationChurn)
{
    constexpr int rounds = 200000;
    FileDescriptor fd = makeEventFd();
    Channel channel(fd.get());
    channel.setEvents(EPOLLIN);
    EpollPoller poller;

    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < rounds; ++i)
    {
        poller.addChannel(&channel);
        poller.removeChannel(&channel);
    }
    const auto elapsed = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - start
    )
                             .count();

    const auto operations = static_cast<std::uint64_t>(rounds) * 2;
    std::cout << "\n[EpollPoller registration churn]\n"
              << "Add/remove pairs: " << rounds << "\n"
              << "Elapsed: " << elapsed << " s\n"
              << "Throughput: "
              << static_cast<std::uint64_t>(operations / elapsed)
              << " operations/s\n";
}
