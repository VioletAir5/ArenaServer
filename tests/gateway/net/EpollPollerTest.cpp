#include "net/EpollPoller.h"

#include <pthread.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "net/Channel.h"
#include "os/FileDescriptor.h"

using service::gateway::Channel;
using service::gateway::EpollPoller;
using service::FileDescriptor;

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

void emptySignalHandler(int)
{
}

class ScopedSignalHandler
{
public:
    explicit ScopedSignalHandler(int signal)
        : signal_(signal)
    {
        struct sigaction action
        {
        };
        action.sa_handler = emptySignalHandler;
        sigemptyset(&action.sa_mask);
        if (::sigaction(signal_, &action, &previous_) < 0)
        {
            throw std::system_error(
                errno,
                std::generic_category(),
                "sigaction"
            );
        }
    }

    ~ScopedSignalHandler()
    {
        ::sigaction(signal_, &previous_, nullptr);
    }

private:
    int signal_;
    struct sigaction previous_
    {
    };
};
} // namespace

TEST(EpollPollerTest, AddedChannelReportsReadableEvent)
{
    FileDescriptor fd = makeEventFd();
    Channel channel(fd.get());
    channel.setEvents(EPOLLIN);
    EpollPoller poller;
    poller.addChannel(&channel);

    uint64_t one = 1;
    ASSERT_EQ(::write(fd.get(), &one, sizeof(one)), sizeof(one));

    std::vector<Channel*> active;
    poller.poll(0, active);

    ASSERT_EQ(active.size(), 1U);
    EXPECT_EQ(active.front(), &channel);
    EXPECT_NE(channel.revents() & EPOLLIN, 0U);
}

TEST(EpollPollerTest, UpdateChangesInterestMask)
{
    FileDescriptor fd = makeEventFd();
    Channel channel(fd.get());
    channel.setEvents(EPOLLIN);
    EpollPoller poller;
    poller.addChannel(&channel);

    std::vector<Channel*> active;
    poller.poll(0, active);
    EXPECT_TRUE(active.empty());

    channel.setEvents(EPOLLOUT);
    poller.updateChannel(&channel);
    poller.poll(0, active);

    ASSERT_EQ(active.size(), 1U);
    EXPECT_EQ(active.front(), &channel);
    EXPECT_NE(channel.revents() & EPOLLOUT, 0U);
}

TEST(EpollPollerTest, RemovedChannelIsNotReported)
{
    FileDescriptor fd = makeEventFd();
    Channel channel(fd.get());
    channel.setEvents(EPOLLIN);
    EpollPoller poller;
    poller.addChannel(&channel);
    poller.removeChannel(&channel);

    uint64_t one = 1;
    ASSERT_EQ(::write(fd.get(), &one, sizeof(one)), sizeof(one));

    std::vector<Channel*> active;
    poller.poll(0, active);
    EXPECT_TRUE(active.empty());
}

TEST(EpollPollerTest, PollClearsPreviousResultsWhenNoEventsAreReady)
{
    FileDescriptor fd = makeEventFd();
    Channel channel(fd.get());
    channel.setEvents(EPOLLIN);
    EpollPoller poller;
    poller.addChannel(&channel);

    uint64_t one = 1;
    ASSERT_EQ(::write(fd.get(), &one, sizeof(one)), sizeof(one));

    std::vector<Channel*> active;
    poller.poll(0, active);
    ASSERT_EQ(active.size(), 1U);

    uint64_t value = 0;
    ASSERT_EQ(::read(fd.get(), &value, sizeof(value)), sizeof(value));
    poller.poll(0, active);
    EXPECT_TRUE(active.empty());
}

TEST(EpollPollerTest, ReportsMoreChannelsThanInitialEventBatch)
{
    constexpr int channelCount = 80;
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

        uint64_t one = 1;
        ASSERT_EQ(
            ::write(fds.back().get(), &one, sizeof(one)),
            sizeof(one)
        );
    }

    std::unordered_set<Channel*> observed;
    std::vector<Channel*> active;
    for (int attempt = 0;
         attempt < 4 && observed.size() < channelCount;
         ++attempt)
    {
        poller.poll(0, active);
        for (Channel* channel : active)
        {
            EXPECT_NE(channel->revents() & EPOLLIN, 0U);
            EXPECT_TRUE(observed.insert(channel).second);
            uint64_t value = 0;
            ASSERT_EQ(
                ::read(channel->fd(), &value, sizeof(value)),
                sizeof(value)
            );
        }
    }

    EXPECT_EQ(observed.size(), channelCount);
}

TEST(EpollPollerTest, InvalidDescriptorIsRejected)
{
    Channel channel(-1);
    channel.setEvents(EPOLLIN);
    EpollPoller poller;

    EXPECT_THROW(poller.addChannel(&channel), std::system_error);
}

TEST(EpollPollerTest, InterruptedWaitClearsPreviousResults)
{
    ScopedSignalHandler handler(SIGUSR1);
    const pthread_t waitingThread = pthread_self();
    std::thread interrupter(
        [waitingThread]
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(20)
            );
            ::pthread_kill(waitingThread, SIGUSR1);
        }
    );

    EpollPoller poller;
    Channel prior(42);
    std::vector<Channel*> active{&prior};
    poller.poll(500, active);
    interrupter.join();

    EXPECT_TRUE(active.empty());
}

TEST(EpollPollerTest, RejectsAllOperationsFromForeignThread)
{
    FileDescriptor registeredFd = makeEventFd();
    FileDescriptor newFd = makeEventFd();
    Channel registered(registeredFd.get());
    Channel additional(newFd.get());
    registered.setEvents(EPOLLIN);
    additional.setEvents(EPOLLIN);
    EpollPoller poller;
    poller.addChannel(&registered);

    std::array<bool, 4> rejected{};
    std::thread other([&]
    {
        const auto rejects = [](auto operation)
        {
            try
            {
                operation();
            }
            catch (const std::logic_error&)
            {
                return true;
            }
            catch (...)
            {
            }
            return false;
        };
        std::vector<Channel*> active;
        rejected[0] = rejects([&] { poller.addChannel(&additional); });
        rejected[1] = rejects([&] { poller.updateChannel(&registered); });
        rejected[2] = rejects([&] { poller.removeChannel(&registered); });
        rejected[3] = rejects([&] { poller.poll(0, active); });
    });
    other.join();

    EXPECT_EQ(rejected, (std::array<bool, 4>{true, true, true, true}));
    poller.addChannel(&additional);
    poller.removeChannel(&additional);
    poller.removeChannel(&registered);
}
