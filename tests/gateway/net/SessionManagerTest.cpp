#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>
#include <utility>

#include "net/SessionManager.h"
#include "net/Socket.h"
#include "os/FileDescriptor.h"

using service::gateway::Session;
using service::gateway::SessionID;
using service::gateway::SessionManager;
using service::gateway::Socket;
using service::FileDescriptor;

namespace {

bool isFdOpen(int fd)
{
    return ::fcntl(fd, F_GETFD) != -1;
}

std::unique_ptr<Session> makeSession(SessionID id, int fd)
{
    FileDescriptor descriptor(fd);
    Socket socket(std::move(descriptor));

    return std::make_unique<Session>(
        id,
        std::move(socket)
    );
}

} // namespace


// 1. 添加 Session 后可以查找到
TEST(SessionManagerTest, AddAndFindSession)
{
    int pipeFds[2];
    ASSERT_EQ(::pipe(pipeFds), 0);

    SessionManager manager;

    constexpr SessionID id = 1;

    EXPECT_TRUE(
        manager.addSession(
            makeSession(id, pipeFds[0])
        )
    );

    const Session* session = manager.findSession(id);

    ASSERT_NE(session, nullptr);
    EXPECT_EQ(session->id(), id);

    ::close(pipeFds[1]);
}


// 2. 不存在的 Session 返回 nullptr
TEST(SessionManagerTest, FindMissingSessionReturnsNullptr)
{
    SessionManager manager;

    const Session* session = manager.findSession(999);

    EXPECT_EQ(session, nullptr);
}


// 3. 重复 SessionID 不应该覆盖原 Session
TEST(SessionManagerTest, DuplicateSessionIdIsRejected)
{
    int pipeA[2];
    int pipeB[2];

    ASSERT_EQ(::pipe(pipeA), 0);
    ASSERT_EQ(::pipe(pipeB), 0);

    SessionManager manager;

    constexpr SessionID id = 10;

    EXPECT_TRUE(
        manager.addSession(
            makeSession(id, pipeA[0])
        )
    );

    EXPECT_FALSE(
        manager.addSession(
            makeSession(id, pipeB[0])
        )
    );

    const Session* session = manager.findSession(id);

    ASSERT_NE(session, nullptr);
    EXPECT_EQ(session->id(), id);

    // 第二个 Session 插入失败后应该立即析构，
    // 它持有的 fd 应该被自动关闭。
    EXPECT_FALSE(isFdOpen(pipeB[0]));

    ::close(pipeA[1]);
    ::close(pipeB[1]);
}


// 4. removeSession 后应该找不到 Session
TEST(SessionManagerTest, RemoveSession)
{
    int pipeFds[2];
    ASSERT_EQ(::pipe(pipeFds), 0);

    constexpr SessionID id = 20;

    SessionManager manager;

    ASSERT_TRUE(
        manager.addSession(
            makeSession(id, pipeFds[0])
        )
    );

    ASSERT_NE(manager.findSession(id), nullptr);

    EXPECT_TRUE(manager.removeSession(id));

    EXPECT_EQ(manager.findSession(id), nullptr);

    ::close(pipeFds[1]);
}


// 5. 删除 Session 时应该释放底层 fd
TEST(SessionManagerTest, RemoveSessionClosesDescriptor)
{
    int pipeFds[2];
    ASSERT_EQ(::pipe(pipeFds), 0);

    const int rawFd = pipeFds[0];

    SessionManager manager;

    ASSERT_TRUE(
        manager.addSession(
            makeSession(30, rawFd)
        )
    );

    ASSERT_TRUE(isFdOpen(rawFd));

    manager.removeSession(30);

    EXPECT_FALSE(isFdOpen(rawFd));

    ::close(pipeFds[1]);
}


// 6. SessionManager 析构时应该释放所有 Session
TEST(SessionManagerTest, DestructorReleasesAllSessions)
{
    int pipeA[2];
    int pipeB[2];

    ASSERT_EQ(::pipe(pipeA), 0);
    ASSERT_EQ(::pipe(pipeB), 0);

    const int fdA = pipeA[0];
    const int fdB = pipeB[0];

    {
        SessionManager manager;

        ASSERT_TRUE(
            manager.addSession(
                makeSession(100, fdA)
            )
        );

        ASSERT_TRUE(
            manager.addSession(
                makeSession(200, fdB)
            )
        );

        EXPECT_TRUE(isFdOpen(fdA));
        EXPECT_TRUE(isFdOpen(fdB));
    }

    // manager 析构
    // -> unique_ptr<Session> 析构
    // -> Session 析构
    // -> Socket 析构
    // -> FileDescriptor 析构
    // -> close(fd)

    EXPECT_FALSE(isFdOpen(fdA));
    EXPECT_FALSE(isFdOpen(fdB));

    ::close(pipeA[1]);
    ::close(pipeB[1]);
}
