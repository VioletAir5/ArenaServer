#include <gtest/gtest.h>

#include <cerrno>
#include <fcntl.h>
#include <type_traits>
#include <unistd.h>

#include <os/FileDescriptor.h>

using service::FileDescriptor;

namespace {

bool isFdOpen(int fd)
{
    errno = 0;

    if (fcntl(fd, F_GETFD) != -1) {
        return true;
    }

    return errno != EBADF;
}

} // namespace


// 1. 验证 FileDescriptor 不可复制、可以移动
TEST(FileDescriptorTest, CopyDisabledMoveEnabled)
{
    static_assert(
        !std::is_copy_constructible_v<FileDescriptor>);

    static_assert(
        !std::is_copy_assignable_v<FileDescriptor>);

    static_assert(
        std::is_move_constructible_v<FileDescriptor>);

    static_assert(
        std::is_move_assignable_v<FileDescriptor>);

    SUCCEED();
}


// 2. 无效 fd 不应该导致异常行为
TEST(FileDescriptorTest, InvalidDescriptor)
{
    FileDescriptor fd{-1};

    EXPECT_FALSE(fd.valid());
    EXPECT_EQ(fd.get(), -1);
}


// 3. 析构函数应该自动 close fd
TEST(FileDescriptorTest, DestructorClosesDescriptor)
{
    int pipeFds[2];

    ASSERT_EQ(pipe(pipeFds), 0);

    const int readFd = pipeFds[0];

    {
        FileDescriptor fd{readFd};

        EXPECT_TRUE(fd.valid());
        EXPECT_TRUE(isFdOpen(readFd));
    }

    // FileDescriptor 已析构
    EXPECT_FALSE(isFdOpen(readFd));

    // 另外一个 fd 没有交给 RAII，需要测试自己清理
    close(pipeFds[1]);
}


// 4. 移动构造应该转移所有权
TEST(FileDescriptorTest, MoveConstructorTransfersOwnership)
{
    int pipeFds[2];

    ASSERT_EQ(pipe(pipeFds), 0);

    const int rawFd = pipeFds[0];

    {
        FileDescriptor source{rawFd};

        ASSERT_TRUE(source.valid());

        FileDescriptor destination{std::move(source)};

        // moved-from 对象必须失效
        EXPECT_FALSE(source.valid());
        EXPECT_EQ(source.get(), -1);

        // 新对象接管原 fd
        EXPECT_TRUE(destination.valid());
        EXPECT_EQ(destination.get(), rawFd);

        EXPECT_TRUE(isFdOpen(rawFd));
    }

    // destination 析构后 fd 被 close
    EXPECT_FALSE(isFdOpen(rawFd));

    close(pipeFds[1]);
}


// 5. 移动赋值需要释放旧资源，再接管新资源
TEST(FileDescriptorTest, MoveAssignmentReleasesOldDescriptor)
{
    int pipeA[2];
    int pipeB[2];

    ASSERT_EQ(pipe(pipeA), 0);
    ASSERT_EQ(pipe(pipeB), 0);

    const int oldFd = pipeA[0];
    const int newFd = pipeB[0];

    {
        FileDescriptor destination{oldFd};
        FileDescriptor source{newFd};

        ASSERT_TRUE(isFdOpen(oldFd));
        ASSERT_TRUE(isFdOpen(newFd));

        destination = std::move(source);

        // destination 原本持有的 oldFd 必须被关闭
        EXPECT_FALSE(isFdOpen(oldFd));

        // source 已经变成 moved-from 状态
        EXPECT_FALSE(source.valid());
        EXPECT_EQ(source.get(), -1);

        // destination 接管 newFd
        EXPECT_TRUE(destination.valid());
        EXPECT_EQ(destination.get(), newFd);
        EXPECT_TRUE(isFdOpen(newFd));
    }

    // destination 析构后 newFd 也应该关闭
    EXPECT_FALSE(isFdOpen(newFd));

    close(pipeA[1]);
    close(pipeB[1]);
}


// 6. 自移动赋值不能把自己的 fd 关闭
TEST(FileDescriptorTest, SelfMoveAssignmentIsSafe)
{
    int pipeFds[2];

    ASSERT_EQ(pipe(pipeFds), 0);

    const int rawFd = pipeFds[0];

    {
        FileDescriptor fd{rawFd};

        ASSERT_TRUE(fd.valid());

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wself-move"

        fd = std::move(fd);

#pragma GCC diagnostic pop

        EXPECT_TRUE(fd.valid());
        EXPECT_EQ(fd.get(), rawFd);
        EXPECT_TRUE(isFdOpen(rawFd));
    }

    EXPECT_FALSE(isFdOpen(rawFd));

    close(pipeFds[1]);
}