#include <gtest/gtest.h>

#include "net/PacketBuffer.h"

#include <cstring>
#include <string>

using service::PacketBuffer;

TEST(PacketBufferTest, InitialState)
{
    PacketBuffer buffer;

    EXPECT_EQ(buffer.readableBytes(), 0);
    EXPECT_GT(buffer.writableBytes(), 0);
}

TEST(PacketBufferTest, AppendData)
{
    PacketBuffer buffer;

    const char data[] = "hello";

    EXPECT_TRUE(buffer.append(data, 5));

    EXPECT_EQ(buffer.readableBytes(), 5);

    EXPECT_EQ(
        std::memcmp(buffer.readPtr(), data, 5),
        0
    );
}

TEST(PacketBufferTest, SkipData)
{
    PacketBuffer buffer;

    const char data[] = "hello";

    ASSERT_TRUE(buffer.append(data, 5));

    EXPECT_TRUE(buffer.skip(2));

    EXPECT_EQ(buffer.readableBytes(), 3);

    EXPECT_EQ(
        std::memcmp(buffer.readPtr(), "llo", 3),
        0
    );
}

TEST(PacketBufferTest, SkipAllDataResetsBuffer)
{
    PacketBuffer buffer;

    const char data[] = "hello";

    ASSERT_TRUE(buffer.append(data, 5));
    ASSERT_TRUE(buffer.skip(5));

    EXPECT_EQ(buffer.readableBytes(), 0);

    // 全部消费后应该可以从头重新写
    const char newData[] = "abc";

    ASSERT_TRUE(buffer.append(newData, 3));

    EXPECT_EQ(buffer.readableBytes(), 3);

    EXPECT_EQ(
        std::memcmp(buffer.readPtr(), newData, 3),
        0
    );
}

TEST(PacketBufferTest, SkipTooMuchFails)
{
    PacketBuffer buffer;

    const char data[] = "hello";

    ASSERT_TRUE(buffer.append(data, 5));

    EXPECT_FALSE(buffer.skip(6));

    // 状态不能被修改
    EXPECT_EQ(buffer.readableBytes(), 5);

    EXPECT_EQ(
        std::memcmp(buffer.readPtr(), data, 5),
        0
    );
}

TEST(PacketBufferTest, NullptrWithNonZeroLengthFails)
{
    PacketBuffer buffer;

    EXPECT_FALSE(buffer.append(nullptr, 10));

    EXPECT_EQ(buffer.readableBytes(), 0);
}

TEST(PacketBufferTest, NullptrWithZeroLengthIsAllowed)
{
    PacketBuffer buffer;

    EXPECT_TRUE(buffer.append(nullptr, 0));

    EXPECT_EQ(buffer.readableBytes(), 0);
}

TEST(PacketBufferTest, ResizeWhenCapacityIsInsufficient)
{
    PacketBuffer buffer(4, 64);

    const char data[] = "abcdefghij";

    ASSERT_TRUE(buffer.append(data, 10));

    EXPECT_EQ(buffer.readableBytes(), 10);

    EXPECT_EQ(
        std::memcmp(buffer.readPtr(), data, 10),
        0
    );
}

TEST(PacketBufferTest, AppendFailsWhenExceedingMaxCapacity)
{
    PacketBuffer buffer(4, 8);

    const char first[] = "1234";
    const char second[] = "56789";

    ASSERT_TRUE(buffer.append(first, 4));

    EXPECT_FALSE(buffer.append(second, 5));

    // 原来的数据必须还在
    EXPECT_EQ(buffer.readableBytes(), 4);

    EXPECT_EQ(
        std::memcmp(buffer.readPtr(), first, 4),
        0
    );
}

TEST(PacketBufferTest, CompactConsumedSpaceBeforeResize)
{
    PacketBuffer buffer(8, 8);

    const char first[] = "abcdef";

    ASSERT_TRUE(buffer.append(first, 6));

    /*
        当前：

        [ a b c d e f _ _ ]
          ^           ^
         read        write
    */

    ASSERT_TRUE(buffer.skip(4));

    /*
        当前逻辑：

        [ x x x x e f _ _ ]
                ^   ^
               read write

        readable = "ef"
        writable = 2

        接下来写 4 字节，尾部空间不够，
        但前面的 consumed 空间足够，所以应该 compact。
    */

    const char second[] = "ghij";

    ASSERT_TRUE(buffer.append(second, 4));

    EXPECT_EQ(buffer.readableBytes(), 6);

    EXPECT_EQ(
        std::memcmp(
            buffer.readPtr(),
            "efghij",
            6
        ),
        0
    );
}

TEST(PacketBufferTest, MultipleAppendMaintainsOrder)
{
    PacketBuffer buffer(4, 64);

    const char first[] = "abc";
    const char second[] = "def";
    const char third[] = "ghi";

    ASSERT_TRUE(buffer.append(first, 3));
    ASSERT_TRUE(buffer.append(second, 3));
    ASSERT_TRUE(buffer.append(third, 3));

    EXPECT_EQ(buffer.readableBytes(), 9);

    EXPECT_EQ(
        std::memcmp(
            buffer.readPtr(),
            "abcdefghi",
            9
        ),
        0
    );
}

TEST(PacketBufferTest, AppendAfterPartialSkip)
{
    PacketBuffer buffer(8, 32);

    ASSERT_TRUE(buffer.append("abcdef", 6));
    ASSERT_TRUE(buffer.skip(3));

    ASSERT_TRUE(buffer.append("ghij", 4));

    EXPECT_EQ(buffer.readableBytes(), 7);

    EXPECT_EQ(
        std::memcmp(
            buffer.readPtr(),
            "defghij",
            7
        ),
        0
    );
}