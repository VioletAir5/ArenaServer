# shared/include/os/FileDescriptor.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；44行。

[对应分析](../../../../01-shared.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Owns a Linux file descriptor through RAII.
 */
#pragma once

// my abstract for file decriptor
namespace service
{
/** @brief Owns a Linux file descriptor through RAII. */
class FileDescriptor
{
private:
    int _fd{-1};

public:
    /** @brief Constructs the file descriptor. */
    explicit FileDescriptor(int);
    /** @brief Constructs the file descriptor. */
    FileDescriptor() = default;
    /** @brief Releases owned resources. */
    ~FileDescriptor();

    // forbid copying
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    // support move construct
    FileDescriptor(FileDescriptor&&) noexcept;
    FileDescriptor& operator=(FileDescriptor&&) noexcept;

    int get() const noexcept
    {
        return _fd;
    };
    bool valid() const noexcept
    {
        return _fd >= 0;
    };

private:
    // release source
    void reset();
};
} // namespace service
~~~
