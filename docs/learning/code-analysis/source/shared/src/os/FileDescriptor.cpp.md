# shared/src/os/FileDescriptor.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；48行。

[对应分析](../../../../01-shared.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Owns a Linux file descriptor through RAII.
 */
#include <unistd.h>

#include <os/FileDescriptor.h>

namespace service
{

FileDescriptor::FileDescriptor(int fd)
    : _fd(fd)
{
}

FileDescriptor::~FileDescriptor()
{
    reset();
}

FileDescriptor& FileDescriptor::operator=(FileDescriptor&& other) noexcept
{
    if (&other == this)
    {
        return *this;
    }
    reset();
    this->_fd = other._fd;
    other._fd = -1;
    return *this;
}

FileDescriptor::FileDescriptor(FileDescriptor&& other) noexcept
    : _fd(other._fd)
{
    other._fd = -1;
}

void FileDescriptor::reset()
{
    if (_fd >= 0)
    {
        close(_fd);
    }
    _fd = -1;
}

} // namespace service
~~~
