#pragma once

#include <functional>
#include <memory>

#include <os/FileDescriptor.h>

namespace service
{

class Channel
{
    using CallbackFunction = std::function<void()>;

private:
    int m_fd;

public:
    Channel(int fd);
    ~Channel();
};

} // namespace service
