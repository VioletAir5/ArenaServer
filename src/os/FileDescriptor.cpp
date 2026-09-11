#include<os/FileDescriptor.h>
#include <unistd.h>

namespace service
{

    FileDescriptor::FileDescriptor(int fd): _fd(fd) 
    {

    }

    FileDescriptor::~FileDescriptor() 
    {
        reset();
    }

    FileDescriptor& FileDescriptor::operator=(FileDescriptor&& other) noexcept
    {
        if (&other == this) {
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

    void FileDescriptor::reset() {
        if(_fd >= 0) {
            close(_fd);
        }
        _fd = -1;
    }

} // namespace service::file