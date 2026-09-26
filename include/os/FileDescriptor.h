#pragma once

// my abstract for file decriptor
namespace service
{
class FileDescriptor
{
private:
    int _fd{-1};

public:
    explicit FileDescriptor(int);
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