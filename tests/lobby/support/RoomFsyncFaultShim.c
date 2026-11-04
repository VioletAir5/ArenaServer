/* Test-only pre-fdatasync gate. It never fabricates a successful
 * sync. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <errno.h>
#include <stdint.h>

int fdatasync(int fd)
{
    void* symbol = dlsym(RTLD_NEXT, "fdatasync");
    int (*next_sync)(int) = NULL;
    _Static_assert(
        sizeof(next_sync) == sizeof(symbol),
        "POSIX function pointer representation"
    );
    memcpy(&next_sync, &symbol, sizeof(next_sync));
    const char* wal = getenv("ARENA_TEST_FSYNC_WAL");
    const char* gate = getenv("ARENA_TEST_FSYNC_SOCKET");
    char link[64], path[4096];
    struct stat info;
    if (!next_sync)
    {
        errno = EIO;
        return -1;
    }
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t length = readlink(link, path, sizeof(path) - 1);
    if (!wal || !gate || length < 0)
        return next_sync(fd);
    path[length] = 0;
    if (strcmp(path, wal) || fstat(fd, &info) ||
        !S_ISREG(info.st_mode))
        return next_sync(fd);
    if (strlen(gate) >= sizeof(((struct sockaddr_un*)0)->sun_path))
    {
        errno = EINVAL;
        return -1;
    }
    int peer = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (peer < 0)
        return -1;
    struct sockaddr_un address = {.sun_family = AF_UNIX};
    memcpy(address.sun_path, gate, strlen(gate) + 1);
    if (connect(peer, (struct sockaddr*)&address, sizeof(address)))
    {
        int saved = errno;
        close(peer);
        if (saved == ENOENT || saved == ECONNREFUSED)
            return next_sync(fd);
        errno = saved;
        return -1;
    }
    uint64_t size = (uint64_t)info.st_size;
    unsigned char request[8];
    for (int i = 0; i < 8; ++i)
        request[7 - i] = (unsigned char)(size >> (i * 8));
    size_t offset = 0;
    while (offset < sizeof(request))
    {
        ssize_t count = send(
            peer,
            request + offset,
            sizeof(request) - offset,
            MSG_NOSIGNAL
        );
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0)
        {
            close(peer);
            errno = EIO;
            return -1;
        }
        offset += (size_t)count;
    }
    struct pollfd wait = {.fd = peer, .events = POLLIN};
    int ready;
    do
    {
        ready = poll(&wait, 1, 15000);
    } while (ready < 0 && errno == EINTR);
    unsigned char answer = 0;
    ssize_t count = ready > 0 ? recv(peer, &answer, 1, 0) : -1;
    close(peer);
    if (count != 1 || answer != 'R')
    {
        errno = EIO;
        return -1;
    }
    return next_sync(fd);
}