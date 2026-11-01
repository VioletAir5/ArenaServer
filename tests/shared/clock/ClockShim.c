/** @file
 * @brief Gives selected test children a civil clock that advances
 * monotonically.
 */
#define _GNU_SOURCE

#include <pthread.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <errno.h>
#include <stdint.h>

static pthread_once_t clock_once = PTHREAD_ONCE_INIT;
static struct timespec civil_origin;
static struct timespec monotonic_origin;
static int initialization_error;

static int parse_origin(const char* text, struct timespec* value)
{
    uint64_t total = 0;
    if (text == NULL || *text == '\0')
        return -1;
    for (const char* digit = text; *digit != '\0'; ++digit)
    {
        if (*digit < '0' || *digit > '9' ||
            total > (INT64_MAX - (uint64_t)(*digit - '0')) / 10)
            return -1;
        total = total * 10 + (uint64_t)(*digit - '0');
    }
    value->tv_sec = (time_t)(total / 1000000000ULL);
    value->tv_nsec = (long)(total % 1000000000ULL);
    return (uint64_t)value->tv_sec == total / 1000000000ULL ? 0 : -1;
}

static void initialize_clock(void)
{
    /* A wrapper can share one origin across the test and all
     * descendants. */
    const char* civil = getenv("ARENA_TEST_CLOCK_REALTIME_NS");
    const char* monotonic = getenv("ARENA_TEST_CLOCK_MONOTONIC_NS");
    if (civil != NULL || monotonic != NULL)
    {
        if (parse_origin(civil, &civil_origin) != 0 ||
            parse_origin(monotonic, &monotonic_origin) != 0)
            initialization_error = EINVAL;
        return;
    }
    /* Raw syscalls avoid reentering an interposed clock during
     * pthread_once. */
    if (syscall(
            SYS_clock_gettime,
            CLOCK_MONOTONIC,
            &monotonic_origin
        ) != 0 ||
        syscall(SYS_clock_gettime, CLOCK_REALTIME, &civil_origin) !=
            0)
        initialization_error = errno;
}

static int stable_civil_time(struct timespec* value)
{
    struct timespec now;
    const int once_error = pthread_once(
        &clock_once,
        initialize_clock
    );
    if (once_error != 0 || initialization_error != 0)
    {
        errno = once_error != 0 ? once_error : initialization_error;
        return -1;
    }
    if (syscall(SYS_clock_gettime, CLOCK_MONOTONIC, &now) != 0)
        return -1;
    value->tv_sec = civil_origin.tv_sec + now.tv_sec -
                    monotonic_origin.tv_sec;
    value->tv_nsec = civil_origin.tv_nsec + now.tv_nsec -
                     monotonic_origin.tv_nsec;
    if (value->tv_nsec < 0)
    {
        --value->tv_sec;
        value->tv_nsec += 1000000000L;
    }
    else if (value->tv_nsec >= 1000000000L)
    {
        ++value->tv_sec;
        value->tv_nsec -= 1000000000L;
    }
    return 0;
}

int clock_gettime(clockid_t clock_id, struct timespec* value)
{
    if (clock_id == CLOCK_REALTIME ||
        clock_id == CLOCK_REALTIME_COARSE)
        return stable_civil_time(value);
    return (int)syscall(SYS_clock_gettime, clock_id, value);
}

int gettimeofday(struct timeval* value, void* timezone)
{
    struct timespec now;
    if (stable_civil_time(&now) != 0)
        return -1;
    value->tv_sec = now.tv_sec;
    value->tv_usec = now.tv_nsec / 1000;
    if (timezone != NULL)
        return (int)syscall(SYS_gettimeofday, NULL, timezone);
    return 0;
}

time_t time(time_t* value)
{
    struct timespec now;
    if (stable_civil_time(&now) != 0)
        return (time_t)-1;
    if (value != NULL)
        *value = now.tv_sec;
    return now.tv_sec;
}