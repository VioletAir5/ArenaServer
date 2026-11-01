/** @file
 * @brief Writes checksummed authority batches on one disk owner and
 * recovers CAS state.
 */
#include "lobby/DurableStateStore.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/crc.hpp>

#include "os/FileDescriptor.h"

namespace service::lobby
{
namespace
{
constexpr std::size_t fileHeaderBytes = 16;
constexpr std::size_t frameHeaderBytes = 24;
constexpr std::string_view fileMagic = "ARLOBBY1";
constexpr std::string_view frameMagic = "KVTX";

/** @brief Uses explicit big-endian integers, independent of host ABI.
 */
void put32(std::string& data, std::uint32_t value)
{
    for (int shift = 24; shift >= 0; shift -= 8)
        data.push_back(static_cast<char>((value >> shift) & 255));
}

void put64(std::string& data, std::uint64_t value)
{
    put32(data, static_cast<std::uint32_t>(value >> 32));
    put32(data, static_cast<std::uint32_t>(value));
}

std::uint32_t get32(std::string_view data)
{
    std::uint32_t result = 0;
    for (const unsigned char value : data.substr(0, 4))
        result = (result << 8) | value;
    return result;
}

std::uint64_t get64(std::string_view data)
{
    return (std::uint64_t(get32(data)) << 32) | get32(data.substr(4));
}

std::uint32_t checksum(std::string_view data)
{
    boost::crc_32_type crc;
    crc.process_bytes(data.data(), data.size());
    return crc.checksum();
}

/** @brief Retries interrupted syscalls without treating a partial
 * write as rollback. */
bool writeAll(
    int descriptor, std::string_view data, std::size_t offset
)
{
    while (!data.empty())
    {
        const auto count = ::pwrite(
            descriptor,
            data.data(),
            data.size(),
            static_cast<off_t>(offset)
        );
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0)
            return false;
        offset += static_cast<std::size_t>(count);
        data.remove_prefix(static_cast<std::size_t>(count));
    }
    return true;
}

bool readAll(int descriptor, std::string& data, std::size_t offset)
{
    std::size_t read = 0;
    while (read < data.size())
    {
        const auto count = ::pread(
            descriptor,
            data.data() + read,
            data.size() - read,
            static_cast<off_t>(offset + read)
        );
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0)
            return false;
        read += static_cast<std::size_t>(count);
    }
    return true;
}

bool syncFile(int descriptor)
{
    for (;;)
    {
        if (::fdatasync(descriptor) == 0)
            return true;
        if (errno != EINTR)
            return false;
    }
}

bool syncDirectory(int descriptor)
{
    for (;;)
    {
        if (::fsync(descriptor) == 0)
            return true;
        if (errno != EINTR)
            return false;
    }
}

[[noreturn]] void invalidJournal()
{
    throw std::runtime_error(
        "invalid or unavailable authority journal"
    );
}

/** @brief Rejects special, linked or externally writable files before
 * touching bytes. */
struct stat inspect(int descriptor)
{
    struct stat result{};
    if (descriptor < 0 || ::fstat(descriptor, &result) != 0 ||
        !S_ISREG(result.st_mode) || result.st_nlink != 1 ||
        result.st_uid != ::geteuid() || (result.st_mode & 0077))
        invalidJournal();
    return result;
}

std::string header()
{
    std::string result(fileMagic);
    put32(result, 1);
    put32(result, checksum(result));
    return result;
}

/** @brief Canonical payloads reject duplicate keys, ambiguous tags
 * and trailing data. */
std::optional<std::vector<DurableStateStore::Mutation>>
decode(std::string_view payload)
{
    if (payload.size() < 4)
        return std::nullopt;
    const auto count = get32(payload);
    payload.remove_prefix(4);
    if (!count || count > 65536 || count > payload.size() / 10)
        return std::nullopt;
    std::vector<DurableStateStore::Mutation> result;
    result.reserve(count);
    std::set<std::string> seen;
    for (std::uint32_t index = 0; index < count; ++index)
    {
        if (payload.size() < 9)
            return std::nullopt;
        const auto tag = static_cast<unsigned char>(payload[0]);
        const auto keyBytes = get32(payload.substr(1));
        const auto valueBytes = get32(payload.substr(5));
        payload.remove_prefix(9);
        if (tag > 1 || !keyBytes || keyBytes > 256 ||
            (tag == 0 && valueBytes) || keyBytes > payload.size() ||
            valueBytes > payload.size() - keyBytes)
            return std::nullopt;
        std::string key(payload.substr(0, keyBytes));
        payload.remove_prefix(keyBytes);
        if (!seen.insert(key).second)
            return std::nullopt;
        std::optional<std::string> value;
        if (tag)
            value.emplace(payload.substr(0, valueBytes));
        payload.remove_prefix(valueBytes);
        result.push_back({std::move(key), std::move(value)});
    }
    if (!payload.empty())
        return std::nullopt;
    return result;
}

std::string
encode(const std::vector<DurableStateStore::Mutation>& mutations)
{
    std::string result;
    put32(result, static_cast<std::uint32_t>(mutations.size()));
    for (const auto& mutation : mutations)
    {
        result.push_back(mutation.value ? 1 : 0);
        put32(
            result,
            static_cast<std::uint32_t>(mutation.key.size())
        );
        put32(
            result,
            mutation.value
                ? static_cast<std::uint32_t>(mutation.value->size())
                : 0
        );
        result += mutation.key;
        if (mutation.value)
            result += *mutation.value;
    }
    return result;
}

std::string frame(std::uint64_t revision, std::string_view payload)
{
    std::string result(frameMagic);
    put32(result, static_cast<std::uint32_t>(payload.size()));
    put64(result, revision);
    put32(result, checksum(payload));
    put32(result, checksum(result));
    result += payload;
    return result;
}

/** @brief Charges detached data and a conservative map-node overhead.
 */
bool apply(
    DurableStateStore::Snapshot& snapshot,
    const std::vector<DurableStateStore::Mutation>& mutations,
    const DurableStateStore::Config& config
)
{
    for (const auto& mutation : mutations)
    {
        if (mutation.value)
            snapshot.values[mutation.key] = *mutation.value;
        else
            snapshot.values.erase(mutation.key);
    }
    if (snapshot.values.size() > config.maxKeys)
        return false;
    std::size_t bytes = 0;
    for (const auto& [key, value] : snapshot.values)
    {
        const auto charge = key.capacity() + value.capacity() + 128;
        if (charge > config.maxStateBytes - bytes)
            return false;
        bytes += charge;
    }
    return true;
}
} // namespace

struct DurableStateStore::State : std::enable_shared_from_this<State>
{
    struct Job
    {
        /// Caller revision checked on the unique writer before any
        /// write.
        std::uint64_t expected;
        /// Owning bounded values; never contains an actor or
        /// transport object.
        std::vector<Mutation> mutations;
        /// Serialized owner delivery retained through real
        /// completion.
        Completion completion;
        /// Queue and callback-retention accounting reserved at
        /// admission.
        std::size_t charge;
    };

    State(boost::asio::any_io_executor executor, Config value)
        : config(std::move(value))
        , strand(boost::asio::make_strand(executor))
        , work(
              boost::asio::make_work_guard(
                  strand.get_inner_executor()
              )
          )
    {
        if (!executor || config.directory.empty() ||
            config.maxBytes < 128 ||
            config.maxBytes > 1024ULL * 1024 * 1024 ||
            config.maxRecordBytes < 32 ||
            config.maxRecordBytes > 4 * 1024 * 1024 ||
            config.maxRecordBytes > config.maxBytes - 40 ||
            !config.maxPending || config.maxPending > 4096 ||
            config.maxPendingBytes < 256 ||
            config.maxPendingBytes > 256 * 1024 * 1024 ||
            config.maxStateBytes < 128 ||
            config.maxStateBytes > 256 * 1024 * 1024 ||
            !config.maxKeys || config.maxKeys > 1000000)
            throw std::invalid_argument(
                "invalid durable authority limits"
            );
        std::filesystem::create_directories(config.directory);
        directory = FileDescriptor(
            ::open(
                config.directory.c_str(),
                O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW
            )
        );
        struct stat directoryInfo{};
        if (!directory.valid() ||
            ::fstat(directory.get(), &directoryInfo) != 0 ||
            directoryInfo.st_uid != ::geteuid() ||
            (directoryInfo.st_mode & 0022))
            invalidJournal();
        lock = FileDescriptor(
            ::openat(
                directory.get(),
                ".authority.lock",
                O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW |
                    O_NONBLOCK,
                0600
            )
        );
        inspect(lock.get());
        if (::flock(lock.get(), LOCK_EX | LOCK_NB) != 0)
            invalidJournal();
        journal = FileDescriptor(
            ::openat(
                directory.get(),
                "authority.wal",
                O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW |
                    O_NONBLOCK,
                0600
            )
        );
        auto before = inspect(journal.get());
        if (before.st_size < 0 ||
            static_cast<std::uintmax_t>(before.st_size) >
                config.maxBytes)
            invalidJournal();
        if (!before.st_size)
        {
            if (!writeAll(journal.get(), header(), 0) ||
                !syncFile(journal.get()))
                invalidJournal();
            before = inspect(journal.get());
        }
        if (!syncDirectory(directory.get()))
            invalidJournal();
        recover(before);
    }

    /** @brief Accepts only an incomplete final frame as a recoverable
     * crash tail. */
    void recover(const struct stat& before)
    {
        const auto size = static_cast<std::size_t>(before.st_size);
        if (size < fileHeaderBytes)
            invalidJournal();
        std::string bytes(fileHeaderBytes, '\0');
        if (!readAll(journal.get(), bytes, 0) || bytes != header())
            invalidJournal();
        auto current = std::make_shared<Snapshot>();
        std::size_t offset = fileHeaderBytes;
        bool tail = false;
        while (offset < size)
        {
            const auto remaining = size - offset;
            bytes.assign(std::min(frameHeaderBytes, remaining), '\0');
            if (!readAll(journal.get(), bytes, offset))
                invalidJournal();
            if (bytes.substr(
                    0,
                    std::min(bytes.size(), frameMagic.size())
                ) !=
                frameMagic.substr(
                    0,
                    std::min(bytes.size(), frameMagic.size())
                ))
                invalidJournal();
            if (remaining < frameHeaderBytes)
            {
                tail = true;
                break;
            }
            if (checksum(std::string_view(bytes).substr(0, 20)) !=
                get32(std::string_view(bytes).substr(20)))
                invalidJournal();
            const auto length = get32(
                std::string_view(bytes).substr(4)
            );
            const auto revision = get64(
                std::string_view(bytes).substr(8)
            );
            const auto crc = get32(
                std::string_view(bytes).substr(16)
            );
            if (!length || length > config.maxRecordBytes ||
                current->revision ==
                    std::numeric_limits<std::uint64_t>::max() ||
                revision != current->revision + 1)
                invalidJournal();
            if (length > remaining - frameHeaderBytes)
            {
                tail = true;
                break;
            }
            bytes.assign(length, '\0');
            if (!readAll(
                    journal.get(),
                    bytes,
                    offset + frameHeaderBytes
                ) ||
                checksum(bytes) != crc)
                invalidJournal();
            const auto mutations = decode(bytes);
            if (!mutations || !apply(*current, *mutations, config))
                invalidJournal();
            current->revision = revision;
            offset += frameHeaderBytes + length;
        }
        const auto after = inspect(journal.get());
        if (before.st_size != after.st_size ||
            before.st_mtim.tv_sec != after.st_mtim.tv_sec ||
            before.st_mtim.tv_nsec != after.st_mtim.tv_nsec ||
            before.st_ctim.tv_sec != after.st_ctim.tv_sec ||
            before.st_ctim.tv_nsec != after.st_ctim.tv_nsec)
            invalidJournal();
        if (tail &&
            ::ftruncate(journal.get(), static_cast<off_t>(offset)) !=
                0)
            invalidJournal();
        // A complete recovered frame may have preceded a lost sync
        // acknowledgement.
        if (!syncFile(journal.get()))
            invalidJournal();
        counts.fileBytes = offset;
        published.store(
            std::move(current),
            std::memory_order_release
        );
    }

    Result process(const Job& job)
    {
        {
            std::lock_guard gate(mutex);
            if (counts.poisoned)
                return std::unexpected(Error::Unavailable);
        }
        const auto current = published.load(
            std::memory_order_acquire
        );
        if (current->revision != job.expected)
        {
            std::lock_guard gate(mutex);
            ++counts.conflicts;
            return std::unexpected(Error::Conflict);
        }
        if (current->revision ==
            std::numeric_limits<std::uint64_t>::max())
            return std::unexpected(Error::Capacity);
        auto next = std::make_shared<Snapshot>(*current);
        if (!apply(*next, job.mutations, config))
            return std::unexpected(Error::Capacity);
        next->revision = current->revision + 1;
        const auto payload = encode(job.mutations);
        const auto bytes = frame(next->revision, payload);
        std::size_t offset;
        {
            std::lock_guard gate(mutex);
            offset = counts.fileBytes;
        }
        if (bytes.size() > config.maxBytes - offset)
            return std::unexpected(Error::Capacity);
        if (!writeAll(journal.get(), bytes, offset) ||
            !syncFile(journal.get()))
        {
            std::lock_guard gate(mutex);
            ++counts.ioFailures;
            counts.poisoned = true;
            counts.accepting = false;
            stopping = true;
            return std::unexpected(Error::OutcomeUnknown);
        }
        published.store(std::move(next), std::memory_order_release);
        {
            std::lock_guard gate(mutex);
            counts.fileBytes += bytes.size();
            ++counts.committed;
        }
        return current->revision + 1;
    }

    /** @brief Pending accounting survives disk completion until owner
     * delivery. */
    void deliver(Job job, Result result) noexcept
    {
        const auto charge = job.charge;
        try
        {
            boost::asio::post(
                strand,
                [self = shared_from_this(),
                 completion = std::move(job.completion),
                 result,
                 charge]() mutable
                {
                    try
                    {
                        completion(result);
                    }
                    catch (...)
                    {
                        std::lock_guard gate(self->mutex);
                        ++self->counts.callbackFailures;
                    }
                    {
                        std::lock_guard gate(self->mutex);
                        --self->counts.pending;
                        self->counts.pendingBytes -= charge;
                    }
                }
            );
        }
        catch (...)
        {
            std::lock_guard gate(mutex);
            --counts.pending;
            counts.pendingBytes -= charge;
            ++counts.deliveryFailures;
            counts.poisoned = true;
            counts.accepting = false;
            stopping = true;
        }
    }

    void run() noexcept
    {
        for (;;)
        {
            Job job;
            {
                std::unique_lock gate(mutex);
                wake.wait(
                    gate,
                    [&] { return stopping || !queue.empty(); }
                );
                if (queue.empty())
                    break;
                job = std::move(queue.front());
                queue.pop_front();
            }
            Result result = std::unexpected(Error::Capacity);
            try
            {
                result = process(job);
            }
            catch (...)
            {
                // All potentially allocating preparation precedes
                // write admission.
            }
            deliver(std::move(job), result);
        }
        journal = FileDescriptor{};
        lock = FileDescriptor{};
        directory = FileDescriptor{};
        {
            std::lock_guard gate(mutex);
            counts.writerStopped = true;
        }
        work.reset();
    }

    /// Construction fixes every file, state and admission limit.
    const Config config;
    /// User completions share one external-executor strand.
    boost::asio::strand<boost::asio::any_io_executor> strand;
    /// Keeps the external executor alive until all writer deliveries
    /// are posted.
    boost::asio::executor_work_guard<boost::asio::any_io_executor>
        work;
    /// The writer closes all three descriptors before publishing
    /// writerStopped.
    FileDescriptor directory, lock, journal;
    /// Only the disk owner publishes complete immutable state copies.
    std::atomic<std::shared_ptr<const Snapshot>> published;
    /// Protects queue admission, shutdown and observable accounting.
    mutable std::mutex mutex;
    /// Accepted jobs and terminal stop wake the unique disk owner.
    std::condition_variable wake;
    /// Pending counts additionally include active and undelivered
    /// jobs.
    std::deque<Job> queue;
    /// All counters are protected by mutex.
    Stats counts;
    /// Disk failure and explicit shutdown both retire admission
    /// permanently.
    bool stopping{false};
    /// Destructor joins only after requesting stop, never from user
    /// callbacks.
    std::mutex joinMutex;
    /// Exactly one thread mutates the journal and working state.
    std::thread writer;
};

DurableStateStore::DurableStateStore(
    boost::asio::any_io_executor executor, Config config
)
    : m_state(
          std::make_shared<State>(
              std::move(executor), std::move(config)
          )
      )
{
    m_state->writer = std::thread([state = m_state]
                                  { state->run(); });
}

DurableStateStore::~DurableStateStore() noexcept
{
    stop();
    std::lock_guard gate(m_state->joinMutex);
    if (m_state->writer.joinable())
        m_state->writer.join();
}

std::shared_ptr<const DurableStateStore::Snapshot>
DurableStateStore::snapshot() const noexcept
{
    return m_state->published.load(std::memory_order_acquire);
}

bool DurableStateStore::commit(
    std::uint64_t expectedRevision,
    std::vector<Mutation> mutations,
    Completion completion
)
{
    const auto state = m_state;
    std::size_t charge = sizeof(State::Job), encoded = 4;
    bool valid = completion && !mutations.empty() &&
                 mutations.size() <= 65536;
    const auto add = [](std::size_t& count,
                        std::size_t amount,
                        std::size_t maximum)
    {
        if (count > maximum || amount > maximum - count)
            return false;
        count += amount;
        return true;
    };
    valid = valid &&
            mutations.capacity() <=
                state->config.maxPendingBytes / sizeof(Mutation) &&
            add(charge,
                mutations.capacity() * sizeof(Mutation),
                state->config.maxPendingBytes);
    std::set<std::string_view> seen;
    try
    {
        for (const auto& mutation : mutations)
        {
            if (!valid)
                break;
            valid = !mutation.key.empty() &&
                    mutation.key.size() <= 256 &&
                    seen.insert(mutation.key).second &&
                    add(encoded,
                        9 + mutation.key.size(),
                        state->config.maxRecordBytes) &&
                    (!mutation.value ||
                     add(encoded,
                         mutation.value->size(),
                         state->config.maxRecordBytes)) &&
                    add(charge,
                        mutation.key.capacity(),
                        state->config.maxPendingBytes) &&
                    (!mutation.value ||
                     add(charge,
                         mutation.value->capacity(),
                         state->config.maxPendingBytes));
        }
        valid = valid && add(charge,
                             encoded + frameHeaderBytes,
                             state->config.maxPendingBytes);
    }
    catch (...)
    {
        valid = false;
    }
    std::lock_guard gate(state->mutex);
    if (!valid || !state->counts.accepting || state->stopping ||
        state->counts.pending >= state->config.maxPending ||
        charge > state->config.maxPendingBytes -
                     state->counts.pendingBytes)
    {
        ++state->counts.rejected;
        return false;
    }
    try
    {
        state->queue.push_back(
            State::Job{
                expectedRevision,
                std::move(mutations),
                std::move(completion),
                charge
            }
        );
    }
    catch (...)
    {
        ++state->counts.rejected;
        return false;
    }
    ++state->counts.pending;
    state->counts.pendingBytes += charge;
    state->wake.notify_one();
    return true;
}

void DurableStateStore::stop() noexcept
{
    const auto state = m_state;
    std::lock_guard gate(state->mutex);
    state->counts.accepting = false;
    state->stopping = true;
    state->wake.notify_one();
}

DurableStateStore::Stats DurableStateStore::stats() const noexcept
{
    std::lock_guard gate(m_state->mutex);
    auto result = m_state->counts;
    result.closed = result.writerStopped && !result.pending &&
                    !result.deliveryFailures;
    return result;
}
} // namespace service::lobby