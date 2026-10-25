/** @file
 * @brief Persists copied room cutoffs without blocking the simulation
 * on disk.
 */
#include "room/ReplayRecorder.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

namespace service::room
{
struct ReplayRecordingState
{
    std::uint64_t id;
    RoomId room;
    std::uint64_t incarnation;
    std::size_t pending{0};
    bool closing{false}, failed{false};
};

struct ReplayRecorderState
{
    struct Work
    {
        std::shared_ptr<ReplayRecordingState> stream;
        ActorConfig config;
        std::optional<Snapshot> initial;
        TickRecord tick{};
        std::size_t bytes{0};
    };

    explicit ReplayRecorderState(ReplayRecorderConfig value)
        : config(std::move(value))
    {
        if (config.directory.empty() || !config.maxPending ||
            !config.maxQueuedBytes || !config.maxActive)
            throw std::invalid_argument(
                "invalid replay recorder limits"
            );
        std::filesystem::create_directories(config.directory);
        const auto stamp = std::chrono::steady_clock::now()
                               .time_since_epoch()
                               .count();
        for (unsigned attempt = 0; attempt < 32; ++attempt)
        {
            output = config.directory /
                     ("run-" + std::to_string(stamp) + "-" +
                      std::to_string(attempt));
            if (std::filesystem::create_directory(output))
                return;
        }
        throw std::runtime_error(
            "cannot create unique replay directory"
        );
    }

    bool measure(Work& work) const
    {
        auto bytes = sizeof(Work);
        const auto add = [&](std::size_t count, std::size_t width)
        {
            if (bytes > config.maxQueuedBytes ||
                count > (config.maxQueuedBytes - bytes) / width)
                return false;
            bytes += count * width;
            return true;
        };
        if (!add(
                work.initial ? work.initial->players.capacity() : 0,
                sizeof(PlayerState)
            ) ||
            !add(work.tick.members.capacity(), sizeof(Member)) ||
            !add(work.tick.inputs.capacity(), sizeof(Input)))
            return false;
        work.bytes = bytes;
        return true;
    }

    void failLocked(
        const std::shared_ptr<ReplayRecordingState>& stream,
        std::string_view reason
    ) noexcept
    {
        if (!stream->failed)
        {
            stream->failed = true;
            ++counts.failed;
        }
        try
        {
            counts.lastError.assign(reason);
        }
        catch (...)
        {
        }
    }

    bool enqueueLocked(Work work)
    {
        if (stopping || !measure(work) ||
            queue.size() >= config.maxPending ||
            work.bytes > config.maxQueuedBytes - queuedBytes)
        {
            ++counts.rejected;
            failLocked(
                work.stream,
                "replay queue admission rejected"
            );
            return false;
        }
        const auto bytes = work.bytes;
        const auto stream = work.stream;
        try
        {
            queue.push_back(std::move(work));
        }
        catch (...)
        {
            ++counts.rejected;
            failLocked(stream, "replay queue allocation failed");
            return false;
        }
        ++stream->pending;
        queuedBytes += bytes;
        wake.notify_one();
        return true;
    }

    auto retiredLocked()
    {
        return std::find_if(
            streams.begin(),
            streams.end(),
            [](const auto& pair)
            { return pair.second->closing && !pair.second->pending; }
        );
    }

    void run() noexcept
    {
        // Only this thread owns writers; room/connection objects
        // never enter these jobs. Closing a stream requires no extra
        // queue slot.
        std::map<std::uint64_t, std::unique_ptr<ReplayWriter>>
            writers;
        for (;;)
        {
            Work work;
            std::shared_ptr<ReplayRecordingState> retired;
            {
                std::unique_lock lock(mutex);
                wake.wait(
                    lock,
                    [&]
                    {
                        return !queue.empty() || stopping ||
                               retiredLocked() != streams.end();
                    }
                );
                const auto ready = retiredLocked();
                if (ready != streams.end())
                {
                    retired = ready->second;
                    streams.erase(ready);
                }
                else if (!queue.empty())
                {
                    work = std::move(queue.front());
                    queue.pop_front();
                    queuedBytes -= work.bytes;
                }
                else if (stopping)
                    break;
                else
                    continue;
            }
            const auto stream = retired ? retired : work.stream;
            try
            {
                const auto
                    partial = output /
                              ("room-" +
                               std::to_string(stream->room.value()) +
                               "-inc-" +
                               std::to_string(stream->incarnation) +
                               ".part");
                const auto complete = partial.parent_path() /
                                      (partial.stem().string() +
                                       ".areplay");
                bool failed;
                {
                    std::lock_guard lock(mutex);
                    failed = stream->failed;
                }
                if (retired)
                {
                    auto writer = writers.find(stream->id);
                    if (!failed && writer != writers.end())
                    {
                        auto result = writer->second->finish();
                        if (!result)
                            throw std::runtime_error(
                                result.error().reason
                            );
                        writers.erase(writer);
                        if (std::filesystem::exists(complete))
                            throw std::runtime_error(
                                "replay target exists"
                            );
                        std::filesystem::rename(partial, complete);
                        std::lock_guard lock(mutex);
                        ++counts.completed;
                    }
                    else if (!failed)
                        throw std::runtime_error(
                            "replay writer missing"
                        );
                    writers.erase(stream->id);
                }
                else if (!failed && work.initial)
                {
                    if (std::filesystem::exists(partial) ||
                        std::filesystem::exists(complete))
                        throw std::runtime_error(
                            "replay file already exists"
                        );
                    auto writer = ReplayWriter::create(
                        partial,
                        work.config,
                        *work.initial,
                        config.fileLimits
                    );
                    if (!writer)
                        throw std::runtime_error(
                            writer.error().reason
                        );
                    writers.emplace(stream->id, std::move(*writer));
                }
                else if (!failed)
                {
                    const auto found = writers.find(stream->id);
                    if (found == writers.end())
                        throw std::runtime_error(
                            "replay writer missing"
                        );
                    auto result = found->second->append(work.tick);
                    if (!result)
                        throw std::runtime_error(
                            result.error().reason
                        );
                }
            }
            catch (const std::exception& error)
            {
                {
                    std::lock_guard lock(mutex);
                    failLocked(stream, error.what());
                }
                writers.erase(stream->id);
            }
            catch (...)
            {
                {
                    std::lock_guard lock(mutex);
                    failLocked(
                        stream,
                        "unexpected replay writer failure"
                    );
                }
                writers.erase(stream->id);
            }
            if (!retired)
            {
                std::lock_guard lock(mutex);
                --stream->pending;
                wake.notify_one();
            }
        }
    }

    void stop() noexcept
    {
        // Serializes concurrent stop calls without holding admission
        // while join waits for the worker to finish accepted jobs.
        std::lock_guard retirement(stopMutex);
        {
            std::lock_guard lock(mutex);
            stopping = true;
            for (auto& [id, stream] : streams)
            {
                (void)id;
                if (!stream->closing)
                    failLocked(
                        stream,
                        "recorder stopped before recording finish"
                    );
                stream->closing = true;
            }
        }
        wake.notify_all();
        if (worker.joinable())
            worker.join();
    }

    const ReplayRecorderConfig config;
    std::filesystem::path output;
    mutable std::mutex mutex;
    std::mutex stopMutex;
    std::condition_variable wake;
    std::deque<Work> queue;
    std::map<std::uint64_t, std::shared_ptr<ReplayRecordingState>>
        streams;
    ReplayRecorderMetrics counts;
    std::size_t queuedBytes{0};
    std::uint64_t nextId{1};
    bool stopping{false};
    std::thread worker;
};

ReplayRecording::ReplayRecording(
    std::shared_ptr<ReplayRecorderState> owner,
    std::shared_ptr<ReplayRecordingState> recording
)
    : m_owner(std::move(owner))
    , m_recording(std::move(recording))
{
}
ReplayRecording::~ReplayRecording() noexcept
{
    {
        std::lock_guard lock(m_owner->mutex);
        if (!m_recording->closing)
            m_owner->failLocked(
                m_recording,
                "recording abandoned before finish"
            );
        m_recording->closing = true;
    }
    m_owner->wake.notify_one();
}
void ReplayRecording::finish() noexcept
{
    {
        std::lock_guard lock(m_owner->mutex);
        m_recording->closing = true;
    }
    m_owner->wake.notify_one();
}
bool ReplayRecording::append(TickRecord tick)
{
    std::lock_guard lock(m_owner->mutex);
    if (m_recording->closing || m_recording->failed)
        return false;
    ReplayRecorderState::Work work;
    work.stream = m_recording;
    work.tick = std::move(tick);
    return m_owner->enqueueLocked(std::move(work));
}
ReplayRecorder::ReplayRecorder(ReplayRecorderConfig config)
    : m_state(
          std::make_shared<ReplayRecorderState>(std::move(config))
      )
{
    m_state->worker = std::thread([state = m_state.get()]
                                  { state->run(); });
}
ReplayRecorder::~ReplayRecorder() noexcept
{
    stop();
}
std::unique_ptr<ReplayRecording>
ReplayRecorder::begin(ActorConfig config, Snapshot initial)
{
    std::lock_guard lock(m_state->mutex);
    if (m_state->stopping ||
        m_state->streams.size() >= m_state->config.maxActive ||
        m_state->nextId == UINT64_MAX)
    {
        ++m_state->counts.rejected;
        return {};
    }
    for (const auto& [id, stream] : m_state->streams)
    {
        (void)id;
        if (stream->room == initial.roomId &&
            stream->incarnation == initial.incarnation)
        {
            ++m_state->counts.rejected;
            return {};
        }
    }
    std::shared_ptr<ReplayRecordingState> stream;
    try
    {
        stream = std::make_shared<ReplayRecordingState>(
            ReplayRecordingState{
                m_state->nextId++,
                initial.roomId,
                initial.incarnation
            }
        );
        m_state->streams.emplace(stream->id, stream);
        ReplayRecorderState::Work work;
        work.stream = stream;
        work.config = config;
        work.initial.emplace(std::move(initial));
        if (!m_state->enqueueLocked(std::move(work)))
        {
            stream->closing = true;
            m_state->wake.notify_one();
            return {};
        }
        return std::unique_ptr<ReplayRecording>(
            new ReplayRecording(m_state, stream)
        );
    }
    catch (...)
    {
        ++m_state->counts.rejected;
        if (stream)
        {
            m_state->failLocked(
                stream,
                "replay recording allocation failed"
            );
            stream->closing = true;
            m_state->wake.notify_one();
        }
        return {};
    }
}
void ReplayRecorder::stop() noexcept
{
    m_state->stop();
}
ReplayRecorderMetrics ReplayRecorder::metrics() const
{
    std::lock_guard lock(m_state->mutex);
    auto result = m_state->counts;
    result.active = m_state->streams.size();
    result.queued = m_state->queue.size();
    result.queuedBytes = m_state->queuedBytes;
    return result;
}
const std::filesystem::path&
ReplayRecorder::directory() const noexcept
{
    return m_state->output;
}
} // namespace service::room
