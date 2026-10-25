/** @file
 * @brief Queues complete room journals for bounded background
 * persistence.
 */
#pragma once
#include <filesystem>
#include <memory>
#include <string>

#include "room/ReplayFile.h"

namespace service::room
{
struct ReplayRecorderConfig
{
    /// Parent directory for a unique run subdirectory.
    std::filesystem::path directory;
    /// Maximum queued jobs, including initial snapshots.
    std::size_t maxPending{512};
    /// Counts retained vector capacity as well as fixed job storage.
    std::size_t maxQueuedBytes{16 * 1024 * 1024};
    /// Maximum concurrent recording admission endpoints.
    std::size_t maxActive{128};
    /// Per-file byte, tick and decoded collection bounds.
    ReplayFileLimits fileLimits;
};
struct ReplayRecorderMetrics
{
    std::uint64_t completed{0}, failed{0}, rejected{0};
    std::size_t active{0}, queued{0}, queuedBytes{0};
    std::string lastError;
};
struct ReplayRecorderState;
struct ReplayRecordingState;

/** @brief Owns one recording admission endpoint, never a room actor.
 *
 * Explicit finish seals accepted ticks. Destroying an unfinished
 * endpoint invalidates the archive. Queue or
 * storage failure invalidates the entire archive rather than
 * publishing a successful file with a missing tick.
 */
class ReplayRecording final
{
public:
    ~ReplayRecording() noexcept;
    ReplayRecording(const ReplayRecording&) = delete;
    /** @brief Transfers a canonical committed cutoff to the disk
     * worker. */
    bool append(TickRecord);
    /** @brief Seals a successfully submitted stream without blocking
     * on disk. */
    void finish() noexcept;

private:
    friend class ReplayRecorder;
    ReplayRecording(
        std::shared_ptr<ReplayRecorderState>,
        std::shared_ptr<ReplayRecordingState>
    );
    std::shared_ptr<ReplayRecorderState> m_owner;
    std::shared_ptr<ReplayRecordingState> m_recording;
};

/** @brief Writes journals on one private thread, independent of room
 * owners.
 *
 * Construction prepares a unique run directory. stop() closes
 * admission, drains accepted work and joins; call it outside
 * latency-sensitive work. Files remain .part until all accepted ticks
 * and the footer are written.
 */
class ReplayRecorder final
{
public:
    explicit ReplayRecorder(ReplayRecorderConfig);
    ~ReplayRecorder() noexcept;
    ReplayRecorder(const ReplayRecorder&) = delete;
    /** @brief Opens bounded admission for an actor's initial
     * snapshot. */
    std::unique_ptr<ReplayRecording> begin(ActorConfig, Snapshot);
    /** @brief Flushes and retires all streams; safe to repeat. */
    void stop() noexcept;
    ReplayRecorderMetrics metrics() const;
    const std::filesystem::path& directory() const noexcept;

private:
    std::shared_ptr<ReplayRecorderState> m_state;
};
} // namespace service::room
