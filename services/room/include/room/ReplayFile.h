/** @file
 * @brief Defines bounded versioned replay files and incremental
 * archive output independent of the network runtime.
 */
#pragma once

#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

#include "room/RoomActor.h"

namespace service::room
{
/** @brief Bounds disk bytes and allocations before decoding counts.
 */
struct ReplayFileLimits
{
    /// Maximum complete file bytes, including header and footer.
    std::uint64_t maxFileBytes{64 * 1024 * 1024};
    /// Maximum archived ticks, independent of rolling actor history.
    std::uint64_t maxTicks{1000000};
    /// Maximum configured and encoded members in a state or cutoff.
    std::uint64_t maxMembers{4096};
    /// Maximum configured and encoded attempts in one tick.
    std::uint64_t maxInputsPerTick{65536};
    /// Maximum accepted original rolling-history configuration.
    std::uint64_t maxHistoryTicks{1000000};
};

enum class ReplayFileErrorCode
{
    Invalid,
    Io,
    Limit
};

/** @brief Separates file failures from replay desynchronization. */
struct ReplayFileError
{
    /// Structural, operating-system or resource-bound failure.
    ReplayFileErrorCode code;
    /// Short English diagnostic suitable for JSON escaping.
    std::string reason;
    /// Absolute record tick when known; absent for header failures.
    std::optional<std::uint64_t> tick;
};

/** @brief Incrementally writes values on one caller-owned thread.
 *
 * finish is required for a valid footer. Destruction closes output
 * without inventing completion. Write or validation failures are
 * terminal. No actor, connection or network executor is retained.
 */
class ReplayWriter final
{
public:
    static constexpr std::uint32_t formatVersion = 1;
    /** @brief Creates/truncates a file and writes its initial state.
     * @param[in] path Destination exclusively owned by the caller.
     * @param[in] config Original deterministic actor configuration.
     * @param[in] checkpoint State before the first appended tick.
     * @param[in] limits Bounds checked before writing/allocating.
     */
    static std::
        expected<std::unique_ptr<ReplayWriter>, ReplayFileError>
        create(
            const std::filesystem::path& path,
            const ActorConfig& config,
            const Snapshot& checkpoint,
            ReplayFileLimits limits = {}
        );
    ~ReplayWriter() noexcept;
    ReplayWriter(const ReplayWriter&) = delete;
    ReplayWriter& operator=(const ReplayWriter&) = delete;
    /** @brief Appends the next canonical cutoff, including empty
     * ticks and rejected attempts, while reserving footer space.
     * Stored checksums are preserved for offline validation.
     */
    std::expected<void, ReplayFileError> append(const TickRecord&);
    /** @brief Writes the footer, flushes and closes the file.
     * Repeated successful finish calls are harmless.
     */
    std::expected<void, ReplayFileError> finish();
    /** @brief Returns bytes handed successfully to the file stream.
     */
    std::uint64_t bytesWritten() const noexcept;
    /** @brief Returns fully written tick records. */
    std::uint64_t ticksWritten() const noexcept;

private:
    struct State;
    explicit ReplayWriter(std::unique_ptr<State>);
    /// File stream and serialization state, with no actor reference.
    std::unique_ptr<State> m_state;
};

/** @brief Decodes strict versions, lengths, limits and completion.
 * State checksums are subsequently validated by RoomActor::replay.
 */
std::expected<ReplayLog, ReplayFileError>
readReplayFile(const std::filesystem::path&, ReplayFileLimits = {});

/** @brief Writes owned values using the incremental archive format.
 * Total ticks are independent of config.historyTicks.
 */
std::expected<void, ReplayFileError> writeReplayFile(
    const std::filesystem::path&,
    const ReplayLog&,
    ReplayFileLimits = {}
);
} // namespace service::room
