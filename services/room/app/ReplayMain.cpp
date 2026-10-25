/** @file
 * @brief Verifies a room archive and emits a machine-readable report.
 */
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "room/ReplayFile.h"

namespace
{
using namespace service::room;

struct Report
{
    /// Stable outcome category; exit codes distinguish desync from
    /// invalid input.
    std::string status{"invalid"};
    /// Human-readable English diagnosis, escaped by the JSON encoder.
    std::string reason;
    /// Absolute failure and available state identifiers, encoded as
    /// strings.
    std::optional<std::uint64_t> failedTick, expected, actual;
    std::optional<std::uint64_t> checkpointTick, finalTick,
        finalChecksum;
    /// All validated post-tick checksums, excluding the initial
    /// checkpoint.
    std::vector<std::uint64_t> checksums;
};

/** @brief Escapes JSON text without adding a third-party parser
 * dependency. */
void quoted(std::ostream& output, std::string_view text)
{
    constexpr char hex[] = "0123456789abcdef";
    output << '"';
    for (const unsigned char value : text)
    {
        if (value == '"' || value == '\\')
            output << '\\' << static_cast<char>(value);
        else if (value < 32)
            output << "\\u00" << hex[value >> 4] << hex[value & 15];
        else
            output << static_cast<char>(value);
    }
    output << '"';
}

void number(std::ostream& output, std::optional<std::uint64_t> value)
{
    if (value)
        output << '"' << *value << '"';
    else
        output << "null";
}

/** @brief Streams the report so output does not duplicate the
 * checksum vector. */
void json(std::ostream& output, const Report& report)
{
    output << "{\"status\":";
    quoted(output, report.status);
    output << ",\"reason\":";
    quoted(output, report.reason);
    output << ",\"failed_tick\":";
    number(output, report.failedTick);
    output << ",\"expected_checksum\":";
    number(output, report.expected);
    output << ",\"actual_checksum\":";
    number(output, report.actual);
    output << ",\"checkpoint_tick\":";
    number(output, report.checkpointTick);
    output << ",\"final_tick\":";
    number(output, report.finalTick);
    output << ",\"final_checksum\":";
    number(output, report.finalChecksum);
    output << ",\"checksums\":[";
    for (std::size_t index = 0; index < report.checksums.size();
         ++index)
    {
        if (index)
            output << ',';
        // Successful replay has already checked contiguous
        // nonwrapping ticks.
        output << "{\"tick\":\"" << *report.checkpointTick + index + 1
               << "\",\"checksum\":\"" << report.checksums[index]
               << "\"}";
    }
    output << "]}\n";
}

/** @brief Prevents a report path from overwriting its input archive.
 */
bool sameFile(
    const std::filesystem::path& input,
    const std::filesystem::path& report
)
{
    std::error_code error;
    if (std::filesystem::equivalent(input, report, error))
        return true;
    error.clear();
    const auto inputPath = std::filesystem::weakly_canonical(
        input,
        error
    );
    if (error)
        return false;
    const auto reportPath = std::filesystem::weakly_canonical(
        report,
        error
    );
    return !error && inputPath == reportPath;
}

/** @brief Decodes and replays using only owned values on this thread.
 */
int verify(const std::filesystem::path& path, Report& report)
{
    auto log = readReplayFile(path);
    if (!log)
    {
        report.status = log.error().code == ReplayFileErrorCode::Io
                            ? "io"
                            : "invalid";
        report.reason = log.error().reason;
        report.failedTick = log.error().tick;
        return 2;
    }
    report.checkpointTick = log->checkpoint.tick;
    auto result = RoomActor::replay(*log);
    if (!result)
    {
        const auto& failure = result.error();
        const bool mismatch = failure.code ==
                              ReplayError::ChecksumMismatch;
        report.status = mismatch ? "desync" : "invalid";
        report.reason = failure.reason;
        report.failedTick = failure.tick;
        report.expected = failure.expectedChecksum;
        report.actual = failure.actualChecksum;
        return mismatch ? 1 : 2;
    }
    report.status = "ok";
    report.reason = "replay validated";
    report.finalTick = result->snapshot.tick;
    report.finalChecksum = result->snapshot.checksum;
    report.checksums = std::move(result->checksums);
    return 0;
}
} // namespace

int main(int argc, char** argv)
{
    Report report;
    int code = 2;
    try
    {
        const bool valid = argc == 2 ||
                           (argc == 4 &&
                            std::string_view(argv[2]) == "--report");
        if (!valid)
            report.reason =
                "usage: arena_replay <log> [--report <json>]";
        else if (argc == 4 && sameFile(argv[1], argv[3]))
            report.reason = "report path aliases replay input";
        else
        {
            code = verify(argv[1], report);
            if (argc == 4)
            {
                std::ofstream file(
                    argv[3],
                    std::ios::binary | std::ios::trunc
                );
                if (file)
                {
                    json(file, report);
                    file.flush();
                    file.close();
                }
                if (!file)
                {
                    report = Report{};
                    report.status = "io";
                    report.reason = "cannot write replay report";
                    code = 2;
                }
            }
        }
        json(std::cout, report);
        std::cout.flush();
        return std::cout ? code : 2;
    }
    catch (const std::exception&)
    {
        Report failed;
        failed.status = "io";
        failed.reason = "replay execution failed";
        json(std::cout, failed);
        return 2;
    }
}
