/** @file
 * @brief Exports complete synced Lobby replay histories while holding
 * its offline journal lock.
 */
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "lobby/ReplayArchive.h"

namespace
{
using namespace std::chrono_literals;
using Store = service::lobby::AtomicJournal;
struct Runtime
{
    boost::asio::io_context context;
    std::unique_ptr<Store> store;
    /** @brief Pumps one shutdown polling budget. False only reports a
     * timeout; destruction of the owned Store still joins its real
     * writer and can wait beyond this budget. No thread is detached
     * or forcefully terminated.
     */
    bool close() noexcept
    {
        if (!store)
            return true;
        store->stop();
        const auto end = std::chrono::steady_clock::now() + 5s;
        while (!store->stats().closed &&
               std::chrono::steady_clock::now() < end)
        {
            try
            {
                context.restart();
                context.run_for(5ms);
            }
            catch (...)
            {
                return false;
            }
            std::this_thread::sleep_for(1ms);
        }
        return store->stats().closed;
    }
    ~Runtime()
    {
        close();
    }
};
void quoted(std::ostream& out, std::string_view value)
{
    constexpr char hex[] = "0123456789abcdef";
    out << '"';
    for (const unsigned char c : value)
        if (c == '"' || c == '\\')
            out << '\\' << static_cast<char>(c);
        else if (c < 32)
            out << "\\u00" << hex[c >> 4] << hex[c & 15];
        else
            out << static_cast<char>(c);
    out << '"';
}
int error(
    Runtime& runtime,
    std::string_view status,
    std::string_view reason,
    const service::lobby::ReplayArchiveError* detail = nullptr
)
{
    const bool closed = runtime.close();
    std::cout << "{\"status\":";
    quoted(std::cout, closed ? status : "cleanup");
    std::cout << ",\"reason\":";
    quoted(
        std::cout,
        closed ? reason
               : "journal worker exceeded shutdown polling budget"
    );
    if (detail)
    {
        if (detail->roomId)
            std::cout << ",\"room_id\":\"" << *detail->roomId << '\"';
        if (detail->incarnation)
            std::cout << ",\"incarnation\":\"" << *detail->incarnation
                      << '\"';
        if (detail->tick)
            std::cout << ",\"tick\":\"" << *detail->tick << '\"';
    }
    std::cout << "}\n";
    return 2;
}
std::string filename(const service::lobby::ReplayArchive& archive)
{
    return "room-" +
           std::to_string(archive.log.checkpoint.roomId.value()) +
           "-incarnation-" +
           std::to_string(archive.log.checkpoint.incarnation) +
           ".arena";
}
bool unoccupied(const std::filesystem::path& path)
{
    std::error_code failure;
    const auto status = std::filesystem::symlink_status(
        path,
        failure
    );
    return (!failure ||
            failure == std::errc::no_such_file_or_directory) &&
           status.type() == std::filesystem::file_type::not_found;
}
int run(int argc, char** argv)
{
    Runtime runtime;
    std::filesystem::path state, output;
    for (int index = 1; index < argc; index += 2)
    {
        if (index + 1 == argc)
            return error(
                runtime,
                "invalid",
                "usage: arena_replay_export --state-dir <path> "
                "--output-dir <path>"
            );
        const std::string_view option(argv[index]);
        if (option == "--state-dir" && state.empty())
            state = argv[index + 1];
        else if (option == "--output-dir" && output.empty())
            output = argv[index + 1];
        else
            return error(
                runtime,
                "invalid",
                "unexpected or repeated replay export option"
            );
    }
    if (state.empty() || output.empty())
        return error(
            runtime,
            "invalid",
            "both --state-dir and --output-dir are required"
        );
    std::error_code failure;
    const auto stateStatus = std::filesystem::symlink_status(
        state,
        failure
    );
    if (failure || !std::filesystem::is_directory(stateStatus))
        return error(
            runtime,
            "invalid",
            "state directory must be an existing nonsymlink directory"
        );
    const auto journalStatus = std::filesystem::symlink_status(
        state / "authority.wal",
        failure
    );
    if (failure || !std::filesystem::is_regular_file(journalStatus) ||
        std::filesystem::file_size(
            state / "authority.wal",
            failure
        ) == 0 ||
        failure)
        return error(
            runtime,
            "invalid",
            "state directory has no existing regular journal"
        );
    try
    {
        Store::Config config;
        config.directory = state;
        runtime.store = std::make_unique<Store>(
            runtime.context.get_executor(),
            config
        );
    }
    catch (...)
    {
        return error(
            runtime,
            "unavailable",
            "cannot acquire and recover the stopped Lobby journal"
        );
    }
    const auto snapshot = runtime.store->snapshot();
    auto archives = service::lobby::assembleReplayArchives(*snapshot);
    if (!archives)
    {
        using Code = service::lobby::ReplayArchiveErrorCode;
        return error(
            runtime,
            archives.error().code == Code::Capacity ? "limit"
            : archives.error().code == Code::Desync ? "desync"
                                                    : "invalid",
            archives.error().reason,
            &archives.error()
        );
    }
    std::filesystem::create_directories(output, failure);
    if (failure ||
        !std::filesystem::is_directory(
            std::filesystem::symlink_status(output, failure)
        ) ||
        failure)
        return error(runtime, "io", "cannot use output directory");
    for (const auto& archive : *archives)
        if (!unoccupied(output / filename(archive)) ||
            !unoccupied(output / (filename(archive) + ".part")))
            return error(
                runtime,
                "io",
                "refusing to overwrite an existing archive path"
            );
    for (const auto& archive : *archives)
    {
        const auto final = output / filename(archive);
        const auto partial = output / (filename(archive) + ".part");
        auto result = service::room::writeReplayFile(
            partial,
            archive.log
        );
        if (!result)
            return error(
                runtime,
                "io",
                "cannot complete replay archive output"
            );
        // A hard-link publication refuses an existing destination
        // atomically.
        if (::link(partial.c_str(), final.c_str()) != 0)
            return error(
                runtime,
                "io",
                "cannot publish completed replay archive"
            );
        if (::unlink(partial.c_str()) != 0)
            return error(
                runtime,
                "io",
                "cannot retire completed replay temporary path"
            );
    }
    if (!runtime.close())
        return error(
            runtime,
            "cleanup",
            "journal worker exceeded shutdown polling budget"
        );
    std::cout << "{\"status\":\"ok\",\"revision\":\""
              << snapshot->revision << "\",\"files\":[";
    bool first = true;
    for (const auto& archive : *archives)
    {
        if (!first)
            std::cout << ',';
        first = false;
        std::cout << "{\"file\":";
        quoted(std::cout, filename(archive));
        std::cout << ",\"room_id\":\""
                  << archive.log.checkpoint.roomId.value()
                  << "\",\"incarnation\":\""
                  << archive.log.checkpoint.incarnation
                  << "\",\"ticks\":\"" << archive.log.ticks.size()
                  << "\",\"final_tick\":\""
                  << archive.finalSnapshot.tick
                  << "\",\"checksum\":\""
                  << archive.finalSnapshot.checksum << "\"}";
    }
    std::cout << "]}\n";
    return std::cout ? 0 : 2;
}
} // namespace
int main(int argc, char** argv)
{
    try
    {
        return run(argc, argv);
    }
    catch (...)
    {
        std::cout << "{\"status\":\"io\",\"reason\":\"replay export "
                     "failed\"}\n";
        return 2;
    }
}
