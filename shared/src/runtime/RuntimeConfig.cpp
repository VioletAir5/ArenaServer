/** @file
 * @brief Validates discovery documents and publishes complete runtime
 * versions.
 */
#include "runtime/RuntimeConfig.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <set>
#include <sstream>
#include <stdexcept>
#include <tuple>
#include <utility>

#include <boost/property_tree/json_parser.hpp>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include "os/FileDescriptor.h"

#include "runtime_config.pb.h"

namespace arena::runtime
{
namespace
{
/** @brief Bounds parser recursion before reading attacker-controlled
 * structure. */
bool boundedNesting(std::string_view json)
{
    std::size_t depth = 0;
    bool quoted = false, escaped = false;
    for (const char c : json)
    {
        if (quoted)
        {
            if (escaped)
                escaped = false;
            else if (c == '\\')
                escaped = true;
            else if (c == '"')
                quoted = false;
        }
        else if (c == '"')
            quoted = true;
        else if (c == '{' || c == '[')
        {
            if (++depth > 16)
                return false;
        }
        else if (c == '}' || c == ']')
        {
            if (depth == 0)
                return false;
            --depth;
        }
    }
    return !quoted && depth == 0;
}

/** @brief Maps protobuf's snake_case aliases to their lowerCamelCase
 * JSON names. */
std::string canonicalField(std::string_view name)
{
    std::string result;
    bool capitalize = false;
    for (char c : name)
    {
        if (c == '_')
        {
            capitalize = true;
            continue;
        }
        if (capitalize && c >= 'a' && c <= 'z')
            c = static_cast<char>(c - 'a' + 'A');
        result.push_back(c);
        capitalize = false;
    }
    return result;
}

/** @brief Rejects duplicate object members while retaining repeated
 * array entries. */
bool uniqueMembers(const boost::property_tree::ptree& value)
{
    std::set<std::string> seen;
    for (const auto& [name, child] : value)
    {
        if ((!name.empty() &&
             !seen.insert(canonicalField(name)).second) ||
            !uniqueMembers(child))
            return false;
    }
    return true;
}

/** @brief Retains JSON boolean kinds before older protobuf parsers
 * coerce strings. */
bool strictBooleans(std::string_view json)
{
    google::protobuf::Struct document;
    if (!google::protobuf::util::JsonStringToMessage(
             std::string(json),
             &document
        )
             .ok())
        return false;
    for (const auto* name : {"desiredReady", "desired_ready"})
    {
        const auto field = document.fields().find(name);
        if (field != document.fields().end() &&
            field->second.kind_case() !=
                google::protobuf::Value::kBoolValue)
            return false;
    }
    const auto nodes = document.fields().find("nodes");
    if (nodes == document.fields().end())
        return true;
    if (nodes->second.kind_case() !=
        google::protobuf::Value::kListValue)
        return false;
    for (const auto& node : nodes->second.list_value().values())
    {
        if (node.kind_case() != google::protobuf::Value::kStructValue)
            return false;
        const auto& fields = node.struct_value().fields();
        const auto ready = fields.find("ready");
        if (ready != fields.end() &&
            ready->second.kind_case() !=
                google::protobuf::Value::kBoolValue)
            return false;
    }
    return true;
}

/** @brief Preserves duplicate keys and boolean kinds before typed
 * normalization. */
bool validDocument(std::string_view json)
{
    if (!boundedNesting(json))
        return false;
    try
    {
        std::istringstream input{std::string(json)};
        boost::property_tree::ptree tree;
        boost::property_tree::read_json(input, tree);
        return uniqueMembers(tree) && strictBooleans(json);
    }
    catch (const boost::property_tree::json_parser_error&)
    {
        return false;
    }
}

/** @brief Restricts service and node identities to bounded public
 * ASCII names. */
bool identifier(std::string_view value, std::size_t maximum)
{
    if (value.empty() || value.size() > maximum ||
        value.front() < 'a' || value.front() > 'z')
        return false;
    return std::all_of(
        value.begin(),
        value.end(),
        [](unsigned char c)
        {
            return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                   c == '_' || c == '-';
        }
    );
}
} // namespace

std::expected<ConfigSnapshot::Ptr, ConfigError>
ConfigSnapshot::parse(std::string_view json)
{
    if (json.empty() || json.size() > maxBytes ||
        !validDocument(json))
        return std::unexpected(ConfigError::Invalid);
    config::RuntimeConfig document;
    google::protobuf::util::JsonParseOptions options;
    options.ignore_unknown_fields = false;
    if (!google::protobuf::util::JsonStringToMessage(
             std::string(json),
             &document,
             options
        )
             .ok() ||
        document.version() == 0 || !document.has_desired_ready() ||
        !document.has_drain_grace_ms() ||
        document.drain_grace_ms() > 10000 ||
        document.nodes_size() == 0 || document.nodes_size() > 128)
        return std::unexpected(ConfigError::Invalid);
    auto result = std::shared_ptr<ConfigSnapshot>(new ConfigSnapshot);
    result->m_version = document.version();
    result->m_ready = document.desired_ready();
    result->m_drainGraceMs = document.drain_grace_ms();
    std::set<std::pair<std::string, std::string>> identities;
    for (const auto& node : document.nodes())
    {
        if (!identifier(node.service(), 32) ||
            !identifier(node.node_id(), 64) ||
            (node.host() != "127.0.0.1" && node.host() != "::1") ||
            node.port() == 0 || node.port() > 65535 ||
            !node.has_ready() ||
            !identities.emplace(node.service(), node.node_id())
                 .second)
            return std::unexpected(ConfigError::Invalid);
        result->m_nodes.push_back(
            Node{
                node.service(),
                node.node_id(),
                node.host(),
                static_cast<std::uint16_t>(node.port()),
                node.ready()
            }
        );
    }
    std::sort(
        result->m_nodes.begin(),
        result->m_nodes.end(),
        [](const Node& left, const Node& right)
        {
            return std::tie(left.service, left.id) <
                   std::tie(right.service, right.id);
        }
    );
    return Ptr(std::move(result));
}

std::expected<ConfigSnapshot::Ptr, ConfigError>
ConfigSnapshot::read(const std::filesystem::path& path)
{
    const int descriptor = ::open(
        path.c_str(),
        O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW
    );
    if (descriptor < 0)
        return std::unexpected(ConfigError::File);
    service::FileDescriptor owned(descriptor);
    struct stat before{}, after{};
    if (::fstat(descriptor, &before) != 0 ||
        !S_ISREG(before.st_mode) || before.st_size <= 0 ||
        before.st_size > static_cast<off_t>(maxBytes))
        return std::unexpected(ConfigError::File);
    std::string content(
        static_cast<std::size_t>(before.st_size),
        '\0'
    );
    std::size_t offset = 0;
    while (offset < content.size())
    {
        const auto count = ::read(
            descriptor,
            content.data() + offset,
            content.size() - offset
        );
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0)
            return std::unexpected(ConfigError::Changed);
        offset += static_cast<std::size_t>(count);
    }
    if (::fstat(descriptor, &after) != 0 ||
        before.st_size != after.st_size ||
        before.st_mtim.tv_sec != after.st_mtim.tv_sec ||
        before.st_mtim.tv_nsec != after.st_mtim.tv_nsec ||
        before.st_ctim.tv_sec != after.st_ctim.tv_sec ||
        before.st_ctim.tv_nsec != after.st_ctim.tv_nsec)
        return std::unexpected(ConfigError::Changed);
    return parse(content);
}

std::optional<Node>
ConfigSnapshot::resolve(std::string_view service) const
{
    const auto found = std::find_if(
        m_nodes.begin(),
        m_nodes.end(),
        [service](const Node& node)
        { return node.service == service && node.ready; }
    );
    return found == m_nodes.end() ? std::nullopt
                                  : std::optional<Node>(*found);
}

ConfigStore::ConfigStore(ConfigSnapshot::Ptr initial)
    : m_current(std::move(initial))
{
    if (!m_current.load())
        throw std::invalid_argument(
            "an initial validated runtime snapshot is required"
        );
}

ConfigSnapshot::Ptr ConfigStore::snapshot() const noexcept
{
    return m_current.load(std::memory_order_acquire);
}

std::expected<void, ConfigError>
ConfigStore::publish(ConfigSnapshot::Ptr candidate, Notify notify)
{
    {
        std::lock_guard lock(m_mutex);
        if (!candidate)
            return std::unexpected(ConfigError::Invalid);
        if (m_notifying)
            return std::unexpected(ConfigError::Busy);
        if (candidate->version() <= snapshot()->version())
            return std::unexpected(ConfigError::Stale);
        m_notifying = true;
        m_current.store(candidate, std::memory_order_release);
    }
    if (notify)
    {
        try
        {
            notify(candidate);
        }
        catch (...)
        {
            m_notificationFailures.fetch_add(
                1,
                std::memory_order_relaxed
            );
        }
    }
    {
        std::lock_guard lock(m_mutex);
        m_notifying = false;
    }
    return {};
}

std::uint64_t ConfigStore::notificationFailures() const noexcept
{
    return m_notificationFailures.load(std::memory_order_relaxed);
}
} // namespace arena::runtime