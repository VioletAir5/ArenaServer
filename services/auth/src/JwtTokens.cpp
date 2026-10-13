#include "auth/JwtTokens.h"

#include <array>
#include <charconv>
#include <cmath>
#include <limits>
#include <set>
#include <sstream>
#include <utility>

#include <boost/property_tree/json_parser.hpp>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>

namespace service::auth
{
namespace
{
using Object = google::protobuf::Struct;
constexpr std::string_view alphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
constexpr std::int64_t maxJsonInteger = 9007199254740991LL;

std::string encode64(std::string_view bytes)
{
    std::string out;
    std::uint32_t buffer = 0;
    unsigned bits = 0;
    for (const unsigned char byte : bytes)
    {
        buffer = (buffer << 8) | byte;
        bits += 8;
        while (bits >= 6)
        {
            bits -= 6;
            out.push_back(alphabet[(buffer >> bits) & 63]);
        }
    }
    if (bits)
        out.push_back(alphabet[(buffer << (6 - bits)) & 63]);
    return out;
}

std::optional<std::string> decode64(std::string_view value, std::size_t limit)
{
    if (value.empty() || value.size() % 4 == 1 || value.size() > limit * 2)
        return {};
    std::string out;
    std::uint32_t buffer = 0;
    unsigned bits = 0;
    for (const char byte : value)
    {
        const auto digit = alphabet.find(byte);
        if (digit == std::string_view::npos)
            return {};
        buffer = (buffer << 6) | static_cast<std::uint32_t>(digit);
        bits += 6;
        if (bits >= 8)
        {
            bits -= 8;
            out.push_back(static_cast<char>((buffer >> bits) & 255));
        }
    }
    if (out.size() > limit || encode64(out) != value)
        return {}; // Reject padding, alternate encodings and unused nonzero bits.
    return out;
}

// Two standard parsers serve distinct checks: ptree retains duplicate keys;
// protobuf Struct retains JSON scalar types (ptree converts everything to text).
// This profile is a flat object, bounded before either parser sees input.
std::optional<Object> parseObject(std::string_view text, std::size_t fields)
{
    bool quoted = false, escaped = false;
    int depth = 0;
    for (const char c : text)
    {
        if (quoted)
        {
            if (escaped) escaped = false;
            else if (c == '\\') escaped = true;
            else if (c == '"') quoted = false;
            continue;
        }
        if (c == '"') quoted = true;
        else if (c == '{' || c == '[')
        {
            if (++depth > 1)
                return {};
        }
        else if (c == '}' || c == ']')
        {
            if (--depth < 0)
                return {};
        }
    }
    const auto first = text.find_first_not_of(" \r\n\t");
    if (quoted || depth || first == std::string_view::npos || text[first] != '{')
        return {};
    try
    {
        boost::property_tree::ptree tree;
        std::istringstream input{std::string(text)};
        boost::property_tree::read_json(input, tree);
        std::set<std::string> keys;
        for (const auto& [key, value] : tree)
            if (key.empty() || !value.empty() || !keys.insert(key).second)
                return {};
        if (keys.size() != fields)
            return {};
        Object object;
        if (!google::protobuf::util::JsonStringToMessage(std::string(text), &object).ok() ||
            object.fields().size() != fields)
            return {};
        return object;
    }
    catch (...) { return {}; }
}

std::optional<std::string> stringField(const Object& object, std::string_view name)
{
    const auto found = object.fields().find(std::string(name));
    if (found == object.fields().end() ||
        found->second.kind_case() != google::protobuf::Value::kStringValue)
        return {};
    return found->second.string_value();
}

std::optional<std::int64_t> timeField(const Object& object, std::string_view name)
{
    const auto found = object.fields().find(std::string(name));
    if (found == object.fields().end() ||
        found->second.kind_case() != google::protobuf::Value::kNumberValue)
        return {};
    const double value = found->second.number_value();
    if (!std::isfinite(value) || value < 0 || value > maxJsonInteger || std::trunc(value) != value)
        return {};
    return static_cast<std::int64_t>(value);
}

std::optional<std::uint64_t> identity(std::string_view text)
{
    std::uint64_t value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || text.front() == '0' || parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size() || !value)
        return {};
    return value;
}

std::optional<std::string> mac(std::string_view input, std::string_view key)
{
    std::array<unsigned char, 32> output{};
    std::size_t length = 0;
    if (!EVP_Q_mac(nullptr, "HMAC", nullptr, "SHA256", nullptr,
                   key.data(), key.size(),
                   reinterpret_cast<const unsigned char*>(input.data()), input.size(),
                   output.data(), output.size(), &length) || length != output.size())
        return {};
    return std::string(reinterpret_cast<const char*>(output.data()), output.size());
}

JwtConfig validateConfig(JwtConfig config)
{
    if (config.key.size() < 32 || config.key.size() > 4096 ||
        config.issuer.empty() || config.issuer.size() > 128 ||
        config.audience.empty() || config.audience.size() > 128 ||
        config.lifetime <= std::chrono::seconds::zero() ||
        config.lifetime > std::chrono::hours(24))
        throw std::invalid_argument("invalid JWT configuration");
    if (!config.now)
        config.now = [] {
            return std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        };
    return config;
}
} // namespace

JwtTokens::JwtTokens(JwtConfig config) : m_config(validateConfig(std::move(config))) {}

std::expected<TokenClaims, AuthError> JwtTokens::verify(std::string_view token) const
{
    const auto invalid = [] { return std::unexpected(AuthError::InvalidToken); };
    if (token.empty() || token.size() > 4096)
        return invalid();
    const auto first = token.find('.');
    const auto last = first == std::string_view::npos ? first : token.find('.', first + 1);
    if (first == std::string_view::npos || last == std::string_view::npos ||
        token.find('.', last + 1) != std::string_view::npos)
        return invalid();
    const auto headerText = decode64(token.substr(0, first), 512);
    const auto payloadText = decode64(token.substr(first + 1, last - first - 1), 2048);
    const auto signature = decode64(token.substr(last + 1), 32);
    if (!headerText || !payloadText || !signature || signature->size() != 32)
        return invalid();
    const auto header = parseObject(*headerText, 2);
    if (!header || stringField(*header, "alg") != "HS256" ||
        stringField(*header, "typ") != "JWT")
        return invalid();
    const auto expected = mac(token.substr(0, last), m_config.key);
    if (!expected)
        return std::unexpected(AuthError::Unavailable);
    if (CRYPTO_memcmp(expected->data(), signature->data(), 32) != 0)
        return invalid();
    const auto payload = parseObject(*payloadText, 7);
    if (!payload || stringField(*payload, "iss") != m_config.issuer ||
        stringField(*payload, "aud") != m_config.audience)
        return invalid();
    const auto sub = stringField(*payload, "sub");
    const auto epochText = stringField(*payload, "epoch");
    const auto issued = timeField(*payload, "iat");
    const auto notBefore = timeField(*payload, "nbf");
    const auto expires = timeField(*payload, "exp");
    const auto player = sub ? identity(*sub) : std::nullopt;
    const auto epoch = epochText ? identity(*epochText) : std::nullopt;
    const auto current = now();
    if (!player || !epoch || !issued || !notBefore || !expires || current < 0 ||
        *issued > current || *notBefore > current || *expires <= current ||
        *notBefore < *issued || *notBefore >= *expires || *issued >= *expires ||
        *expires - *issued > m_config.lifetime.count())
        return invalid();
    return TokenClaims{arena::domain::PlayerId{*player}, *epoch, *expires};
}

std::expected<IssuedToken, AuthError> JwtTokens::issue(
    arena::domain::PlayerId player, std::uint64_t epoch) const
{
    const auto issued = now();
    if (!epoch || issued < 0 || issued > maxJsonInteger - m_config.lifetime.count())
        return std::unexpected(AuthError::Unavailable);
    const auto expires = issued + m_config.lifetime.count();
    Object payload;
    const auto string = [&](const char* key, std::string value)
    { (*payload.mutable_fields())[key].set_string_value(std::move(value)); };
    const auto number = [&](const char* key, std::int64_t value)
    { (*payload.mutable_fields())[key].set_number_value(static_cast<double>(value)); };
    string("iss", m_config.issuer);
    string("aud", m_config.audience);
    string("sub", std::to_string(player.value()));
    string("epoch", std::to_string(epoch));
    number("iat", issued);
    number("nbf", issued);
    number("exp", expires);
    std::string json;
    if (!google::protobuf::util::MessageToJsonString(payload, &json).ok())
        return std::unexpected(AuthError::Unavailable);
    auto input = encode64(R"({"alg":"HS256","typ":"JWT"})") + "." + encode64(json);
    const auto signature = mac(input, m_config.key);
    if (!signature)
        return std::unexpected(AuthError::Unavailable);
    return IssuedToken{input + "." + encode64(*signature), {player, epoch, expires}};
}
} // namespace service::auth
