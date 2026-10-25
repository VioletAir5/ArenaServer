#include "auth/FilePasswordAccounts.h"

#include <charconv>
#include <fstream>
#include <stdexcept>
#include <unordered_set>
namespace service::auth
{
FilePasswordAccounts::FilePasswordAccounts(
    const std::filesystem::path& path
)
{
    constexpr std::size_t maxBytes = 8 * 1024 * 1024;
    std::ifstream input(path, std::ios::binary);
    if (!input)
        throw std::invalid_argument("cannot open account file");
    // Cap allocation even when another process grows the file during
    // loading.
    std::string data(maxBytes + 1, '\0');
    input.read(
        data.data(),
        static_cast<std::streamsize>(data.size())
    );
    const auto size = static_cast<std::size_t>(input.gcount());
    if (input.bad() || !size || size > maxBytes)
        throw std::invalid_argument("invalid account file size");
    data.resize(size);
    std::unordered_set<std::uint64_t> players;
    std::string_view remaining(data);
    while (!remaining.empty())
    {
        const auto newline = remaining.find('\n');
        auto row = remaining.substr(0, newline);
        remaining = newline == std::string_view::npos
                        ? std::string_view{}
                        : remaining.substr(newline + 1);
        if (row.ends_with('\r'))
            row.remove_suffix(1);
        const auto first = row.find('\t');
        const auto second = first == std::string_view::npos
                                ? first
                                : row.find('\t', first + 1);
        if (row.size() > 512 || first == std::string_view::npos ||
            second == std::string_view::npos)
            throw std::invalid_argument("invalid account row");
        const auto account = row.substr(0, first),
                   id = row.substr(first + 1, second - first - 1),
                   digest = row.substr(second + 1);
        if (account.empty() || account.size() > 128 ||
            !Pbkdf2PasswordDigest::validEncoding(digest))
            throw std::invalid_argument("invalid account value");
        for (unsigned char c : account)
            if (c < 33 || c > 126)
                throw std::invalid_argument("invalid account name");
        std::uint64_t player = 0;
        const auto p = std::from_chars(
            id.data(),
            id.data() + id.size(),
            player
        );
        if (p.ec != std::errc{} || p.ptr != id.data() + id.size() ||
            !player || m_accounts.size() == 10000 ||
            !players.insert(player).second ||
            !m_accounts
                 .emplace(
                     std::string(account),
                     PasswordAccount{
                         arena::domain::PlayerId(player),
                         std::string(digest)
                     }
                 )
                 .second)
            throw std::invalid_argument(
                "duplicate or invalid account identity"
            );
    }
}
std::expected<PasswordAccount, AuthError>
FilePasswordAccounts::find(std::string_view account) const
{
    const auto i = m_accounts.find(std::string(account));
    if (i == m_accounts.end())
        return std::unexpected(AuthError::InvalidCredentials);
    return i->second;
}
} // namespace service::auth
