#include "auth/PasswordDigest.h"

#include <array>
#include <charconv>
#include <optional>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
namespace service::auth
{
namespace
{
constexpr std::string_view prefix = "$arena-pbkdf2-sha256$";
constexpr int defaultRounds = 600000;
using Salt = std::array<unsigned char, 16>;
using Key = std::array<unsigned char, 32>;
struct Encoded
{
    int rounds;
    Salt salt;
    Key key;
};
int nibble(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    return -1;
}
template <std::size_t N>
bool decode(std::string_view s, std::array<unsigned char, N>& b)
{
    if (s.size() != 2 * N)
        return false;
    for (std::size_t i = 0; i < N; ++i)
    {
        const int h = nibble(s[2 * i]), l = nibble(s[2 * i + 1]);
        if (h < 0 || l < 0)
            return false;
        b[i] = static_cast<unsigned char>((h << 4) | l);
    }
    return true;
}
std::optional<Encoded> parse(std::string_view s)
{
    if (!s.starts_with(prefix) || s.size() > 160)
        return {};
    s.remove_prefix(prefix.size());
    const auto split = s.find('$');
    if (split == std::string_view::npos)
        return {};
    Encoded e{};
    const auto rounds = s.substr(0, split);
    const auto p = std::from_chars(
        rounds.data(),
        rounds.data() + rounds.size(),
        e.rounds
    );
    if (rounds.empty() || rounds.front() == '0' ||
        p.ec != std::errc{} ||
        p.ptr != rounds.data() + rounds.size() || e.rounds < 100000 ||
        e.rounds > 2000000)
        return {};
    s.remove_prefix(split + 1);
    if (s.size() != 97 || s[32] != '$' ||
        !decode(s.substr(0, 32), e.salt) ||
        !decode(s.substr(33), e.key))
        return {};
    return e;
}
template <std::size_t N>
std::string hex(const std::array<unsigned char, N>& b)
{
    constexpr char alphabet[] = "0123456789abcdef";
    std::string s(2 * N, '0');
    for (std::size_t i = 0; i < N; ++i)
    {
        s[2 * i] = alphabet[b[i] >> 4];
        s[2 * i + 1] = alphabet[b[i] & 15];
    }
    return s;
}
bool derive(
    std::string_view p, const Salt& salt, int rounds, Key& key
)
{
    return PKCS5_PBKDF2_HMAC(
               p.data(),
               static_cast<int>(p.size()),
               salt.data(),
               static_cast<int>(salt.size()),
               rounds,
               EVP_sha256(),
               static_cast<int>(key.size()),
               key.data()
           ) == 1;
}
} // namespace
bool Pbkdf2PasswordDigest::validEncoding(std::string_view d)
{
    return parse(d).has_value();
}
std::expected<std::string, AuthError>
Pbkdf2PasswordDigest::hash(std::string_view password) const
{
    if (password.empty() || password.size() > 1024)
        return std::unexpected(AuthError::InvalidCredentials);
    Salt salt{};
    Key key{};
    if (RAND_bytes(salt.data(), static_cast<int>(salt.size())) != 1 ||
        !derive(password, salt, defaultRounds, key))
        return std::unexpected(AuthError::Unavailable);
    auto result = std::string(prefix) +
                  std::to_string(defaultRounds) + "$" + hex(salt) +
                  "$" + hex(key);
    OPENSSL_cleanse(key.data(), key.size());
    return result;
}
std::expected<bool, AuthError> Pbkdf2PasswordDigest::verify(
    std::string_view password, std::string_view stored
) const
{
    if (password.empty() || password.size() > 1024)
        return std::unexpected(AuthError::InvalidCredentials);
    const auto e = parse(stored);
    if (!e)
        return std::unexpected(AuthError::Unavailable);
    Key key{};
    if (!derive(password, e->salt, e->rounds, key))
        return std::unexpected(AuthError::Unavailable);
    const bool equal = CRYPTO_memcmp(
                           key.data(),
                           e->key.data(),
                           key.size()
                       ) == 0;
    OPENSSL_cleanse(key.data(), key.size());
    return equal;
}
} // namespace service::auth
