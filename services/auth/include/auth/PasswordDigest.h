#pragma once

#include <expected>
#include <string>
#include <string_view>

namespace service::auth
{
enum class AuthError
{
    InvalidCredentials,
    InvalidToken,
    Unavailable,
    Superseded
};

// Stored digests are opaque, algorithm-versioned values. Production
// adapters must supply a salted password KDF; there is deliberately
// no plain SHA fallback.
class PasswordDigest
{
public:
    virtual ~PasswordDigest() = default;
    virtual std::expected<std::string, AuthError>
    hash(std::string_view password) const = 0;
    virtual std::expected<bool, AuthError> verify(
        std::string_view password, std::string_view storedDigest
    ) const = 0;
};

/** @brief Random-salt PBKDF2-HMAC-SHA256 with bounded work and
 * constant-time equality. */
class Pbkdf2PasswordDigest final : public PasswordDigest
{
public:
    std::expected<std::string, AuthError>
        hash(std::string_view) const override;
    std::expected<bool, AuthError>
        verify(std::string_view, std::string_view) const override;
    /** @brief Validates the algorithm version, rounds and exact
     * salt/key encoding. */
    static bool validEncoding(std::string_view);
};
class UnavailablePasswordDigest final : public PasswordDigest
{
public:
    std::expected<std::string, AuthError>
    hash(std::string_view) const override
    {
        return std::unexpected(AuthError::Unavailable);
    }
    std::expected<bool, AuthError>
    verify(std::string_view, std::string_view) const override
    {
        return std::unexpected(AuthError::Unavailable);
    }
};
} // namespace service::auth
