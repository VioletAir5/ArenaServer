#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include "arena/domain/Identity.h"
#include "auth/PasswordDigest.h"

namespace service::auth
{
struct JwtConfig
{
    std::string key; // At least 32 random bytes; never a password or
                     // a default key.
    std::string issuer{"arena-auth"};
    std::string audience{"arena-gateway"};
    std::chrono::seconds lifetime{std::chrono::minutes(5)};
    std::function<std::int64_t()> now;
};

struct TokenClaims
{
    arena::domain::PlayerId playerId;
    std::uint64_t epoch;
    std::int64_t expiresAt;
};
struct IssuedToken
{
    std::string encoded;
    TokenClaims claims;
};

// Closed HS256 profile: typ=JWT; iss/aud/sub/iat/nbf/exp/epoch
// required. sub and epoch use canonical decimal strings; times are
// JSON integer numbers. No algorithm negotiation, token-selected key
// URLs, or unsigned tokens.
class JwtTokens final
{
public:
    explicit JwtTokens(JwtConfig);
    std::expected<TokenClaims, AuthError>
    verify(std::string_view token) const;
    std::expected<IssuedToken, AuthError>
    issue(arena::domain::PlayerId, std::uint64_t epoch) const;
    std::int64_t now() const
    {
        return m_config.now();
    }
    std::function<std::int64_t()> clock() const
    {
        return m_config.now;
    }

private:
    const JwtConfig m_config;
};
} // namespace service::auth
