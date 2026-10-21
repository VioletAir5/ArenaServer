#pragma once

#include "net/LoginHandler.h"
#include "net/PreLoginHandshake.h"

namespace service::gateway
{
// Strict test doubles only; never selected by the application.
class TestPasswordAccounts final
    : public service::auth::PasswordAccounts
{
public:
    std::expected<
        service::auth::PasswordAccount,
        service::auth::AuthError>
    find(std::string_view account) const override
    {
        if (account != "alice")
            return std::unexpected(
                service::auth::AuthError::InvalidCredentials
            );
        return service::auth::PasswordAccount{
            arena::domain::PlayerId{42},
            "test-only-digest"
        };
    }
};
class TestPasswordDigest final : public service::auth::PasswordDigest
{
public:
    std::expected<std::string, service::auth::AuthError>
    hash(std::string_view) const override
    {
        return std::unexpected(service::auth::AuthError::Unavailable);
    }
    std::expected<bool, service::auth::AuthError> verify(
        std::string_view password, std::string_view digest
    ) const override
    {
        return password == "test-password" &&
               digest == "test-only-digest";
    }
};

inline ConnectionHandlerFactory makeAuthTestFactory()
{
    auto passwords = std::make_shared<
        service::auth::LoginAuthenticator>(
        std::make_shared<TestPasswordAccounts>(),
        std::make_shared<TestPasswordDigest>()
    );
    service::auth::JwtConfig config;
    config
        .key = "0123456789abcdef0123456789abcdef"; // Test-only, never
                                                   // a default key.
    config.now = [] { return 1700000000LL; };
    auto routes = makeLoginMessageRegistry(
        std::move(passwords),
        std::make_shared<service::auth::JwtTokens>(std::move(config)),
        std::make_shared<AccountSessions>()
    );
    return [routes](ConnectionContext::Ptr context)
    {
        return makePreLoginHandshakeHandler(
            std::move(context),
            routes
        );
    };
}
} // namespace service::gateway
