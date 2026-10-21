#pragma once

#include <memory>

#include "arena/domain/Identity.h"
#include "auth/PasswordDigest.h"

namespace service::auth
{
struct PasswordAccount
{
    arena::domain::PlayerId playerId;
    std::string digest;
};

// Thread-safe lookup adapter. Unknown accounts return
// InvalidCredentials, backend failures return Unavailable. Neither
// path includes account/password in logs.
class PasswordAccounts
{
public:
    virtual ~PasswordAccounts() = default;
    virtual std::expected<PasswordAccount, AuthError>
    find(std::string_view account) const = 0;
};

class UnavailablePasswordAccounts final : public PasswordAccounts
{
public:
    std::expected<PasswordAccount, AuthError>
    find(std::string_view) const override
    {
        return std::unexpected(AuthError::Unavailable);
    }
};

class LoginAuthenticator final
{
public:
    LoginAuthenticator(
        std::shared_ptr<const PasswordAccounts>,
        std::shared_ptr<const PasswordDigest>
    );
    std::expected<arena::domain::PlayerId, AuthError> authenticate(
        std::string_view account, std::string_view password
    ) const;

private:
    const std::shared_ptr<const PasswordAccounts> m_accounts;
    const std::shared_ptr<const PasswordDigest> m_digest;
};
} // namespace service::auth
