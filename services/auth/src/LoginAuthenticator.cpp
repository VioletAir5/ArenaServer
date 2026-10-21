#include "auth/LoginAuthenticator.h"

#include <stdexcept>
#include <utility>

namespace service::auth
{
LoginAuthenticator::LoginAuthenticator(
    std::shared_ptr<const PasswordAccounts> accounts,
    std::shared_ptr<const PasswordDigest> digest
)
    : m_accounts(std::move(accounts))
    , m_digest(std::move(digest))
{
    if (!m_accounts || !m_digest)
        throw std::invalid_argument(
            "password authentication requires adapters"
        );
}

std::expected<arena::domain::PlayerId, AuthError>
LoginAuthenticator::authenticate(
    std::string_view account, std::string_view password
) const
{
    if (account.empty() || account.size() > 128 || password.empty() ||
        password.size() > 1024)
        return std::unexpected(AuthError::InvalidCredentials);
    auto record = m_accounts->find(account);
    if (!record)
        return std::unexpected(record.error());
    if (record->digest.empty() || record->digest.size() > 4096)
        return std::unexpected(AuthError::Unavailable);
    const auto verified = m_digest->verify(password, record->digest);
    if (!verified)
        return std::unexpected(verified.error());
    if (!*verified)
        return std::unexpected(AuthError::InvalidCredentials);
    return record->playerId;
}
} // namespace service::auth
