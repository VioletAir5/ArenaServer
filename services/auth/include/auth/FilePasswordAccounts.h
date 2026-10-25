#pragma once
#include <filesystem>
#include <unordered_map>

#include "auth/LoginAuthenticator.h"
namespace service::auth
{
/** @brief Loads a bounded, immutable account snapshot before workers
 * start. Rows contain account<TAB>player-id<TAB>versioned-digest.
 * Reload needs restart.
 */
class FilePasswordAccounts final : public PasswordAccounts
{
public:
    explicit FilePasswordAccounts(const std::filesystem::path&);
    std::expected<PasswordAccount, AuthError>
        find(std::string_view) const override;

private:
    /// Published read-only; concurrent lookups need no lock.
    std::unordered_map<std::string, PasswordAccount> m_accounts;
};
} // namespace service::auth
