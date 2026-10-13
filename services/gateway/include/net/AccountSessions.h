#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "auth/JwtTokens.h"

namespace service::gateway
{
struct SessionBinding
{
    arena::domain::PlayerId playerId;
    std::uint64_t connectionId;
    std::uint64_t epoch;
};
class AccountSessions;

class SessionLease final
{
public:
    const SessionBinding binding;
    bool current() const;
    void release() noexcept;

private:
    friend class AccountSessions;
    SessionLease(SessionBinding, std::int64_t, std::function<std::int64_t()>,
                 std::function<void()>, std::weak_ptr<AccountSessions>);
    const std::int64_t m_expiresAt;
    const std::function<std::int64_t()> m_now;
    const std::function<void()> m_kick;
    const std::weak_ptr<AccountSessions> m_directory;
    std::atomic<bool> m_valid{true};
};
struct SessionGrant
{
    std::shared_ptr<SessionLease> lease;
    std::string token;
};

// Shared by every listener/worker in one gateway process. Only the short
// replacement/conditional-release transaction holds m_mutex; kick runs after it.
// A disconnected record retains epoch until its JWT expires, permitting resume.
class AccountSessions final : public std::enable_shared_from_this<AccountSessions>
{
public:
    explicit AccountSessions(std::size_t maxAccounts = 100000);
    std::expected<SessionGrant, service::auth::AuthError> install(
        arena::domain::PlayerId, std::uint64_t connectionId,
        std::optional<service::auth::TokenClaims> resume,
        const service::auth::JwtTokens&, std::function<void()> kick);
    void detach(SessionBinding) noexcept;
    std::size_t activeCount() const;
    std::optional<std::uint64_t> currentEpoch(arena::domain::PlayerId) const;

private:
    struct Entry
    {
        std::uint64_t epoch;
        std::int64_t expiresAt;
        std::shared_ptr<SessionLease> active;
    };
    const std::size_t m_maxAccounts;
    mutable std::mutex m_mutex;
    std::unordered_map<std::uint64_t, Entry> m_accounts;
    // Random boot seed, then process-wide monotonic epoch. Erasing/recreating an
    // account never resets it; separate processes still require a shared authority.
    std::uint64_t m_epoch{0};
};
} // namespace service::gateway
