#include "net/AccountSessions.h"

#include <limits>
#include <stdexcept>
#include <utility>

#include <openssl/rand.h>
#include <spdlog/spdlog.h>

namespace service::gateway
{
SessionLease::SessionLease(
    SessionBinding id,
    std::int64_t expires,
    std::function<std::int64_t()> now,
    std::function<void()> kick,
    std::weak_ptr<AccountSessions> directory
)
    : binding(id)
    , m_expiresAt(expires)
    , m_now(std::move(now))
    , m_kick(std::move(kick))
    , m_directory(std::move(directory))
{
}

bool SessionLease::current() const
{
    return m_valid.load(std::memory_order_acquire) &&
           m_now() < m_expiresAt;
}

void SessionLease::release() noexcept
{
    m_valid.store(false, std::memory_order_release);
    if (auto directory = m_directory.lock())
        directory->detach(binding);
}

AccountSessions::AccountSessions(std::size_t maximum)
    : m_maxAccounts(maximum)
{
    if (!maximum)
        throw std::invalid_argument(
            "account session capacity must be positive"
        );
    if (RAND_bytes(
            reinterpret_cast<unsigned char*>(&m_epoch),
            sizeof(m_epoch)
        ) != 1)
        throw std::runtime_error("session epoch entropy unavailable");
    m_epoch &= (std::uint64_t{1} << 63) -
               1; // Reserve growth space; no reset-to-zero ABA on
                  // restart.
}

std::expected<SessionGrant, service::auth::AuthError>
AccountSessions::install(
    arena::domain::PlayerId player,
    std::uint64_t connection,
    std::optional<service::auth::TokenClaims> resume,
    const service::auth::JwtTokens& tokens,
    std::function<void()> kick
)
{
    using service::auth::AuthError;
    if (!connection || !kick || weak_from_this().expired())
        throw std::invalid_argument(
            "session install requires owned directory and closer"
        );
    std::shared_ptr<SessionLease> old;
    SessionGrant result;
    {
        std::lock_guard lock(m_mutex);
        const auto now = tokens.now();
        auto found = m_accounts.find(player.value());
        if (resume && (resume->playerId != player ||
                       found == m_accounts.end() ||
                       found->second.epoch != resume->epoch ||
                       found->second.expiresAt != resume->expiresAt ||
                       now >= resume->expiresAt))
            return std::unexpected(AuthError::Superseded);
        if (found == m_accounts.end() &&
            m_accounts.size() >= m_maxAccounts)
        {
            for (auto at = m_accounts.begin();
                 at != m_accounts.end();)
            {
                if (at->second.expiresAt <= now)
                {
                    if (at->second.active)
                        at->second.active->m_valid.store(
                            false,
                            std::memory_order_release
                        );
                    at = m_accounts.erase(at);
                }
                else
                    ++at;
            }
            if (m_accounts.size() >= m_maxAccounts)
                return std::unexpected(AuthError::Unavailable);
        }
        if (m_epoch == std::numeric_limits<std::uint64_t>::max())
            return std::unexpected(AuthError::Unavailable);
        const auto next = m_epoch + 1;
        // Prepare token/lease and allocate the map entry before
        // revoking old state.
        auto token = tokens.issue(player, next);
        if (!token)
            return std::unexpected(token.error());
        const auto expires = token->claims.expiresAt;
        auto lease = std::shared_ptr<SessionLease>(new SessionLease(
            {player, connection, next},
            expires,
            tokens.clock(),
            std::move(kick),
            weak_from_this()
        ));
        result = SessionGrant{
            std::move(lease),
            std::move(token->encoded)
        };
        auto [entry, inserted] = m_accounts.try_emplace(
            player.value(),
            Entry{next, expires, {}}
        );
        (void)inserted;
        old = entry->second.active;
        if (old)
            old->m_valid.store(false, std::memory_order_release);
        entry->second = Entry{next, expires, result.lease};
        m_epoch = next; // Replacement linearizes here while holding
                        // the directory lock.
    }
    if (old)
    {
        try
        {
            old->m_kick();
        }
        catch (...)
        {
            try
            {
                spdlog::warn(
                    "event=session_kick_failed player_id={} epoch={}",
                    old->binding.playerId.value(),
                    old->binding.epoch
                );
            }
            catch (...)
            {
            }
        }
    }
    try
    {
        spdlog::info(
            "event=session_install player_id={} connection_id={} "
            "epoch={}",
            player.value(),
            connection,
            result.lease->binding.epoch
        );
    }
    catch (...)
    {
    } // Logging cannot turn a committed replacement into a failed
      // login.
    return result;
}

void AccountSessions::detach(SessionBinding binding) noexcept
{
    std::lock_guard lock(m_mutex);
    auto found = m_accounts.find(binding.playerId.value());
    if (found == m_accounts.end() ||
        found->second.epoch != binding.epoch ||
        !found->second.active ||
        found->second.active->binding.connectionId !=
            binding.connectionId)
        return;
    found->second.active->m_valid.store(
        false,
        std::memory_order_release
    );
    found->second.active.reset();
}

std::size_t AccountSessions::activeCount() const
{
    std::lock_guard lock(m_mutex);
    std::size_t count = 0;
    for (const auto& [id, entry] : m_accounts)
    {
        (void)id;
        if (entry.active && entry.active->current())
            ++count;
    }
    return count;
}

std::optional<std::uint64_t>
AccountSessions::currentEpoch(arena::domain::PlayerId player) const
{
    std::lock_guard lock(m_mutex);
    const auto found = m_accounts.find(player.value());
    if (found == m_accounts.end())
        return {};
    return found->second.epoch;
}
} // namespace service::gateway
