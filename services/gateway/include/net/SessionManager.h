/** @file
 * @brief Finds and manages active sessions.
 */
#include <memory>
#include <unordered_map>

#include "net/Session.h"

namespace service::gateway
{
/** @brief Finds and manages active sessions. */
class SessionManager
{

private:
    std::unordered_map<SessionID, std::unique_ptr<Session>> _sessions;

public:
    /** @brief Constructs the session manager. */
    SessionManager() = default;

    /** @brief Releases owned resources. */
    ~SessionManager() = default;

    /** @brief Copy construction is disabled. */
    SessionManager(const SessionManager&) = delete;
    SessionManager& operator=(const SessionManager&) = delete;

    /** @brief Constructs the session manager. */
    SessionManager(SessionManager&&) noexcept = default;
    SessionManager& operator=(SessionManager&&) noexcept = default;

    /** @brief Adds session. */
    bool addSession(std::unique_ptr<Session>);
    /** @brief Finds session. */
    const Session* findSession(SessionID);
    /** @brief Removes session. */
    bool removeSession(SessionID);

    std::size_t size() const noexcept
    {
        return _sessions.size();
    }
};
} // namespace service::gateway
