#include <memory>
#include <unordered_map>

#include "net/Session.h"

namespace service
{
class SessionManager
{

private:
    std::unordered_map<SessionID, std::unique_ptr<Session>> _sessions;

public:
    SessionManager() = default;

    ~SessionManager() = default;

    SessionManager(const SessionManager&) = delete;
    SessionManager& operator=(const SessionManager&) = delete;

    SessionManager(SessionManager&&) noexcept = default;
    SessionManager& operator=(SessionManager&&) noexcept = default;

    bool addSession(std::unique_ptr<Session>);
    const Session* findSession(SessionID);
    bool removeSession(SessionID);

    std::size_t size() const noexcept
    {
        return _sessions.size();
    }
};
} // namespace service
