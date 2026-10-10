/** @file
 * @brief Finds and manages active sessions.
 */
#include "net/SessionManager.h"

namespace service::gateway
{

bool SessionManager::addSession(std::unique_ptr<Session> session)
{
    if (!session)
    {
        return false;
    }

    const SessionID id = session->id();

    auto [it, inserted] = _sessions.emplace(id, std::move(session));

    return inserted;
}

const Session* SessionManager::findSession(SessionID id)
{
    auto it = _sessions.find(id);

    if (it == _sessions.end())
    {
        return nullptr;
    }

    return it->second.get();
}

bool SessionManager::removeSession(SessionID id)
{
    return _sessions.erase(id) > 0;
}

} // namespace service::gateway
