# services/gateway/src/net/SessionManager.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；40行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
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
~~~
