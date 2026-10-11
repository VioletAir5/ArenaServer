# services/gateway/include/net/SessionManager.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；45行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
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
~~~
