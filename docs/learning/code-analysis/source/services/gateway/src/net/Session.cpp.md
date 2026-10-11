# services/gateway/src/net/Session.cpp

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；15行。

[对应分析](../../../../../05-runtime-routing.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Stores authenticated session identity.
 */
#include <net/Session.h>

#include <utility>

namespace service::gateway
{
Session::Session(SessionID id, Socket&& socket)
    : _id{id}
    , _socket(std::move(socket))
{
}
} // namespace service::gateway
~~~
