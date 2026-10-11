# services/gateway/src/net/ConnectionId.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；11行。

[对应分析](../../../../../04-transports.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
/** @file
 * @brief Allocates process-wide connection identifiers.
 */
#pragma once
#include <cstdint>

namespace service::gateway::detail
{
// Process-wide identity shared by all transport backends.
std::uint64_t nextConnectionId();
} // namespace service::gateway::detail
~~~
