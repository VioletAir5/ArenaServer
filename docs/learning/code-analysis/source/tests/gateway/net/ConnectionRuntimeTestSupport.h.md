# tests/gateway/net/ConnectionRuntimeTestSupport.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；63行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#pragma once
#include "net/AsioConnect.h"
#include "net/BackpressureFeature.h"
#include "net/ConnectionRuntime.h"
#include "net/ConnectionSchedulers.h"
#include "net/TcpConnection.h"
namespace service::gateway
{
template <class T>
std::shared_ptr<ConnectionRuntime> testRuntime(
    const std::shared_ptr<T>& connection,
    ConnectionScheduler scheduler,
    ConnectionHandlerFactory handlers = {},
    ConnectionFeatureFactory features = {}
)
{
    auto info = std::make_shared<ConnectionContext>(
        connection->id(),
        InetAddress("127.0.0.1", 1),
        ConnectionClock::now()
    );
    auto runtime = makeConnectionRuntime(
        connection,
        info,
        std::move(scheduler),
        handlers,
        features
    );
    std::weak_ptr<ConnectionRuntime> weak = runtime;
    connection->setOutputCallback(
        [weak](auto bytes)
        {
            if (auto r = weak.lock())
                r->outputChanged(bytes);
        }
    );
    connection->setCloseCallback(
        [weak](auto*)
        {
            if (auto r = weak.lock())
                r->transportClosed();
        }
    );
    if constexpr (std::is_same_v<T, TcpConnection>)
        connection->setMessageCallback(
            [weak](TcpConnection&, std::string_view bytes)
            {
                if (auto r = weak.lock())
                    r->received(std::as_bytes(std::span(bytes)));
            }
        );
    else
        connection->setMessageCallback(
            [weak](IConnection&, auto bytes)
            {
                if (auto r = weak.lock())
                    r->received(bytes);
            }
        );
    runtime->start();
    return runtime;
}
} // namespace service::gateway
~~~
