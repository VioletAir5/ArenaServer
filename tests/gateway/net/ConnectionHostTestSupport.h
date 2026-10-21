#pragma once
#include "net/AsioConnect.h"
#include "net/BackpressureFeature.h"
#include "net/ConnectionHost.h"
#include "net/ConnectionSchedulers.h"
#include "net/TcpConnection.h"
namespace service::gateway
{
template <class T>
std::shared_ptr<ConnectionHost> testHost(
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
    auto runtime = makeConnectionHost(
        connection,
        info,
        std::move(scheduler),
        handlers,
        features
    );
    std::weak_ptr<ConnectionHost> weak = runtime;
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
            [weak](ITransport&, auto bytes)
            {
                if (auto r = weak.lock())
                    r->received(bytes);
            }
        );
    runtime->start();
    return runtime;
}
} // namespace service::gateway
