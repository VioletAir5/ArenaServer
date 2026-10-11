# tests/gateway/net/ConnectionContextTestHandler.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；61行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#pragma once

#include <chrono>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

#include "net/ConnectionHandler.h"

// Test-only raw command handler. Report actual socket metadata and
// exercise session state on the same owner on which the factory was
// invoked.
inline service::gateway::ConnectionHandlerFactory
makeContextTestFactory()
{
    using namespace service::gateway;
    const auto baseOwner = std::this_thread::get_id();
    return [baseOwner](ConnectionContext::Ptr context)
    {
        const auto owner = std::this_thread::get_id();
        if (context->session->loginState() !=
            ConnectionSession::LoginState::Pending)
            throw std::runtime_error(
                "new connection session is not pending"
            );
        return [context = std::move(context), owner, baseOwner](
                   IConnection& connection,
                   std::span<const std::byte> bytes
               )
        {
            if (owner != std::this_thread::get_id() ||
                connection.id() != context->connectionId)
                throw std::runtime_error(
                    "factory/callback owner or identity mismatch"
                );
            if (bytes.size() != 1)
                throw std::runtime_error("expected one test command");
            const auto command = std::to_integer<char>(bytes.front());
            if (command == 'A')
                context->session->markAuthenticated();
            if (command == 'E')
                context->session->expireLogin();
            const auto age = std::chrono::duration_cast<
                                 std::chrono::milliseconds>(
                                 ConnectionContext::Clock::now() -
                                 context->acceptedAt
            )
                                 .count();
            std::ostringstream output;
            output << context->connectionId << '|'
                   << context->peer.ip() << '|'
                   << context->peer.port() << '|' << age << '|'
                   << (owner != baseOwner) << '|'
                   << static_cast<int>(context->session->loginState())
                   << '\n';
            const auto response = output.str();
            connection.send(std::as_bytes(std::span(response)));
        };
    };
}
~~~
