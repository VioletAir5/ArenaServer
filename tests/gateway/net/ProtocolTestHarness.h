#pragma once

#include <gtest/gtest.h>

#include "net/ConnectionHost.h"
#include "protocol/PacketCodec.hpp"

namespace service::gateway::test
{
struct MemoryTransport : ITransport
{
    bool open{true};
    unsigned closes{0};
    std::string output;
    std::uint64_t id() const noexcept override
    {
        return 1;
    }
    void send(std::span<const std::byte> bytes) override
    {
        output.append(
            reinterpret_cast<const char*>(bytes.data()),
            bytes.size()
        );
    }
    void shutdown() override
    {
    }
    void close() override
    {
        open = false;
        ++closes;
    }
    bool connected() const noexcept override
    {
        return open;
    }
    void pauseRead() override
    {
    }
    void resumeRead() override
    {
    }
    std::size_t pendingBytes() const noexcept override
    {
        return 0;
    }
};

struct ProtocolTest : testing::Test
{
    RejectionCounters::Ptr
        counters = std::make_shared<RejectionCounters>();
    ConnectionContext::Ptr
        context = std::make_shared<ConnectionContext>(
            1,
            InetAddress("127.0.0.1", 1),
            ConnectionClock::now(),
            counters
        );
    std::shared_ptr<MemoryTransport>
        transport = std::make_shared<MemoryTransport>();
    std::vector<std::function<void()>> posts;
    std::vector<std::function<void()>> deadlines;
    std::shared_ptr<ConnectionHost> host;

    void start(ConnectionHandler handler)
    {
        host = std::make_shared<ConnectionHost>(
            transport,
            context,
            ConnectionScheduler{
                [this](auto task)
                { posts.push_back(std::move(task)); },
                [this](auto, auto task)
                {
                    deadlines.push_back(std::move(task));
                    return ConnectionTimer{};
                }
            },
            std::move(handler)
        );
        host->start();
    }

    static std::string
    frame(std::string_view payload, std::uint16_t type = 1)
    {
        const auto result = protocol::PacketCodec::encode(
            type,
            7,
            std::as_bytes(std::span(payload))
        );
        return {result->peek(), result->readableBytes()};
    }
    void receive(std::string_view input)
    {
        host->received(std::as_bytes(std::span(input)));
    }
};
} // namespace service::gateway::test
