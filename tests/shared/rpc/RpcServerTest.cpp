#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <gtest/gtest.h>

#include "rpc/Server.h"

namespace arena::internal_rpc
{
namespace
{
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Tcp = boost::asio::ip::tcp;

bool until(
    const std::function<bool()>& ready,
    std::chrono::milliseconds timeout = 2000ms
)
{
    const auto end = Clock::now() + timeout;
    do
    {
        if (ready())
            return true;
        std::this_thread::sleep_for(1ms);
    } while (Clock::now() < end);
    return ready();
}

std::size_t descriptors()
{
    return std::distance(
        std::filesystem::directory_iterator("/proc/self/fd"),
        std::filesystem::directory_iterator{}
    );
}

Server::Config config()
{
    Server::Config result;
    result.service = "auth";
    result.callerTokens = {{"lobby", std::string(32, 's')}};
    return result;
}

arena::rpc::Request request(std::chrono::milliseconds budget = 1000ms)
{
    arena::rpc::Request result;
    result.set_request_id(71);
    result.set_correlation_id("server-contract");
    result.set_method("auth.echo");
    result.set_service_token(std::string(32, 's'));
    result.set_protocol_version(protocolVersion);
    result.set_caller_service("lobby");
    result.set_target_service("auth");
    result.add_service_path("lobby");
    result.set_deadline_unix_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        )
            .count() +
        budget.count()
    );
    return result;
}

arena::rpc::Response
response(arena::rpc::Status status = arena::rpc::OK)
{
    arena::rpc::Response result;
    result.set_status(status);
    return result;
}

Server::Handlers echo(std::atomic<int>* calls = nullptr)
{
    return {
        {"auth.echo",
         [calls](auto input, auto, auto complete)
             -> Server::Cancellation
         {
             if (calls)
                 ++*calls;
             auto result = response();
             result.set_request_id(999);
             result.set_correlation_id("must-not-escape");
             result.set_protocol_version(99);
             result.set_payload(input.payload());
             complete(result);
             complete(result);
             return {};
         }}
    };
}

struct RunningServer
{
    explicit RunningServer(
        Server::Config configured,
        Server::Handlers handlers,
        int threads = 1
    )
        : work(boost::asio::make_work_guard(io))
        , server(
              std::make_unique<Server>(
                  io.get_executor(),
                  std::move(configured),
                  std::move(handlers)
              )
          )
    {
        for (int index = 0; index < threads; ++index)
            workers.emplace_back([this] { io.run(); });
    }
    ~RunningServer()
    {
        if (release)
            release();
        if (server)
        {
            server->stop();
            EXPECT_TRUE(until([&]
                              { return server->stats().stopped; }));
            server.reset();
        }
        work.reset();
        io.stop();
        for (auto& worker : workers)
            worker.join();
        io.restart();
        io.poll();
    }
    std::uint16_t port() const
    {
        return server->port();
    }
    boost::asio::io_context io;
    boost::asio::executor_work_guard<
        boost::asio::io_context::executor_type>
        work;
    std::unique_ptr<Server> server;
    std::vector<std::thread> workers;
    std::function<void()> release;
};

struct RawPeer
{
    explicit RawPeer(std::uint16_t port, bool smallWindow = false)
        : socket(io)
    {
        socket.open(Tcp::v4());
        if (smallWindow)
            socket.set_option(
                boost::asio::socket_base::receive_buffer_size(1024)
            );
        socket.connect(
            {boost::asio::ip::address_v4::loopback(), port}
        );
    }
    void send(const arena::rpc::Request& value, bool fragment = false)
    {
        sendBody(value.SerializeAsString(), fragment);
    }
    void sendBody(const std::string& body, bool fragment = false)
    {
        std::string bytes(4, '\0');
        const auto size = static_cast<std::uint32_t>(body.size());
        for (unsigned index = 0; index != 4; ++index)
            bytes[index] = static_cast<char>(
                size >> (24 - 8 * index)
            );
        bytes += body;
        if (!fragment)
            boost::asio::write(socket, boost::asio::buffer(bytes));
        else
            for (std::size_t index = 0; index < bytes.size();
                 index += 7)
                boost::asio::write(
                    socket,
                    boost::asio::buffer(
                        bytes.data() + index,
                        std::min<std::size_t>(7, bytes.size() - index)
                    )
                );
    }
    bool readBytes(char* target, std::size_t length)
    {
        socket.non_blocking(true);
        std::size_t used = 0;
        bool failed = false;
        const bool done = until(
            [&]
            {
                boost::system::error_code error;
                used += socket.read_some(
                    boost::asio::buffer(target + used, length - used),
                    error
                );
                if (error &&
                    error != boost::asio::error::would_block &&
                    error != boost::asio::error::try_again)
                    failed = true;
                return used == length || failed;
            }
        );
        return done && !failed && used == length;
    }
    std::optional<arena::rpc::Response> receive()
    {
        std::array<unsigned char, 4> header{};
        if (!readBytes(
                reinterpret_cast<char*>(header.data()),
                header.size()
            ))
            return {};
        std::uint32_t size = 0;
        for (const auto byte : header)
            size = (size << 8) | byte;
        if (!size || size > Client::maxFrame)
            return {};
        std::string body(size, '\0');
        arena::rpc::Response result;
        if (!readBytes(body.data(), body.size()) ||
            !result.ParseFromString(body))
            return {};
        return result;
    }
    bool eof()
    {
        socket.non_blocking(true);
        return until(
            [&]
            {
                char byte;
                boost::system::error_code error;
                socket.read_some(
                    boost::asio::buffer(&byte, 1),
                    error
                );
                return error &&
                       error != boost::asio::error::would_block &&
                       error != boost::asio::error::try_again;
            }
        );
    }
    void close()
    {
        boost::system::error_code ignored;
        socket.close(ignored);
    }
    boost::asio::io_context io;
    Tcp::socket socket;
};

arena::rpc::Response nativeCall(
    RunningServer& server,
    arena::rpc::Request input,
    std::chrono::milliseconds timeout = 1000ms
)
{
    Client client(
        server.io.get_executor(),
        {boost::asio::ip::address_v4::loopback(), server.port()}
    );
    auto promise = std::make_shared<
        std::promise<arena::rpc::Response>>();
    auto future = promise->get_future();
    Options options;
    options.timeout = timeout;
    auto accepted = client.call(
        std::move(input),
        options,
        [promise](auto output)
        { promise->set_value(std::move(output)); }
    );
    if (!accepted)
        return response(accepted.error());
    if (future.wait_for(2s) != std::future_status::ready)
    {
        ADD_FAILURE() << "native callback did not complete";
        return response(arena::rpc::INTERNAL);
    }
    return future.get();
}

TEST(
    RpcServerTest,
    InvalidConfigurationAndBindFailureReleaseDescriptors
)
{
    const auto baseline = descriptors();
    {
        boost::asio::io_context io;
        auto invalid = config();
        invalid.service = "Auth";
        EXPECT_THROW(
            Server(io.get_executor(), invalid, echo()),
            std::invalid_argument
        );
        invalid = config();
        invalid.callerTokens.clear();
        EXPECT_THROW(
            Server(io.get_executor(), invalid, echo()),
            std::invalid_argument
        );
        invalid = config();
        invalid.maxActive = 0;
        EXPECT_THROW(
            Server(io.get_executor(), invalid, echo()),
            std::invalid_argument
        );
        Tcp::acceptor occupied(
            io,
            {boost::asio::ip::address_v4::loopback(), 0}
        );
        invalid = config();
        invalid.endpoint = occupied.local_endpoint();
        EXPECT_THROW(
            Server(io.get_executor(), invalid, echo()),
            boost::system::system_error
        );
    }
    EXPECT_EQ(descriptors(), baseline);
}

TEST(
    RpcServerTest,
    FragmentedWireAndNativeClientPreserveFrameworkMetadata
)
{
    std::atomic<int> calls{0};
    auto handlers = echo(&calls);
    handlers["auth.context"] =
        [](auto input,
           Context context,
           auto complete) -> Server::Cancellation
    {
        EXPECT_TRUE(input.service_token().empty());
        EXPECT_EQ(
            context.servicePath,
            (std::vector<std::string>{"gateway", "lobby", "auth"})
        );
        EXPECT_EQ(context.correlationId, input.correlation_id());
        EXPECT_LE(context.deadline - Clock::now(), 1000ms);
        complete(response());
        return {};
    };
    RunningServer server(config(), std::move(handlers));
    RawPeer peer(server.port());
    auto input = request();
    input.set_payload(std::string(12000, 'p'));
    peer.send(input, true);
    const auto output = peer.receive();
    ASSERT_TRUE(output);
    EXPECT_EQ(output->status(), arena::rpc::OK);
    EXPECT_EQ(output->request_id(), input.request_id());
    EXPECT_EQ(output->correlation_id(), input.correlation_id());
    EXPECT_EQ(output->protocol_version(), protocolVersion);
    EXPECT_EQ(output->payload(), input.payload());
    EXPECT_EQ(nativeCall(server, input).payload(), input.payload());
    input = request();
    input.set_method("auth.context");
    input.clear_service_path();
    input.add_service_path("gateway");
    input.add_service_path("lobby");
    EXPECT_EQ(nativeCall(server, input).status(), arena::rpc::OK);
    ASSERT_TRUE(
        until([&] { return server.server->stats().active == 0; })
    );
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(server.server->stats().completed, 3);
}

TEST(
    RpcServerTest,
    StrictVersionRolePathAndMethodErrorsNeverEnterBusiness
)
{
    std::atomic<int> calls{0};
    RunningServer server(config(), echo(&calls));
    std::vector<std::pair<arena::rpc::Request, arena::rpc::Status>>
        cases;
    auto add = [&](auto mutate, arena::rpc::Status status)
    {
        auto value = request();
        mutate(value);
        cases.emplace_back(std::move(value), status);
    };
    add([](auto& value) { value.set_protocol_version(0); },
        arena::rpc::INCOMPATIBLE_VERSION);
    add([](auto& value) { value.set_protocol_version(2); },
        arena::rpc::INCOMPATIBLE_VERSION);
    add([](auto& value)
        { value.set_service_token(std::string(32, 'z')); },
        arena::rpc::UNAUTHENTICATED);
    add([](auto& value) { value.set_caller_service("gateway"); },
        arena::rpc::UNAUTHENTICATED);
    add([](auto& value) { value.set_target_service("lobby"); },
        arena::rpc::INVALID_ARGUMENT);
    add([](auto& value) { value.add_service_path("lobby"); },
        arena::rpc::CALL_CYCLE);
    add(
        [](auto& value)
        {
            value.add_service_path("auth");
            value.add_service_path("lobby");
        },
        arena::rpc::CALL_CYCLE
    );
    add([](auto& value) { value.clear_service_path(); },
        arena::rpc::INVALID_ARGUMENT);
    add([](auto& value) { value.add_service_path("gateway"); },
        arena::rpc::INVALID_ARGUMENT);
    add(
        [](auto& value)
        {
            value.clear_service_path();
            value.add_service_path("invalid node");
            value.add_service_path("lobby");
        },
        arena::rpc::INVALID_ARGUMENT
    );
    add(
        [](auto& value)
        {
            value.clear_service_path();
            for (int index = 0; index != 7; ++index)
                value.add_service_path(
                    "node-" + std::to_string(index)
                );
            value.add_service_path("lobby");
        },
        arena::rpc::CALL_CYCLE
    );
    add([](auto& value)
        { value.set_correlation_id(std::string(3000, 'z')); },
        arena::rpc::INVALID_ARGUMENT);
    add([](auto& value) { value.set_method("auth.missing"); },
        arena::rpc::NOT_FOUND);
    add([](auto& value)
        { value.set_idempotency_key(std::string(1, '\0')); },
        arena::rpc::INVALID_ARGUMENT);
    add([](auto& value) { value.set_deadline_unix_ms(1); },
        arena::rpc::DEADLINE_EXCEEDED);
    for (const auto& [input, expected] : cases)
    {
        RawPeer peer(server.port());
        peer.send(input);
        const auto output = peer.receive();
        ASSERT_TRUE(output) << expected;
        EXPECT_EQ(output->status(), expected);
        EXPECT_EQ(output->protocol_version(), protocolVersion);
        EXPECT_TRUE(output->payload().empty());
    }
    EXPECT_EQ(calls, 0);
    EXPECT_EQ(server.server->stats().active, 0);
    EXPECT_EQ(server.server->stats().rejected, cases.size());
}

TEST(RpcServerTest, DeadlineRetainsPhysicalLoanUntilForeignCompletion)
{
    Server::Completion pending;
    std::mutex mutex;
    std::atomic<int> calls{0}, cancellations{0};
    auto configured = config();
    configured.maxActive = 1;
    RunningServer server(
        configured,
        {{"auth.echo",
          [&](auto, auto, auto complete) -> Server::Cancellation
          {
              if (calls++ == 0)
              {
                  std::lock_guard lock(mutex);
                  pending = std::move(complete);
                  return [&] { ++cancellations; };
              }
              complete(response());
              return {};
          }}}
    );
    server.release = [&]
    {
        std::lock_guard lock(mutex);
        if (pending)
            pending(response(arena::rpc::CANCELLED));
    };
    EXPECT_EQ(
        nativeCall(server, request(30ms), 30ms).status(),
        arena::rpc::DEADLINE_EXCEEDED
    );
    ASSERT_TRUE(until([&] { return cancellations.load() == 1; }));
    EXPECT_EQ(server.server->stats().active, 1);
    EXPECT_EQ(
        nativeCall(server, request()).status(),
        arena::rpc::OVERLOADED
    );
    EXPECT_EQ(calls, 1);
    std::thread finished(
        [&]
        {
            std::lock_guard lock(mutex);
            pending(response());
            pending(response());
        }
    );
    finished.join();
    ASSERT_TRUE(
        until([&] { return server.server->stats().active == 0; })
    );
    EXPECT_EQ(nativeCall(server, request()).status(), arena::rpc::OK);
    EXPECT_EQ(server.server->stats().accepted, 2);
    EXPECT_EQ(server.server->stats().completed, 2);
    EXPECT_EQ(server.server->stats().cancelled, 1);
}

TEST(
    RpcServerTest,
    SynchronousCompletionAndThrowingCancellationRemainSingleOwner
)
{
    std::atomic<int> cancellations{0};
    auto routes = echo();
    routes["auth.immediate"] =
        [&](auto, auto, auto complete) -> Server::Cancellation
    {
        complete(response());
        return [&] { ++cancellations; };
    };
    routes["auth.throw"] =
        [](auto, auto, auto) -> Server::Cancellation
    {
        throw std::runtime_error(
            "private diagnostic must not escape"
        );
    };
    routes["auth.cancel"] =
        [&](auto, auto, auto complete) -> Server::Cancellation
    {
        return [&, complete]
        {
            ++cancellations;
            complete(response(arena::rpc::CANCELLED));
            throw std::runtime_error("cancel cleanup diagnostic");
        };
    };
    RunningServer server(config(), std::move(routes));
    auto input = request();
    input.set_method("auth.immediate");
    EXPECT_EQ(nativeCall(server, input).status(), arena::rpc::OK);
    EXPECT_EQ(cancellations, 0);
    input.set_method("auth.throw");
    EXPECT_EQ(
        nativeCall(server, input).status(),
        arena::rpc::INTERNAL
    );
    input.set_method("auth.cancel");
    EXPECT_EQ(
        nativeCall(server, input, 25ms).status(),
        arena::rpc::DEADLINE_EXCEEDED
    );
    ASSERT_TRUE(
        until([&] { return server.server->stats().active == 0; })
    );
    EXPECT_EQ(cancellations, 1);
}

TEST(RpcServerTest, DisconnectAndRepeatedStopWaitForActualCleanup)
{
    Server::Completion pending;
    std::mutex mutex;
    std::atomic<int> cancellations{0};
    RunningServer server(
        config(),
        {{"auth.echo",
          [&](auto, auto, auto complete) -> Server::Cancellation
          {
              std::lock_guard lock(mutex);
              pending = std::move(complete);
              return [&] { ++cancellations; };
          }}}
    );
    server.release = [&]
    {
        std::lock_guard lock(mutex);
        if (pending)
            pending(response());
    };
    RawPeer peer(server.port());
    peer.send(request());
    ASSERT_TRUE(
        until([&] { return server.server->stats().active == 1; })
    );
    peer.close();
    ASSERT_TRUE(until([&] { return cancellations.load() == 1; }));
    std::vector<std::thread> stoppers;
    for (int index = 0; index != 8; ++index)
        stoppers.emplace_back([&] { server.server->stop(0ms); });
    for (auto& stopper : stoppers)
        stopper.join();
    EXPECT_TRUE(server.server->stats().stopping);
    EXPECT_FALSE(server.server->stats().stopped);
    EXPECT_EQ(server.server->stats().active, 1);
    {
        std::lock_guard lock(mutex);
        pending(response());
        pending(response());
    }
    ASSERT_TRUE(until([&]
                      { return server.server->stats().stopped; }));
    EXPECT_EQ(cancellations, 1);
    EXPECT_EQ(server.server->stats().completed, 1);
}

TEST(RpcServerTest, StopGraceAllowsAdmittedResponseAndClosesIdlePeers)
{
    Server::Completion pending;
    std::mutex mutex;
    std::atomic<int> cancellations{0};
    RunningServer server(
        config(),
        {{"auth.echo",
          [&](auto, auto, auto complete) -> Server::Cancellation
          {
              std::lock_guard lock(mutex);
              pending = std::move(complete);
              return [&] { ++cancellations; };
          }}}
    );
    server.release = [&]
    {
        std::lock_guard lock(mutex);
        if (pending)
            pending(response());
    };
    RawPeer idle(server.port());
    RawPeer peer(server.port());
    peer.send(request());
    ASSERT_TRUE(until(
        [&]
        {
            return server.server->stats().active == 1 &&
                   server.server->stats().peers == 2;
        }
    ));
    server.server->stop(500ms);
    ASSERT_TRUE(idle.eof());
    {
        std::lock_guard lock(mutex);
        pending(response());
    }
    const auto output = peer.receive();
    ASSERT_TRUE(output);
    EXPECT_EQ(output->status(), arena::rpc::OK);
    ASSERT_TRUE(until([&]
                      { return server.server->stats().stopped; }));
    EXPECT_EQ(cancellations, 0);
}

TEST(
    RpcServerTest, SlowAndMalformedPeersAreBoundedAndCapacityRecovers
)
{
    auto configured = config();
    configured.maxPeers = 2;
    configured.readTimeout = 40ms;
    RunningServer server(configured, echo());
    RawPeer first(server.port()), second(server.port());
    const std::string partial(2, '\0');
    boost::asio::write(first.socket, boost::asio::buffer(partial));
    boost::asio::write(second.socket, boost::asio::buffer(partial));
    ASSERT_TRUE(until([&]
                      { return server.server->stats().peers == 2; }));
    RawPeer overflow(server.port());
    EXPECT_TRUE(overflow.eof());
    EXPECT_TRUE(first.eof());
    EXPECT_TRUE(second.eof());
    ASSERT_TRUE(until([&]
                      { return server.server->stats().peers == 0; }));
    {
        RawPeer oversized(server.port());
        const std::array<unsigned char, 4> header{0, 4, 0, 1};
        boost::asio::write(
            oversized.socket,
            boost::asio::buffer(header)
        );
        EXPECT_TRUE(oversized.eof());
    }
    {
        RawPeer malformed(server.port());
        malformed.sendBody(std::string(1, static_cast<char>(0x80)));
        EXPECT_TRUE(malformed.eof());
    }
    EXPECT_EQ(nativeCall(server, request()).status(), arena::rpc::OK);
    EXPECT_EQ(server.server->stats().active, 0);
}

TEST(
    RpcServerTest,
    DestructionAndRetainedDuplicateCompletionReleaseDescriptors
)
{
    const auto baseline = descriptors();
    Server::Completion retained;
    std::mutex mutex;
    {
        RunningServer server(
            config(),
            {{"auth.echo",
              [&](auto, auto, auto complete) -> Server::Cancellation
              {
                  std::lock_guard lock(mutex);
                  retained = std::move(complete);
                  return [] {};
              }}}
        );
        server.release = [&]
        {
            std::lock_guard lock(mutex);
            if (retained)
                retained(response());
        };
        RawPeer peer(server.port());
        peer.send(request());
        ASSERT_TRUE(
            until([&] { return server.server->stats().active == 1; })
        );
        server.server.reset();
        EXPECT_TRUE(peer.eof());
        {
            std::lock_guard lock(mutex);
            retained(response());
        }
        // A retained duplicate cannot post after the real completion.
        {
            std::lock_guard lock(mutex);
            retained(response());
        }
    }
    retained(response());
    retained = {};
    EXPECT_EQ(descriptors(), baseline);
}

TEST(
    RpcServerTest,
    SlowResponseReaderReleasesItsSocketWithinWriteDeadline
)
{
    auto configured = config();
    configured.writeTimeout = 100ms;
    RunningServer server(
        configured,
        {{"auth.echo",
          [](auto input, auto, auto complete) -> Server::Cancellation
          {
              auto output = response();
              if (input.payload() == "large")
                  output.set_payload(std::string(240 * 1024, 'r'));
              complete(std::move(output));
              return {};
          }}}
    );
    RawPeer peer(server.port(), true);
    auto input = request();
    input.set_payload("large");
    peer.send(input);
    ASSERT_TRUE(
        until([&] { return server.server->stats().completed == 1; })
    );
    // A locked small receive window and finite server send buffer
    // keep the write pending after the actual handler has completed.
    EXPECT_EQ(server.server->stats().peers, 1);
    const auto started = Clock::now();
    ASSERT_TRUE(until(
        [&] { return server.server->stats().peers == 0; },
        750ms
    ));
    EXPECT_LT(Clock::now() - started, 750ms);
    EXPECT_EQ(server.server->stats().active, 0);
    EXPECT_EQ(nativeCall(server, request()).status(), arena::rpc::OK);
}

TEST(
    RpcServerTest,
    ForeignCompletionsSerializeAcrossMultipleExecutorWorkers
)
{
    std::mutex mutex;
    std::vector<Server::Completion> completions;
    std::atomic<int> entered{0}, overlapped{0};
    RunningServer server(
        config(),
        {{"auth.echo",
          [&](auto, auto, auto complete) -> Server::Cancellation
          {
              if (entered.fetch_add(1) != 0)
                  ++overlapped;
              {
                  std::lock_guard lock(mutex);
                  completions.push_back(std::move(complete));
              }
              --entered;
              return {};
          }}},
        2
    );
    server.release = [&]
    {
        std::lock_guard lock(mutex);
        for (auto& complete : completions)
            complete(response(arena::rpc::CANCELLED));
    };
    std::vector<std::unique_ptr<RawPeer>> peers;
    for (int index = 0; index != 16; ++index)
    {
        peers.push_back(std::make_unique<RawPeer>(server.port()));
        peers.back()->send(request());
    }
    ASSERT_TRUE(until(
        [&]
        {
            std::lock_guard lock(mutex);
            return completions.size() == 16;
        }
    ));
    EXPECT_EQ(server.server->stats().active, 16);
    std::vector<std::thread> finishers;
    for (int index = 0; index != 16; ++index)
        finishers.emplace_back(
            [&, index]
            {
                completions[index](response());
                completions[index](response());
            }
        );
    for (auto& finisher : finishers)
        finisher.join();
    for (auto& peer : peers)
    {
        const auto output = peer->receive();
        ASSERT_TRUE(output);
        EXPECT_EQ(output->status(), arena::rpc::OK);
    }
    ASSERT_TRUE(
        until([&] { return server.server->stats().active == 0; })
    );
    EXPECT_EQ(server.server->stats().completed, 16);
    EXPECT_EQ(overlapped, 0);
}

TEST(
    RpcServerTest,
    StrictClientRejectsLegacySuccessWithoutReplayingRequest
)
{
    RunningServer server(config(), echo());
    boost::asio::io_context legacyIo;
    Tcp::acceptor listener(
        legacyIo,
        {boost::asio::ip::address_v4::loopback(), 0}
    );
    listener.non_blocking(true);
    const auto legacyEndpoint = listener.local_endpoint();
    std::atomic<int> requests{0}, transportFailures{0};
    std::jthread legacy(
        [&]
        {
            Tcp::socket peer(legacyIo);
            boost::system::error_code error;
            bool accepted = false;
            const bool ready = until(
                [&]
                {
                    listener.accept(peer, error);
                    accepted = !error;
                    return accepted ||
                           (error !=
                                boost::asio::error::would_block &&
                            error != boost::asio::error::try_again);
                }
            );
            if (!ready || !accepted)
            {
                ++transportFailures;
                return;
            }
            peer.non_blocking(true);
            const auto readExact =
                [&](char* bytes, std::size_t length)
            {
                std::size_t used = 0;
                bool failed = false;
                const bool done = until(
                    [&]
                    {
                        boost::system::error_code readError;
                        used += peer.read_some(
                            boost::asio::buffer(
                                bytes + used,
                                length - used
                            ),
                            readError
                        );
                        failed = readError &&
                                 readError != boost::asio::error::
                                                  would_block &&
                                 readError !=
                                     boost::asio::error::try_again;
                        return used == length || failed;
                    }
                );
                return done && !failed && used == length;
            };
            std::array<unsigned char, 4> header{};
            if (!readExact(
                    reinterpret_cast<char*>(header.data()),
                    header.size()
                ))
            {
                ++transportFailures;
                return;
            }
            std::uint32_t length = 0;
            for (const auto byte : header)
                length = (length << 8) | byte;
            if (!length || length > Client::maxFrame)
            {
                ++transportFailures;
                return;
            }
            std::string body(length, char{});
            arena::rpc::Request input;
            if (!readExact(body.data(), body.size()) ||
                !input.ParseFromString(body))
            {
                ++transportFailures;
                return;
            }
            ++requests;
            auto output = response();
            output.set_request_id(input.request_id());
            output.set_correlation_id(input.correlation_id());
            output.set_payload("legacy-success");
            // The old peer has no version field; a successful payload
            // cannot downgrade an explicitly versioned new call.
            body = output.SerializeAsString();
            std::string frame(4, char{});
            for (unsigned index = 0; index != 4; ++index)
                frame[index] = static_cast<char>(
                    body.size() >> (24 - 8 * index)
                );
            frame += body;
            peer.non_blocking(false);
            boost::asio::write(
                peer,
                boost::asio::buffer(frame),
                error
            );
            if (error)
                ++transportFailures;
            peer.close(error);
            const auto end = Clock::now() + 60ms;
            while (Clock::now() < end)
            {
                Tcp::socket replay(legacyIo);
                listener.accept(replay, error);
                if (!error)
                {
                    ++requests;
                    replay.close(error);
                }
                std::this_thread::sleep_for(1ms);
            }
        }
    );
    Client client(server.io.get_executor(), legacyEndpoint);
    auto promise = std::make_shared<
        std::promise<arena::rpc::Response>>();
    auto future = promise->get_future();
    auto accepted = client.call(
        request(),
        Options{},
        [promise](auto reply)
        { promise->set_value(std::move(reply)); }
    );
    const bool completed = future.wait_for(2s) ==
                           std::future_status::ready;
    if (!completed && accepted)
        accepted->cancel();
    legacy.join();
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(completed);
    const auto output = future.get();
    EXPECT_EQ(output.status(), arena::rpc::INCOMPATIBLE_VERSION);
    EXPECT_FALSE(output.retryable());
    EXPECT_EQ(requests, 1);
    EXPECT_EQ(transportFailures, 0);
    EXPECT_EQ(client.inflight(), 0);
}
} // namespace
} // namespace arena::internal_rpc
