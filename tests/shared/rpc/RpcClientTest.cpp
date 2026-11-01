#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>
#include <gtest/gtest.h>

#include "rpc/Client.h"

namespace arena::internal_rpc
{
namespace
{
using namespace std::chrono_literals;
using namespace std::string_literals;
using Tcp = boost::asio::ip::tcp;
using Clock = std::chrono::steady_clock;

enum class Mode
{
    Echo,
    Fragmented,
    Silent,
    ShortHeader,
    CloseAfterRead,
    ZeroLength,
    Oversize,
    Malformed,
    WrongId,
    WrongCorrelation,
    UnknownStatus,
    RemoteError
};

struct Peer : std::enable_shared_from_this<Peer>
{
    struct Session : std::enable_shared_from_this<Session>
    {
        Session(
            boost::asio::any_io_executor executor,
            std::weak_ptr<Peer> owner
        )
            : socket(std::move(executor))
            , owner(std::move(owner))
        {
            ++live;
        }
        ~Session()
        {
            --live;
        }
        static inline std::atomic<int> live{0};
        Tcp::socket socket;
        std::weak_ptr<Peer> owner;
        std::array<unsigned char, 4> header{};
        std::string input, output;
        std::size_t offset{0};

        void read()
        {
            boost::asio::async_read(
                socket,
                boost::asio::buffer(header),
                [self = shared_from_this()](auto error, std::size_t)
                {
                    if (error)
                        return;
                    std::uint32_t size = 0;
                    for (const auto byte : self->header)
                        size = (size << 8) | byte;
                    if (!size || size > Client::maxFrame)
                        return;
                    self->input.resize(size);
                    boost::asio::async_read(
                        self->socket,
                        boost::asio::buffer(self->input),
                        [self](auto error, std::size_t)
                        {
                            if (error)
                                return;
                            const auto owner = self->owner.lock();
                            arena::rpc::Request request;
                            if (!owner ||
                                !request.ParseFromString(self->input))
                                return;
                            owner->requests.push_back(request);
                            self->reply(request, owner->mode);
                        }
                    );
                }
            );
        }
        void reply(const arena::rpc::Request& request, Mode mode)
        {
            if (mode == Mode::Silent)
                return;
            if (mode == Mode::CloseAfterRead)
            {
                boost::system::error_code ignored;
                socket.close(ignored);
                return;
            }
            arena::rpc::Response response;
            response.set_request_id(request.request_id());
            response.set_correlation_id(request.correlation_id());
            response.set_payload(request.payload());
            if (mode == Mode::WrongId)
                response.set_request_id(request.request_id() + 1);
            if (mode == Mode::WrongCorrelation)
                response.set_correlation_id("other");
            if (mode == Mode::RemoteError)
            {
                response.set_status(arena::rpc::OVERLOADED);
                response.set_retryable(true);
                response.clear_payload();
            }
            auto body = response.SerializeAsString();
            if (mode == Mode::Malformed)
                body.assign(1, static_cast<char>(0x80));
            if (mode == Mode::UnknownStatus)
            {
                body.push_back(static_cast<char>(0x18));
                body.push_back(static_cast<char>(99));
            }
            auto size = static_cast<std::uint32_t>(body.size());
            if (mode == Mode::ZeroLength)
                size = 0;
            if (mode == Mode::Oversize)
                size = Client::maxFrame + 1;
            output.resize(4);
            for (unsigned i = 0; i != 4; ++i)
                output[i] = static_cast<char>(size >> (24 - 8 * i));
            if (mode == Mode::ShortHeader)
                output.resize(2);
            else
                output += body;
            if (mode == Mode::Fragmented)
                writeFragment();
            else
                boost::asio::async_write(
                    socket,
                    boost::asio::buffer(output),
                    [self = shared_from_this(),
                     mode](auto, std::size_t)
                    {
                        if (mode != Mode::ShortHeader)
                        {
                            boost::system::error_code ignored;
                            self->socket.close(ignored);
                        }
                    }
                );
        }
        void writeFragment()
        {
            if (offset == output.size())
            {
                boost::system::error_code ignored;
                socket.close(ignored);
                return;
            }
            const auto size = std::min<std::size_t>(
                7,
                output.size() - offset
            );
            boost::asio::async_write(
                socket,
                boost::asio::buffer(output.data() + offset, size),
                [self = shared_from_this()](
                    auto error,
                    std::size_t size
                )
                {
                    if (!error)
                    {
                        self->offset += size;
                        self->writeFragment();
                    }
                }
            );
        }
    };

    explicit Peer(boost::asio::io_context& context, Mode mode)
        : executor(boost::asio::make_strand(context))
        , listener(executor)
        , mode(mode)
    {
        listener.open(Tcp::v4());
        listener.bind(
            Tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0)
        );
        listener.listen();
    }
    void accept()
    {
        auto session = std::make_shared<Session>(
            executor,
            weak_from_this()
        );
        listener.async_accept(
            session->socket,
            [weak = weak_from_this(), session](auto error)
            {
                if (const auto self = weak.lock(); self && !error)
                {
                    self->sessions.push_back(session);
                    session->read();
                    self->accept();
                }
            }
        );
    }
    Tcp::endpoint endpoint() const
    {
        return listener.local_endpoint();
    }
    void stop()
    {
        boost::system::error_code ignored;
        listener.close(ignored);
        for (const auto& session : sessions)
            session->socket.close(ignored);
        sessions.clear();
    }
    boost::asio::strand<boost::asio::io_context::executor_type>
        executor;
    Tcp::acceptor listener;
    Mode mode;
    std::vector<std::shared_ptr<Session>> sessions;
    std::vector<arena::rpc::Request> requests;
};

arena::rpc::Request request(std::string payload = "binary\0payload"s)
{
    arena::rpc::Request result;
    result.set_method("storage.echo");
    result.set_payload(std::move(payload));
    result.set_idempotency_key("stable-key");
    result.set_service_token(std::string(32, 's'));
    return result;
}

class RpcClientTest : public testing::Test
{
protected:
    boost::asio::io_context context;
    std::shared_ptr<Peer> peer;
    std::unique_ptr<Client> client;

    void open(Mode mode = Mode::Echo, std::size_t limit = 128)
    {
        peer = std::make_shared<Peer>(context, mode);
        peer->accept();
        client = std::make_unique<Client>(
            context.get_executor(),
            peer->endpoint(),
            limit
        );
    }
    void pump(std::chrono::milliseconds duration = 15ms)
    {
        context.restart();
        context.run_for(duration);
    }
    template <class Condition> bool until(Condition condition)
    {
        const auto deadline = Clock::now() + 2s;
        while (!condition() && Clock::now() < deadline)
            pump(5ms);
        return condition();
    }
    void TearDown() override
    {
        if (client)
        {
            client->stop();
            client.reset();
        }
        context.restart();
        context.poll();
        if (peer)
        {
            peer->stop();
            peer.reset();
        }
        context.restart();
        context.poll();
        EXPECT_EQ(Peer::Session::live.load(), 0);
    }
};

TEST_F(
    RpcClientTest,
    FragmentedWirePreservesPayloadMetadataAndParentBudget
)
{
    open(Mode::Fragmented);
    std::vector<arena::rpc::Response> replies;
    const std::string binary(8192, '\0');
    Options options;
    options.timeout = 2s;
    options.context = Context{Clock::now() + 500ms, "parent-trace"};
    const auto before =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        )
            .count();
    const auto admitted = client->call(
        request(binary),
        options,
        [&](auto response) { replies.push_back(std::move(response)); }
    );
    ASSERT_TRUE(admitted);
    ASSERT_TRUE(until([&] { return replies.size() == 1; }));
    EXPECT_EQ(replies[0].status(), arena::rpc::OK);
    EXPECT_EQ(replies[0].payload(), binary);
    EXPECT_EQ(replies[0].correlation_id(), "parent-trace");
    EXPECT_FALSE(replies[0].retryable());
    ASSERT_EQ(peer->requests.size(), 1);
    const auto& observed = peer->requests[0];
    EXPECT_NE(observed.request_id(), 0);
    EXPECT_EQ(observed.request_id(), replies[0].request_id());
    EXPECT_EQ(observed.idempotency_key(), "stable-key");
    EXPECT_EQ(observed.service_token(), std::string(32, 's'));
    EXPECT_GE(
        observed.deadline_unix_ms(),
        static_cast<std::uint64_t>(before)
    );
    EXPECT_LE(
        observed.deadline_unix_ms(),
        static_cast<std::uint64_t>(before + 550)
    );
    EXPECT_EQ(client->inflight(), 0);
    admitted->cancel();
    pump();
    EXPECT_EQ(replies.size(), 1);
}

TEST_F(
    RpcClientTest, AdmissionRejectsOverloadWithoutPostingCallbackQueue
)
{
    open(Mode::Silent, 1);
    int callbacks = 0;
    const auto first = client->call(
        request(),
        Options{},
        [&](auto response)
        {
            ++callbacks;
            EXPECT_EQ(response.status(), arena::rpc::CANCELLED);
        }
    );
    ASSERT_TRUE(first);
    for (int i = 0; i != 1000; ++i)
    {
        const auto rejected = client->call(
            request(),
            Options{},
            [&](auto) { ++callbacks; }
        );
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error(), arena::rpc::OVERLOADED);
    }
    EXPECT_EQ(client->inflight(), 1);
    EXPECT_EQ(callbacks, 0);
    first->cancel();
    ASSERT_TRUE(until([&] { return callbacks == 1; }));
    EXPECT_EQ(peer->requests.size(), 0);
    EXPECT_EQ(client->inflight(), 0);
}

TEST_F(
    RpcClientTest,
    ThousandExpiredParentsNeverOpenSocketOrRetainCallback
)
{
    open();
    int callbacks = 0;
    Options options;
    options.context = Context{Clock::now() - 1ms, "expired"};
    for (int i = 0; i != 1000; ++i)
    {
        const auto rejected = client->call(
            request(),
            options,
            [&](auto) { ++callbacks; }
        );
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error(), arena::rpc::DEADLINE_EXCEEDED);
    }
    pump();
    EXPECT_EQ(client->inflight(), 0);
    EXPECT_EQ(peer->requests.size(), 0);
    EXPECT_EQ(callbacks, 0);
}

TEST_F(
    RpcClientTest, DeadlineIncludesExecutorQueueAndPartialResponseRead
)
{
    open(Mode::ShortHeader);
    int callbacks = 0;
    auto queued = client->call(
        request(),
        Options{1ms, {}},
        [&](auto response)
        {
            ++callbacks;
            EXPECT_EQ(
                response.status(),
                arena::rpc::DEADLINE_EXCEEDED
            );
        }
    );
    ASSERT_TRUE(queued);
    std::this_thread::sleep_for(5ms);
    ASSERT_TRUE(until([&] { return callbacks == 1; }));
    EXPECT_TRUE(peer->requests.empty());
    auto reading = client->call(
        request(),
        Options{50ms, {}},
        [&](auto response)
        {
            ++callbacks;
            EXPECT_EQ(
                response.status(),
                arena::rpc::DEADLINE_EXCEEDED
            );
            EXPECT_FALSE(response.retryable());
        }
    );
    ASSERT_TRUE(reading);
    ASSERT_TRUE(until([&] { return callbacks == 2; }));
    EXPECT_EQ(peer->requests.size(), 1);
    EXPECT_EQ(client->inflight(), 0);
}

TEST_F(RpcClientTest, ConcurrentCancellationCoalescesAndCompletesOnce)
{
    open(Mode::Silent);
    int callbacks = 0;
    auto accepted = client->call(
        request(),
        Options{},
        [&](auto response)
        {
            ++callbacks;
            EXPECT_EQ(response.status(), arena::rpc::CANCELLED);
        }
    );
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(until([&] { return peer->requests.size() == 1; }));
    std::vector<std::thread> threads;
    for (int i = 0; i != 8; ++i)
        threads.emplace_back(
            [handle = *accepted]
            {
                for (int n = 0; n != 1000; ++n)
                    handle.cancel();
            }
        );
    for (auto& thread : threads)
        thread.join();
    ASSERT_TRUE(until([&] { return callbacks == 1; }));
    pump(60ms);
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(client->inflight(), 0);
}

TEST_F(
    RpcClientTest,
    StopAndDestructionCancelAcceptedCallsWithSafeRetainedHandles
)
{
    open(Mode::Silent, 16);
    std::vector<Call> handles;
    int callbacks = 0;
    for (int i = 0; i != 16; ++i)
    {
        auto accepted = client->call(
            request(),
            Options{},
            [&](auto response)
            {
                ++callbacks;
                EXPECT_EQ(response.status(), arena::rpc::CANCELLED);
            }
        );
        ASSERT_TRUE(accepted);
        handles.push_back(*accepted);
    }
    client->stop();
    auto rejected = client->call(
        request(),
        Options{},
        [](auto) { FAIL(); }
    );
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), arena::rpc::UNAVAILABLE);
    client.reset();
    ASSERT_TRUE(until([&] { return callbacks == 16; }));
    for (const auto& handle : handles)
        handle.cancel();
    pump();
    EXPECT_EQ(callbacks, 16);
}

TEST_F(
    RpcClientTest,
    PreconnectFailureIsUnavailableAndPostsendLossIsUnknown
)
{
    open(Mode::CloseAfterRead);
    std::vector<arena::rpc::Response> replies;
    ASSERT_TRUE(client->call(
        request(),
        Options{},
        [&](auto r) { replies.push_back(std::move(r)); }
    ));
    ASSERT_TRUE(until([&] { return replies.size() == 1; }));
    EXPECT_EQ(replies[0].status(), arena::rpc::OUTCOME_UNKNOWN);
    EXPECT_FALSE(replies[0].retryable());
    EXPECT_EQ(peer->requests.size(), 1);
    boost::asio::io_context unused;
    Tcp::acceptor temporary(
        unused,
        Tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0)
    );
    const auto closedEndpoint = temporary.local_endpoint();
    temporary.close();
    Client unavailable(context.get_executor(), closedEndpoint);
    ASSERT_TRUE(unavailable.call(
        request(),
        Options{},
        [&](auto r) { replies.push_back(std::move(r)); }
    ));
    ASSERT_TRUE(until([&] { return replies.size() == 2; }));
    EXPECT_EQ(replies[1].status(), arena::rpc::UNAVAILABLE);
    EXPECT_TRUE(replies[1].retryable());
    EXPECT_EQ(unavailable.inflight(), 0);
}

TEST_F(
    RpcClientTest,
    BoundsProtocolIdentityAndStatusBeforeAcceptingResponse
)
{
    for (const auto mode :
         {Mode::ZeroLength,
          Mode::Oversize,
          Mode::Malformed,
          Mode::WrongId,
          Mode::WrongCorrelation,
          Mode::UnknownStatus})
    {
        open(mode);
        int callbacks = 0;
        auto accepted = client->call(
            request(),
            Options{},
            [&](auto response)
            {
                ++callbacks;
                EXPECT_EQ(response.status(), arena::rpc::INTERNAL);
                EXPECT_FALSE(response.retryable());
            }
        );
        ASSERT_TRUE(accepted);
        ASSERT_TRUE(until([&] { return callbacks == 1; }));
        EXPECT_EQ(client->inflight(), 0);
        client->stop();
        client.reset();
        peer->stop();
        peer.reset();
        pump();
    }
}

TEST_F(RpcClientTest, InvalidMetadataAndFramesRejectBeforeAdmission)
{
    open();
    auto check =
        [&](arena::rpc::Request value, Options options = Options{})
    {
        auto result = client->call(
            std::move(value),
            options,
            [](auto) { FAIL(); }
        );
        EXPECT_FALSE(result);
        if (!result)
        {
            EXPECT_EQ(result.error(), arena::rpc::INVALID_ARGUMENT);
        }
    };
    auto bad = request();
    bad.set_method("SQL");
    check(bad);
    bad = request();
    bad.set_correlation_id("line\nbreak");
    check(bad);
    bad = request();
    bad.set_idempotency_key(std::string(129, 'k'));
    check(bad);
    bad = request();
    bad.set_service_token(std::string(257, 's'));
    check(bad);
    check(request(std::string(Client::maxFrame, 'x')));
    check(request(), Options{0ms, {}});
    check(request(), Options{31s, {}});
    EXPECT_EQ(client->inflight(), 0);
    pump();
    EXPECT_TRUE(peer->requests.empty());
    EXPECT_THROW(
        (Client(
            context.get_executor(),
            Tcp::endpoint(
                boost::asio::ip::make_address("127.0.0.2"),
                12
            )
        )),
        std::invalid_argument
    );
    EXPECT_THROW(
        (Client(context.get_executor(), peer->endpoint(), 0)),
        std::invalid_argument
    );
}

TEST_F(
    RpcClientTest,
    RemoteTypedFailuresAndThrowingCallbackLeaveCapacityUsable
)
{
    open(Mode::RemoteError, 1);
    int callbacks = 0;
    ASSERT_TRUE(client->call(
        request(),
        Options{},
        [&](auto response)
        {
            ++callbacks;
            EXPECT_EQ(response.status(), arena::rpc::OVERLOADED);
            EXPECT_TRUE(response.retryable());
            throw 1;
        }
    ));
    ASSERT_TRUE(until([&] { return callbacks == 1; }));
    EXPECT_EQ(client->inflight(), 0);
    ASSERT_TRUE(
        client->call(request(), Options{}, [&](auto) { ++callbacks; })
    );
    ASSERT_TRUE(until([&] { return callbacks == 2; }));
    EXPECT_EQ(client->inflight(), 0);
}

TEST_F(
    RpcClientTest,
    MultithreadAdmissionAndExecutorWorkersSerializeCompletions
)
{
    open(Mode::Echo, 128);
    std::atomic<int> callbacks{0}, inside{0}, maximum{0}, rejected{0};
    std::vector<std::thread> callers;
    for (int i = 0; i != 8; ++i)
        callers.emplace_back(
            [&]
            {
                for (int n = 0; n != 16; ++n)
                {
                    auto result = client->call(
                        request(),
                        Options{2s, {}},
                        [&](auto response)
                        {
                            const auto current = ++inside;
                            int previous = maximum.load();
                            while (previous < current &&
                                   !maximum.compare_exchange_weak(
                                       previous,
                                       current
                                   ))
                            {
                            }
                            EXPECT_EQ(
                                response.status(),
                                arena::rpc::OK
                            );
                            std::this_thread::yield();
                            --inside;
                            ++callbacks;
                        }
                    );
                    if (!result)
                        ++rejected;
                }
            }
        );
    for (auto& caller : callers)
        caller.join();
    EXPECT_EQ(rejected.load(), 0);
    std::vector<std::thread> workers;
    for (int i = 0; i != 4; ++i)
        workers.emplace_back([&] { context.run_for(250ms); });
    for (auto& worker : workers)
        worker.join();
    EXPECT_EQ(callbacks.load(), 128);
    EXPECT_EQ(maximum.load(), 1);
    EXPECT_EQ(client->inflight(), 0);
    EXPECT_EQ(peer->requests.size(), 128);
}

TEST_F(
    RpcClientTest,
    RepeatedSuccessAndCancellationRecoverPhysicalDescriptors
)
{
    open(Mode::Echo, 16);
    int callbacks = 0;
    ASSERT_TRUE(client->call(
        request(),
        Options{},
        [&](auto response)
        {
            ++callbacks;
            EXPECT_EQ(response.status(), arena::rpc::OK);
        }
    ));
    ASSERT_TRUE(until([&] { return callbacks == 1; }));
    context.restart();
    context.poll();
    const auto descriptors = []
    {
        std::size_t count = 0;
        for (const auto& entry :
             std::filesystem::directory_iterator("/proc/self/fd"))
        {
            (void)entry;
            ++count;
        }
        return count;
    };
    const auto baseline = descriptors();
    for (int round = 0; round != 16; ++round)
    {
        for (int i = 0; i != 16; ++i)
        {
            const bool cancelled = i % 2 == 0;
            auto accepted = client->call(
                request(),
                Options{},
                [&, cancelled](auto response)
                {
                    ++callbacks;
                    EXPECT_EQ(
                        response.status(),
                        cancelled ? arena::rpc::CANCELLED
                                  : arena::rpc::OK
                    );
                }
            );
            ASSERT_TRUE(accepted);
            if (cancelled)
                accepted->cancel();
        }
        ASSERT_TRUE(
            until([&] { return callbacks == 1 + (round + 1) * 16; })
        );
        context.restart();
        context.poll();
        EXPECT_EQ(client->inflight(), 0);
        EXPECT_EQ(descriptors(), baseline);
    }
}

TEST_F(
    RpcClientTest, ParentDeadlineErrorStaysWithinTwentyMilliseconds
)
{
    open(Mode::ShortHeader);
    for (int sample = 0; sample != 20; ++sample)
    {
        const auto deadline = Clock::now() + 30ms;
        Options options;
        options.timeout = 2s;
        options.context = Context{deadline, "measured-parent"};
        int callbacks = 0;
        Clock::time_point completed;
        const auto accepted = client->call(
            request(),
            options,
            [&](auto response)
            {
                completed = Clock::now();
                ++callbacks;
                EXPECT_EQ(
                    response.status(),
                    arena::rpc::DEADLINE_EXCEEDED
                );
                EXPECT_FALSE(response.retryable());
            }
        );
        ASSERT_TRUE(accepted);
        ASSERT_TRUE(until([&] { return callbacks == 1; }));
        EXPECT_GE(completed, deadline);
        EXPECT_LE(completed - deadline, 20ms);
        EXPECT_EQ(client->inflight(), 0);
        EXPECT_EQ(
            peer->requests.size(),
            static_cast<std::size_t>(sample + 1)
        );
        accepted->cancel();
        context.restart();
        context.poll();
        EXPECT_EQ(callbacks, 1);
    }
}

TEST_F(RpcClientTest, RetryableStormNeverReplaysNonIdempotentRequests)
{
    open(Mode::RemoteError);
    for (int index = 0; index != 100; ++index)
    {
        auto command = request();
        command.set_method("match.allocate_ids");
        command.set_idempotency_key(
            "allocation-" + std::to_string(index)
        );
        int callbacks = 0;
        const auto admitted = client->call(
            std::move(command),
            Options{},
            [&](auto response)
            {
                ++callbacks;
                EXPECT_EQ(response.status(), arena::rpc::OVERLOADED);
                EXPECT_TRUE(response.retryable());
            }
        );
        ASSERT_TRUE(admitted);
        ASSERT_TRUE(until([&] { return callbacks == 1; }));
        EXPECT_EQ(client->inflight(), 0);
        ASSERT_EQ(
            peer->requests.size(),
            static_cast<std::size_t>(index + 1)
        );
        EXPECT_EQ(
            peer->requests.back().idempotency_key(),
            "allocation-" + std::to_string(index)
        );
    }
    pump();
    EXPECT_EQ(peer->requests.size(), 100);
}

} // namespace
} // namespace arena::internal_rpc
