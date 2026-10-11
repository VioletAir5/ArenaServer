# 18 · d1bdc4a 完整diff

提交：d1bdc4aa31b08fb1ca580822344d9a5d9b35418a；父提交：841acb0fb18f40409c90970103f7ea25c7b137d8。

原题：Guard unauthenticated packet routing and count policy rejections。日期：2026-10-10T12:17:23+08:00。

SHA256（原始diff字节）：c6dafa3e27706647b99ad549da2ccf9dc835f56706070f706297b94b25ec9540。

[代码分析](../09-commit-code.md#commit-18)。使用git show --format= --no-ext-diff --no-renames --binary --unified=3 d1bdc4aa31b08fb1ca580822344d9a5d9b35418a生成。

~~~diff
diff --git a/services/gateway/app/main.cpp b/services/gateway/app/main.cpp
index 6d9d012..e1a8a28 100644
--- a/services/gateway/app/main.cpp
+++ b/services/gateway/app/main.cpp
@@ -41,8 +41,8 @@ int main(int argc, char** argv)
     }
     if (protocol == "handshake")
     {
-        handlerFactory = [](ConnectionContext::Ptr)
-        { return makePreLoginHandshakeHandler(); };
+        handlerFactory = [](ConnectionContext::Ptr connection)
+        { return makePreLoginHandshakeHandler(std::move(connection)); };
     }
     const InetAddress address("127.0.0.1", 8888);
     if (backend == "epoll")
diff --git a/services/gateway/include/net/ConnectionContext.h b/services/gateway/include/net/ConnectionContext.h
index dc0289c..b5f2dab 100644
--- a/services/gateway/include/net/ConnectionContext.h
+++ b/services/gateway/include/net/ConnectionContext.h
@@ -6,6 +6,7 @@
 #include <thread>
 
 #include "net/InetAddress.h"
+#include "net/RejectionCounters.h"
 
 namespace service::gateway
 {
@@ -53,12 +54,15 @@ struct ConnectionContext final
     ConnectionContext(
         std::uint64_t id,
         InetAddress peerAddress,
-        Clock::time_point accepted
+        Clock::time_point accepted,
+        RejectionCounters::Ptr counters = defaultRejectionCounters()
     );
 
     const std::uint64_t connectionId;
     const InetAddress peer;
     const Clock::time_point acceptedAt;
     const std::shared_ptr<ConnectionSession> session;
+    // Shared atomic counters may be read from monitoring/other workers.
+    const RejectionCounters::Ptr rejections;
 };
 } // namespace service::gateway
diff --git a/services/gateway/include/net/PacketPipeline.h b/services/gateway/include/net/PacketPipeline.h
index 59f7d24..de8fbfc 100644
--- a/services/gateway/include/net/PacketPipeline.h
+++ b/services/gateway/include/net/PacketPipeline.h
@@ -21,6 +21,7 @@ struct PacketContext
     const protocol::PacketHeader& header;
     std::string_view payload;
     std::optional<arena::protocol::Envelope> envelope{};
+    RejectionReason rejectionReason{RejectionReason::HandlerRejected};
 };
 
 enum class PacketHandleResult
@@ -56,6 +57,19 @@ public:
     PacketHandleResult handle(PacketContext& context) override;
 };
 
+// Place after EnvelopeDecodeHandler and before login/business routing.
+// Pending sessions permit only request-type LoginRequest and Ping. Only the
+// actual authentication handler may mark the session authenticated.
+class AuthenticationGateHandler final : public PacketHandler
+{
+public:
+    explicit AuthenticationGateHandler(std::shared_ptr<ConnectionSession>);
+    PacketHandleResult handle(PacketContext&) override;
+
+private:
+    std::shared_ptr<ConnectionSession> m_session;
+};
+
 // Owns the chain head. Framing stays in PacketCodec; nodes see
 // complete frames.
 class PacketPipeline
@@ -72,5 +86,9 @@ private:
 // frames. Reject (including an unhandled frame) and callback
 // exceptions close it.
 ConnectionMessageCallback
-makePacketHandler(PacketPipeline pipeline, std::size_t maxBodySize);
+makePacketHandler(
+    PacketPipeline pipeline,
+    std::size_t maxBodySize,
+    RejectionCounters::Ptr counters = defaultRejectionCounters()
+);
 } // namespace service::gateway
diff --git a/services/gateway/include/net/PreLoginHandshake.h b/services/gateway/include/net/PreLoginHandshake.h
index 96b2af6..f74819b 100644
--- a/services/gateway/include/net/PreLoginHandshake.h
+++ b/services/gateway/include/net/PreLoginHandshake.h
@@ -12,7 +12,7 @@ public:
     PacketHandleResult handle(PacketContext& context) override;
 };
 
-// A fresh decoder -> Ping chain per connection. Other messages are
-// rejected at the tail; authentication and login routing are planned.
-ConnectionMessageCallback makePreLoginHandshakeHandler();
+// A fresh decoder -> authentication gate -> Ping chain per connection.
+// Login authentication and business routing are still planned.
+ConnectionMessageCallback makePreLoginHandshakeHandler(ConnectionContext::Ptr);
 } // namespace service::gateway
diff --git a/services/gateway/include/net/RejectionCounters.h b/services/gateway/include/net/RejectionCounters.h
new file mode 100644
index 0000000..2e52a00
--- /dev/null
+++ b/services/gateway/include/net/RejectionCounters.h
@@ -0,0 +1,51 @@
+#pragma once
+
+#include <array>
+#include <atomic>
+#include <cstddef>
+#include <cstdint>
+#include <memory>
+#include <string_view>
+
+namespace service::gateway
+{
+// Fixed labels bound metric cardinality. These count rejected
+// packets/policy events, not every transport disconnect (EOF, reset,
+// stop, etc.).
+enum class RejectionReason : std::size_t
+{
+    PacketRateExceeded,
+    PacketTypeNotAllowed,
+    Unauthenticated,
+    InvalidFrame,
+    InvalidEnvelope,
+    HandlerRejected,
+    CallbackFailure,
+    SlowClient,
+    Count
+};
+
+class RejectionCounters final
+{
+public:
+    using Ptr = std::shared_ptr<RejectionCounters>;
+    static constexpr auto kReasons = static_cast<std::size_t>(
+        RejectionReason::Count
+    );
+    using Snapshot = std::array<std::uint64_t, kReasons>;
+
+    void record(RejectionReason reason) noexcept;
+    std::uint64_t count(RejectionReason reason) const noexcept;
+    // Each element is atomic; a live snapshot is not one transaction.
+    Snapshot snapshot() const noexcept;
+    static std::string_view name(RejectionReason reason) noexcept;
+
+private:
+    std::array<std::atomic<std::uint64_t>, kReasons> m_counts{};
+};
+
+// Process-wide default, shared by TCP/WS and all workers. Inject a
+// separate instance into ConnectionContext/packet handlers for
+// isolated scopes/tests.
+RejectionCounters::Ptr defaultRejectionCounters();
+} // namespace service::gateway
\ No newline at end of file
diff --git a/services/gateway/src/net/AuthenticationGateHandler.cpp b/services/gateway/src/net/AuthenticationGateHandler.cpp
new file mode 100644
index 0000000..534d000
--- /dev/null
+++ b/services/gateway/src/net/AuthenticationGateHandler.cpp
@@ -0,0 +1,46 @@
+#include <iostream>
+#include <stdexcept>
+#include <utility>
+
+#include "net/PacketPipeline.h"
+
+namespace service::gateway
+{
+AuthenticationGateHandler::AuthenticationGateHandler(
+    std::shared_ptr<ConnectionSession> session
+)
+    : m_session(std::move(session))
+{
+    if (!m_session)
+        throw std::invalid_argument(
+            "authentication gate requires session"
+        );
+}
+PacketHandleResult
+AuthenticationGateHandler::handle(PacketContext& context)
+{
+    // Fail closed if this node was installed before
+    // EnvelopeDecodeHandler.
+    if (!context.envelope)
+    {
+        context.rejectionReason = RejectionReason::InvalidEnvelope;
+        return PacketHandleResult::Reject;
+    }
+    const auto state = m_session->loginState();
+    const bool
+        preLoginRequest = context.header.m_type ==
+                              protocol::PacketHeader::kEchoRequest &&
+                          (context.envelope->has_login_request() ||
+                           context.envelope->has_ping());
+    if (state == ConnectionSession::LoginState::Authenticated ||
+        (state == ConnectionSession::LoginState::Pending &&
+         preLoginRequest))
+        return PacketHandler::handle(context);
+
+    context.rejectionReason = RejectionReason::Unauthenticated;
+    std::cerr << "connection unauthenticated packet id="
+              << context.connection.id()
+              << " type=" << context.header.m_type << '\n';
+    return PacketHandleResult::Reject;
+}
+} // namespace service::gateway
\ No newline at end of file
diff --git a/services/gateway/src/net/BackpressureFeature.cpp b/services/gateway/src/net/BackpressureFeature.cpp
index 93443f3..edc6eb7 100644
--- a/services/gateway/src/net/BackpressureFeature.cpp
+++ b/services/gateway/src/net/BackpressureFeature.cpp
@@ -30,6 +30,7 @@ void BackpressureFeature::onOutputBufferChanged(
             ConnectionClock::now() + m_timeout,
             [](IConnectionControl& c)
             {
+                c.context().rejections->record(RejectionReason::SlowClient);
                 std::cerr << "connection slow client timeout id="
                           << c.context().connectionId << '\n';
                 c.requestClose();
diff --git a/services/gateway/src/net/ConnectionContext.cpp b/services/gateway/src/net/ConnectionContext.cpp
index 2e6c93a..581e359 100644
--- a/services/gateway/src/net/ConnectionContext.cpp
+++ b/services/gateway/src/net/ConnectionContext.cpp
@@ -51,17 +51,19 @@ bool ConnectionSession::expireLogin()
 ConnectionContext::ConnectionContext(
     std::uint64_t id,
     InetAddress peerAddress,
-    Clock::time_point accepted
+    Clock::time_point accepted,
+    RejectionCounters::Ptr counters
 )
     : connectionId(id)
     , peer(std::move(peerAddress))
     , acceptedAt(accepted)
     , session(std::make_shared<ConnectionSession>())
+    , rejections(std::move(counters))
 {
-    if (id == 0)
+    if (id == 0 || !rejections)
     {
         throw std::invalid_argument(
-            "connection context requires nonzero id"
+            "connection context requires nonzero id and rejection counters"
         );
     }
 }
diff --git a/services/gateway/src/net/ConnectionRuntime.cpp b/services/gateway/src/net/ConnectionRuntime.cpp
index 24bd0c0..ed261ce 100644
--- a/services/gateway/src/net/ConnectionRuntime.cpp
+++ b/services/gateway/src/net/ConnectionRuntime.cpp
@@ -100,6 +100,7 @@ void ConnectionRuntime::start()
     }
     catch (...)
     {
+        m_context->rejections->record(RejectionReason::CallbackFailure);
         requestClose();
     }
     finishEvent();
@@ -118,6 +119,7 @@ void ConnectionRuntime::received(std::span<const std::byte> data)
     }
     catch (...)
     {
+        m_context->rejections->record(RejectionReason::CallbackFailure);
         requestClose();
     }
     finishEvent();
@@ -148,6 +150,7 @@ void ConnectionRuntime::outputChanged(std::size_t bytes)
     }
     catch (...)
     {
+        m_context->rejections->record(RejectionReason::CallbackFailure);
         requestClose();
     }
     m_notifyingOutput = false;
@@ -279,6 +282,7 @@ ConnectionTimer ConnectionRuntime::scheduleAt(
                 }
                 catch (...)
                 {
+                    self->m_context->rejections->record(RejectionReason::CallbackFailure);
                     self->requestClose();
                 }
                 self->finishEvent();
@@ -307,6 +311,7 @@ FeatureResult ConnectionRuntime::inspectPacket(PacketView packet)
     }
     catch (...)
     {
+        m_context->rejections->record(RejectionReason::CallbackFailure);
         result = FeatureResult::Close;
     }
     if (result == FeatureResult::Close)
diff --git a/services/gateway/src/net/PacketPipeline.cpp b/services/gateway/src/net/PacketPipeline.cpp
index e3b9120..6e5031f 100644
--- a/services/gateway/src/net/PacketPipeline.cpp
+++ b/services/gateway/src/net/PacketPipeline.cpp
@@ -48,6 +48,7 @@ EnvelopeDecodeHandler::handle(PacketContext& context)
             decoded
         ))
     {
+        context.rejectionReason = RejectionReason::InvalidEnvelope;
         return PacketHandleResult::Reject;
     }
     context.envelope.emplace(std::move(decoded));
@@ -55,8 +56,14 @@ EnvelopeDecodeHandler::handle(PacketContext& context)
 }
 
 ConnectionMessageCallback
-makePacketHandler(PacketPipeline pipeline, std::size_t maxBodySize)
+makePacketHandler(
+    PacketPipeline pipeline,
+    std::size_t maxBodySize,
+    RejectionCounters::Ptr counters
+)
 {
+    if (!counters)
+        throw std::invalid_argument("packet handler requires rejection counters");
     if (maxBodySize > kMaxPacketSize)
     {
         throw std::invalid_argument(
@@ -66,11 +73,13 @@ makePacketHandler(PacketPipeline pipeline, std::size_t maxBodySize)
     auto codec = std::make_shared<protocol::PacketCodec>(maxBodySize);
     auto chain = std::make_shared<PacketPipeline>(std::move(pipeline)
     );
-    return [codec, chain = std::move(chain)](
+    return [codec, chain = std::move(chain), counters = std::move(counters)](
                IConnection& connection,
                std::span<const std::byte> bytes
            )
     {
+        if (!connection.connected())
+            return;
         try
         {
             const auto status = codec->feed(
@@ -101,6 +110,7 @@ makePacketHandler(PacketPipeline pipeline, std::size_t maxBodySize)
                     if (chain->process(context) ==
                         PacketHandleResult::Reject)
                     {
+                        counters->record(context.rejectionReason);
                         connection.close();
                         return false;
                     }
@@ -109,6 +119,7 @@ makePacketHandler(PacketPipeline pipeline, std::size_t maxBodySize)
             );
             if (status == protocol::FrameDecodeStatus::Invalid)
             {
+                counters->record(RejectionReason::InvalidFrame);
                 std::cerr << "PacketCodec invalid frame id="
                           << connection.id() << '\n';
                 connection.close();
@@ -116,12 +127,14 @@ makePacketHandler(PacketPipeline pipeline, std::size_t maxBodySize)
         }
         catch (const std::exception& error)
         {
+            counters->record(RejectionReason::CallbackFailure);
             std::cerr << "PacketPipeline callback failed: "
                       << error.what() << '\n';
             connection.close();
         }
         catch (...)
         {
+            counters->record(RejectionReason::CallbackFailure);
             connection.close();
         }
     };
diff --git a/services/gateway/src/net/PacketWhitelistFeature.cpp b/services/gateway/src/net/PacketWhitelistFeature.cpp
index cc7c0b3..78b06b3 100644
--- a/services/gateway/src/net/PacketWhitelistFeature.cpp
+++ b/services/gateway/src/net/PacketWhitelistFeature.cpp
@@ -25,6 +25,7 @@ FeatureResult PacketWhitelistFeature::onPacket(
             packet.type
         ))
         return FeatureResult::Continue;
+    control.context().rejections->record(RejectionReason::PacketTypeNotAllowed);
     std::cerr << "connection packet type rejected id="
               << control.context().connectionId
               << " type=" << packet.type << '\n';
diff --git a/services/gateway/src/net/PreLoginHandshake.cpp b/services/gateway/src/net/PreLoginHandshake.cpp
index ed2b338..a3a806f 100644
--- a/services/gateway/src/net/PreLoginHandshake.cpp
+++ b/services/gateway/src/net/PreLoginHandshake.cpp
@@ -2,6 +2,7 @@
 
 #include <memory>
 #include <span>
+#include <stdexcept>
 #include <utility>
 
 #include "net/PacketBuffer.h"
@@ -44,13 +45,18 @@ PacketHandleResult PingHandler::handle(PacketContext& context)
                : PacketHandleResult::Reject;
 }
 
-ConnectionMessageCallback makePreLoginHandshakeHandler()
+ConnectionMessageCallback makePreLoginHandshakeHandler(ConnectionContext::Ptr context)
 {
+    if (!context)
+        throw std::invalid_argument("handshake requires connection context");
     auto decoder = std::make_unique<EnvelopeDecodeHandler>();
-    decoder->setNext(std::make_unique<PingHandler>());
+    auto gate = std::make_unique<AuthenticationGateHandler>(context->session);
+    gate->setNext(std::make_unique<PingHandler>());
+    decoder->setNext(std::move(gate));
     return makePacketHandler(
         PacketPipeline(std::move(decoder)),
-        kMaxPacketSize
+        kMaxPacketSize,
+        context->rejections
     );
 }
 } // namespace service::gateway
diff --git a/services/gateway/src/net/RejectionCounters.cpp b/services/gateway/src/net/RejectionCounters.cpp
new file mode 100644
index 0000000..8b80005
--- /dev/null
+++ b/services/gateway/src/net/RejectionCounters.cpp
@@ -0,0 +1,47 @@
+#include "net/RejectionCounters.h"
+
+namespace service::gateway
+{
+void RejectionCounters::record(RejectionReason reason) noexcept
+{
+    const auto index = static_cast<std::size_t>(reason);
+    if (index < kReasons)
+        m_counts[index].fetch_add(1, std::memory_order_relaxed);
+}
+std::uint64_t RejectionCounters::count(RejectionReason reason
+) const noexcept
+{
+    const auto index = static_cast<std::size_t>(reason);
+    return index < kReasons
+               ? m_counts[index].load(std::memory_order_relaxed)
+               : 0;
+}
+RejectionCounters::Snapshot
+RejectionCounters::snapshot() const noexcept
+{
+    Snapshot result{};
+    for (std::size_t i = 0; i < kReasons; ++i)
+        result[i] = m_counts[i].load(std::memory_order_relaxed);
+    return result;
+}
+std::string_view RejectionCounters::name(RejectionReason reason
+) noexcept
+{
+    constexpr std::array<std::string_view, kReasons> names{
+        "packet_rate_exceeded",
+        "packet_type_not_allowed",
+        "unauthenticated",
+        "invalid_frame",
+        "invalid_envelope",
+        "handler_rejected",
+        "callback_failure",
+        "slow_client"};
+    const auto index = static_cast<std::size_t>(reason);
+    return index < kReasons ? names[index] : "unknown";
+}
+RejectionCounters::Ptr defaultRejectionCounters()
+{
+    static auto counters = std::make_shared<RejectionCounters>();
+    return counters;
+}
+} // namespace service::gateway
\ No newline at end of file
diff --git a/services/gateway/src/net/TokenBucketFeature.cpp b/services/gateway/src/net/TokenBucketFeature.cpp
index 9005305..2d0b9bc 100644
--- a/services/gateway/src/net/TokenBucketFeature.cpp
+++ b/services/gateway/src/net/TokenBucketFeature.cpp
@@ -43,6 +43,7 @@ TokenBucketFeature::onPacket(IConnectionControl& control, PacketView)
         m_tokens -= 1;
         return FeatureResult::Continue;
     }
+    control.context().rejections->record(RejectionReason::PacketRateExceeded);
     if (m_exhausted == FeatureResult::Close)
         std::cerr << "connection packet rate exceeded id="
                   << control.context().connectionId << '\n';
diff --git a/tests/gateway/net/ConnectionFeatureTest.cpp b/tests/gateway/net/ConnectionFeatureTest.cpp
index 8a648b8..a69c83b 100644
--- a/tests/gateway/net/ConnectionFeatureTest.cpp
+++ b/tests/gateway/net/ConnectionFeatureTest.cpp
@@ -249,7 +249,9 @@ TEST_F(
     WhitelistRejectsValidEnvelopeBeforeBusinessHandler
 )
 {
-    auto runtime = make(makePreLoginHandshakeHandler());
+    auto runtime = make(makePreLoginHandshakeHandler(
+        std::make_shared<ConnectionContext>(1, InetAddress("127.0.0.1", 1), now)
+    ));
     runtime->features().add<PacketWhitelistFeature>(
         std::vector<std::uint16_t>{2}
     );
diff --git a/tests/gateway/net/SecurityPolicyTest.cpp b/tests/gateway/net/SecurityPolicyTest.cpp
new file mode 100644
index 0000000..35f3fa0
--- /dev/null
+++ b/tests/gateway/net/SecurityPolicyTest.cpp
@@ -0,0 +1,407 @@
+#include <memory>
+#include <span>
+#include <stdexcept>
+#include <string>
+#include <thread>
+#include <vector>
+
+#include <gtest/gtest.h>
+
+#include "net/ConnectionRuntime.h"
+#include "net/PacketPipeline.h"
+#include "net/PacketWhitelistFeature.h"
+#include "net/PreLoginHandshake.h"
+#include "net/TokenBucketFeature.h"
+#include "protocol/EnvelopeCodec.hpp"
+#include "protocol/PacketCodec.hpp"
+
+namespace
+{
+using namespace service::gateway;
+struct Transport final : IConnection
+{
+    bool open = true;
+    int closes = 0;
+    std::uint64_t id() const noexcept override
+    {
+        return 1;
+    }
+    void send(std::span<const std::byte>) override
+    {
+    }
+    void shutdown() override
+    {
+    }
+    void close() override
+    {
+        open = false;
+        ++closes;
+    }
+    bool connected() const noexcept override
+    {
+        return open;
+    }
+    void pauseRead() override
+    {
+    }
+    void resumeRead() override
+    {
+    }
+    std::size_t pendingBytes() const noexcept override
+    {
+        return 0;
+    }
+};
+struct Router final : PacketHandler
+{
+    explicit Router(int& calls)
+        : calls(calls)
+    {
+    }
+    PacketHandleResult handle(PacketContext&) override
+    {
+        ++calls;
+        return PacketHandleResult::Handled;
+    }
+    int& calls;
+};
+struct SecurityPolicyTest : testing::Test
+{
+    RejectionCounters::Ptr
+        counters = std::make_shared<RejectionCounters>();
+    ConnectionContext::Ptr
+        context = std::make_shared<ConnectionContext>(
+            1,
+            InetAddress("127.0.0.1", 1),
+            ConnectionClock::now(),
+            counters
+        );
+    std::shared_ptr<Transport>
+        transport = std::make_shared<Transport>();
+    int routed = 0;
+    std::vector<std::function<void()>> posted;
+
+    std::shared_ptr<ConnectionRuntime> make()
+    {
+        auto decoder = std::make_unique<EnvelopeDecodeHandler>();
+        auto gate = std::make_unique<AuthenticationGateHandler>(
+            context->session
+        );
+        gate->setNext(std::make_unique<Router>(routed));
+        decoder->setNext(std::move(gate));
+        return std::make_shared<ConnectionRuntime>(
+            transport,
+            context,
+            ConnectionScheduler{
+                [&](auto task) { posted.push_back(std::move(task)); },
+                [](auto, auto) { return ConnectionTimer{}; }},
+            makePacketHandler(
+                PacketPipeline(std::move(decoder)),
+                256,
+                counters
+            )
+        );
+    }
+    std::string
+    frame(std::string_view payload, std::uint16_t type = 1)
+    {
+        auto result = service::protocol::PacketCodec::encode(
+            type,
+            7,
+            std::as_bytes(std::span(payload))
+        );
+        return {result->peek(), result->readableBytes()};
+    }
+    std::string ping(std::uint16_t type = 1)
+    {
+        arena::protocol::Envelope value;
+        value.mutable_ping()->set_nonce(42);
+        return frame(
+            *service::protocol::EnvelopeCodec::encode(value),
+            type
+        );
+    }
+    std::string business()
+    {
+        // Error is a valid non-handshake envelope; the test router
+        // accepts it only after authentication. No new production
+        // message type is added.
+        arena::protocol::Envelope value;
+        value.mutable_error()->set_code(42);
+        return frame(*service::protocol::EnvelopeCodec::encode(value)
+        );
+    }
+    void receive(
+        const std::shared_ptr<ConnectionRuntime>& runtime,
+        std::string_view bytes
+    )
+    {
+        runtime->received(std::as_bytes(std::span(bytes)));
+    }
+};
+TEST_F(
+    SecurityPolicyTest,
+    BurstStopsBeforeDecodeRouteAndRemainingCoalescedFrames
+)
+{
+    auto runtime = make();
+    runtime->features().add<TokenBucketFeature>(
+        2,
+        0.1,
+        FeatureResult::Close,
+        [] { return ConnectionClock::time_point{}; }
+    );
+    runtime->start();
+    receive(runtime, ping() + ping() + frame("\x62\x80") + ping());
+    EXPECT_EQ(routed, 2);
+    EXPECT_FALSE(runtime->connected());
+    EXPECT_EQ(
+        counters->count(RejectionReason::PacketRateExceeded),
+        1
+    );
+    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 0);
+    EXPECT_EQ(posted.size(), 1);
+    receive(runtime, ping());
+    runtime->close();
+    EXPECT_EQ(
+        counters->count(RejectionReason::PacketRateExceeded),
+        1
+    );
+    posted.front()();
+    runtime->transportClosed();
+    EXPECT_EQ(transport->closes, 1);
+}
+TEST_F(
+    SecurityPolicyTest, DropCountsEachRejectedPacketWithoutRoutingIt
+)
+{
+    auto runtime = make();
+    runtime->features().add<TokenBucketFeature>(
+        1,
+        0.1,
+        FeatureResult::Drop,
+        [] { return ConnectionClock::time_point{}; }
+    );
+    runtime->start();
+    receive(runtime, ping() + ping() + ping());
+    EXPECT_EQ(routed, 1);
+    EXPECT_TRUE(runtime->connected());
+    EXPECT_EQ(
+        counters->count(RejectionReason::PacketRateExceeded),
+        2
+    );
+}
+TEST_F(SecurityPolicyTest, PendingBusinessCannotReachRouter)
+{
+    auto runtime = make();
+    runtime->start();
+    receive(runtime, business() + ping());
+    EXPECT_EQ(routed, 0);
+    EXPECT_FALSE(runtime->connected());
+    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
+    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 0);
+}
+TEST_F(SecurityPolicyTest, PendingPingAndLoginDoNotAuthenticate)
+{
+    auto runtime = make();
+    runtime->start();
+    arena::protocol::Envelope login;
+    login.mutable_login_request()->set_account("alice");
+    receive(
+        runtime,
+        ping() +
+            frame(*service::protocol::EnvelopeCodec::encode(login))
+    );
+    EXPECT_EQ(routed, 2);
+    EXPECT_EQ(
+        context->session->loginState(),
+        ConnectionSession::LoginState::Pending
+    );
+    receive(runtime, business());
+    EXPECT_EQ(routed, 2);
+    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
+}
+TEST_F(
+    SecurityPolicyTest,
+    HandshakePayloadCannotBypassGateWithBusinessOuterType
+)
+{
+    auto runtime = make();
+    runtime->start();
+    receive(runtime, ping(2));
+    EXPECT_EQ(routed, 0);
+    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
+}
+TEST_F(SecurityPolicyTest, AuthenticatedBusinessReachesRouter)
+{
+    auto runtime = make();
+    runtime->start();
+    ASSERT_TRUE(context->session->markAuthenticated());
+    receive(runtime, business());
+    EXPECT_EQ(routed, 1);
+    EXPECT_TRUE(runtime->connected());
+    EXPECT_EQ(counters->snapshot(), RejectionCounters::Snapshot{});
+}
+TEST_F(SecurityPolicyTest, ExpiredSessionCannotRestartLoginOrPing)
+{
+    auto runtime = make();
+    runtime->start();
+    ASSERT_TRUE(context->session->expireLogin());
+    EXPECT_FALSE(context->session->markAuthenticated());
+    receive(runtime, ping());
+    EXPECT_EQ(routed, 0);
+    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 1);
+}
+TEST_F(
+    SecurityPolicyTest, WhitelistRejectionIsCountedOnceBeforeDecode
+)
+{
+    auto runtime = make();
+    runtime->features().add<PacketWhitelistFeature>(
+        std::vector<std::uint16_t>{1}
+    );
+    runtime->start();
+    receive(runtime, ping(2));
+    runtime->close();
+    EXPECT_EQ(routed, 0);
+    EXPECT_EQ(
+        counters->count(RejectionReason::PacketTypeNotAllowed),
+        1
+    );
+    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 0);
+}
+TEST_F(SecurityPolicyTest, MalformedEnvelopeCountsDecodeReasonOnly)
+{
+    auto runtime = make();
+    runtime->start();
+    receive(runtime, frame("\x62\x80") + ping());
+    EXPECT_EQ(routed, 0);
+    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 1);
+    EXPECT_EQ(counters->count(RejectionReason::Unauthenticated), 0);
+    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 0);
+}
+TEST_F(SecurityPolicyTest, MalformedFrameCountsFrameReasonOnly)
+{
+    auto runtime = make();
+    runtime->start();
+    auto bytes = ping();
+    bytes[0] = 'X';
+    receive(runtime, bytes);
+    EXPECT_EQ(routed, 0);
+    EXPECT_EQ(counters->count(RejectionReason::InvalidFrame), 1);
+    EXPECT_EQ(counters->count(RejectionReason::InvalidEnvelope), 0);
+}
+TEST_F(
+    SecurityPolicyTest,
+    TailRejectAndCallbackFailureHaveDistinctReasons
+)
+{
+    auto callback = makePacketHandler(
+        PacketPipeline(std::make_unique<PacketHandler>()),
+        256,
+        counters
+    );
+    const auto bytes = ping();
+    callback(*transport, std::as_bytes(std::span(bytes)));
+    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 1);
+
+    struct Throwing : PacketHandler
+    {
+        PacketHandleResult handle(PacketContext&) override
+        {
+            throw std::runtime_error("business failure");
+        }
+    };
+    transport->open = true;
+    auto throwing = makePacketHandler(
+        PacketPipeline(std::make_unique<Throwing>()),
+        256,
+        counters
+    );
+    auto runtime = std::make_shared<ConnectionRuntime>(
+        transport,
+        context,
+        ConnectionScheduler{
+            [](auto) {},
+            [](auto, auto) { return ConnectionTimer{}; }},
+        std::move(throwing)
+    );
+    runtime->start();
+    receive(runtime, ping());
+    EXPECT_EQ(counters->count(RejectionReason::CallbackFailure), 1);
+    EXPECT_EQ(counters->count(RejectionReason::HandlerRejected), 1);
+}
+TEST(
+    SecurityPolicyConfigurationTest,
+    GateAndCountersFailClosedOnMissingDependencies
+)
+{
+    EXPECT_THROW(
+        AuthenticationGateHandler(nullptr),
+        std::invalid_argument
+    );
+    EXPECT_THROW(
+        makePreLoginHandshakeHandler(nullptr),
+        std::invalid_argument
+    );
+    EXPECT_THROW(
+        (ConnectionContext(
+            1,
+            InetAddress("127.0.0.1", 1),
+            ConnectionClock::now(),
+            nullptr
+        )),
+        std::invalid_argument
+    );
+    auto session = std::make_shared<ConnectionSession>();
+    AuthenticationGateHandler gate(session);
+    Transport transport;
+    service::protocol::PacketHeader header;
+    PacketContext packet{transport, header, {}};
+    EXPECT_EQ(gate.handle(packet), PacketHandleResult::Reject);
+    EXPECT_EQ(
+        packet.rejectionReason,
+        RejectionReason::InvalidEnvelope
+    );
+}
+TEST(
+    RejectionCountersTest,
+    SharedWorkersDoNotLoseCountsAndCountsOutliveConnection
+)
+{
+    auto counters = std::make_shared<RejectionCounters>();
+    std::vector<std::thread> workers;
+    for (int i = 0; i < 4; ++i)
+        workers.emplace_back(
+            [counters]
+            {
+                for (int j = 0; j < 10000; ++j)
+                    counters->record(RejectionReason::Unauthenticated
+                    );
+            }
+        );
+    for (auto& worker : workers)
+        worker.join();
+    {
+        ConnectionContext context(
+            1,
+            InetAddress("127.0.0.1", 1),
+            ConnectionClock::now(),
+            counters
+        );
+        context.rejections->record(RejectionReason::PacketRateExceeded
+        );
+    }
+    EXPECT_EQ(
+        counters->count(RejectionReason::Unauthenticated),
+        40000
+    );
+    EXPECT_EQ(counters->snapshot()[0], 1);
+    for (std::size_t i = 0; i < RejectionCounters::kReasons; ++i)
+        EXPECT_NE(
+            RejectionCounters::name(static_cast<RejectionReason>(i)),
+            "unknown"
+        );
+    EXPECT_EQ(defaultRejectionCounters(), defaultRejectionCounters());
+}
+} // namespace
\ No newline at end of file
diff --git a/tests/gateway/net/WebSocketTestHost.cpp b/tests/gateway/net/WebSocketTestHost.cpp
index 0dd1b7d..620d7f6 100644
--- a/tests/gateway/net/WebSocketTestHost.cpp
+++ b/tests/gateway/net/WebSocketTestHost.cpp
@@ -33,8 +33,8 @@ int main(int argc, char** argv)
     const std::string origin = argv[3];
     const std::string mode = argv[4];
     boost::asio::io_context context;
-    ConnectionHandlerFactory handshake = [](ConnectionContext::Ptr)
-    { return makePreLoginHandshakeHandler(); };
+    ConnectionHandlerFactory handshake = [](ConnectionContext::Ptr connection)
+    { return makePreLoginHandshakeHandler(std::move(connection)); };
     ConnectionFeatureFactory features;
     if (mode == "features")
     {
~~~
