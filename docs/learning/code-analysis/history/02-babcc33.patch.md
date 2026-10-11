# 02 · babcc33 完整diff

提交：babcc33813baeaf3a2135bbac6c53c723b12ccad；父提交：698ca5c22272e87bf0eadd7b5f3164afa322206c。

原题：Extract ITcpServer lifecycle interface。日期：2026-10-05T17:56:45+08:00。

SHA256（原始diff字节）：9e6eccaa3c0265c03c9e8a588f60d77bf96d606c7ae874753d46ab94663094fb。

[代码分析](../09-commit-code.md#commit-02)。使用git show --format= --no-ext-diff --no-renames --binary --unified=3 babcc33813baeaf3a2135bbac6c53c723b12ccad生成。

~~~diff
diff --git a/services/gateway/app/main.cpp b/services/gateway/app/main.cpp
index 5a481c7..181edba 100644
--- a/services/gateway/app/main.cpp
+++ b/services/gateway/app/main.cpp
@@ -1,6 +1,8 @@
 #include <chrono>
+#include <memory>
 
 #include "net/EventLoop.h"
+#include "net/ITcpServer.h"
 #include "net/InetAddress.h"
 #include "net/TcpServer.h"
 
@@ -11,7 +13,8 @@ int main()
     EventLoop loop;
 
     InetAddress addr("127.0.0.1", 8888);
-    TcpServer server(&loop, addr, 1000, std::chrono::seconds(60), 4);
+    std::unique_ptr<ITcpServer> server = std::make_unique<TcpServer>(
+        &loop, addr, 1000, std::chrono::seconds(60), 4);
 
     loop.loop();
 }
diff --git a/services/gateway/include/net/ITcpServer.h b/services/gateway/include/net/ITcpServer.h
new file mode 100644
index 0000000..92dc924
--- /dev/null
+++ b/services/gateway/include/net/ITcpServer.h
@@ -0,0 +1,39 @@
+#pragma once
+
+#include <chrono>
+#include <cstddef>
+
+namespace service::gateway
+{
+// Management operations and destruction belong to the server's owning
+// execution context. The caller keeps that context alive until after
+// destruction. No automatic cross-thread dispatch is promised.
+// Backend-specific construction/startup is outside this interface.
+class ITcpServer
+{
+public:
+    static constexpr std::chrono::milliseconds kDefaultStopGrace{
+        std::chrono::seconds(5)};
+
+    virtual ~ITcpServer() noexcept = default;
+
+    // Stop accepting, reject new application sends, drain queued output
+    // and send FIN. Force-close remaining connections at the deadline.
+    // With active connections, observe completion via stopped().
+    // Repeated requests are ignored.
+    // Negative grace throws std::invalid_argument before changing state.
+    // This does not stop the caller's externally owned execution context.
+    virtual void stop(
+        std::chrono::milliseconds grace = kDefaultStopGrace) = 0;
+
+    // True only after all connections are reclaimed and the backend's
+    // owned I/O workers are cleaned up and joined.
+    virtual bool stopped() const noexcept = 0;
+    // Includes accepted sockets awaiting registration, until their
+    // owner acknowledges cleanup. Zero does not by itself mean stopped.
+    virtual std::size_t connectionCount() const noexcept = 0;
+    virtual std::size_t maxConnections() const noexcept = 0;
+    // Configured additional I/O workers; zero uses the external context.
+    virtual std::size_t workerCount() const noexcept = 0;
+};
+} // namespace service::gateway
diff --git a/services/gateway/include/net/TcpServer.h b/services/gateway/include/net/TcpServer.h
index a3d7053..24ad805 100644
--- a/services/gateway/include/net/TcpServer.h
+++ b/services/gateway/include/net/TcpServer.h
@@ -10,6 +10,7 @@
 #include "net/Acceptor.h"
 #include "net/EventLoopThreadPool.h"
 #include "net/InetAddress.h"
+#include "net/ITcpServer.h"
 #include "net/TimerQueue.h"
 
 namespace service::gateway
@@ -21,7 +22,7 @@ enum class TcpServerState
     Stopped
 };
 
-class TcpServer
+class TcpServer final : public ITcpServer
 {
 public:
     // Construct, query, stop and destroy on the base loop thread.
@@ -35,7 +36,7 @@ public:
             std::chrono::seconds(60),
         std::size_t workerCount = 0
     );
-    ~TcpServer() noexcept;
+    ~TcpServer() noexcept override;
 
     TcpServer(const TcpServer&) = delete;
     TcpServer& operator=(const TcpServer&) = delete;
@@ -44,15 +45,15 @@ public:
     // close stragglers at the deadline. Stopped includes worker
     // cleanup and join; it does not quit the externally owned base
     // loop.
-    void
-    stop(std::chrono::milliseconds grace = std::chrono::seconds(5));
+    void stop(
+        std::chrono::milliseconds grace = kDefaultStopGrace) override;
     // Read only on the base EventLoop thread. Count includes accepted
     // sockets queued for worker registration until cleanup is
     // acknowledged.
-    bool stopped() const noexcept;
-    std::size_t connectionCount() const noexcept;
-    std::size_t maxConnections() const noexcept;
-    std::size_t workerCount() const noexcept;
+    bool stopped() const noexcept override;
+    std::size_t connectionCount() const noexcept override;
+    std::size_t maxConnections() const noexcept override;
+    std::size_t workerCount() const noexcept override;
 
 private:
     struct Worker;
diff --git a/tests/gateway/net/ConnectionOwnershipTest.cpp b/tests/gateway/net/ConnectionOwnershipTest.cpp
index 725b8c6..562e4d4 100644
--- a/tests/gateway/net/ConnectionOwnershipTest.cpp
+++ b/tests/gateway/net/ConnectionOwnershipTest.cpp
@@ -15,6 +15,7 @@
 #include <vector>
 
 #include "net/EventLoop.h"
+#include "net/ITcpServer.h"
 #include "net/InetAddress.h"
 #include "net/Socket.h"
 #include "net/TcpConnection.h"
@@ -281,7 +282,8 @@ TEST(TcpServerOwnershipTest, QueuedRemovalIsSafeAfterServerDestruction)
     reservation.close();
 
     EventLoop loop;
-    auto server = std::make_unique<TcpServer>(&loop, address, 4);
+    std::unique_ptr<service::gateway::ITcpServer> server =
+        std::make_unique<TcpServer>(&loop, address, 4);
     Socket peer = Socket::createTcp();
     ASSERT_EQ(::connect(peer.get(), address.sockaddrPtr(),
                         address.length()), 0);
diff --git a/tests/gateway/net/TcpServerTestHost.cpp b/tests/gateway/net/TcpServerTestHost.cpp
index 418461b..724c315 100644
--- a/tests/gateway/net/TcpServerTestHost.cpp
+++ b/tests/gateway/net/TcpServerTestHost.cpp
@@ -10,6 +10,7 @@
 
 #include "net/Channel.h"
 #include "net/EventLoop.h"
+#include "net/ITcpServer.h"
 #include "net/InetAddress.h"
 #include "net/TcpServer.h"
 #include "os/FileDescriptor.h"
@@ -41,7 +42,7 @@ int main(int argc, char** argv)
     }
 
     service::gateway::EventLoop loop;
-    service::gateway::TcpServer* serverPtr = nullptr;
+    service::gateway::ITcpServer* serverPtr = nullptr;
     service::gateway::TimerId stopPollId = 0;
     service::gateway::Channel stopChannel(stopFd.get());
     stopChannel.setEvents(EPOLLIN);
diff --git a/tests/gateway/net/TcpServerWorkerTest.cpp b/tests/gateway/net/TcpServerWorkerTest.cpp
index 3e11303..8277b2b 100644
--- a/tests/gateway/net/TcpServerWorkerTest.cpp
+++ b/tests/gateway/net/TcpServerWorkerTest.cpp
@@ -1,3 +1,5 @@
+#include "net/ITcpServer.h"
+
 #include <arpa/inet.h>
 #include <sys/socket.h>
 #include <unistd.h>
@@ -99,8 +101,11 @@ TEST(TcpServerWorkerTest, StopsAndJoinsIdlePoolBeforeReportingStopped)
     EventLoop base;
     const auto baseline = openFds();
     {
-        TcpServer server(&base, InetAddress(0, true), 16, 60s, 4);
+        TcpServer implementation(&base, InetAddress(0, true), 16, 60s, 4);
+        ITcpServer& server = implementation;
         EXPECT_EQ(server.workerCount(), 4u);
+        EXPECT_EQ(server.maxConnections(), 16u);
+        EXPECT_EQ(server.connectionCount(), 0u);
         EXPECT_FALSE(server.stopped());
         EXPECT_EQ(
             openFds(),
@@ -126,7 +131,7 @@ TEST(
         EventLoop base;
         std::vector<Socket> peers;
         const auto address = unusedAddress();
-        auto server = std::make_unique<TcpServer>(
+        std::unique_ptr<ITcpServer> server = std::make_unique<TcpServer>(
             &base,
             address,
             128,
@@ -188,7 +193,8 @@ TEST(
     EventLoop base;
     const auto address = unusedAddress();
     std::vector<Socket> peers;
-    TcpServer server(&base, address, 16, 60s, 4);
+    TcpServer implementation(&base, address, 16, 60s, 4);
+    ITcpServer& server = implementation;
     for (int i = 0; i < 8; ++i)
     {
         peers.push_back(connectTo(address));
@@ -295,3 +301,18 @@ TEST(
         ""
     );
 }
+
+TEST(ITcpServerTest, NegativeGraceDoesNotChangeRunningState)
+{
+    EventLoop base;
+    std::unique_ptr<ITcpServer> server = std::make_unique<TcpServer>(
+        &base, InetAddress(0, true), 4);
+
+    EXPECT_EQ(server->workerCount(), 0u);
+    EXPECT_THROW(server->stop(-1ms), std::invalid_argument);
+    EXPECT_FALSE(server->stopped());
+    EXPECT_EQ(server->connectionCount(), 0u);
+    server->stop(0ms);
+    EXPECT_TRUE(server->stopped());
+    EXPECT_NO_THROW(server->stop());
+}
~~~
