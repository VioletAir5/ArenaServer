# 05 · a9eb1da 完整diff

提交：a9eb1da2bfab08e5219016e71cd8ea14dbea87a3；父提交：146518c4e70f5788472e67d7f3020b07487b4222。

原题：Clarify AsioConnect lifecycle and read phases。日期：2026-10-07T02:02:01+08:00。

SHA256（原始diff字节）：3bbd19d6fcd8981dfa148ba35cd73b70a92167059523c7c8de50576cc80d2170。

[代码分析](../09-commit-code.md#commit-05)。使用git show --format= --no-ext-diff --no-renames --binary --unified=3 a9eb1da2bfab08e5219016e71cd8ea14dbea87a3生成。

~~~diff
diff --git a/services/gateway/include/net/AsioConnect.h b/services/gateway/include/net/AsioConnect.h
index 546309d..5a5bc4b 100644
--- a/services/gateway/include/net/AsioConnect.h
+++ b/services/gateway/include/net/AsioConnect.h
@@ -66,6 +66,26 @@ private:
     using ReadBuffer = std::array<std::byte, 8192>;
     using WriteBuffer = std::vector<std::byte>;
 
+    // TCP half-close lifecycle; start() is an independent activation step.
+    enum class State
+    {
+        Connected,
+        PeerHalfClosed,
+        WriteDraining,
+        WriteDrainingPeerClosed,
+        WriteHalfClosed,
+        Closed
+    };
+
+    // A completed read may wait here while delivery is paused.
+    enum class ReadState
+    {
+        Idle,
+        InFlight,
+        Ready,
+        Delivering
+    };
+
     void requireOwner() const;
     void readNext();
     void deliverRead();
@@ -97,16 +117,11 @@ private:
     std::size_t m_frontOffset{0};
     std::size_t m_pendingBytes{0};
 
+    State m_state{State::Connected};
     bool m_started{false};
-    bool m_closed{false};
+    ReadState m_readState{ReadState::Idle};
     bool m_readPaused{false};
-    bool m_reading{false};
-    bool m_readReady{false};
-    bool m_delivering{false};
     bool m_writing{false};
-    bool m_shutdownRequested{false};
-    bool m_writeHalfClosed{false};
-    bool m_peerHalfClosed{false};
     bool m_aboveHighWater{false};
 };
 } // namespace service::gateway
diff --git a/services/gateway/src/net/AsioConnect.cpp b/services/gateway/src/net/AsioConnect.cpp
index d06d03d..ead3e3f 100644
--- a/services/gateway/src/net/AsioConnect.cpp
+++ b/services/gateway/src/net/AsioConnect.cpp
@@ -63,7 +63,8 @@ std::uint64_t AsioConnect::id() const noexcept
 void AsioConnect::send(std::span<const std::byte> bytes)
 {
     requireOwner();
-    if (m_closed || m_shutdownRequested || bytes.empty())
+    if ((m_state != State::Connected &&
+         m_state != State::PeerHalfClosed) || bytes.empty())
     {
         return;
     }
@@ -100,30 +101,33 @@ void AsioConnect::send(std::span<const std::byte> bytes)
 void AsioConnect::shutdown()
 {
     requireOwner();
-    if (m_closed || m_shutdownRequested)
+    if (m_state != State::Connected &&
+        m_state != State::PeerHalfClosed)
     {
         return;
     }
-    m_shutdownRequested = true;
+    m_state = m_state == State::PeerHalfClosed
+                  ? State::WriteDrainingPeerClosed
+                  : State::WriteDraining;
     finishWrite();
 }
 
 void AsioConnect::close()
 {
     requireOwner();
-    if (m_closed)
+    if (m_state == State::Closed)
     {
         return;
     }
     auto keepAlive = weak_from_this().lock();
-    m_closed = true;
+    m_state = State::Closed;
     boost::system::error_code ignored;
     m_socket.close(ignored);
     m_idleTimer.cancel(ignored);
     m_slowTimer.cancel(ignored);
     m_output.clear();
     m_frontOffset = m_pendingBytes = 0;
-    m_readReady = false;
+    m_readState = ReadState::Idle;
     // Completion handlers retain buffers, never the connection.
     // Notify once; the server defers map erasure until this dispatch
     // has returned.
@@ -148,7 +152,7 @@ void AsioConnect::close()
 
 bool AsioConnect::connected() const noexcept
 {
-    return !m_closed;
+    return m_state != State::Closed;
 }
 
 void AsioConnect::pauseRead()
@@ -162,7 +166,7 @@ void AsioConnect::resumeRead()
     requireOwner();
     auto keepAlive = weak_from_this().lock();
     m_readPaused = false;
-    if (m_readReady && !m_delivering)
+    if (m_readState == ReadState::Ready)
     {
         deliverRead();
     }
@@ -198,7 +202,7 @@ void AsioConnect::setCloseCallback(CloseCallback callback)
 void AsioConnect::start()
 {
     requireOwner();
-    if (m_started || m_closed)
+    if (m_started || m_state == State::Closed)
     {
         return;
     }
@@ -234,12 +238,15 @@ void AsioConnect::requireOwner() const
 
 void AsioConnect::readNext()
 {
-    if (!m_started || m_closed || m_peerHalfClosed || m_readPaused ||
-        m_reading || m_readReady || m_delivering)
+    if (!m_started ||
+        (m_state != State::Connected &&
+         m_state != State::WriteDraining &&
+         m_state != State::WriteHalfClosed) ||
+        m_readPaused || m_readState != ReadState::Idle)
     {
         return;
     }
-    m_reading = true;
+    m_readState = ReadState::InFlight;
     auto buffer = m_input;
     m_socket.async_read_some(
         boost::asio::buffer(*buffer),
@@ -247,14 +254,13 @@ void AsioConnect::readNext()
          buffer](boost::system::error_code error, std::size_t bytes)
         {
             auto self = weak.lock();
-            if (!self || self->m_closed)
+            if (!self || self->m_state == State::Closed)
             {
                 return;
             }
-            self->m_reading = false;
             self->m_readBytes = bytes;
             self->m_readError = error;
-            self->m_readReady = true;
+            self->m_readState = ReadState::Ready;
             if (bytes != 0)
             {
                 self->m_lastActivity = Clock::now();
@@ -269,14 +275,14 @@ void AsioConnect::readNext()
 
 void AsioConnect::deliverRead()
 {
-    if (m_closed || !m_readReady || m_readPaused || m_delivering)
+    if (m_state == State::Closed ||
+        m_readState != ReadState::Ready || m_readPaused)
     {
         return;
     }
     const auto bytes = m_readBytes;
     const auto error = m_readError;
-    m_readReady = false;
-    m_delivering = true;
+    m_readState = ReadState::Delivering;
     if (bytes != 0 && m_messageCallback)
     {
         auto callback = m_messageCallback;
@@ -298,14 +304,21 @@ void AsioConnect::deliverRead()
             close();
         }
     }
-    m_delivering = false;
-    if (m_closed)
+    m_readState = ReadState::Idle;
+    if (m_state == State::Closed)
     {
         return;
     }
     if (error == boost::asio::error::eof)
     {
-        m_peerHalfClosed = true;
+        if (m_state == State::WriteHalfClosed)
+        {
+            close();
+            return;
+        }
+        m_state = m_state == State::WriteDraining
+                      ? State::WriteDrainingPeerClosed
+                      : State::PeerHalfClosed;
         finishWrite();
     }
     else if (error)
@@ -320,7 +333,8 @@ void AsioConnect::deliverRead()
 
 void AsioConnect::writeNext()
 {
-    if (!m_started || m_closed || m_writing || m_output.empty())
+    if (!m_started || m_state == State::Closed ||
+        m_writing || m_output.empty())
     {
         return;
     }
@@ -336,7 +350,7 @@ void AsioConnect::writeNext()
          buffer](boost::system::error_code error, std::size_t bytes)
         {
             auto self = weak.lock();
-            if (!self || self->m_closed)
+            if (!self || self->m_state == State::Closed)
             {
                 return;
             }
@@ -366,11 +380,12 @@ void AsioConnect::writeNext()
 
 void AsioConnect::finishWrite()
 {
-    if (m_closed || m_pendingBytes != 0)
+    if (m_state == State::Closed || m_pendingBytes != 0)
     {
         return;
     }
-    if (m_shutdownRequested && !m_writeHalfClosed)
+    if (m_state == State::WriteDraining ||
+        m_state == State::WriteDrainingPeerClosed)
     {
         boost::system::error_code error;
         m_socket.shutdown(
@@ -382,9 +397,14 @@ void AsioConnect::finishWrite()
             ioError(error);
             return;
         }
-        m_writeHalfClosed = true;
+        if (m_state == State::WriteDrainingPeerClosed)
+        {
+            close();
+            return;
+        }
+        m_state = State::WriteHalfClosed;
     }
-    if (m_peerHalfClosed)
+    if (m_state == State::PeerHalfClosed)
     {
         close();
     }
@@ -392,7 +412,7 @@ void AsioConnect::finishWrite()
 
 void AsioConnect::updateWatermark()
 {
-    if (m_closed)
+    if (m_state == State::Closed)
     {
         return;
     }
@@ -407,7 +427,7 @@ void AsioConnect::updateWatermark()
             [weak = weak_from_this(), phase](auto error)
             {
                 auto self = weak.lock();
-                if (!error && self && !self->m_closed &&
+                if (!error && self && self->m_state != State::Closed &&
                     self->m_aboveHighWater &&
                     self->m_highWaterSince == phase)
                 {
@@ -451,7 +471,7 @@ void AsioConnect::armIdleTimer()
         [weak = weak_from_this()](auto error)
         {
             auto self = weak.lock();
-            if (error || !self || self->m_closed)
+            if (error || !self || self->m_state == State::Closed)
             {
                 return;
             }
diff --git a/tests/gateway/net/AsioConnectTest.cpp b/tests/gateway/net/AsioConnectTest.cpp
index 1cfd709..3468aaa 100644
--- a/tests/gateway/net/AsioConnectTest.cpp
+++ b/tests/gateway/net/AsioConnectTest.cpp
@@ -125,6 +125,27 @@ TEST_F(AsioConnectTest, PausedInFlightReadIsDeliveredOnResume)
     EXPECT_EQ(received, "held bytes next");
 }
 
+TEST_F(AsioConnectTest, QueuedSendAndShutdownBeforeStartDrainAfterStart)
+{
+    const std::string payload = "queued before start";
+    connection->send(std::as_bytes(std::span(payload)));
+    connection->shutdown();
+    EXPECT_EQ(connection->pendingBytes(), payload.size());
+
+    connection->start();
+    pump();
+    EXPECT_EQ(receivePeer(), payload);
+    char byte;
+    boost::system::error_code error;
+    peer.read_some(boost::asio::buffer(&byte, 1), error);
+    EXPECT_EQ(error, boost::asio::error::eof);
+    EXPECT_TRUE(connection->connected());
+
+    peer.shutdown(Tcp::socket::shutdown_send);
+    pump();
+    EXPECT_FALSE(connection->connected());
+}
+
 TEST_F(AsioConnectTest, ShutdownDrainsAndStillReceivesUntilPeerFin)
 {
     std::string received;
~~~
