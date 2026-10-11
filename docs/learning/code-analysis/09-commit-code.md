# 09 28次commit：代码对照与机制分析

固定范围 v0.1..v0.2。片段来自每次提交相对第一父提交的原始diff；+是新增，-是删除，空格是上下文。节选可能从hunk中间开始，不是可单独应用的patch；完整diff逐提交保留。不能把这些历史片段当当前v0.2源码。

每项结合[原逐提交讲解](../v01-v02-index.md)阅读，并链接当前对应机制章。纯注释/清理/报告提交如实说明，没有为其虚构运行行为。完整源码与测试见[总导航](README.md)。

<a id="commit-01"></a>

## 01 · 698ca5c · Fix IConnection abstraction and organize TcpConnection implementation

日期：2026-10-05T17:44:50+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](04-transports.md)；[完整diff](history/01-698ca5c.patch.md)。

新增的 virtual send(span) 只是 Interface 入口；相邻注释给出复制输入、shutdown排空、close幂等、connected含半关闭的调用契约。读取实现时要继续追 outputBuffer、Channel注销与回调延迟回收，不能因为有虚函数就认定线程安全。这里仍是owner调用，跨线程send在06/07才加入。

<!-- verified-history 698ca5c22272e87bf0eadd7b5f3164afa322206c 089c0dbb3fbd57cc879af57ccd3541ead67d48bcabe437138596caa7b4939f86 26-53 -->
节选文件：`services/gateway/include/net/IConnection.h`，原始完整diff第26–53行。

~~~diff
+    // Stable, nonzero identity for this connection's lifetime.
+    virtual std::uint64_t id() const noexcept = 0;
+
+    // Sends immediately or copies queued bytes before returning. The
+    // caller may release the input afterwards. Ignored after shutdown
+    // starts or close; an output-limit violation closes the connection.
+    virtual void send(std::span<const std::byte> data) = 0;
+
+    // Stop accepting sends, drain queued bytes, then shut down the write
+    // half. Continue receiving until peer EOF or an error/timeout.
+    virtual void shutdown() = 0;
+    // Logical close and notification, without flushing pending bytes.
+    // Owner unregisters I/O before final release. Callback code must not
+    // synchronously destroy this connection. Idempotent.
+    virtual void close() = 0;
+
+    // True until logical close, including draining and half-closed
+    // states. Does not imply that new sends are accepted.
+    virtual bool connected() const noexcept = 0;
+
+    virtual void pauseRead() = 0;
+    // Resume delivery of already available bytes as well as future data.
+    virtual void resumeRead() = 0;
+
+    // User-space output bytes awaiting a successful write. Excludes
+    // bytes already handed to the kernel.
+    virtual std::size_t pendingBytes() const noexcept = 0;
+};
~~~

接口使上层不必知道epoll，但显式保留半关闭和所有权要求。v0.2业务最终面对Runtime facade，接口名称相同不代表运行路径仍直接到TcpConnection。

<a id="commit-02"></a>

## 02 · babcc33 · Extract ITcpServer lifecycle interface

日期：2026-10-05T17:56:45+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](04-transports.md)；[完整diff](history/02-babcc33.patch.md)。

stop默认grace与stopped是两个不同阶段：停止接受不等于连接已回收，connectionCount含等待注册/回收的票据。代码把管理查询从具体TcpServer抽出，不要求不同后端具有相同构造参数。

<!-- verified-history babcc33813baeaf3a2135bbac6c53c723b12ccad 9e6eccaa3c0265c03c9e8a588f60d77bf96d606c7ae874753d46ab94663094fb 49-68 -->
节选文件：`services/gateway/include/net/ITcpServer.h`，原始完整diff第49–68行。

~~~diff
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
~~~

审查停止链要找owner排水、期限强关、清理回执和worker join。WebSocketServer在v0.2仍未实现此Interface，不能把TCP契约直接套在WS。

<a id="commit-03"></a>

## 03 · f6749f3 · Implement Boost.Asio connection and TCP server backend

日期：2026-10-05T19:01:55+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](04-transports.md)；[完整diff](history/03-f6749f3.patch.md)。

先分配AcceptedFd holder，再release socket；这样分配异常发生时原socket仍持有FD。release成功后以RAII holder跨线程投递，worker在自己的io_context assign。这里移动的是FD的所有权，不是把Asio socket对象带到另一个context服务。

<!-- verified-history f6749f37ee2b685bee6358e4b356a40fde88dbb2 6fb1ce25b7e944513255dfd6cf24dfc53177cd0cc3eae34c7617e571b67ddc99 1304-1331 -->
节选文件：`services/gateway/src/net/AsioTcpServer.cpp`，原始完整diff第1304–1331行。

~~~diff
+                        );
+                    }
+                    // Allocate the holder before release, so
+                    // allocation failure leaves the socket's RAII
+                    // ownership intact.
+                    auto accepted = std::make_shared<AcceptedFd>(-1);
+                    accepted->fd = socket->release(error);
+                    if (error)
+                    {
+                        self->acceptError(error);
+                        return;
+                    }
+                    auto worker = self->workers
+                                      [self->nextWorker++ %
+                                       self->workers.size()];
+                    self->admissions.insert(ticket);
+                    try
+                    {
+                        worker->dispatch(
+                            [worker, accepted, ticket]
+                            { worker->add(ticket, accepted); }
+                        );
+                    }
+                    catch (...)
+                    {
+                        self->admissions.erase(ticket);
+                        throw;
+                    }
~~~

admission票据先于worker注册，因此任何post/setup失败都要回滚；正常关闭直到base收到回执才减计数。完整diff含worker/context生命周期，片段只展示accept移交。

<a id="commit-04"></a>

## 04 · 146518c · Add packet codec and gateway echo integration

日期：2026-10-06T17:13:31+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](03-protocol.md)；[完整diff](history/04-146518c.patch.md)。

当时PacketCodec从4字节长度前缀逐步确定目标长度，不足时等待更多字节，再把完整PacketView交给callback。memcpy+ntohl避免直接把非对齐网络字节解释成本机对象。该历史版尚没有magic；不能拿当前8字节prefix解释这段。

<!-- verified-history 146518c4e70f5788472e67d7f3020b07487b4222 cfc7b7b90ff5b2988fe332a6fa19337ef86fc0b3698a8c3b7a39808d2bd1dcd2 529-556 -->
节选文件：`shared/src/protocol/PacketCodec.cpp`，原始完整diff第529–556行。

~~~diff
+        {
+            const auto size = m_input.readableBytes();
+            std::size_t target = PacketHeader::kLengthSize;
+            if (size >= PacketHeader::kLengthSize)
+            {
+                PacketHeader header;
+                auto status = PacketHeaderCodec::decode(
+                    m_input.peek(),
+                    size,
+                    header
+                );
+                std::uint32_t networkLength;
+                std::memcpy(
+                    &networkLength,
+                    m_input.peek(),
+                    sizeof(networkLength)
+                );
+                const auto bodyLength = ::ntohl(networkLength);
+                if (status == HeaderDecodeStatus::Invalid ||
+                    bodyLength > m_maxBodySize)
+                {
+                    return m_status = FrameDecodeStatus::Invalid;
+                }
+                target = size < PacketHeader::kHeaderSize
+                             ? PacketHeader::kHeaderSize
+                             : PacketHeader::kLengthSize + bodyLength;
+                if (size == target &&
+                    status == HeaderDecodeStatus::Complete)
~~~

回调只借用解析存储，异步业务需要复制；分片/粘包不能用一次read判断完整包。后来magic提交改变早期目标，v0.2的完整递增解析见协议章。

<a id="commit-05"></a>

## 05 · a9eb1da · Clarify AsioConnect lifecycle and read phases

日期：2026-10-07T02:02:01+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](04-transports.md)；[完整diff](history/05-a9eb1da.patch.md)。

State枚举把peer FIN、写排水、写半关闭组合成明确阶段；另一个ReadState保留Idle/InFlight/Ready，因为已完成但暂停投递的读取与TCP关闭阶段是正交信息。单一枚举可以消除重复生命周期布尔，却不必把所有条件做成巨型状态积。

<!-- verified-history a9eb1da2bfab08e5219016e71cd8ea14dbea87a3 3bbd19d6fcd8981dfa148ba35cd73b70a92167059523c7c8de50576cc80d2170 4-31 -->
节选文件：`services/gateway/include/net/AsioConnect.h`，原始完整diff第4–31行。

~~~diff
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
~~~

写排空后才能SHUT_WR；收到FIN不等于立即丢输出。start是否激活也独立于连接已建立，这就是不能机械删除全部状态位的原因。

<a id="commit-06"></a>

## 06 · c01f0aa · Serialize concurrent AsioConnect sends and cancellation

日期：2026-10-07T02:23:09+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](04-transports.md)；[完整diff](history/06-c01f0aa.patch.md)。

drainSendQueue在owner取走mutex保护的ingress，再并入owner输出队列；正在异步写的front不能扩容，只有安全tail可合并。完整diff还给send加预算/单次post、completion bind_executor(strand)、operation_aborted分支及closeOnce。

<!-- verified-history c01f0aa3abe2ac290e4c0ce554979b1d10d784e5 b6ef35fec7f15ef37b8d90216aad8a0cb31cd782b2fe3e4191077c1c95fbc62e 211-238 -->
节选文件：`services/gateway/src/net/AsioConnect.cpp`，原始完整diff第211–238行。

~~~diff
+                self->drainSendQueue();
+            }
+        });
+    }
+}
+
+void AsioConnect::drainSendQueue()
+{
+    requireOwner();
+    std::deque<std::shared_ptr<WriteBuffer>> incoming;
+    {
+        std::lock_guard lock(m_sendMutex);
+        incoming.swap(m_sendQueue);
+        m_sendDrainPosted = false;
+    }
+    if (m_state == State::Closed || incoming.empty())
     {
-        auto buffer = std::make_shared<WriteBuffer>();
-        buffer->reserve(std::max<std::size_t>(8192, bytes.size()));
-        buffer->assign(bytes.begin(), bytes.end());
-        m_output.push_back(std::move(buffer));
+        return;
+    }
+    for (auto& buffer : incoming)
+    {
+        // The active front buffer must not move while Asio uses it.
+        if (!m_output.empty() &&
+            (!m_writing || m_output.size() > 1) &&
~~~

strand串行调度与m_writing禁止重复启动一起约束write_some；缓冲由completion强持，连接弱持。timer的aborted直接返回，I/O取消走closeOnce，不能统一成同一种失败处理。

<a id="commit-07"></a>

## 07 · c91409a · Support concurrent TcpConnection sends with bounded queue

日期：2026-10-07T20:01:49+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](04-transports.md)；[完整diff](history/07-c91409a.patch.md)。

锁保护m_acceptingSends、pendingBytes与sendQueue，硬界先检查再复制。外部线程只进ingress并合并一次drain投递；owner独占Socket和PacketBuffer。因此不需要让可移动Buffer的写索引承担多生产者发布协议。

<!-- verified-history c91409a8ca2088d0fcd04c62d5977562fbd19303 c5c64e0d28cf574bf4b829feb1dde6b1d6fe9fe81005b5266036d19582ce00d8 107-134 -->
节选文件：`services/gateway/src/net/TcpConnection.cpp`，原始完整diff第107–134行。

~~~diff
+    // A watermark callback may close and release the caller on the owner.
+    auto keepAlive = owner ? weak_from_this().lock()
+                           : std::shared_ptr<TcpConnection>{};
+    bool overflow = false;
+    bool scheduleDrain = false;
+    {
+        std::lock_guard lock(m_sendMutex);
+        if (!m_acceptingSends)
+        {
+            return;
+        }
+        if (data.size() > kHardOutputLimit - m_pendingBytes)
+        {
+            m_acceptingSends = false;
+            overflow = true;
+        }
+        else
+        {
+            // Only ingress buffers are mutable; the owner has not
+            // borrowed this tail for a socket write.
+            if (!m_sendQueue.empty() &&
+                m_sendQueue.back().size() + data.size() <= 64 * 1024)
+            {
+                auto& tail = m_sendQueue.back();
+                tail.insert(tail.end(), data.begin(), data.end());
+            }
+            else
+            {
~~~

这里支持并发send，不表示close/shutdown/析构可任意线程执行。消费者完成写入后减预算；关闭清入口和计数，owner输出存储还随回收释放，不能把计数归零误写成所有容量立即free。

<a id="commit-08"></a>

## 08 · 84e2ef8 · Add versioned Envelope protobuf schema and bounded codec

日期：2026-10-07T23:52:17+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](03-protocol.md)；[完整diff](history/08-84e2ef8.patch.md)。

reserved 2及旧名称同时保留，阻止退役tag以后被另一语义复用；oneof使用独立新tag，protobuf解析器可以保留未知字段。旧fixture与新schema是双向读写兼容测试的具体输入。

<!-- verified-history 84e2ef8d0e153c8574a2885b24b61c0be9ec0323 168423442e91a01ad15033e25da32d0122a82369d77b05e2f2a59e888ae661be 83-110 -->
节选文件：`shared/proto/envelope.proto`，原始完整diff第83–110行。

~~~diff
+
+// Stable payload schema inside the existing length-prefixed PacketCodec frame.
+// Never renumber a field or reuse a retired tag. Tag 2 was legacy_client_tag
+// in the frozen v1 compatibility fixture.
+message Envelope {
+  uint64 request_id = 1;
+  reserved 2;
+  reserved "legacy_client_tag";
+  uint64 sent_at_unix_ms = 3;
+
+  oneof payload {
+    LoginRequest login_request = 10;
+    LoginResponse login_response = 11;
+    Ping ping = 12;
+    Pong pong = 13;
+    Error error = 14;
+  }
+}
+
+message LoginRequest {
+  string account = 1;
+  bytes credential = 2;
+  string client_version = 3;
+}
+
+message LoginResponse {
+  bool accepted = 1;
+  string session_token = 2;
~~~

兼容指字段层：旧端能忽略新字段不等于理解新业务。codec的总大小/递归深度检查仍对未知字段和未知业务输入生效；本提交没有MsgId enum，它在20才加入。

<a id="commit-09"></a>

## 09 · fd13a53 · Add packet header magic and update framing

日期：2026-10-08T00:02:44+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](03-protocol.md)；[完整diff](history/09-fd13a53.patch.md)。

valid新增magic判定；编码先htonl再memcpy，后续type/request/version偏移整体后移。不是只在某个客户端prepend常量，而是header codec与流式decoder一起改变prefix长度和输入验证顺序。

<!-- verified-history fd13a53d36be044bc0bb1556afbcfcf2638c9ef9 97aed2523418b5ba0b8436afaea9b7459d315cdd5b7fcb4eb0938a1b35c02246 130-157 -->
节选文件：`shared/src/protocol/PacketHeaderCodec.cpp`，原始完整diff第130–157行。

~~~diff
+++ b/shared/src/protocol/PacketHeaderCodec.cpp
@@ -12,7 +12,8 @@ namespace
 {
 bool valid(const PacketHeader& header) noexcept
 {
-    return header.m_length >= PacketHeader::kMetadataSize &&
+    return header.m_magic == PacketHeader::kMagic &&
+           header.m_length >= PacketHeader::kMetadataSize &&
            header.m_length <= PacketHeader::kMaxPacketSize &&
            header.m_version == PacketHeader::kCurrentVersion &&
            (header.m_type == PacketHeader::kEchoRequest ||
@@ -37,15 +38,23 @@ bool PacketHeaderCodec::encode(
     const std::uint16_t networkType = ::htons(header.m_type);
     const std::uint32_t networkRequestId = ::htonl(header.m_requestId
     );
-    std::memcpy(bytes.data(), &networkLength, sizeof(networkLength));
+    const std::uint32_t networkMagic = ::htonl(header.m_magic);
+
+    std::memcpy(bytes.data(), &networkMagic, sizeof(networkMagic));
+    std::memcpy(
+        bytes.data() + PacketHeader::kMagicSize,
+        &networkLength,
+        sizeof(networkLength)
+    );
     std::memcpy(
-        bytes.data() + PacketHeader::kLengthSize,
+        bytes.data() + PacketHeader::kMagicSize +
+            PacketHeader::kLengthSize,
~~~

这一修改破坏旧外层wire格式，和protobuf字段向前兼容是两回事。magic能快速识别错流，不能认证发送者，也不是加密完整性校验。

<a id="commit-10"></a>

## 10 · 992a42b · Add complete-packet processing pipeline

日期：2026-10-08T00:28:17+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/10-992a42b.patch.md)。

m_stages有序执行，只有Continue进入下一个stage；其他结果立即返回。空stage拒绝注册，pipeline持有函数及其捕获状态，这提供当时的过滤/解码/匹配扩展点。

<!-- verified-history 992a42bb1881400f22de8b0c64fc4cfbb515a654 96c381fb347f00802f24bfacdba012027364a7f2296be5225ac7f8b7a849c26c 191-218 -->
节选文件：`services/gateway/src/net/PacketPipeline.cpp`，原始完整diff第191–218行。

~~~diff
+void PacketPipeline::addStage(Stage stage)
+{
+    if (!stage)
+    {
+        throw std::invalid_argument("empty packet stage");
+    }
+    m_stages.push_back(std::move(stage));
+}
+
+PacketStageResult PacketPipeline::process(PacketContext& context) const
+{
+    for (const auto& stage : m_stages)
+    {
+        const auto result = stage(context);
+        if (result != PacketStageResult::Continue)
+        {
+            return result;
+        }
+    }
+    return PacketStageResult::Continue;
+}
+
+PacketStageResult PacketPipeline::decodeEnvelope(PacketContext& context)
+{
+    arena::protocol::Envelope decoded;
+    if (!protocol::EnvelopeCodec::decode(
+            std::as_bytes(std::span(context.payload)), decoded))
+    {
~~~

这不是后来用户要求的class Handler写法。此时可复用功能已有串联位置，但组合顺序、callback view寿命及Reject关闭仍须明确；12才将继续执行改成base forwarding。

<a id="commit-11"></a>

## 11 · 3942122 · Add Beast WebSocket transport and pre-login handshake

日期：2026-10-08T20:16:54+08:00；[原设计理由](../v01-v02-commits-01.md)；[当前机制](04-transports.md)；[完整diff](history/11-3942122.patch.md)。

WS构造配置HTTP头8KiB、body0、message上限并设binary输出；升级成功后的应用字节仍复用PacketCodec和握手业务。read_message_max控制的是重组message，不能拿一个frame长度替代它。

<!-- verified-history 39421226657bb0dd4607b094c1ffca186beb55df c17e118d2f24e78eab9d6117af5d5485b51480e8128bdb7c05b7dd0d72606732 294-321 -->
节选文件：`services/gateway/src/net/WebSocketServer.cpp`，原始完整diff第294–321行。

~~~diff
+        , m_allowedOrigin(std::move(origin))
+        , m_handler(std::move(handler))
+        , m_removed(std::move(removed))
+    {
+        m_request.header_limit(8192);
+        m_request.body_limit(0);
+        m_ws.read_message_max(kMaxMessageBytes);
+        m_ws.binary(true);
+    }
+
+    std::uint64_t id() const noexcept override
+    {
+        return m_id;
+    }
+    bool connected() const noexcept override
+    {
+        return m_open.load();
+    }
+    std::size_t pendingBytes() const noexcept override
+    {
+        std::lock_guard lock(m_sendMutex);
+        return m_pendingBytes;
+    }
+
+    void send(std::span<const std::byte> bytes) override
+    {
+        if (bytes.empty())
+            return;
~~~

当时m_open只是bool快照，13才收敛生命周期；完整diff还含Origin、路径、deadline、输出队列和close code日志。TCP/WS复用业务不等于共享整个服务器停止模型。

<a id="commit-12"></a>

## 12 · fc55741 · Refactor packet pipeline into handler chain

日期：2026-10-09T23:11:40+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/12-fc55741.patch.md)。

Stage函数列表改成PacketHandler基类和unique_ptr next，setNext转移链节点所有权。派生过滤器放行或匹配器不命中时调用PacketHandler::handle；已经Handled后不能再转发造成双响应。

<!-- verified-history fc55741776984cf4eb0eafc00d461430e16d1375 06687c33d8c072534cfb98f633e5002cae0e3d50500960c1750f96409b7ec5a7 115-142 -->
节选文件：`services/gateway/include/net/PacketPipeline.h`，原始完整diff第115–142行。

~~~diff
-// Configured once per connection, run serially by that connection's owner.
-// Framing stays in PacketCodec; stages only see complete frames.
-class PacketPipeline
+// Each connection owns a fresh chain. Configure links before
+// installing it; handle() and node state belong to that connection's
+// execution context.
+class PacketHandler
 {
 public:
-    using Stage = std::function<PacketStageResult(PacketContext&)>;
+    virtual ~PacketHandler() = default;
 
-    void addStage(Stage stage);
-    PacketStageResult process(PacketContext& context) const;
+    void setNext(std::unique_ptr<PacketHandler> next);
+
+    // A filter that passes, or a router that does not match, calls
+    // this base implementation to forward. An unhandled frame is
+    // rejected at the tail.
+    virtual PacketHandleResult handle(PacketContext& context);
+
+private:
+    std::unique_ptr<PacketHandler> m_next;
+};
 
-    // Opt-in stage for protobuf payloads. Later stages can read
-    // context.envelope; malformed or over-limit payloads are rejected.
-    static PacketStageResult decodeEnvelope(PacketContext& context);
~~~

每连接新链、owner修改，末尾默认Reject。unique_ptr清楚表达线性独占；节点注册在安装前完成，运行中没有热更新契约。

<a id="commit-13"></a>

## 13 · b40ed33 · Unify WebSocket connection lifecycle state

日期：2026-10-09T23:47:30+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](04-transports.md)；[完整diff](history/13-b40ed33.patch.md)。

abort用exchange(Closed)作一次性入口，重复或迟到回调只返回；onRequest先验证ReadingHttp，防关闭后HTTP完成重新升级。新的阶段枚举给每个迁移明确前置条件。

<!-- verified-history b40ed339ee9711c970026aebb3d36a500c3c48e3 ad1cf892d61a85e7bf6c89b691167b9fbbde840953a5c39c3d876ea3af4fb8a8 40-67 -->
节选文件：`services/gateway/src/net/WebSocketServer.cpp`，原始完整diff第40–67行。

~~~diff
                               << self->m_id << '\n';
@@ -186,10 +197,8 @@ public:
 
     void abort() noexcept
     {
-        if (m_finished)
+        if (m_state.exchange(State::Closed) == State::Closed)
             return;
-        m_finished = true;
-        m_open = false;
         {
             std::lock_guard lock(m_sendMutex);
             m_acceptingSends = false;
@@ -207,6 +216,8 @@ public:
 private:
     void onRequest(Error error)
     {
+        if (m_state.load() != State::ReadingHttp)
+            return;
         if (error)
         {
             abort();
@@ -242,18 +253,21 @@ private:
             return;
         }
         auto self = shared_from_this();
+        m_state = State::Upgrading;
         m_ws.async_accept(
~~~

atomic state允许快照读取，不使WS对象/队列任意线程安全。reading/writing/readPaused仍表达独立操作状态；Open之外connected=false，与TCP半关闭语义不同。

<a id="commit-14"></a>

## 14 · 9fa7fda · Add connection context to transport handler factories

日期：2026-10-10T00:30:17+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/14-9fa7fda.patch.md)。

factory从不带上下文变为接收ConnectionContext::Ptr，owner按accept时捕获的peer和steady时间建立Session。ConnectionHandler wrapper持Context保证callback状态寿命；这是工厂签名和全部调用方一起迁移。

<!-- verified-history 9fa7fdaed716b9dfa72895f1b60b020f6dce123f 976a0ec569bd35095898644b984d131f4ad1f20da470adc597428fcd87005fa8 160-177 -->
节选文件：`services/gateway/include/net/ConnectionHandler.h`，原始完整diff第160–177行。

~~~diff
+
+// Copied once per worker; invoked once per connection on its owner
+// before input delivery (before HTTP Upgrade for WS). Return fresh
+// callback state. The immutable context may be retained; session
+// access stays on its owner. Captures shared across worker copies
+// need their own synchronization.
 using ConnectionHandlerFactory = std::function<
-    ConnectionMessageCallback()>;
+    ConnectionMessageCallback(ConnectionContext::Ptr)>;
+
+// Installs a lifetime wrapper that retains context even if the
+// factory ignores it. An empty factory selects raw echo; an empty
+// result is invalid.
+ConnectionMessageCallback makeConnectionHandler(
+    const ConnectionHandlerFactory& factory,
+    ConnectionContext::Ptr context
+);
 } // namespace service::gateway
~~~

Context不持socket、不是全局IP计数表，也不启动登录timer。它能延长metadata寿命但不保证连接仍在线；Session变更仍归owner。

<a id="commit-15"></a>

## 15 · 3022736 · Add connection runtime and owner-confined feature pipeline

日期：2026-10-10T01:03:34+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/15-3022736.patch.md)。

事件入口强持self，start冻结Feature列表并增加eventDepth；所有Feature回调完成/异常收敛后才能完成closed通知。这个栈深计数解决callback中请求close与通知析构发生在同一个调用栈的问题。

<!-- verified-history 3022736c57ee75bd0d02c3103194f868d888578d ca103a3a669bbdc862b289be94e4d1a54af8223e46efc3761e457f92ecabf62f 307-334 -->
节选文件：`services/gateway/src/net/ConnectionRuntime.cpp`，原始完整diff第307–334行。

~~~diff
+        throw std::logic_error(
+            "runtime start requires shared ownership"
+        );
+    auto keep = shared_from_this();
+    m_started = true;
+    m_features.m_sealed = true;
+    ++m_eventDepth;
+    try
+    {
+        for (auto& feature : m_features.m_features)
+        {
+            feature->onConnected(*this);
+            if (m_closing)
+                break;
+        }
+    }
+    catch (...)
+    {
+        requestClose();
+    }
+    finishEvent();
+    outputChanged(m_transport->pendingBytes());
+}
+void ConnectionRuntime::received(std::span<const std::byte> data)
+{
+    requireOwner();
+    if (!m_started || m_closing)
+        return;
~~~

requestClose关逻辑gate后延迟transport.close；pauseReasons按各策略位独立释放。该提交只有Runtime核心和scheduler/单元，三transport接线在下一提交，不能把模块存在等同已运行。

<a id="commit-16"></a>

## 16 · 8bf665c · Share runtime backpressure across epoll Asio and WebSocket transports

日期：2026-10-10T01:19:25+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/16-8bf665c.patch.md)。

注册表由shared connection改为Entry{runtime, borrowed transport}，runtime.start在回调接线后执行。transport弱捕获Runtime，Runtime强持transport，阻止循环持有并把策略寿命集中到连接owner。

<!-- verified-history 8bf665ce712576dc3185d4c0b191808088153f82 a4f8002adf7994e1aaaa97327f58dcdd3e20cfb83f599a3f621fe0ccb5509fb9 1474-1501 -->
节选文件：`services/gateway/src/net/TcpServer.cpp`，原始完整diff第1474–1501行。

~~~diff
                     {
                         worker->deferRemoval(id);
                     }
                 }
             );
-            connections.emplace(id, connection);
+            connections.emplace(id, Entry{runtime, connection.get()});
+            runtime->start();
             auto [timer, inserted] = idleTimers.emplace(id, 0);
             (void)inserted;
             timer->second = loop->runAfter(
@@ -226,15 +224,6 @@ struct TcpServer::Worker : std::enable_shared_from_this<Worker>
         );
     }
 
-    void checkSlowConnections()
-    {
-        const auto now = std::chrono::steady_clock::now();
-        for (auto& [id, connection] : connections)
-        {
-            connection->closeIfSlow(now);
-        }
-    }
-
     void drain()
     {
         for (auto& [id, connection] : connections)
@@ -256,11 +245,6 @@ struct TcpServer::Worker : std::enable_shared_from_this<Worker>
~~~

水位/慢读从三实现抽到BackpressureFeature；硬输出界仍在transport。非空FeatureFactory负责完整配置而不是隐式叠加默认。也正是Entry改动使旧GDB脚本布局假设失效。

<a id="commit-17"></a>

## 17 · 841acb0 · Apply reusable token bucket and whitelist features after packet framing

日期：2026-10-10T01:37:37+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/17-841acb0.patch.md)。

tokens先按steady时间补充到capacity，再比较1个令牌并扣除；选择Drop/Close决定丢当前完整帧还是终止。惰性补充不用每连接周期timer，注入clock让时间测试确定。

<!-- verified-history 841acb0fb18f40409c90970103f7ea25c7b137d8 b98820de92d02b922a83c90a7c05e8d97d3a4cf9641bf8d9d6d66008f6219aeb 216-230 -->
节选文件：`services/gateway/src/net/TokenBucketFeature.cpp`，原始完整diff第216–230行。

~~~diff
+                                   .count();
+        m_tokens = std::min(m_capacity, m_tokens + elapsed * m_rate);
+        m_last = now;
+    }
+    if (m_tokens >= 1)
+    {
+        m_tokens -= 1;
+        return FeatureResult::Continue;
+    }
+    if (m_exhausted == FeatureResult::Close)
+        std::cerr << "connection packet rate exceeded id="
+                  << control.context().connectionId << '\n';
+    return m_exhausted;
+}
+} // namespace service::gateway
~~~

inspectPacket位于framing之后、protobuf之前，按完整包计而不按read块计；白名单匹配外层type。此时main尚未启用该桶，每连接桶也不能限制重连/共享IP。

<a id="commit-18"></a>

## 18 · d1bdc4a · Guard unauthenticated packet routing and count policy rejections

日期：2026-10-10T12:17:23+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/18-d1bdc4a.patch.md)。

Authenticated放行，Pending只让允许的Login/Ping进入下一节点，其他设置固定Unauthenticated原因并Reject。它检查Session状态而不凭外层type白名单判断业务已认证。

<!-- verified-history d1bdc4aa31b08fb1ca580822344d9a5d9b35418a c6dafa3e27706647b99ad549da2ccf9dc835f56706070f706297b94b25ec9540 195-213 -->
节选文件：`services/gateway/src/net/AuthenticationGateHandler.cpp`，原始完整diff第195–213行。

~~~diff
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
~~~

允许Login经过gate不是实现密码验证，后续没有匹配handler仍拒绝。固定原因计数用atomic relaxed用于统计；到v0.2默认Router已经接替此gate的位置。

<a id="commit-19"></a>

## 19 · 0cfc94c · Enable handshake admission policies and verify rejection counts across transports

日期：2026-10-10T12:24:09+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](06-build-and-entry.md)；[完整diff](history/19-0cfc94c.patch.md)。

main在handshake模式同时安装Backpressure、TokenBucket和outer type白名单，工厂每连接产出fresh Feature。配置顺序是实际运行顺序，测试中的类存在本身不能证明产品入口启用了它。

<!-- verified-history 0cfc94cacf82b0bc572dcb15c86786adcc7b36c8 369579a9be2aad2b96d4ef9024ea01158e93975265f08f3e79e404dcdfba11a3 44-71 -->
节选文件：`services/gateway/app/main.cpp`，原始完整diff第44–71行。

~~~diff
+    if (protocol == "handshake")
+    {
+        featureFactory =
+            [](ConnectionPipeline& pipeline, const ConnectionContext&)
+        {
+            pipeline.add<BackpressureFeature>();
+            pipeline.add<TokenBucketFeature>(200.0, 100.0);
+            pipeline.add<PacketWhitelistFeature>(
+                std::vector<std::uint16_t>{1}
+            );
+        };
     }
     const InetAddress address("127.0.0.1", 8888);
     if (backend == "epoll")
@@ -55,7 +76,8 @@ int main(int argc, char** argv)
                 1000,
                 std::chrono::seconds(60),
                 4,
-                handlerFactory
+                handlerFactory,
+                featureFactory
             );
         loop.loop();
         return 0;
@@ -71,7 +93,8 @@ int main(int argc, char** argv)
             1000,
             std::chrono::seconds(60),
             4,
~~~

此时网络宿主覆盖五配置与计数；test profile合成认证不是生产登录。标题admission指包处理准入，没有实现共享IP连接上限。

<a id="commit-20"></a>

## 20 · 6a1362b · Add typed MsgId dispatch with request correlation and owner deadlines

日期：2026-10-10T13:16:49+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/20-6a1362b.patch.md)。

generation为pending调用的身份补充，handler completion同时捕获id和本代generation；finish只结束相同pending代。request id可在旧调用结束后复用，迟到旧回调不能完成新请求。

<!-- verified-history 6a1362b9c177c34f0d15446c400acd56f7797572 4b5a168283a45650793ed3111c6614e6c73259ffbbf823776a4d6173ece98211 469-496 -->
节选文件：`services/gateway/src/net/MessageRouter.cpp`，原始完整diff第469–496行。

~~~diff
+    );
+    if (!control)
+        return reject(arena::protocol::ERR_INTERNAL);
+    entry.deadline = ConnectionClock::now() +
+                     route->second.options.timeout;
+    const auto id = request->requestId;
+    const auto ticket = entry.generation;
+    m_state->pending.emplace(id, std::move(entry));
+    const RequestCompletion complete =
+        [weak = std::weak_ptr<State>(m_state),
+         owner = m_state->owner,
+         id,
+         ticket](MessageReply result)
+    {
+        if (std::this_thread::get_id() != owner)
+            throw std::logic_error(
+                "request completion requires connection owner"
+            );
+        if (auto state = weak.lock())
+            state->finish(id, ticket, std::move(result));
+    };
+    try
+    {
+        auto timer = control->scheduleAt(
+            m_state->pending.at(id).deadline,
+            [weak = std::weak_ptr<State>(m_state),
+             id,
+             ticket](IConnectionControl&)
~~~

State拥有RequestMessage/Envelope/deadline/RAII timer，completion弱持State；timer与handler统一finish再核deadline。完成必须交回owner，接口不自动post，也不能抢占阻塞handler。

<a id="commit-21"></a>

## 21 · be7d216 · Route default handshake requests and verify typed replies across transports

日期：2026-10-10T13:27:30+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/21-be7d216.patch.md)。

diff明确删decoder→gate→Ping链并装decoder→MessageRouter，而非在旧链后再加router。default registry给Ping/Pong和Login/Unavailable注册公开route、seal后共享。

<!-- verified-history be7d2161f6bde799643cfde846724a95f453e4bd 79aa48170252680fe245de905253971c274c1fb5e452a8e650da1e7691501cef 229-241 -->
节选文件：`services/gateway/src/net/PreLoginHandshake.cpp`，原始完整diff第229–241行。

~~~diff
+            "handshake requires connection context"
+        );
     auto decoder = std::make_unique<EnvelopeDecodeHandler>();
-    auto gate = std::make_unique<AuthenticationGateHandler>(context->session);
-    gate->setNext(std::make_unique<PingHandler>());
-    decoder->setNext(std::move(gate));
+    decoder->setNext(std::make_unique<MessageRouter>(
+        context,
+        registry ? std::move(registry) : defaultMessageRegistry()
+    ));
     return makePacketHandler(
         PacketPipeline(std::move(decoder)),
         kMaxPacketSize,
~~~

业务错误、未知ID、timeout用Typed Error保留request id；malformed framing/protobuf仍关闭。该提交把20的核心接入实际默认链，并提交客户端协议表和跨传输测试。

<a id="commit-22"></a>

## 22 · 68906b1 · Clean up dispatch sources and remove redundant host setup

日期：2026-10-10T13:28:55+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](07-tests.md)；[完整diff](history/22-68906b1.patch.md)。

context.run之后重新makeRpcTestFactory的死代码被删除：宿主构造时已复制factory，run返回也没有重新装配server。这是清理误导性局部赋值，其他修改主要是末尾newline。

<!-- verified-history 68906b12cef486bf3d46b9eaaf22762dd315b1b3 2fccd437002e10f6b287298a4cf610894bf009a13b61d29ba4b8143cb015acaf 70-79 -->
节选文件：`tests/gateway/net/WebSocketTestHost.cpp`，原始完整diff第70–79行。

~~~diff
+++ b/tests/gateway/net/WebSocketTestHost.cpp
@@ -98,8 +98,6 @@ int main(int argc, char** argv)
     );
     std::cout << "WS_TEST_READY" << std::endl;
     context.run();
-    if (mode == "rpc")
-        handshake = makeRpcTestFactory();
     if (mode == "security")
         printSecurityTestCounters();
     return 0;
~~~

不能因为文件名含Router就把这个提交讲成第二次实现路由。没有新的运行状态/线程/消息契约，完整diff支持这个限定。

<a id="commit-23"></a>

## 23 · 7ff856d · Bound routed responses by the gateway packet budget

日期：2026-10-10T13:32:42+08:00；[原设计理由](../v01-v02-commits-02.md)；[当前机制](03-protocol.md)；[完整diff](history/23-7ff856d.patch.md)。

原payload合法判断变为还需size≤gateway body预算−7字节metadata。内层Envelope合法并不保证外层Packet合法，超限改为固定小Internal响应，再编码保留关联ID。

<!-- verified-history 7ff856d67cf9ba959d53698de88a32a6f13bc812 24157b10613080916f9d0af9c10f45a5a77610628fef4e9e6a4ee4e89acf1174 14-23 -->
节选文件：`services/gateway/src/net/MessageRouter.cpp`，原始完整diff第14–23行。

~~~diff
             entry.explicitMsgId ? payloadMsgId(reply) : 0
         );
         auto payload = protocol::EnvelopeCodec::encode(reply);
-        if (!payload)
+        if (!payload || payload->size() >
+                            kMaxPacketSize -
+                                protocol::PacketHeader::kMetadataSize)
         {
             code = arena::protocol::ERR_INTERNAL;
             setError();
~~~

queue硬界限制积压，不能代替单帧界。该修复位于组合协议层，不修改shared通用4MiB上限；新增边界响应测试验证真正预算。

<a id="commit-24"></a>

## 24 · d5cf131 · docs: add English source comments

日期：2026-10-10T15:11:40+08:00；[原设计理由](../v01-v02-commits-03.md)；[当前机制](06-build-and-entry.md)；[完整diff](history/24-d5cf131.patch.md)。

这里新增@file/@brief和成员说明，而继承关系、成员类型、函数体保持原机制。本提交给既有Runtime/transport/工具补导航文字，没有增加策略或并发保证。

<!-- verified-history d5cf13141d48b2ab1dd9a2373ae0760ab98eaf8c 5d2766db2708fb779c8d1a4b50c0ed6beaec463836fce49e774c00754c77999b 596-623 -->
节选文件：`services/gateway/include/net/ConnectionRuntime.h`，原始完整diff第596–623行。

~~~diff
diff --git a/services/gateway/include/net/ConnectionRuntime.h b/services/gateway/include/net/ConnectionRuntime.h
index a7351d0..4a2d4e3 100644
--- a/services/gateway/include/net/ConnectionRuntime.h
+++ b/services/gateway/include/net/ConnectionRuntime.h
@@ -1,3 +1,6 @@
+/** @file
+ * @brief Coordinates transport, context, features, and packet dispatch.
+ */
 #pragma once
 #include <atomic>
 #include <thread>
@@ -11,6 +14,7 @@ namespace service::gateway
 // features. Transport callbacks capture weak Runtime. All
 // events/control/final release belong to owner; send() alone may
 // delegate from foreign callers.
+/** @brief Coordinates transport, context, features, and packet dispatch. */
 class ConnectionRuntime final
     : public IConnection,
       public IConnectionControl,
@@ -24,43 +28,68 @@ public:
         ConnectionScheduler,
         ConnectionMessageCallback
     );
+    /** @brief Releases owned resources. */
     ~ConnectionRuntime() noexcept override;
+    /** @brief Returns the connection policy pipeline. */
     ConnectionPipeline& features();
     void start();
~~~

注释只作为阅读入口，后续必须对实现核验。比如GDB注释loaded by inspect.py已经与真实入口不一致，不能从英文brief推导当前调用链。

<a id="commit-25"></a>

## 25 · 8217f36 · Add seeded TCP and WebSocket chaos runner with sanitizer telemetry

日期：2026-10-11T16:40:50+08:00；[原设计理由](../v01-v02-commits-03.md)；[当前机制](07-tests.md)；[完整diff](history/25-8217f36.patch.md)。

成功RTT按0.1ms bucket累计，quantile用全程样本数量ceil排序累计得到bin上界，区别于ChurnMonitor最近窗口排序。输出还保存每hostFD/RSS/counter/正常退出诊断证据。

<!-- verified-history 8217f36543cac488c95957e138f0d0ff17e34207 75c984a7da63114bb2058e4974f239b07a5fa6b851e60a33c65a05c81fe2ae01 356-383 -->
节选文件：`tests/gateway/net/ChaosRun.py`，原始完整diff第356–383行。

~~~diff
+                self.name, "slow-reader path not exercised", self.rejections)
+
+    def summary(self):
+        def quantile(histogram, fraction):
+            if not histogram:
+                return None
+            rank = math.ceil(sum(histogram.values()) * fraction)
+            count = 0
+            for bucket, occurrences in sorted(histogram.items()):
+                count += occurrences
+                if count >= rank:
+                    return bucket / 10
+        with self.lock:
+            return {"name": self.name, "pid": self.process.pid,
+                    "profile": self.profile, "ws": self.ws,
+                    "binary": self.binary, "baseline_fd": self.baseline_fd,
+                    "fd_peak": self.fd_peak,
+                    "connections_peak_sampled": self.connections_peak_sampled,
+                    "rss_start_mib": self.rss_start_mib,
+                    "rss_last_mib": self.rss_last_mib,
+                    "rss_peak_mib": self.rss_peak_mib,
+                    "connect_p99_ms": quantile(self.connect_latency, .99),
+                    "healthy_rtt_p99_ms": quantile(self.healthy_rtt, .99),
+                    "healthy_rtt_p50_ms": quantile(self.healthy_rtt, .5),
+                    "counts": dict(self.counts), "rejections": self.rejections,
+                    "diagnostics": self.errors,
+                    "exit_code": self.process.poll()}
+
~~~

线程actor有锁累加指标，日志持续排空；六宿主随机混合场景，必须实际命中SlowClient且正常退出LSan。此提交新增测试负载，不修改生产I/O。

<a id="commit-26"></a>

## 26 · 51aaeff · Reassemble WebSocket continuation frames in the chaos client

日期：2026-10-11T16:44:27+08:00；[原设计理由](../v01-v02-commits-03.md)；[当前机制](07-tests.md)；[完整diff](history/26-51aaeff.patch.md)。

客户端只在第一个binary opcode2后接受continuation0，积累payload到FIN，控制ping/pong不改变message状态，并限制单frame/总message1MiB。原receive只拿一个frame的路径被替换。

<!-- verified-history 51aaeff811f0924192cc44cf88eade0e4b0e6fc4 94686efebfadeff622a439811bca93fbb2b8aac3d6eb664e8a53bd315a14094a 50-76 -->
节选文件：`tests/gateway/net/ChaosRun.py`，原始完整diff第50–76行。

~~~diff
+        payload = exact(peer, size)
+        if opcode in (9, 10):
+            assert first & 0x80 and size <= 125
+            if opcode == 9:
+                send_frame(peer, 10, payload)
+            continue
+        assert opcode == (0 if started else 2), ("unexpected WS opcode", opcode)
+        started = True
+        result.extend(payload)
+        assert len(result) <= 1024 * 1024, "unbounded WS response"
+        if first & 0x80:
+            return bytes(result)
+
 def timestamp():
     return datetime.datetime.now(datetime.timezone(
         datetime.timedelta(hours=8))).isoformat(timespec="seconds")
@@ -150,9 +179,7 @@ class Host:
 
     def receive(self, peer):
         if self.ws:
-            opcode, data = read_frame(peer)
-            assert opcode == 2, (self.name, opcode)
-            return data
+            return read_ws_message(peer)
         return read_tcp_packet(peer)
 
     def expect_closed(self, peer):
~~~

4096+15是合法分片，不应修改Beast去迎合错误客户端。真实4KiB/64KiB短测锁定回归；失败长run后重新计满30分钟，不拼接失败窗口。

<a id="commit-27"></a>

## 27 · 5705fcb · Document gateway v0.2 threats and chaos acceptance boundaries

日期：2026-10-11T16:46:58+08:00；[原设计理由](../v01-v02-commits-03.md)；[当前机制](05-runtime-routing.md)；[完整diff](history/27-5705fcb.patch.md)。

这是威胁表的Markdown diff，显示T07的状态门禁与真实认证开放项。它把现有源码锚点、这轮证据、未覆盖条件并排，防止测试宿主合成认证被误认为生产鉴权。

<!-- verified-history 5705fcb25d6207a82f7466c82d1547703a263e82 8ff9240fe7820141a2ded6838c5c99cad77636df44e16a0541365fbc41048b53 23-50 -->
节选文件：`docs/security/threats-v0.2.md`，原始完整diff第23–50行。

~~~diff
+| T01 | 非法magic、长度、版本、类型导致错误分配/解析 | shared/src/protocol/PacketHeaderCodec.cpp、PacketCodec.cpp；gateway body≤256KiB，元数据7字节；PacketPipeline关闭并计InvalidFrame | magic/length/version/type随机穿插；要求EOF/RST或WS1008 | 不进行全输入空间fuzz；4MiB是shared上限，不是gateway配置 |
+| T02 | 巨大/深度protobuf与未知字段放大内存或栈 | shared/src/protocol/EnvelopeCodec.cpp：256KiB、16层、局部parse后Swap；显式未知MsgId仍有同样边界 | malformed protobuf、24层Error、payloadless未知MsgId1001；InvalidEnvelope/HandlerRejected计数 | 单包界不等于总进程内存预算；本轮未覆盖所有protobuf wire组合 |
+| T03 | 高频完整包占用owner CPU | TokenBucketFeature.cpp；main握手每连接突发200、补充100包/s；解码/业务前扣令牌 | 每轮512包burst，要求早关闭且rate计数>0 | 桶按完整包工作；不限制原始字节流、不跨连接共享；新连接可重新得到突发额度 |
+| T04 | 慢读、输出堆积、并发send破坏帧 | BackpressureFeature.cpp：512KiB暂停/256KiB恢复、5s慢客户端；transport 1MiB queued+inflight硬界；TCP ingress mutex/owner、Asio/WS串行write | echo profile合法64KiB包＋不读响应；每轮FD回基线，SlowClient计数>0 | 本轮每peer由一个actor发送，不能代替concurrent-send/TSan；背压通常先于硬界触发 |
+| T05 | 频繁RST/FIN、半包、重连触发UAF、重复close或残留timer | Runtime延迟close/事件强owner；transport弱回调、operation_aborted、owner删除；WS close/abort状态与5s强制结束 | 随机半包RST/截断FIN、健康连接重连、ASan/UBSan与最终LSan正常退出 | 只覆盖实际调度；无本轮TSan、跨机网络分区、SIGKILL或OOM注入 |
+| T06 | 非可信Origin、非binary或超长WS消息绕过应用策略 | WebSocketServer.cpp：精确Origin/路径、HTTP头8KiB/body0、握手期限；binary；WS message≤256KiB+8 | 坏Origin403、text1003、超长1009、应用拒绝1008 | 原生客户端可伪造Origin；需真实认证，Origin不能代替它；本轮未测TLS |
+| T07 | 未认证连接执行业务 | MessageRouter.cpp在Handler前按route检查Session；MessageRouteOptions.requiresAuthentication默认true；default Login返回Unavailable2002 | 此轮使用默认公开Ping/Unavailable Login，无真实私有业务；认证门禁依据源码及tests/gateway/net/MessageDispatchTest.cpp | **开放：真实认证服务、授权/撤销、重放防护未实现**；不能宣称已具备生产认证 |
+| T08 | 慢速输入/不登录占连接 | TCP已有idle；WS握手期限；每server容量上限 | 半包关闭已覆盖；峰值阶段在容量内 | **开放：零输入登录期限、完整包解析期限、IP并发上限/全局准入未实现**；持续少量输入可能延长idle |
+| T09 | 同IP多连接或TCP+WS各自容量叠加耗尽总内存 | TcpServer/AsioTcpServer/WebSocketServer各自maxConnections，输入/输出/pending有连接级界 | 峰值阶段每宿主64，六宿主合计384；FD按轮归零至基线 | **开放：共享IP表/共享总内存预算**；此次未达到1000 TCP或100 WS宿主上限，不能声称验证了容量拒绝 |
+| T10 | 请求id复用、迟到完成、阻塞业务导致错配或无限挂起 | MessageRouter::State ID+generation、pending≤64、owner timer、finish再校验deadline；固定Typed Error | 正常请求id/响应校验，unknown1001；专门deadline/reuse/cancel见既有MessageDispatchTest | 本轮默认同步Ping，未注入异步业务timeout；owner timer不能抢占阻塞Handler；业务必须非阻塞 |
+| T11 | 日志泄漏凭据/异常文本，日志量放大磁盘/CPU | router只记录connection/request/msg/code/outcome；Typed Error固定文本，不发送未知what() | 本轮保存拒绝计数和诊断；工具日志每host3×5MiB轮转 | 生产spdlog同步输出，无全局日志限速/持久化轮转配置；自定义handler应沿用脱敏规则 |
+| T12 | 字段号复用、把WS frame或message当唯一Packet造成兼容/数据错误 | Envelope标签2 reserved，msg_id新号4；PacketCodec处理应用流；ChaosRun.read_ws_message重组continuation | 初次短run暴露客户端frame=message假设；真实4KiB/64KiB回归修复；最终运行重新计满30分钟 | 旧schema能读字段不代表理解新业务；未来协议变更保留双向兼容/未知字段与边界测试 |
+| T13 | FD资源耗尽/运行库依赖与异常恢复 | accept退避/备用FD/生命周期已存在；相关FdExhaustion集成测试 | 本轮不降低RLIMIT，检查正常FD回收 | **验证缺口：既有epoll满FD测试受UBSan内部pipe(EMFILE)冲突影响**；不能用此chaos结果覆盖该专项 |
+| T14 | 数据链路嗅探、凭据窃取 | 当前main仅loopback，无TLS | loopback实测 | **外部部署前：TLS或可信TLS终止、认证凭据策略、监听与防火墙配置**；本轮不覆盖外网/跨机 |
+
+## v0.2 判定边界
+
+0 crash/0 leak的验收是“本次完整窗口内无宿主意外退出或sanitizer诊断，逐轮FD回基线，六宿主正常退出并完成LeakSanitizer检查”。这是受测执行路径的证据，不是对所有输入、竞态、未来业务或部署环境的证明。
+
+v0.2标记当前传输/协议/chaos验收里程碑；开放威胁不通过本任务顺便实现。高优先后续顺序：真实认证与授权 → 登录/解析期限 → 共享IP与总资源准入 → TLS部署与日志资源策略。
+
+## 来源锚点
+
+- services/gateway/app/main.cpp：默认raw与显式asio handshake、loopback、策略工厂、监听容量。
+- services/gateway/include/net/PacketBuffer.h、BackpressureFeature.h、MessageRouter.h：gateway/输出/pending预算。
+- services/gateway/src/net/{TcpConnection,AsioConnect,WebSocketServer,ConnectionRuntime,ConnectionSchedulers}.cpp：I/O、队列、取消、owner及关闭。
+- services/gateway/src/net/{PacketPipeline,TokenBucketFeature,PacketWhitelistFeature,MessageRouter,PreLoginHandshake}.cpp：早拒、解码、认证路由和错误映射。
+- shared/src/protocol/{PacketHeaderCodec,PacketCodec,EnvelopeCodec}.cpp、shared/proto/envelope.proto：线格式与解析界。
~~~

没有C++行为变化。应从T03/T08/T09/T10/T11理解速率、慢输入、共享资源、业务期限和日志分别在哪层；列风险不等于已经实现对应防护。

<a id="commit-28"></a>

## 28 · b6dd283 · Record v0.2 thirty-minute chaos acceptance and raw metrics

日期：2026-10-11T17:16:54+08:00；[原设计理由](../v01-v02-commits-03.md)；[当前机制](07-tests.md)；[完整diff](history/28-b6dd283.patch.md)。

这是报告与原始数据提交，PASS绑定单调工作窗口、231轮、六宿主正常退出和sanitizer/FD证据。tested_source_commit仍为分片修复51aaeff，后两提交仅文档；annotated v0.2随后指向本报告提交。

<!-- verified-history b6dd2835a050f3b118e2739ff65f992ed44bca5e 4ea8cf00fa43b6bda20ee44babebd57e3cda00f28130a30cfb868a45f2432b4a 10362-10389 -->
节选文件：`docs/reports/v0.2-chaos-20261011.md`，原始完整diff第10362–10389行。

~~~diff
+++ b/docs/reports/v0.2-chaos-20261011.md
@@ -0,0 +1,121 @@
+# v0.2：30分钟 TCP/WS chaos 报告
+
+## 验收
+
+**PASS：0 crash / 0 detected leak。** 最终完整工作窗口 **1802.626 秒**（要求≥1800秒），231轮；六宿主各231次FD恢复，全部退出0，ASan/UBSan/LeakSanitizer诊断0，callback_failure=0。未强杀或关闭泄漏检查。连接峰值阶段六宿主共384条；健康响应33284，重连5544，随机RST2772，截断FIN1386，慢读周期693。
+
+开始：2026-10-11T16:44:27+08:00；最终采集/退出：2026-10-11T17:12:53+08:00（Asia/Shanghai）。工作窗口不包含之前的准备/失败尝试；最后一批次完整结束可能略超过1800秒。
+
+计时依据是time.monotonic()的1802.626秒。WSL起止ISO墙钟差为1706秒，与单调计时相差96.626秒；该墙钟差异原因未定位，ISO时间仅用于日志定位，不作为持续时间门槛。脚本没有修改系统时钟。
+
+0 leak表示本次受测路径的逐轮FD恢复及正常退出时LSan未发现泄漏，不是RSS必须回到初始值或对所有输入/调度的证明。
+
+## 版本、环境、profile
+
+- 受测源码提交：51aaeff811f0924192cc44cf88eade0e4b0e6fc4；后续提交仅为威胁清单/报告。原用户基线d5cf131。
+- g++-15 (Ubuntu 15.2.0-15ubuntu1~22~ppa2) 15.2.0，Python 3.10.12，WSL Linux 6.18.40.1-microsoft-standard-WSL2，loopback。
+- epoll/Asio TCP各4workers；WS base owner strand。每种两profile，共六个插桩宿主：chaos=默认Envelope Ping/Unavailable Login；chaos_packet=二进制echo测试。
+- 每连接Backpressure(512KiB高/256KiB低，5s慢读)、TokenBucket(200突发/100包每秒)、outer type1白名单。输出1MiB硬界；默认每TCP宿主1000连接、WS宿主100连接；峰值64/host未达到容量上限。
+- ASAN_OPTIONS=detect_leaks=1:halt_on_error=1:abort_on_error=1；UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1。
+- seed=20261011固定负载选择/数据，线程与网络调度仍不确定。未修改内核网络参数、RLIMIT、ASan quarantine或生产实现。
+
+## P99、RSS、连接峰值与拒绝数
+
+| Host | 连接数/已打开 | 健康响应数 | RTT P50 / P99 ms | Connect/Upgrade P99 ms | RSS初始 / 峰值 / drained MiB | 连接峰值（FD增量） | 拒绝事件 |
+| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
+| epoll-chaos | 7080 | 5548 | 8.1 / 211.6 | 15.2 | 23.72 / 371.17 / 371.17 | 64 | 2079 |
~~~

保留RSS尾段增长、墙钟差异和未覆盖项，不能从0 detected leak推导RSS已稳定。完整diff包含JSON/CSV/manifest，不在节选里重复上万采样行。

