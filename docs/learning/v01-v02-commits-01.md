# v0.1 → v0.2：逐提交设计讲解（一）

范围：01–11。入口：[学习导航](v01-v02-index.md)，下一章：[Runtime、策略与消息路由](v01-v02-commits-02.md)。

顺序由git log --reverse v0.1..v0.2确定。“为什么”基于diff、接口契约与既有Change Note还原，不声称知道未记录的主观动机。路径相对仓库；历史源码用git show SHA:path，改动用git show SHA -- path。测试说明是提交的验证面，本次文档任务没有重跑历史测试。

## 01 · 698ca5c · 2026-10-05 · IConnection：先稳定使用契约

原题：Fix IConnection abstraction and organize TcpConnection implementation。

**原来与原因。** v0.1已有epoll、worker、半关闭、水位及有界输出，但业务直接依赖TcpConnection。新增IConnection让替换I/O实现时，上层仍使用同一send/shutdown/close契约；同时整理原生实现，属于真实行为和接口修复。

**技术与路径。** TcpConnection final实现IConnection，保留send(pointer,length)。send(span)返回前使用或复制字节；shutdown排空输出后SHUT_WR；close逻辑关门与回收通知；connected在排水/半关闭仍为true。虚析构保证多态回收，原子分配非零稳定id，区别于可复用FD及服务器票据。

**线程与失败。** 当时除id外操作和最终释放均owner；shared_ptr只延长寿命。先注销Channel，再析构Socket；关闭回调不能同步delete当前连接。EPOLLHUP但SO_ERROR为0时继续读/EOF处理，保留半关闭和输出排水。

**取舍与扩展。** 接口藏起epoll与Buffer，但调用方仍须理解connected不等于可继续send；继承接口也不自动线程安全。v0.2中业务收到ConnectionRuntime的IConnection facade。

**证据。** services/gateway/include/net/IConnection.h、TcpConnection.h；src/net/TcpConnection.cpp；tests/gateway/net/IConnectionTest.cpp验证多态、暂停、半关闭、字节所有权、ID及owner析构。原笔记: ArenaServer/changes/2026-10-05-iconnection-review-fixes.md (Obsidian)。

## 02 · babcc33 · 2026-10-05 · ITcpServer：统一管理面

原题：Extract ITcpServer lifecycle interface。

**原因。** 单连接有了接口，服务器仍绑定具体实现。ITcpServer提取stop/stopped/connectionCount/maxConnections/workerCount；main与宿主持有unique_ptr<ITcpServer>。

**机制。** 构造具体server即监听；stop默认5秒：停准入→owner排水/FIN→期限强关→回收确认→worker cleanup/join→Stopped。计数包含待注册及待回收确认票据，0连接不等于Stopped；接口未提供start/restart。

**线程与取舍。** 构造、管理、查询及析构归base；外部EventLoop须更长寿。多态析构不迁移线程，不替调用方停止loop。管理面与单连接I/O分开，后端自行处理停止屏障。

**扩展。** 新server Adapter实现同一管理契约，不必统一构造参数。v0.2 WebSocketServer没有继承ITcpServer，不能假设它有workerCount。

**证据。** ITcpServer.h、TcpServer.h、main.cpp；TcpServerWorkerTest.cpp、ConnectionOwnershipTest.cpp及TcpServerTestHost.cpp的基类查询、停止、析构验证。原笔记: ArenaServer/changes/2026-10-05-itcpserver-interface.md (Obsidian)。

## 03 · f6749f3 · 2026-10-05 · Asio：第二个实际实现

原题：Implement Boost.Asio connection and TCP server backend。

**原因。** 用真实第二种I/O实现检验接口：上层契约复用，epoll内部不改成Asio。新增AsioConnect、AsioTcpServer与公共ConnectionId分配器。

**运行链。** base async_accept→容量/票据→socket.release到AcceptedFd RAII→post worker→worker socket.assign→回调注册/start→async_read_some→发送队列→async_write_some/offset→close→延迟erase→base回执。只移交FD，Asio socket对象留在原context服务。每worker独立context、一个run线程；0 worker使用base。

**寿命与失败。** read_some/write_some可部分成功；活动写Buffer不能释放/重分配。completion强持Buffer、弱持连接；取消后迟到完成安全返回。base work guard防止worker回执前run结束。cleanup投递→取消完成排空→join构成停止屏障。EMFILE/ENFILE用备用FD与退避，构造失败回滚workers。

**取舍。** 当时send仍owner，尚无strand，水位策略在transport内；Linux FD策略使它并非跨平台实现。v0.2仍每context一个owner，后来strand没有把管理操作改成任意线程可调用。

**证据。** AsioConnect.h/.cpp、AsioTcpServer.h/.cpp、ConnectionId.cpp、main.cpp；AsioConnectTest.cpp、AsioTcpServerTest.cpp与0/4-worker真实TCP测试。原笔记: ArenaServer/changes/2026-10-05-boost-asio-backend.md (Obsidian)，含历史验证与满FD/UBSan限制。

## 04 · 146518c · 2026-10-06 · PacketCodec：字节流成为应用包

原题：Add packet codec and gateway echo integration。

**原因。** TCP一次read可能半包/粘包，raw echo没有消息边界。shared负责流式framing，gateway决定业务策略，两个transport共用同一codec。

**路径与机制。** ConnectionHandlerFactory每连接创建独立回调→PacketCodec.feed→完整header/payload→当时PacketEcho检查type→编码响应/echo header request id→IConnection.send。decoder仅保留一个未完成帧，多完整帧循环处理；NeedMore正常，Invalid/Stopped终止。payload view只在callback内有效，不能留给异步业务。

**线程与资源。** 每连接owner串行feed，不允许重入；shared通用body界4MiB，gateway实例256KiB。先验证声明长度，再等待/处理body；callback异常终止decoder并关闭。

**取舍与扩展。** 外层type/request id/version独立于payload编码；以后可用protobuf。此时尚无magic、Envelope与Runtime。v0.2 framing仍在shared，不能把账号/业务路由写进PacketCodec。

**证据。** shared/include/protocol/PacketCodec.hpp、src/protocol/PacketCodec.cpp；gateway ConnectionHandler.h、PacketEcho.cpp；PacketCodecIntegrationTest.py共测epoll/Asio×0/4 workers。原笔记: ArenaServer/changes/2026-10-05-packet-codec-transports.md (Obsidian)日期早于实际提交，本文按Git排序。

## 05 · a9eb1da · 2026-10-07 · 状态机：互斥与正交分开

原题：Clarify AsioConnect lifecycle and read phases。

**原因。** closed/shutdownRequested/writeHalfClosed/peerHalfClosed易出现矛盾；reading/readReady/delivering也互斥。enum约束合法组合并明确事件转移。

**机制。** State为Connected、PeerHalfClosed、WriteDraining、WriteDrainingPeerClosed、WriteHalfClosed、Closed；ReadState为Idle→InFlight→Ready→Delivering→Idle。暂停期间完成读留在Ready，resume先交付。EOF时若写已半关可close；排水中转到WriteDrainingPeerClosed，输出完成后再关闭。

**为什么不只有一个state。** 生命周期、一次读的阶段、暂停条件和活动写可以同时存在：WriteDraining时仍可InFlight读。全并成enum产生状态乘积；started是激活步骤，readPaused和writing分别控制独立维度。

**线程与扩展。** 当时owner串行，enum不是并发同步手段。新增状态须给出事件转移与不变量。v0.2继续分开读阶段和生命周期。

**证据。** AsioConnect.h/.cpp、AsioConnectTest.cpp的暂停/恢复、EOF交付场景。原笔记: ArenaServer/changes/2026-10-06-asio-connect-state-machine.md (Obsidian)。

## 06 · c01f0aa · 2026-10-07 · Asio并发send：入口排队，I/O串行

原题：Serialize concurrent AsioConnect sends and cancellation。

**原因。** 并发send不能改活动输出或重叠async_write；仅strand还不能限制post之前的无界复制，需要入口先预留有界字节。

**路径。** 发送线程→m_sendMutex保护发送门/pending/m_sendQueue→复制独立Buffer→合并drain post到strand→owner移入m_output→唯一async_write_some→完成扣字节/更新offset→下一块。pending包括入口、活动及未报告完成字节，复制前检查1MiB。

**取消与寿命。** 完成绑定strand，m_writing保证单写。closeOnce关门/取消/清队列/只通知一次；operation_aborted分支不交付正常数据。活动Buffer随completion存活，连接弱引用可失效。shutdown先关入口，排完已接纳输出再FIN。

**取舍。** mutex只保护跨线程入口，socket/state仍owner。发送者调用期间须共享保活，最终析构owner。并发顺序按入队锁获取，不承诺调用开始时间排序；每次send字节连续。strand只序列化，不取消单owner限制。

**证据。** AsioConnect::send/drainSendQueue/writeNext/closeOnce；AsioConnectTest.cpp新增并发、上限、shutdown、取消释放测试。原笔记: ArenaServer/changes/2026-10-07-asio-concurrent-send.md (Obsidian)。

## 07 · c91409a · 2026-10-07 · 原生TCP使用同一并发思路

原题：Support concurrent TcpConnection sends with bounded queue。

**原因。** 原生PacketBuffer会扩容、移动内容并改变多个索引；原子写位置只预留地址，不能保证内存不移动或reader不读未完成写入。

**路径。** send→mutex保护deque<vector<byte>>和预算→EventLoop.queueInLoop/eventfd→owner drainSendQueue→sendInLoop→非阻塞send(2)→部分写/EAGAIN余量进m_outputBuffer→EPOLLOUT继续。Socket/Channel/活动Buffer仍单owner。

**边界与寿命。** 复制前1MiB预算包含入口及未写出数据；关门/入队互斥。close清入口，活动Buffer由owner延迟回收，弱drain任务不访问已析构对象。新增enable_shared_from_this保护任务，shared_ptr并不授权外线程最终析构。

**取舍与扩展。** 小队列把并发复杂度集中在入口，避免多生产者可变Buffer的提交/扩容/回收协议。实际socket写仍串行。v0.2 Feature只控制策略，不替代transport硬界。

**证据。** TcpConnection.h/.cpp::send/drainSendQueue/sendInLoop、IConnection.h；ConnectionLifecycleTest.cpp验证并发、上限、close/shutdown与Buffer隔离。原笔记: ArenaServer/changes/2026-10-07-tcp-concurrent-send.md (Obsidian)。

## 08 · 84e2ef8 · 2026-10-07 · Envelope：稳定字段与有界解析

原题：Add versioned Envelope protobuf schema and bounded codec。

**原因。** Packet仅帧格式；Login/Ping/Error需业务数据模型。protobuf支持未知字段，但不自动保证资源预算及退役字段不复用。

**机制。** request_id=1、sent_at_unix_ms=3；oneof LoginRequest10/LoginResponse11/Ping12/Pong13/Error14；tag2及legacy_client_tag名字reserved。Error.causes递归。EnvelopeCodec用CodedInputStream限256KiB/16层、全量消费；局部parsed验证成功才Swap，失败output不变；encode也检查大小/深度/有效payload。

**兼容与取舍。** 冻结legacy schema测旧读新、新读旧、未知字段保留/转发。字段能读取不代表旧端认识新业务。该提交只加shared codec与测试，未将默认raw切为protobuf，也没有真实鉴权。

**扩展。** 新字段选新号码，删除继续reserved；oneof增加还要更新codec和route成功响应验证。v0.2后加msg_id=4，旧编号不变；此时Error.code还是uint32字段，稳定enum语义稍后加入。

**证据。** shared/proto/envelope.proto、EnvelopeCodec.hpp/.cpp、shared/CMakeLists.txt；tests/shared/protocol/EnvelopeCodecTest.cpp、compat/legacy_envelope.proto。原笔记: ArenaServer/changes/2026-10-07-envelope-protobuf-compatibility.md (Obsidian)。

## 09 · fd13a53 · 2026-10-08 · Magic：提前识别协议

原题：Add packet header magic and update framing。

**原因。** 长度开头难判断Arena协议；magic让前4字节就能拒绝错误协议，同时统一prefix/body计算。

**线格式。** 大端magic4（0x4152454E，AREN）+bodyLength4+type2+requestId4+version1+payload。prefix8、metadata7、header15；bodyLength=7+payload，总帧=8+bodyLength。先magic再length再完整body；memcpy+ntohl/htons避开未对齐强转。

**失败与兼容。** 错magic直接Invalid/关闭，不扫描重同步。**外层wire破坏性升级：旧无magic客户端必须升级。** protobuf兼容不等于外层兼容；magic不是MAC/签名/认证。

**扩展。** 手写客户端统一偏移/长度；gateway256KiB是body预算，protobuf payload还须减7。7ff856d修复响应的这一预算差。

**证据。** PacketHeader.hpp、PacketHeaderCodec.hpp/.cpp、PacketCodec.cpp；PacketCodecTest/PacketHeaderCodecTest与PacketCodecIntegrationTest更新magic和边界。原笔记: ArenaServer/changes/2026-10-08-packet-header-magic.md (Obsidian)。

## 10 · 992a42b · 2026-10-08 · 完整包处理点从echo拆出

原题：Add complete-packet processing pipeline。

**原因。** PacketEcho混合framing/类型/响应；继续加鉴权/路由会堆分支。先建立完整包后的可组合处理位置。

**当时机制。** PacketPipeline是vector<std::function<PacketStageResult(PacketContext&)>>：codec→Context(connection/header/payload/optional Envelope)→stage顺序执行。Continue继续、Handled结束当前帧、Reject关连接。decodeEnvelope可选，echo迁到stage。

**寿命与失败。** Context每帧临时，view只活到callback返回；每连接配置一次/owner串行。Reject或异常终止输入处理，不能close后继续处理同次粘包。没有全局共享有状态pipeline。

**取舍与演进。** 此提交是函数stage列表，尚非class Handler链；它先确定framing后扩展点，fc55741再换表达方式无需动codec。不能用v0.2 unique_ptr Handler倒写历史。

**证据。** PacketPipeline.h/.cpp、PacketEcho.cpp；PacketPipelineTest.cpp覆盖顺序、短路、protobuf、Reject/异常。原笔记: ArenaServer/changes/2026-10-08-packet-pipeline.md (Obsidian)。

## 11 · 3942122 · 2026-10-08 · Beast WS：复用应用协议

原题：Add Beast WebSocket transport and pre-login handshake。

**原因。** 浏览器需HTTP Upgrade/WS控制协议，但Packet与业务不应重写。新增独立WebSocketServer和内部IConnection实现。

**路径。** HTTP解析→路径/Origin精确匹配→Upgrade→binary message→同PacketCodec/stage→Envelope Ping→Pong→Packet编码→WS send queue。TCP/WS都可登录前Ping；应用预登录交换不等于已有Login认证。

**资源与并发。** HTTP头8KiB/body0/握手期限；WS message≤256KiB+8，输出queue+inflight≤1MiB。入口mutex，strand串行读写/close，唯一活动write。text/超长/策略close code分别1003/1009/1008；等待活动写后close handshake，5秒期限可abort，活动Buffer由操作保活。

**取舍与扩展。** Origin可被原生客户端伪造；此时未统一BackpressureFeature。main仅asio handshake同时开TCP8888/WS8889；epoll宿主可复用handler不等于main允许epoll handshake。WS frame/message与Packet独立，chaos客户端后来暴露fragmentation误读。

**证据。** WebSocketServer.h/.cpp、PreLoginHandshake.cpp、main.cpp；WebSocketIntegrationTest.py/WebSocketTestHost.cpp测试双传输、Origin、限额、close、慢读。原笔记: ArenaServer/changes/2026-10-08-beast-websocket-transport.md (Obsidian)。
