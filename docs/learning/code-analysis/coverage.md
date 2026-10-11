# 覆盖与核验说明

范围为固定v0.2的152个作者维护文件、26296行原文；机制解释按模块和方法组组织，完整原文逐文件保留。下表是人工解释的定位，不把“文件名在正文出现”当成语义正确的自动证明。

## 逐项证据与边界

| 要求 | 证据 | 不能由它单独证明 |
| --- | --- | --- |
| 全部作者维护代码 | Git树独立枚举与inventory集合相等；152份源码逐字节/哈希比对；各模块章节阅读 | 第三方/生成代码的行为、所有输入都正确 |
| 代码旁有机制解释 | 01–08按调用链、方法组、状态/线程/寿命、失败与扩展解释；下表定位正文 | 生成器不能判断中文机制解释，需源码审查 |
| 每次历史改动有代码 | 28/28解释、历史节选及全diff与每提交Git对象一致 | 某次历史测试已在本轮重跑 |
| 测试代码可追踪 | 43个C++/Python文件、378宏/函数入口、静态断言例子和每文件激励说明 | 不是378已执行case；helper、参数化展开另算 |
| 资料可打开/追踪 | repo/vault Markdown链接检查，镜像正文与原文/diff比较；报告链接按镜像位置适配 | 浏览器页面/服务器再次运行的可用性 |
| 固定版本、不改生产 | 本任务提交仅docs/learning；v0.2未移动 | 标签之后未来生产改变仍与本资料一致 |

初轮检查补齐PacketBufferTest正文，并修复Obsidian中5个报告引用。27段当前源码节选、28段历史节选/28全diff均核对Git；没有通过删除文件或缩小覆盖集合避开缺项。

## 全文件的解释位置

| 文件 | 分析 | 入口小节/文件地图 |
| --- | --- | --- |
| `.gitignore` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `.vscode/c_cpp_properties.json` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `CMakePresets.json` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `docs/architecture/workspace.dsl` | [分析](06-build-and-entry.md) | 章首/文件地图 |
| `services/gateway/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `services/gateway/app/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `services/gateway/app/main.cpp` | [分析](06-build-and-entry.md) | 3. main.cpp：命令行分支即实际装配 |
| `services/gateway/include/net/Acceptor.h` | [分析](02-reactor.md) | Acceptor：LT监听、资源错误退避 |
| `services/gateway/include/net/AsioConnect.h` | [分析](04-transports.md) | 4. AsioConnect：同一个owner，两条异步操作状态 |
| `services/gateway/include/net/AsioTcpServer.h` | [分析](04-transports.md) | 5. AsioTcpServer：把FD从base context转给worker |
| `services/gateway/include/net/BackpressureFeature.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/Channel.h` | [分析](02-reactor.md) | Channel：事件路由没有Socket所有权 |
| `services/gateway/include/net/ConnectionContext.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/ConnectionFeature.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/ConnectionHandler.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/ConnectionRuntime.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/ConnectionSchedulers.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/EpollPoller.h` | [分析](02-reactor.md) | EpollPoller：内核事件与借用Channel指针 |
| `services/gateway/include/net/EventLoop.h` | [分析](02-reactor.md) | EventLoop：一个owner，两个执行入口 |
| `services/gateway/include/net/EventLoopThread.h` | [分析](02-reactor.md) | EventLoopThread：发布栈内EventLoop不是发布所有权 |
| `services/gateway/include/net/EventLoopThreadPool.h` | [分析](02-reactor.md) | EventLoopThreadPool：网络worker与普通ThreadPool不同 |
| `services/gateway/include/net/IConnection.h` | [分析](04-transports.md) | 1. Interface：共有操作与具体契约 |
| `services/gateway/include/net/ITcpServer.h` | [分析](04-transports.md) | 1. Interface：共有操作与具体契约 |
| `services/gateway/include/net/InetAddress.h` | [分析](02-reactor.md) | Socket与InetAddress：FD所有权与地址值分开 |
| `services/gateway/include/net/MessageRouter.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/PacketBuffer.h` | [分析](03-protocol.md) | 阅读范围 |
| `services/gateway/include/net/PacketEcho.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/PacketPipeline.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/PacketWhitelistFeature.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/PreLoginHandshake.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/RejectionCounters.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/Session.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/SessionManager.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/Socket.h` | [分析](02-reactor.md) | Socket与InetAddress：FD所有权与地址值分开 |
| `services/gateway/include/net/TcpConnection.h` | [分析](04-transports.md) | 2. TcpConnection：跨线程入口与owner输出 |
| `services/gateway/include/net/TcpServer.h` | [分析](04-transports.md) | 1. Interface：共有操作与具体契约 |
| `services/gateway/include/net/Timer.h` | [分析](02-reactor.md) | Timer与TimerQueue：一个timerfd托住多连接期限 |
| `services/gateway/include/net/TimerQueue.h` | [分析](02-reactor.md) | Timer与TimerQueue：一个timerfd托住多连接期限 |
| `services/gateway/include/net/TokenBucketFeature.h` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/include/net/WebSocketServer.h` | [分析](04-transports.md) | 6. WebSocketServer与内部WebSocketConnection |
| `services/gateway/src/net/Accptor.cpp` | [分析](02-reactor.md) | Acceptor：LT监听、资源错误退避 |
| `services/gateway/src/net/AsioConnect.cpp` | [分析](04-transports.md) | 4. AsioConnect：同一个owner，两条异步操作状态 |
| `services/gateway/src/net/AsioTcpServer.cpp` | [分析](04-transports.md) | 5. AsioTcpServer：把FD从base context转给worker |
| `services/gateway/src/net/AuthenticationGateHandler.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/BackpressureFeature.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `services/gateway/src/net/Channel.cpp` | [分析](02-reactor.md) | Channel：事件路由没有Socket所有权 |
| `services/gateway/src/net/ConnectionContext.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/ConnectionHandler.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/ConnectionId.cpp` | [分析](04-transports.md) | 章首/文件地图 |
| `services/gateway/src/net/ConnectionId.h` | [分析](04-transports.md) | ConnectionId的两种身份 |
| `services/gateway/src/net/ConnectionRuntime.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/ConnectionSchedulers.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/EpollPoller.cpp` | [分析](02-reactor.md) | EpollPoller：内核事件与借用Channel指针 |
| `services/gateway/src/net/EventLoop.cpp` | [分析](02-reactor.md) | EventLoop：一个owner，两个执行入口 |
| `services/gateway/src/net/EventLoopThread.cpp` | [分析](02-reactor.md) | EventLoopThread：发布栈内EventLoop不是发布所有权 |
| `services/gateway/src/net/EventLoopThreadPool.cpp` | [分析](02-reactor.md) | EventLoopThreadPool：网络worker与普通ThreadPool不同 |
| `services/gateway/src/net/InetAddress.cpp` | [分析](02-reactor.md) | Socket与InetAddress：FD所有权与地址值分开 |
| `services/gateway/src/net/MessageRouter.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/PacketBuffer.cpp` | [分析](03-protocol.md) | 阅读范围 |
| `services/gateway/src/net/PacketEcho.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/PacketPipeline.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/PacketWhitelistFeature.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/PreLoginHandshake.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/RejectionCounters.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/Session.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/SessionManager.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/Socket.cpp` | [分析](02-reactor.md) | Socket与InetAddress：FD所有权与地址值分开 |
| `services/gateway/src/net/TcpConnection.cpp` | [分析](04-transports.md) | 2. TcpConnection：跨线程入口与owner输出 |
| `services/gateway/src/net/TcpServer.cpp` | [分析](04-transports.md) | 3. TcpServer：控制线程不触碰worker连接 |
| `services/gateway/src/net/TimerQueue.cpp` | [分析](02-reactor.md) | Timer与TimerQueue：一个timerfd托住多连接期限 |
| `services/gateway/src/net/TokenBucketFeature.cpp` | [分析](05-runtime-routing.md) | 1. 文件地图 |
| `services/gateway/src/net/WebSocketServer.cpp` | [分析](04-transports.md) | 6. WebSocketServer与内部WebSocketConnection |
| `shared/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `shared/include/concurrent/BlockingQueue.h` | [分析](01-shared.md) | BlockingQueue：条件变量守护谓词，而非守护通知次数 |
| `shared/include/concurrent/ThreadPool.h` | [分析](01-shared.md) | ThreadPool：通用计算任务池，不是网络Reactor线程池 |
| `shared/include/os/FileDescriptor.h` | [分析](01-shared.md) | FileDescriptor：资源只允许一个所有者 |
| `shared/include/protocol/Buffer.hpp` | [分析](03-protocol.md) | 阅读范围 |
| `shared/include/protocol/EnvelopeCodec.hpp` | [分析](03-protocol.md) | 阅读范围 |
| `shared/include/protocol/PacketCodec.hpp` | [分析](03-protocol.md) | 阅读范围 |
| `shared/include/protocol/PacketHeader.hpp` | [分析](03-protocol.md) | 阅读范围 |
| `shared/include/protocol/PacketHeaderCodec.hpp` | [分析](03-protocol.md) | 阅读范围 |
| `shared/proto/envelope.proto` | [分析](03-protocol.md) | 阅读范围 |
| `shared/src/concurrent/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `shared/src/concurrent/ThreadPool.cpp` | [分析](01-shared.md) | ThreadPool：通用计算任务池，不是网络Reactor线程池 |
| `shared/src/os/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `shared/src/os/FileDescriptor.cpp` | [分析](01-shared.md) | FileDescriptor：资源只允许一个所有者 |
| `shared/src/protocol/Buffer.cpp` | [分析](03-protocol.md) | 阅读范围 |
| `shared/src/protocol/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `shared/src/protocol/EnvelopeCodec.cpp` | [分析](03-protocol.md) | 阅读范围 |
| `shared/src/protocol/PacketCodec.cpp` | [分析](03-protocol.md) | 阅读范围 |
| `shared/src/protocol/PacketHeaderCodec.cpp` | [分析](03-protocol.md) | 阅读范围 |
| `tests/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `tests/gateway/net/AsioConnectTest.cpp` | [分析](07-tests.md) | 4. 连接与服务器测试逐文件 |
| `tests/gateway/net/AsioTcpServerTest.cpp` | [分析](07-tests.md) | 4. 连接与服务器测试逐文件 |
| `tests/gateway/net/AsioTcpServerTestHost.cpp` | [分析](07-tests.md) | 7. 三个独立host的整个生命周期 |
| `tests/gateway/net/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `tests/gateway/net/ChannelTest.cpp` | [分析](07-tests.md) | 3. Reactor/资源/线程测试逐文件 |
| `tests/gateway/net/ChaosRun.py` | [分析](07-tests.md) | 8. Python网络测试逐文件 |
| `tests/gateway/net/ChaosRunFramingTest.py` | [分析](07-tests.md) | 8. Python网络测试逐文件 |
| `tests/gateway/net/ChaosTestProfile.h` | [分析](07-tests.md) | 6. 测试profile头：每个替代行为为何存在 |
| `tests/gateway/net/ConnectionContextIntegrationTest.py` | [分析](07-tests.md) | 8. Python网络测试逐文件 |
| `tests/gateway/net/ConnectionContextTest.cpp` | [分析](07-tests.md) | 5. Context/Runtime/政策/责任链/Router测试逐文件 |
| `tests/gateway/net/ConnectionContextTestHandler.h` | [分析](07-tests.md) | 6. 测试profile头：每个替代行为为何存在 |
| `tests/gateway/net/ConnectionFeatureIntegrationTest.py` | [分析](07-tests.md) | 8. Python网络测试逐文件 |
| `tests/gateway/net/ConnectionFeatureTest.cpp` | [分析](07-tests.md) | 5. Context/Runtime/政策/责任链/Router测试逐文件 |
| `tests/gateway/net/ConnectionFeatureTestProfile.h` | [分析](07-tests.md) | 6. 测试profile头：每个替代行为为何存在 |
| `tests/gateway/net/ConnectionLifecycleTest.cpp` | [分析](07-tests.md) | 4. 连接与服务器测试逐文件 |
| `tests/gateway/net/ConnectionOwnershipTest.cpp` | [分析](07-tests.md) | 4. 连接与服务器测试逐文件 |
| `tests/gateway/net/ConnectionRuntimeTest.cpp` | [分析](07-tests.md) | 5. Context/Runtime/政策/责任链/Router测试逐文件 |
| `tests/gateway/net/ConnectionRuntimeTestSupport.h` | [分析](07-tests.md) | 5. Context/Runtime/政策/责任链/Router测试逐文件 |
| `tests/gateway/net/EpollPollerPerformanceTest.cpp` | [分析](07-tests.md) | 10. 三种性能测试逐文件 |
| `tests/gateway/net/EpollPollerTest.cpp` | [分析](07-tests.md) | 3. Reactor/资源/线程测试逐文件 |
| `tests/gateway/net/EventLoopTest.cpp` | [分析](07-tests.md) | 3. Reactor/资源/线程测试逐文件 |
| `tests/gateway/net/EventLoopThreadPoolTest.cpp` | [分析](07-tests.md) | 3. Reactor/资源/线程测试逐文件 |
| `tests/gateway/net/EventLoopThreadTest.cpp` | [分析](07-tests.md) | 3. Reactor/资源/线程测试逐文件 |
| `tests/gateway/net/IConnectionTest.cpp` | [分析](07-tests.md) | 4. 连接与服务器测试逐文件 |
| `tests/gateway/net/MessageDispatchIntegrationTest.py` | [分析](07-tests.md) | 8. Python网络测试逐文件 |
| `tests/gateway/net/MessageDispatchTest.cpp` | [分析](07-tests.md) | 5. Context/Runtime/政策/责任链/Router测试逐文件 |
| `tests/gateway/net/MessageDispatchTestProfile.h` | [分析](07-tests.md) | 6. 测试profile头：每个替代行为为何存在 |
| `tests/gateway/net/PacketBufferTest.cpp` | [分析](07-tests.md) | 3. Reactor/资源/线程测试逐文件 |
| `tests/gateway/net/PacketCodecIntegrationTest.py` | [分析](07-tests.md) | 8. Python网络测试逐文件 |
| `tests/gateway/net/PacketPipelineTest.cpp` | [分析](07-tests.md) | 5. Context/Runtime/政策/责任链/Router测试逐文件 |
| `tests/gateway/net/SecurityPolicyIntegrationTest.py` | [分析](07-tests.md) | 8. Python网络测试逐文件 |
| `tests/gateway/net/SecurityPolicyTest.cpp` | [分析](07-tests.md) | 5. Context/Runtime/政策/责任链/Router测试逐文件 |
| `tests/gateway/net/SecurityPolicyTestProfile.h` | [分析](07-tests.md) | 6. 测试profile头：每个替代行为为何存在 |
| `tests/gateway/net/SessionManagerTest.cpp` | [分析](07-tests.md) | 3. Reactor/资源/线程测试逐文件 |
| `tests/gateway/net/SocketTest.cpp` | [分析](07-tests.md) | 3. Reactor/资源/线程测试逐文件 |
| `tests/gateway/net/TcpServerChurnDashboard.html` | [分析](08-observability-tools.md) | 文件和阅读路径 |
| `tests/gateway/net/TcpServerChurnMonitor.py` | [分析](08-observability-tools.md) | 文件和阅读路径 |
| `tests/gateway/net/TcpServerIntegrationTest.py` | [分析](07-tests.md) | 8. Python网络测试逐文件 |
| `tests/gateway/net/TcpServerTestHost.cpp` | [分析](07-tests.md) | 7. 三个独立host的整个生命周期 |
| `tests/gateway/net/TcpServerWorkerTest.cpp` | [分析](07-tests.md) | 4. 连接与服务器测试逐文件 |
| `tests/gateway/net/TimerQueueTest.cpp` | [分析](07-tests.md) | 3. Reactor/资源/线程测试逐文件 |
| `tests/gateway/net/WebSocketIntegrationTest.py` | [分析](07-tests.md) | 8. Python网络测试逐文件 |
| `tests/gateway/net/WebSocketTestHost.cpp` | [分析](07-tests.md) | 7. 三个独立host的整个生命周期 |
| `tests/shared/concurrent/BlockingQueuePerformanceTest.cpp` | [分析](07-tests.md) | 10. 三种性能测试逐文件 |
| `tests/shared/concurrent/BlockingQueueTest.cpp` | [分析](07-tests.md) | 2. shared单元测试逐文件 |
| `tests/shared/concurrent/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `tests/shared/concurrent/ThreadPoolPerformanceTest.cpp` | [分析](07-tests.md) | 10. 三种性能测试逐文件 |
| `tests/shared/concurrent/ThreadPoolTest.cpp` | [分析](07-tests.md) | 2. shared单元测试逐文件 |
| `tests/shared/os/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `tests/shared/os/FileDescriptorTest.cpp` | [分析](07-tests.md) | 2. shared单元测试逐文件 |
| `tests/shared/protocol/CMakeLists.txt` | [分析](06-build-and-entry.md) | 1. 全部构建文件与依赖传播 |
| `tests/shared/protocol/EnvelopeCodecTest.cpp` | [分析](07-tests.md) | 2. shared单元测试逐文件 |
| `tests/shared/protocol/PacketCodecTest.cpp` | [分析](07-tests.md) | 2. shared单元测试逐文件 |
| `tests/shared/protocol/PacketHeaderCodecTest.cpp` | [分析](07-tests.md) | 2. shared单元测试逐文件 |
| `tests/shared/protocol/compat/legacy_envelope.proto` | [分析](07-tests.md) | 2. shared单元测试逐文件 |
| `tools/gateway_inspect/capture.py` | [分析](08-observability-tools.md) | 文件和阅读路径 |
| `tools/gateway_inspect/capture_gdb.py` | [分析](08-observability-tools.md) | 文件和阅读路径 |
| `tools/gateway_inspect/dashboard.html` | [分析](08-observability-tools.md) | 文件和阅读路径 |

## 本次实际执行

仅运行文档生成器/静态核验、Git diff检查和证据比对；没有运行历史测试、网络服务、负载或profiler。脚本结果见[verification.json](verification.json)；构建/chaos的历史运行证据另见相应章节和归档报告。

这次学习任务不会顺便改Timer deadline、WS connected或GDB布局等观察项。它们被标为静态观察或当前限制，避免把未复现/未修复的内容写成已通过验收。
