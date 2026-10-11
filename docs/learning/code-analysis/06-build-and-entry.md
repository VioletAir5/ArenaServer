# 06 构建、入口与目标架构DSL

固定v0.2。代码接口能做什么与main实际启用什么需要分开。本章覆盖所有CMake、presets/编辑配置、忽略配置、main和目标workspace.dsl；测试注册也在[07](07-tests.md)说明。

## 1. 全部构建文件与依赖传播

| 文件 | 作用与分析 |
| --- | --- |
| `CMakeLists.txt` | CMake≥3.23、C++23 required、compile_commands；UBSan默认开；固定googletest归档；加入shared/gateway/tests |
| `shared/CMakeLists.txt` | 建arena_shared；include和libprotobuf PUBLIC；自定义protoc生成envelope.pb.cc/h，再target dependency保证编译前存在 |
| `shared/src/os/CMakeLists.txt` | GLOB CONFIGURE_DEPENDS *.cpp加到shared，FileDescriptor实现 |
| `shared/src/concurrent/CMakeLists.txt` | 同样收集ThreadPool.cpp；模板BlockingQueue在头 |
| `shared/src/protocol/CMakeLists.txt` | 同样收集Buffer/Header/Packet/Envelope codecs |
| `services/gateway/CMakeLists.txt` | Boost≥1.74 CONFIG、Threads、spdlog；arena_gateway PUBLIC依赖shared/Boost/Threads/spdlog |
| `services/gateway/src/net/CMakeLists.txt` | GLOB *.cpp加gateway；含旧Session等未接当前业务路径的编译代码 |
| `services/gateway/app/CMakeLists.txt` | ArenaServer main链接gateway，警告及条件UBSan compile/link flags |
| `tests/CMakeLists.txt` | arena_tests/arena_perf_tests，统一收集与gtest_discover；-no-pie、条件UBSan |
| `tests/shared/os/CMakeLists.txt` | 调用arena_collect_tests收FileDescriptorTest |
| `tests/shared/concurrent/CMakeLists.txt` | 同一收集函数，把*PerformanceTest分到perf目标 |
| `tests/shared/protocol/CMakeLists.txt` | 冻结legacy proto独立生成/依赖/include到arena_tests，再收协议测试 |
| `tests/gateway/net/CMakeLists.txt` | 三宿主、Python解释器、真实网络CTest矩阵、timerfd_create test-only wrap |
| `CMakePresets.json` | 三个Linux GCC15构建/测试preset，隔离build目录与sanitizer组合 |
| `.vscode/c_cpp_properties.json` | Linux IntelliSense、g++15/C++23、build/compile_commands引用；不决定真实编译 |
| `.gitignore` | 忽略build产物/日志/cache/local-notes及本地agent/editor规则；不删除文件 |
| `docs/architecture/workspace.dsl` | Structurizr目标模型，不是生产执行代码 |

根工程FetchContent固定zip commit，DOWNLOAD_EXTRACT_TIMESTAMP控制解包时间；首次configure需已有缓存或能下载。所有service scope并未都加入构建，只有gateway是实现目标。没有vcpkg安装/启动数据库或隐式Auth进程。

shared生成目录向gateway/test PUBLIC传播，因为EnvelopeCodec头直接包含envelope.pb.h；GLOB CONFIGURE_DEPENDS使新增cpp触发CMake重新检查，但不是根据C++ include自动找实现。Boost.System指定header-only宏，没要求链接boost_system动态库。

## 2. Presets和sanitizer含义

gcc15-base为hidden，Linux条件、Unix Makefiles、/usr/bin/gcc-15与g++-15、Debug；gcc15/build，gcc15-asan/build-asan，gcc15-tsan/build-tsan。ASan额外-fsanitize=address,undefined及frame pointer；TSan单独thread、-g/-no-pie，关闭UBSan。源码注释说明此分离是安装运行库vptr probe冲突的处理，不能同时混用两preset二进制。

普通及ASan test preset排除名字含Performance，jobs4；TSan只include列出的Reactor/TCP ownership及四worker网络，jobs1/timeout60，排除death/FD exhaustion。因此“ctest --preset gcc15-tsan全绿”不等于Asio/WS/router所有路径都跑了TSan。

测试和三host使用-no-pie注释解释WSL下ASan启动问题，生产ArenaServer的app CMake未加此flag。root UBSan开关通过各target compile/link设置；这些配置证明插桩路径，不证明本次学习文档任务重新运行了服务器或全部测试。

## 3. main.cpp：命令行分支即实际装配

文件：`services/gateway/app/main.cpp`。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/app/main.cpp 33-43 -->
原文：`services/gateway/app/main.cpp:L33–L43`；[完整文件](source/services/gateway/app/main.cpp.md)。

~~~cpp
    const std::string_view backend = argc >= 2 ? argv[1] : "epoll";
    const std::string_view protocol = argc >= 3 ? argv[2] : "raw";
    if (argc > 4 || (backend != "epoll" && backend != "asio") ||
        (protocol != "raw" && protocol != "packet" &&
         protocol != "handshake") ||
        (protocol == "handshake" && backend != "asio") ||
        (argc == 4 && protocol != "handshake"))
    {
        std::cerr << "Usage: ArenaServer [epoll|asio] "
                     "[raw|packet|handshake] [ws-origin]\n";
        return 2;
~~~

| 命令模式 | Handler | Features | 监听器 |
| --- | --- | --- | --- |
| 无参数 / epoll raw | 空factory→raw byte echo | 空factory→默认Backpressure | epoll TCP，127.0.0.1:8888 |
| epoll packet | makePacketEchoHandler | 默认Backpressure | 同上 |
| asio raw | raw byte echo | 默认Backpressure | Asio TCP，同端口 |
| asio packet | Packet echo | 默认Backpressure | 同上 |
| asio handshake [origin] | EnvelopeDecode→MessageRouter | 背压＋桶200/100＋outer type {1} | Asio TCP8888及WS8889 |

参数最多4个；backend/protocol必须在枚举字符串中；handshake只接受asio；第4参数仅允许handshake作为Origin；非法usage返回2。因此epoll transport虽能通过测试host跑Envelope Router，**当前main CLI不能选epoll handshake**。

handlerFactory每连接创建fresh decoder/chain，registry可共享sealed常量。默认raw直接echo，不经过PacketCodec，所以onPacket限流/白名单未在raw字节路径运行。handshake显式提供FeatureFactory，必须自己加Backpressure，否则空factory默认不会自动补进来。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/app/main.cpp 58-70 -->
原文：`services/gateway/app/main.cpp:L58–L70`；[完整文件](source/services/gateway/app/main.cpp.md)。

~~~cpp
    ConnectionFeatureFactory featureFactory;
    if (protocol == "handshake")
    {
        featureFactory =
            [](ConnectionPipeline& pipeline, const ConnectionContext&)
        {
            pipeline.add<BackpressureFeature>();
            pipeline.add<TokenBucketFeature>(200.0, 100.0);
            pipeline.add<PacketWhitelistFeature>(
                std::vector<std::uint16_t>{1}
            );
        };
    }
~~~

两个TCP都max1000、idle60s、4额外workers。WS同max1000，运行在Asio base owner而非TCP4worker pool。默认Origin=http://localhost:3000，可用argv替换精确值；监听loopback，真实认证与TLS未启用。

## 4. main的退出与资源寿命

epoll分支创建loop后server、loop.loop。没有注册SIGINT/SIGTERM的优雅stop逻辑；正常退出作用域时server先析构，loop仍活。不能把测试宿主的信号处理写成main epoll也有。

Asio分支的声明顺序context→signals→poll→server→ws→waitStopped使析构时servers早于context；signal_set监听SIGINT/TERM，一次callback中server.stop、ws.stop，再10ms timer轮询TCP stopped。waitStopped返回时不context.stop，依赖server guard释放及取消completion自然排空run。

TCP stopped包含workers join；WS stop立即abort，与TCP grace不同。运行结束handshake才打印defaultRejectionCounters的8槽；该同进程计数合并TCP/WS，不能从这里反推每种连接的数量。main没有最外层try/catch恢复任意构造异常，端口冲突等会抛到进程外；不应称它已具备完整生产部署管理。

## 5. 测试构建的实际矩阵

arena_collect_tests先找*Test.cpp，再移走*PerformanceTest.cpp。普通包含unit/ownership/real-loop C++测试；perf是独立可执行。test net CMake还建：

- arena_tcp_server_test_host、arena_asio_tcp_server_test_host、arena_websocket_test_host；不是main executable。
- TcpServerIntegrationTest.py按churn/limit/slow/idle/rst/stop/stop_fast/stream/fd_limit/fd_limit_stop注册epoll；4workers重复8场景；Asio 0/4重复10场景。
- PacketCodecTransport为epoll/asio×0/4。
- ConnectionContext/Feature/Security每项epoll/asio×0/4＋WS；MsgDispatch还加Default WS入口（总六配置）。
- WS HandshakeLimits、ChaosRun_WebSocketFraming是短程；30min ChaosRun主动执行，未塞进每次CTest。

Python网络测试RUN_SERIAL TRUE、TIMEOUT30或60，避免这些子进程网络阶段争抢资源；不代表宿主内部没有4worker并行。-Wl,--wrap=timerfd_create只给arena_tests模拟创建失败，production/perf宿主没有注入该wrap。

legacy_envelope.proto是旧descriptor测试fixture，生成独立namespace、不覆盖生产Envelope schema；同一进程可互相Serialize/Parse检验字段兼容。生成的pb.cc/h不在152份作者源码快照之中。

## 6. .gitignore和编辑配置

.gitignore模式忽略编译目标、build-*、日志等，故chaos原始日志可能只在本机；已归档docs/reports JSON/CSV不因为其数据属性而自动消失。AGENTS.md/.clang-format/.clangd等被忽略，是当前仓库追踪选择，不代表它们不能约束本会话。

VSCode compileCommands优先提供实际include/flags；配置fallback cppStandard23与Linux gcc模式只是编辑器。ASan目录若不同，默认仍指build，可能和当前选preset不一致；文档按配置现状解释，不更改用户编辑器设置。

## 7. workspace.dsl逐块看：目标，不是当前实现清单

Structurizr DSL的hierarchical identifiers支持arena.gateway等层级引用。model声明client/admin、Redis/MySQL外部系统，以及六目标容器：Gateway/Auth/Lobby/Match/Room/Storage。代码中C++17标签是目标图的旧说明，实际CMake已C++23；Gateway TcpServer/Connection/Codec等仍标Planned也落后于v0.2。SessionManager标Implemented描述映射Session，但当前实现只持Socket且未接Runtime登录，必须以[05](05-runtime-routing.md)为准。

- Gateway图表达accept→admission→loop→poller→channel→connection→codec/dispatcher/router，以及outbound/liveness/session；其中共享Admission、跨业务服务Router、Supervisor并未按图全部实现。
- Auth目标login/token/permission；Lobby presence/friends/party/chat；Match queue/rules/allocator/assignment。
- Room目标单owner lifecycle、playerState、输入校验、fixed tick、combat、snapshot/reconnect；这些是规划，不是gateway的ThreadPool正在运行游戏逻辑。
- Storage目标facade/cache/persistence/database连接Redis/MySQL；没有对应连接或库在当前CMake启用。

系统关系写gateway→auth/lobby/match/room，auth/room/lobby→storage及match→room。deploymentEnvironment Target写pool实例2..N/1..N等逻辑部署，不是本机已启动多少进程。

views提供system context/container、六模块图、login/match-to-room/reconnect dynamic、target deployment共12视图；autoLayout和styles只影响呈现。Person、系统/容器/模块、External、Implemented/InProgress、Planned标签定颜色/虚线/opacity，不改变运行行为。

DSL中的WS optional later也已落后当前Beast实现；这份设计仍作为历史目标保留。本任务不把其Planned标签全改成Implemented；当前状态说明见本套代码分析和v02-current-design。渲染PNG/SVG/PUML是DSL输出附件，未重复当作者代码分析。

## 8. 阅读与验证

构建Interface的正确使用要知道依赖环境、生成顺序、sanitizer隔离和CTest筛选；只看main.cpp不足以解释哪些代码已链接以及测试如何穿过Seam。

可按repo配置执行cmake --preset gcc15、cmake --build --preset gcc15、ctest --preset gcc15；本任务只做代码/doc核对，不为了文档重跑无关负载。已执行验证另见[30min报告](../../reports/v0.2-chaos-20261011.md)。下一章[全部测试](07-tests.md)解释每个测试文件、profile和断言；[工具章](08-observability-tools.md)解释观测脚本与输出。
