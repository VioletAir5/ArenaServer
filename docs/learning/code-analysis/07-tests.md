# 07 全部测试、宿主、profile及验收工具代码

固定v0.2。[源码索引](source-index.md)覆盖每个文件；[测试入口和断言索引](test-case-index.md)由Git对象生成，列出43个cpp/py文件的378个入口（含helper，不等于378个验收case）。本章解释测试为什么能验证机制、fake在哪里，以及仍不能证明什么。构建矩阵详见[06](06-build-and-entry.md)。

## 1. 怎样读这些测试

先找fixture的transport/socket、owner、scheduler，再看激励，最后看断言。fake timer允许人为调用旧callback检验取消/generation；真实loop/socket测试检验OS/Asio行为；独立host＋Python跨进程检验协议和FD恢复。三者验证范围不同。

静态断言索引保留宏名与源码行、原始断言例子；C++按TEST宏分段统计，因此不作为AST精确统计；参数化TEST_P要在CTest发现后展开。Python函数既有测试也有编码、采样和网络helper，不能按函数数报告通过项。

本次学习文档工作没有重跑全仓。已有通过证据属于其报告/提交时的版本和环境；不要把这里列举的全部文件都写进“本次运行全绿”。

## 2. shared单元测试逐文件

路径前缀`tests/shared/`。

| 文件 | 机制、激励与关键检查 |
| --- | --- |
| `os/FileDescriptorTest.cpp` | compile-time copy禁用/move可用；实际pipe/FD验证默认invalid、析构关闭、move源失效、赋值释放旧FD、自move安全。只拥有一个FD不会建立线程同步 |
| `concurrent/BlockingQueueTest.cpp` | 容量0、FIFO/满空、move-only；线程阻塞后push/pop/close唤醒；closed继续drain但不能push；MPMC记录唯一项检验丢失/重复。等待测试需在退出时join，不能把析构队列当唤醒机制 |
| `concurrent/ThreadPoolTest.cpp` | 一次start状态、start前/stop后submit拒绝；future结果/void/参数/move-only/exception；排空queued、等待running；submit与shutdown竞态/并发shutdown/析构join/hooks计数，检验shared队列生命周期 |
| `protocol/PacketHeaderCodecTest.cpp` | 原始大端字节和prepend不移动payload；Buffer压缩/增长/失败不污染；4/8/15分阶段、type/version、最大值、随机chunk一万帧/截断前缀、回调停止与magic早拒。Header完整与body完整分开 |
| `protocol/PacketCodecTest.cpp` | Bytewise header/二进制/粘包，aggregate大于单帧界仍能逐帧处理；configured长度早拒；Stopped终态、异常/重入Invalid、4MiBshared ceiling、encode拒非法 |
| `protocol/EnvelopeCodecTest.cpp` | 新旧descriptor互读、unknown字段反序列化/重序列化保留、retired字段、稳定tags；巨大credential/递归Error/malformed/payloadless/trailingJunk拒绝且output不变 |
| `protocol/compat/legacy_envelope.proto` | 冻结旧schema独立arena.protocol.legacy包：仍有Envelope tag2/client_tag，无新时间/msg_id，旧Error无causes；同wire字段允许跨descriptor读，不含header magic兼容证明 |

对应CMake文件均在[06](06-build-and-entry.md)登记：shared/os、shared/concurrent调用收集函数；shared/protocol还生成legacy pb文件并添加依赖，生成代码不重复当作者源码。

### 协议拒绝具体看什么

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e tests/shared/protocol/EnvelopeCodecTest.cpp 134-164 -->
原文：`tests/shared/protocol/EnvelopeCodecTest.cpp:L134–L164`；[完整文件](source/tests/shared/protocol/EnvelopeCodecTest.cpp.md)。

~~~cpp
TEST(EnvelopeCodecTest, RejectsOversizedPayloadAndLeavesOutputUnchanged) {
    New huge;
    huge.mutable_login_request()->set_credential(
        std::string(EnvelopeCodec::kMaxBytes, 'x'));
    EXPECT_FALSE(EnvelopeCodec::encode(huge));
    std::string wire;
    ASSERT_TRUE(huge.SerializeToString(&wire));
    ASSERT_GT(wire.size(), EnvelopeCodec::kMaxBytes);
    New output;
    output.set_request_id(9);
    EXPECT_FALSE(decode(wire, output));
    EXPECT_EQ(output.request_id(), 9);
}

TEST(EnvelopeCodecTest, RejectsDeeplyNestedError) {
    New deep;
    auto* error = deep.mutable_error();
    for (int i = 0; i < EnvelopeCodec::kMaxDepth + 1; ++i)
        error = error->add_causes();
    error->set_code(1);
    EXPECT_FALSE(EnvelopeCodec::encode(deep));
    std::string wire;
    ASSERT_TRUE(deep.SerializeToString(&wire));
    New parsed;
    EXPECT_FALSE(decode(wire, parsed));
    New shallow;
    shallow.mutable_error()->set_code(1);
    const auto valid = EnvelopeCodec::encode(shallow);
    ASSERT_TRUE(valid);
    EXPECT_TRUE(decode(*valid, parsed));
}
~~~

这个例子分别检测encode对象限制与decode wire限制；serialize直接生成越界wire是为了绕开被测encode，防止测试只验证同一个入口拦两次。output旧request_id=9保留证明失败不提交半解析结果；浅Error正例防止“所有Error都拒绝”也能让负例全绿。

## 3. Reactor/资源/线程测试逐文件

路径前缀`tests/gateway/net/`。

| 文件 | 验证对象与限制 |
| --- | --- |
| `PacketBufferTest.cpp` | gateway Buffer adapter：初始/append/skip、空指针零/非零长度、失败保持原数据、增长/先压缩、消费后重新写及多次append顺序；继承shared Buffer的同一storage，readPtr/skip别名，默认1MiB容量界。14个case是顺序buffer契约，没有并发写证明 |
| `SocketTest.cpp` | 创建/bind/listen/accept、fcntl nonblocking/CLOEXEC、析构FD关闭；真实loopback，直接验证Socket而非Runtime |
| `ChannelTest.cpp` | interest/revents、仅匹配callback、HUP、缺callback、mask helper更新与失败回滚；tie保活所有同次callback及expired抑制；Channel借用FD |
| `EpollPollerTest.cpp` | eventfd加入/修改/删除，poll清旧结果，多于初始batch扩展、invalid FD、信号中断清结果；不执行应用handler |
| `EventLoopTest.cpp` | owner runInLoop，4producer共40万task、foreign quit唤醒idle poll；ready列表中remove另一个channel能跳过已复制事件，直接验证UAF防护 |
| `TimerQueueTest.cpp` | deadline顺序、cancel earliest/repeating、callback增删、invalid/foreign拒绝、大量cancel索引及10万timer一次完成/取消、析构丢callback；不是登录期限Feature测试 |
| `EventLoopThreadTest.cpp` | 未start析构、多次独立启动、worker执行/stop join、先退出、异常传播、当前callback等待、selfjoin拒绝、startup并发stop/重复join/worker requestStop；death测试检查错误owner析构终止 |
| `EventLoopThreadPoolTest.cpp` | 0worker借base、round robin owner、重建无FD漏、timerfd创建失败回滚可再启动、foreign/selfstop、先停全workers再join、单worker异常仍join其他、producer任务、death路径 |
| `SessionManagerTest.cpp` | add/find/missing/duplicate、remove/析构关闭真实Socket FD；只验证旧map模块，不能证明当前登录已接该map |

线程测试经promise、atomic/barrier或condition协调重要时序；部分测试用超时作挂起上限，机器调度仍可能影响。timerfd_create失败通过test executable linker wrap实现，不能把这个故障注入理解成修改生产accept/worker代码。

## 4. 连接与服务器测试逐文件

| 文件 | 关键行为 |
| --- | --- |
| `IConnectionTest.cpp` | 通过IConnection引用验证id稳定、close once、pause/resume已有数据、半关闭仍收、queued input复制/排空FIN；virtual owner析构关闭FD |
| `ConnectionLifecycleTest.cpp` | 原生socketpair＋EventLoop：并发send整record不混写、accepted foreign发送晚于peerFIN、入口overflow/close清队列、SIGPIPE抑制、EOF、accept属性、双向半关闭/排空、Feature水位/慢peer/硬界、stop幂等/foreign拒绝 |
| `ConnectionOwnershipTest.cpp` | 参数化TCP终止场景、弱引用/回收次数；message callback释放最后外部owner后仍能完成，抛异常仍回收；server已销毁时queued removal安全 |
| `TcpServerWorkerTest.cpp` | 真worker loops：idle pool stop/join后Stopped、active析构和late通知、FIN fan-out与deadline、foreign注册/构造与death最终释放；负grace不改running |
| `AsioConnectTest.cpp` | 真loopback sockets/context pump：输入复制与顺序、paused in-flight Ready恢复、start前send/shutdown、partial writes/peerFIN、背压硬界、回调释放owner、并发send/overflow、close/cancel释放buffer与weakself、foreign mutation、跨backend id、FD属性 |
| `AsioTcpServerTest.cpp` | Interface stop/join且外部context可用；active/queued析构与立即stop handoff；foreign管理、非法配置/bind失败FD恢复 |

### 并发send测试究竟证明了什么

原生测试4producer各8条4096字节record，每条用唯一字节值；peer读到FIN后核总长度，再按recordSize检查每段同值、每个值仅出现一次、pending=0，最后owner DEL/release。因此它能暴露交错写、丢失/重复、FIN早到；不同producer间总顺序不预先规定。

Asio测试先用256KiB初始输出和小SO_SNDBUF制造in-flight，再并发加入同类record；completion链把数据排出。测试不是只检查一个m_writing位值，而是检查真实网络输出；源码唯一写guard与取消分支见[04](04-transports.md)。这类测试结合TSan才进一步覆盖竞态，但不能证明所有调度。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e tests/gateway/net/ConnectionLifecycleTest.cpp 155-190 -->
原文：`tests/gateway/net/ConnectionLifecycleTest.cpp:L155–L190`；[完整文件](source/tests/gateway/net/ConnectionLifecycleTest.cpp.md)。

~~~cpp
            loop.quit();
        }
    );
    loop.loop();
    for (auto& sender : senders)
    {
        sender.join();
    }
    reader.join();
    EXPECT_FALSE(timedOut);
    EXPECT_EQ(readError, 0);
    EXPECT_EQ(received.size(), producers * records * recordSize);
    if (received.size() == producers * records * recordSize)
    {
        std::array<int, producers * records> seen{};
        for (std::size_t offset = 0; offset < received.size();
             offset += recordSize)
        {
            const char value = received[offset];
            ASSERT_GE(value, 33);
            ASSERT_LT(value, 33 + static_cast<int>(seen.size()));
            ++seen[value - 33];
            EXPECT_EQ(
                received.substr(offset, recordSize),
                std::string(recordSize, value)
            );
        }
        for (int count : seen)
        {
            EXPECT_EQ(count, 1);
        }
    }
    EXPECT_EQ(connection->pendingBytes(), 0);
    loop.removeChannel(&connection->channel());
    connection.reset();
}
~~~

## 5. Context/Runtime/政策/责任链/Router测试逐文件

| 文件 | 测试Seam与实际观察 |
| --- | --- |
| `ConnectionContextTest.cpp` | id/peer/accepted时间保留、两个Session独立、终态互斥、foreign访问抛异常；忽略Context的handler也由wrapper延长寿命；缺Context/空factory结果拒绝 |
| `ConnectionRuntimeTest.cpp` | fake Transport和defer/timer容器直接控制事件：多reason pause、packet短路、seal、取消/关闭不调用Feature、output重入合并、实际close延后onClosed、背压滞回与旧timer失效、配置拒绝 |
| `ConnectionFeatureTest.cpp` | fake Now/Transport：桶突发补充/封顶、每连接独立、Drop/Close、非法浮点，完整帧才扣、Drop后继续粘包、whitelist早于业务、raw不生成packet事件、onClosed等真实通知 |
| `PacketPipelineTest.cpp` | 记录节点顺序，Handled/Reject短路、tail拒、empty head；decode前后与malformed、半帧不进handler、fresh chain状态隔离/拷贝callback保活、exception丢剩帧、历史PingHandler forwarding |
| `SecurityPolicyTest.cpp` | decoder/可选AuthenticationGate/业务记录节点；rate burst先于decode、Drop逐包计数、Pending/Ping/Login/Expired/Authenticated门状态、outer type不能绕过、各reason准确一次、missing fail closed与跨worker原子计数 |
| `MessageDispatchTest.cpp` | fake Scheduler可触发timeout/旧timer；unknown/type/payload/auth、Typed/Untyped异常、pending64、重复id终止原请求、复用generation、取消后late completion、回复预算及spy spdlog脱敏/关联、registry配置 |
| `ConnectionRuntimeTestSupport.h` | 给真TcpConnection/AsioConnect装Context/Runtime、scheduler，weak output/close/read接线并start；与server同类的接线，不是另一个业务实现 |

Runtime fake不会验证timerfd/Asio本身，timer测试用真实loop补它；Router fake验证迟到timer与completion逻辑，RPC网络profile再验证真实deadline回包。原生past deadline adapter差异并没有专门past时刻回归，因此不能从这些fake测试推出那条路径全绿。

## 6. 测试profile头：每个替代行为为何存在

| 文件 | 注入行为与边界 |
| --- | --- |
| `ConnectionContextTestHandler.h` | raw单字节Q/A/E命令，输出id\|peer IP\|port\|age\|是否worker\|login state；验证factory和callback同owner/同id，A/E只用于状态测试 |
| `ConnectionFeatureTestProfile.h` | 背压＋outer{1}白名单＋桶capacity2/rate0.1，让第3完整帧可确定拒绝，顺序与main握手略不同 |
| `SecurityPolicyTestProfile.h` | decoder→gate→Ping→test login→test business；test-user/test-only才markAuthenticated，以Error作为测试业务、atomic routed计数；输出8原因和routed。**不是生产鉴权** |
| `MessageDispatchTestProfile.h` | sealed Ping、Login账户wait不complete/100ms超时，business抛2001，crash抛不应泄露的文本，auth标Session；synthetic MsgId100需认证并回nonce42，验证Router而非真实Auth系统 |
| `ChaosTestProfile.h` | main相同200/100桶＋{1}白名单＋背压，用于Envelope及合法大echo；退出打印CHAOS_REJECTIONS固定labels |
| `ConnectionRuntimeTestSupport.h` | 真transport接Runtime的模板support，已在上一节解释；不得在生产main引用这些测试头 |

“测试完成登录”来自替代模块，不应回填为“生产Login已可验证凭据”。

## 7. 三个独立host的整个生命周期

`TcpServerTestHost.cpp`解析port/idle/workers/profile，按profile建handler/Features。先sigprocmask阻塞TERM/USR1使workers继承，再signalfd/Channel注册base；TERM quit并在作用域析构server，USR1 stop(300ms)并10ms timer等Stopped再quit。scope结束先server析构、再DEL stopChannel；打印安全/chaos计数。READY通知使Python统计FD之前内部资源已经建立。

`AsioTcpServerTestHost.cpp`相同profiles，但signal_set处理；TERM stop(0)，USR1 stop(300ms)，steady timer等Stopped并cancel signal_set，context自然退出。通过同一个Python scenario驱动，两backend实现差异留在host。

`WebSocketTestHost.cpp`同context启动Asio TCP与WS（每个cap100、TCP0worker），各profile共享工厂。flood模式WS回调反复send128KiB×16主动触硬界。信号ws.stop/tcp.stop(0)，run排空；READY包含两listener可用状态。它能验证TCP/WS同handler协议，不能作为main WS部署性能配置。

## 8. Python网络测试逐文件

| 文件 | 输入、响应和回收检查 |
| --- | --- |
| `TcpServerIntegrationTest.py` | reserve_port、子进程READY、/proc FD基线；10scenario：三轮1000churn、1001拒绝、慢读、随机字节stream、idle刷新后失活、SO_LINGER RST、协作stop/期限、真实RLIMIT64 EMFILE/backoff恢复或stop。末尾正常exit/sanitizer过滤，失败保留日志 |
| `PacketCodecIntegrationTest.py` | shared真实网络契约：独立半帧、逐字节15头、>1MiB累计粘包随机切片且并发读、最大256KiB包体、magic/length/type/version非法、截断FIN、恢复新peer/FD/管理stop |
| `WebSocketIntegrationTest.py` | 手写masked客户端frame/Upgrade、原始Packet/Ping编码；TCP/WS Pong，Origin403、text1003、超长1009、peer4001日志、flood1008/pending≤1MiB。read_frame只读frame，适合这些短回复，不能当一般WS message组装器 |
| `ConnectionContextIntegrationTest.py` | raw单命令，经TCP/WS读取六项元信息与真实getsockname比较；两连接状态独立且终态不可互转，worker0/4 owner值正确 |
| `ConnectionFeatureIntegrationTest.py` | 同profile在5配置：拆帧不多扣、两个peer独立、粘包逐帧限流、第三帧close/type2白名单close、日志理由及FD恢复 |
| `SecurityPolicyIntegrationTest.py` | 两个Ping后第3 malformed帧rate先拒；未登录业务/错误credential/Ping后业务拒；测试credential后业务可达；非法frame/envelope/whitelist。退出严格比较8原因＋routed=N，WS有1008 |
| `MessageDispatchIntegrationTest.py` | varint/fields小解析器检查原wire id7＋inner id；Default unknown1001与start/end成对；rpc profile验证1001/1002/1004/2001/9000/1003、auth后Msg100/Pong，秘密异常文本不入日志及FD恢复 |
| `ChaosRunFramingTest.py` | 真Beast的4096/65536 payload echo，通过ChaosRun.read_ws_message重组continuation；4111帧总长跨默认fragment，从客户端修复假设 |
| `ChaosRun.py` | 六插桩宿主、随机混合actor、统计/FD循环、正常退出及sanitizer诊断；下一节逐方法说明 |
| `TcpServerChurnMonitor.py`、`TcpServerChurnDashboard.html` | 测试目录里的负载/观测工具，完整函数/页面分析在[08](08-observability-tools.md)；不是普通CTest断言 |

reserve_port关闭预留socket再启动有极小端口竞争窗口，READY失败会被识别，不宣称原子保留端口。仅一些脚本显式设置ASAN/UBSAN env，其他inherit调用环境；所有sanitizer证据需关联实际运行命令。失败cleanup允许kill以回收子进程，但强杀不能作为LSan通过依据。

MessageDispatchIntegration的fields仅支持所需varint/length-delimited字段，测试helper不是可以拿去替代生产protobuf decoder的完整协议解析器。

## 9. ChaosRun.py逐组分析

- `read_ws_message`：检查RSV/mask、frame≤1MiB、binary→continuation顺序与总量≤1MiB；允许控制ping/pong穿插、ping回pong，FIN完成才返回。避免frame=message错误。
- `timestamp`提供Asia/Shanghai日志时间；计时门槛用monotonic，不用墙钟差。
- `Host.__init__/logs/wait_ready/health`：启动对应binary/profile；强制detect_leaks=1与halt_on_error；独立日志reader识别READY/诊断/counters，5MiB×3轮转；READY后取FD基线。进程早退或诊断立即失败。
- `Host.connect/send/receive/exchange`：成功connect/WS Upgrade单独计耗时；exchange随机nonce/bytes、TCP拆片，精确比整个reply；成功RTT用0.1ms上取整bin累计全run，不把失败/超时混成功样本。
- `Host.job`：healthy/reconnect/reset/truncated、magic/length/version/type、whitelist、protobuf/24层depth/unknown、512帧burst、WS Origin/text/size、合法64KiB慢读（缩小接收buffer、不读6.2s）。每actor随机数独立seed，网络调度不确定。
- `expect_closed`检查TCP EOF/RST或WS1008并统计；可读到合法前序响应，不把这些bytes当拒绝失败。慢read job不在socket侧逐字节等close，而是退出slow_client计数及FD恢复补验。
- `wait_fd/stop/summary`：每轮等待确切FD基线，退出USR1并join日志reader、exit0/无diagnostic、八counter齐全/必需路径>0；summary histogram累计quantile及RSS/FD峰。
- `Run.__init__`要求output新目录exist_ok=false，保留旧尝试；sample取/proc RSS/HWM及FD-baseline作为连接近似，snapshot/write_live临时文件replace原子发布。
- `telemetry`每秒采样，错误记failure；surge同时打开各host peak连接并等待FD确认平台、sample后RST关闭恢复基线。
- `execute`启动3backend×2profile、开始monotonic窗口；64线程执行混合actors，每轮全future完成再FD恢复，10轮一次surge；到deadline后完成当前轮，所以可略超时。drained采样后每host正常stop，所有检查成立才passed。
- finally停采样/回收子进程/写summary.json及metrics.csv；forced kill写“no leak pass”；CLI duration≥10、peak1..80，默认duration1800/seed20261011/peak64。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e tests/gateway/net/ChaosRun.py 495-507 -->
原文：`tests/gateway/net/ChaosRun.py:L495–L507`；[完整文件](source/tests/gateway/net/ChaosRun.py.md)。

~~~python
            self.workload_seconds = round(time.monotonic() - self.start, 3)
            assert self.workload_seconds >= self.args.duration
            self.finished.set()
            if sampler:
                sampler.join(timeout=5)
            assert not self.failure, self.failure
            self.sample("drained")
            for host in self.hosts:
                host.stop()
            status = "passed"
        except BaseException as error:
            self.failure.append(repr(error))
            traceback.print_exc()
~~~

0 leak在本工具表示FD恢复＋正常sanitizer退出无检测泄漏，不是RSS回初值；RSS的allocator/quarantine影响及两个profile尾段仍增长以[实测报告](../../reports/v0.2-chaos-20261011.md)为准。FD差是连接近似，因为此profile内其他FD稳定；不应把这算法不经验证用到会动态开文件的生产服务。

## 10. 三种性能测试逐文件

`BlockingQueuePerformanceTest.cpp`：SPSC一百万项校验checksum，MPMC4×4合一百万校验消耗数；输出items/s，测的是锁/条件变量/调度组合，没有每个item tail latency或网络。

`ThreadPoolPerformanceTest.cpp`：Mandelbrot escape计算700×700、4workers/队列1024、10repeat；串行、不同任务拆分的并行计算耗时/checksum比较。每任务写独立image范围、future.get同步；不是网络I/O worker容量，也不能用speedup直接推TCP性能。

`EpollPollerPerformanceTest.cpp`：128 eventfd×10000轮写1/poll/read，校验事件后输出events/s；另一200000轮ADD/DEL输出operations/s。poll(0)主动benchmark不等于生产事件loop等待模式，不含Packet/protobuf/业务处理。

三文件进入arena_perf_tests，默认test preset过滤Performance。源码没有通用P99性能门槛；微基准也不能代替30分钟混合注入的RTT/RSS采样。

## 11. 覆盖与仍需的证据

取消/weak buffer/并发record、FD/owner死亡、协议兼容与错误映射各有聚焦测试；真实网络矩阵检验公共Interface的多个Adapter，而不是为每transport重写业务测试。

仍不能据此保证：全输入fuzz、全部并发调度、TSan跨所有模块、长周期RSS稳态、生产凭据/TLS/global Admission、跨机/OOM/SIGKILL恢复。测试case索引说明代码在验证什么；某次实际执行是否通过需查对应运行输出。v0.2报告是231轮/1802.626秒和18项额外网络回归，未声称本轮全仓执行。

[下一章：观测工具与页面](08-observability-tools.md)。完整源码附件、具体case原始断言可从[索引](test-case-index.md)继续逐文件阅读。
