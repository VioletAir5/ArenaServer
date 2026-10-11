# v0.2 当前架构与扩展入口

**当前实现快照：v0.2 = b6dd2835a050f3b118e2739ff65f992ed44bca5e。** 学习文档在tag后提交，不改变该快照。本文按源码描述；docs/architecture/README.md里的Auth/Lobby/Match/Room/Storage是目标设计，不是已部署调用链。入口：[学习导航](v01-v02-index.md)。

## 1. 先建立六个职责的心智模型

| 层 | 真实模块 | 回答的问题 | 不应承担的事 |
| --- | --- | --- | --- |
| 监听/准入/停止 | TcpServer、AsioTcpServer、WebSocketServer | 谁接收FD、创建连接、负责回收？ | protobuf业务解析 |
| Transport | TcpConnection、AsioConnect、WS内部WebSocketConnection | 字节如何读写、取消、半关闭、排队？ | 账号规则、MsgId注册 |
| 连接协调 | ConnectionRuntime、Context/Session、Scheduler | 事件交给谁、哪个owner、何时暂停/关闭？ | 具体epoll/syscall或业务结果 |
| 连接策略 | ConnectionPipeline/IConnectionFeature | 这条连接的资源策略是否允许继续？ | 跨server全局IP数量、业务payload语义 |
| framing与处理链 | shared PacketCodec/EnvelopeCodec；gateway PacketPipeline | 字节组成哪个完整包、怎样解码/过滤？ | 驱动socket、完成跨线程业务 |
| 请求路由 | MessageRegistry/MessageRouter/MessageHandler | MsgId对应谁、鉴权、请求id、结果/期限？ | 保证网络实际送达、阻塞外部服务 |

这里Interface不仅是函数签名，还包括owner、view寿命、调用顺序、资源预算和错误模式。Transport是IConnection上的Adapter；Runtime也实现同一Interface，供业务使用。真正的Seam在工厂、Feature接口、PacketHandler和Handler注册处：改变业务/策略不需要编辑三份I/O实现。

### 三个容易混淆的名字

- ConnectionContext：某连接不可变metadata；里面共享Session是owner可变状态。不拥有FD/全局IP计数。
- ConnectionPipeline：vector<unique_ptr<IConnectionFeature>>，接收生命周期、输出和完整包事件；不是PacketHandler链。
- PacketPipeline：拥有PacketHandler链头；PacketContext只活一次完整帧callback。默认decoder后接Router。
- MessageHandler：注册表中的业务函数，签名是拥有的RequestMessage和completion；它不继承PacketHandler。

## 2. 一次默认Ping怎样走完

~~~mermaid
flowchart TD
 A[accept / HTTP Upgrade] --> B[Transport read]
 B --> C[Runtime.received]
 C --> D[每连接 PacketCodec.feed]
 D --> E[完整帧 inspectPacket]
 E --> F[TokenBucket / PacketWhitelist]
 F --> G[EnvelopeDecodeHandler]
 G --> H[MessageRouter]
 H --> I[公开 Ping Handler]
 I --> J[completion / State.finish]
 J --> K[响应预算 / EnvelopeCodec / PacketCodec]
 K --> L[Runtime.send]
 L --> M[Transport入口队列]
 M --> N[owner串行 socket write]
~~~

这是handshake handler配置后的路径。raw handler不经过PacketCodec；packet echo经过codec但不经过Envelope/Router。

细节：

1. accept抓取实际peer与steady acceptedAt。TCP将Socket/FD和metadata交给worker，在owner建Context/Session/transport/Runtime；WS在base建这些对象，但Runtime.start等Upgrade成功。
2. Runtime.received把自己的IConnection facade交给业务回调；因此codec完成时可dynamic_cast到PacketFeatureHost。
3. framing完整后扣包令牌，再检查outer type白名单，之后才protobuf解码。半包不是一个可扣令牌的业务包。
4. Router优先Envelope.request_id；0时回退header ID。msg_id为0从oneof推导，旧消息仍可用；显式未知ID返回1001。
5. registry查公开Ping；pending登记id/generation/期限和owner timer；Handler回Pong，finish erase/cancel/检查deadline。
6. 响应Envelope request_id规范化，header ID原样echo；编码后从有界发送入口交到owner。request_end outcome=reply不保证客户端已经收到。
7. 输出变化单独反馈Runtime→BackpressureFeature，与上面包链共享连接控制，但不是业务路由的下一节点。

## 3. 三种I/O为何共享策略，却保留各自机制

| 项目 | 原生TCP | Asio TCP | WS |
| --- | --- | --- | --- |
| owner | 每连接EventLoop worker；0 worker为base | 每worker独立io_context/唯一run线程；0为base | base io_context/connection strand，无worker分片 |
| 外线程send入口 | mutex/deque字节块 | mutex/deque共享Buffer | mutex/deque字符串Buffer |
| 唤醒/串行 | queueInLoop/eventfd | post strand | post strand |
| 活动写 | 非阻塞send、offset/Buffer、EPOLLOUT | 单async_write_some、部分完成offset | 单Beast async_write，控制帧由Beast协议处理 |
| 读暂停 | EPOLLIN控制；ET恢复主动继续读 | 不发下一读，已完成最多8KiB Ready暂存 | 不发下一读，完成message可有界readReady暂存 |
| 关闭 | 逻辑close、Channel注销、owner回收Socket | closeOnce/cancel，迟到completion弱连接强Buffer | close handshake/5秒期限/abort，一次移除 |
| timer | scheduler通过既有TimerQueue/timerfd | scheduler绑定transport strand的steady_timer | 同Asio；HTTP/close另有协议timer |

**约束没有被strand取消：** 当前AsioTcpServer要求每context在owner线程run。不能直接改成同context多线程run就认为Session/State安全。send是跨线程许可的入口；Feature、Session、route completion、管理和最终释放仍归owner。

**为什么不锁活动Buffer？** 不共享它：发送线程只访问入口及预算，owner独占活动输出。锁保护的是入口线性化，不是每一步I/O。原子写索引无法独自解决可变Buffer扩容、发布顺序、未完成数据和close并发。

TCP lifecycle的connected在半关闭/排水仍可能true；WS transport只在Open为true。Runtime逻辑closing gate让业务在请求关闭后马上停send/read交付。不要用connected作为“保证发送成功”的承诺。

## 4. 所有权：谁保持谁活着

~~~mermaid
flowchart LR
 R[server owner registry Entry] -->|strong| U[ConnectionRuntime]
 U -->|strong| T[transport / Socket]
 U -->|strong| C[Context / Session]
 U -->|strong| F[Feature nodes]
 U -->|strong handler callback| P[PacketCodec / PacketPipeline]
 P -->|unique nodes| M[MessageRouter / State]
 T -. weak callbacks .-> U
 M -->|owned request| Q[Envelope copy / Context]
 X[completion / timer] -. weak .-> M
 O[inflight operation] -->|strong| Z[Buffer]
~~~

registry Entry里的transport裸指针只是借用，所有权在Runtime。Context不持FD，也不反向拥有Runtime。weak回调避免循环；进入Runtime事件时局部strong guard保护本次调用，eventDepth保证onClosed不会打断正在运行的Feature。原生Channel tie与owner注销另保护epoll分发寿命；WS操作回调还会保活session。

requestClose首先关闭Runtime逻辑gate并暂停读，实际close投到owner。transport通知实际关闭，Runtime onClosed每Feature至多一次；TCP表erase延迟离开dispatch，WS遵守自己的close/abort移除流程。最终释放owner，执行器必须仍活着。

shutdown与close不同：TCP shutdown拒绝新send、排空已接纳输出再FIN，继续收对端数据；close丢弃输出并回收。WS用WS close协议，不能把TCP半关闭状态逐个照搬。

## 5. 资源策略：有界在哪一层

| 界/机制 | 实际作用 | 不能推出的结论 |
| --- | --- | --- |
| transport输出1MiB | 复制入口前限制queued+active/inflight | 所有连接总内存已有共享预算 |
| 背压高512/低256KiB、5秒 | 高水位停读，低水位恢复，持续拥塞close | 已实现登录期限 |
| handshake桶200突发/100包每秒 | 完整包在protobuf前早拒；默认超限Close | 原始字节限速/IP共享桶 |
| outer type白名单{1} | 限Packet方向/type | 按MsgId授权所有业务 |
| gateway body256KiB | 7字节metadata加payload的单帧界 | shared通用4MiB界可直接用于gateway |
| Envelope256KiB/16层 | 限单protobuf解析/编码 | 出站可忽略metadata预算 |
| Router pending≤64、默认5秒 | 限每连接活动请求，超时typed1004 | 能抢占阻塞Handler或释放外部任务持有 |
| 每server容量 | 本监听器连接数量上限 | TCP/WS共享IP/总内存上限 |
| shared计数8固定槽 | 拒绝事件跨worker原子累计 | 每个计数都是关闭连接、去重IP |

Feature暂停按reason bitmask管理，某策略清自己的理由不能恢复另一策略仍要求暂停的读。Pipeline start后冻结，无热删除。**空FeatureFactory默认Backpressure，非空factory必须完整配置所需策略。**

## 6. 如何扩展：先选位置，再写行为

### A. 新增业务MsgId

1. 在shared/proto/envelope.proto新增稳定编号及payload；退役号码reserved，不能复用。
2. 检查EnvelopeCodec::validEnvelope；新oneof必须能合法编码/解析。
3. 更新MessageRouter.cpp的payloadMsgId及State.respond成功响应校验。**当前成功响应只支持Pong/LoginResponse；仅register新id不等于任意新返回消息自动可用。**
4. 创建新MessageRegistry，注册Ping/Login及新Handler后seal；defaultMessageRegistry已seal，不能运行时追加。
5. 在ConnectionHandlerFactory传该registry给makePreLoginHandshakeHandler(context, registry)；它被多连接共享只读，各连接State独立。
6. 新业务默认requiresAuthentication=true；仅真实验证成功后在owner markAuthenticated。数据库/远程认证移到非阻塞工作路径，completion回owner。handler共享捕获的可变资源要独立同步。
7. 更新docs/protocol.md及旧/新schema、route error/id/deadline、TCP/WS网络契约测试。框架不提供跨线程自动completion post接口，应显式设计交还owner和取消。

成功/业务错误以MessageReply expected或BusinessError表达；不把内部what()发给客户端。默认Login返回Unavailable2002只是占位，不能据此当作已经登录。

### B. 新增完整包过滤Handler

继承PacketHandler；放行调用PacketHandler::handle(context)，拒绝设置rejectionReason并返回Reject，已处理返回Handled。需要Envelope内容的过滤节点放decoder之后，配置在每连接工厂。不要保存header/payload view；异步请求复制为拥有的数据。当前默认Router是终止业务节点，新增过滤放它之前。

### C. 新增连接级规则Feature

用onConnected安期限、onPacket作廉价包策略、onOutputBufferChanged作输出策略、onClosed取消token；通过IConnectionControl控制reason pause或requestClose。示例是main已有的真实配置，需相关头文件及service::gateway命名空间：

~~~cpp
ConnectionFeatureFactory policies =
    [](ConnectionPipeline& p, const ConnectionContext&) {
        p.add<BackpressureFeature>();
        p.add<TokenBucketFeature>(200.0, 100.0);
        p.add<PacketWhitelistFeature>(
            std::vector<std::uint16_t>{1});
    };
~~~

**规划例子：LoginTimeoutFeature目前不存在。** 合理扩展是在onConnected按context.acceptedAt+期限调用scheduleAt，Pending到期在owner expireLogin/requestClose，onClosed取消token；Ping不等于认证。WS hook在Upgrade后，acceptedAt仍是accept时，因此应有意识决定期限包含Upgrade时间，不能无意重新给满期限。

### D. 新增IP并发上限

这是**规划**的共享Admission模块，不是每连接Feature复制IP map。它应在accept后的准入位置、创建Runtime之前，按真实peer申请租约；TCP/WS共享同一服务实例。租约要覆盖worker post未执行、setup/Upgrade失败、stop和正常close，最多释放一次；跨owner计数需mutex或固定owner投递。Context保存资料，不负责全局注册表。当前只有每server容量，尚无该模块。

### E. 新增transport

实现IConnection契约及底层read/output/close回调；在server accept路径建Context/Runtime并提供正确Scheduler。追踪FD迁移、callbacks、pending预算、唯一写、操作Buffer完成寿命、stop回执和owner析构。先用共享Packet与Feature/路由真实网络测试验证，再考虑新I/O特有测试。不要把背压/鉴权复制进新transport。

## 7. 最短源码阅读路线与练习

1. main.cpp：看raw/packet/handshake实际装配。默认epoll raw；handshake CLI只接受asio，启动TCP8888/WS8889，loopback。
2. TcpServer.cpp::Worker::addConnection或AsioTcpServer.cpp worker接线；WS acceptNext接线：找强Runtime和weak callbacks。
3. ConnectionRuntime.cpp::received/outputChanged/requestClose/scheduleAt：理解owner、重入、gate、timer寿命。
4. PacketPipeline.cpp::makePacketHandler：看framing后Feature先执行、Drop/Close短路。
5. PreLoginHandshake.cpp：默认decoder→Router及sealed公开Ping/Login。
6. MessageRouter.cpp::handle/State::finish/respond：找ID、auth、pending、generation、deadline、错误和出站预算。
7. 再看具体transport的send/drain/write/close：确认共享策略没有代替I/O机制。

阅读后自己回答：

- 两个线程send时，哪段需要mutex，哪段不用？Buffer由谁复制/释放？
- high水位暂停后Application也暂停，low水位为什么不能立刻恢复？
- 一个request超时后复用ID，旧completion怎样被忽略？
- 新protobuf消息为何同时需要codec与成功响应验证改动？
- WS Upgrade前谁创建Context，Feature何时启动？登录期限从哪个时间算？
- 为什么测试认证成功不证明生产已有凭据系统？
- header body≤256KiB时payload为什么还要减7？

答案可分别回到逐提交06/07、15/16、20、08/23、14/16、18/21及09/23核对，不需要从第一行socket代码重新读整个项目。

## 8. 已验收与未实现

v0.2发布报告有1802.626秒chaos、0 crash/0检测到泄漏、18项补充网络回归；不是全输入/全调度证明，两个Envelope场景RSS尾段仍增长。参考仓库docs/reports/v0.2-chaos-20261011.md与docs/security/threats-v0.2.md。

真实认证/授权、LoginTimeoutFeature、解析期限、共享IP/总内存Admission、TLS、WS worker分片、全局日志策略均仍开放。其他服务目录只有规划；raw/packet演示路径没有Envelope身份门。请求deadline已实现，连接登录deadline未实现。本文不把未来接口图画成当前调用链。

## 9. 来源与继续阅读

源码目录：services/gateway/include/net、services/gateway/src/net、services/gateway/app/main.cpp；shared/include/protocol、shared/src/protocol、shared/proto/envelope.proto。历史详细验证见三章逐提交链接的Change Note，本次只做文档/source核对。

既有概念笔记：owner/多reactor、Buffer寿命、背压滞回、protobuf字段演进、request id/generation/deadline，都可从Obsidian的knowledge/network查找。已有当前实现长篇笔记是architecture/gateway-runtime.md；本文提供从版本演进到实际扩展步骤的阅读入口。
