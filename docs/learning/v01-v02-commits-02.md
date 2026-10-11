# v0.1 → v0.2：逐提交设计讲解（二）

范围：12–23，共12个提交。入口：[导航](v01-v02-index.md)，前一章：[接口、传输、协议](v01-v02-commits-01.md)，下一章：[注释与发布验收](v01-v02-commits-03.md)。证据方法与第一章相同，历史验证不冒充本次重新执行。

## 12 · fc55741 · 2026-10-09 · class Handler责任链

原题：Refactor packet pipeline into handler chain。

**原因。** stage列表已有扩展位置，用户希望通过继承Handler、setNext和base forwarding增加有状态过滤/匹配逻辑。改表达方式后，新增处理器可作为独立类维护。

**技术与路径。** PacketHandler持unique_ptr next；setNext转移所有权；handle默认转发，无next则Reject。EnvelopeDecodeHandler解码后调用base；PingHandler不匹配时转发、匹配后Handled。PacketPipeline只拥有链头，codec仍外置。原Continue枚举消失，继续链由明确调用PacketHandler::handle完成。

**所有权与失败。** 每连接全新链，唯一所有权避免节点共享/循环持有；节点状态owner串行。末尾未处理默认拒绝，不把遗漏route当成功。Context仍借用view，异常/Reject走既有关门。

**取舍与扩展。** Handler实现必须只在放行/不匹配时base forwarding；处理完不能继续导致重复响应。配置前装好next，运行中不热换。unique_ptr比示例shared_ptr更准确表达线性每连接所有权。v0.2默认链后来改为decoder→MessageRouter；旧PingHandler仍可组合使用。

**证据。** PacketPipeline.h/.cpp、PacketEcho.h/.cpp、PreLoginHandshake.h/.cpp；PacketPipelineTest.cpp扩展顺序/未匹配/尾部拒绝测试。原笔记: ArenaServer/changes/2026-10-09-packet-handler-chain.md (Obsidian)。

## 13 · b40ed33 · 2026-10-09 · WS生命周期不再靠多个布尔猜测

原题：Unify WebSocket connection lifecycle state。

**原因。** open/upgraded/closing/closeStarted/finished重复表达阶段，且原connected可能在HTTP尚未Upgrade时被视为可用。

**机制。** State为ReadingHttp→Upgrading→Open→ClosePendingWrite→CloseHandshake→Closed；错误可跳Closed。connected只在Open为true。onRequest/accept完成检查当前阶段，避免关闭后迟到Upgrade重新打开；abort通过exchange(Closed)保证只一次资源清理/通知。

**线程与取舍。** 生命周期owner修改，atomic快照供connected读取；原有reading/readPaused/writing仍保留，因为这些是正交操作条件。atomic state不等于Socket和队列可任意线程修改。关闭前尚未Open直接abort，Open后走WS close协议。

**扩展与验证面。** 新HTTP/WS阶段应加明确迁移守卫，不恢复布尔组合。本提交只改WebSocketServer.cpp；既有WebSocketTransport_HandshakeLimits检查网络生命周期，未新增专门观察握手中connected的测试。原笔记: ArenaServer/changes/2026-10-09-websocket-lifecycle-state.md (Obsidian)。

## 14 · 9fa7fda · 2026-10-10 · ConnectContext不是IP连接数量表

原题：Add connection context to transport handler factories。

**原因。** 过滤/鉴权需要可靠peer、accept时间、连接身份与session；不能让业务解析HTTP字段猜IP，也不能每次再从已RST socket查询peer。

**机制。** 新ConnectionContext包含不可变connectionId/peer/acceptedAt和共享ConnectionSession；handler工厂接收Context::Ptr，在worker owner创建状态。accept时捕获地址/steady时间，WS时间在Upgrade之前。工厂签名和调用方一起更新；ConnectionHandler集中做Context寿命封装、空结果检查和raw echo fallback，并没有自动兼容旧factory签名。

**线程与寿命。** Session在owner构造/变更，初始Pending，可markAuthenticated或expireLogin；Context共享只延长数据寿命，不拥有socket/timer/connection，也不授权跨线程访问Session。后台任务能保存Context但不能据此假定连接活着。

**取舍与扩展。** metadata是单连接资料，不是跨连接Admission。此提交没有IP数量映射、token bucket或登录期限；Session.expireLogin也不自动启动定时器。以后IP上限应在共享准入模块，用租约保证计数释放；不能把全局表嵌入每连接Context。

**证据。** ConnectionContext.h/.cpp、ConnectionHandler.h/.cpp、三个server工厂调用；ConnectionContextTest.cpp和ConnectionContextIntegrationTest.py验证每连接隔离、peer/accept时间与owner约束。原笔记: ArenaServer/changes/2026-10-10-connection-context.md (Obsidian)。

## 15 · 3022736 · 2026-10-10 · Runtime/Feature先建立统一策略宿主

原题：Add connection runtime and owner-confined feature pipeline。

**原因。** 每新增水位、令牌、登录超时都改三种Connection会使策略重复。把机制与策略分开：transport管I/O，Runtime接事件，Feature实现可复用连接规则。

**机制。** ConnectionRuntime实现IConnection facade、IConnectionControl和PacketFeatureHost，拥有transport/context/handler/ConnectionPipeline。Feature hooks为onConnected/onPacket/onOutputBufferChanged/onClosed；start冻结注册。Scheduler用defer/at隐藏EventLoop与Asio差异；ConnectionTimer是move-only RAII取消token。

**线程与关闭。** Runtime/Feature/control归owner；pauseReasons位图使各策略只清自己的暂停理由，清零才物理resume。requestClose立即关闭逻辑发送/读取gate，owner延迟transport.close；事件局部强引用和eventDepth保住调用栈，onClosed等实际transportClosed且最多一次。timer弱引用Runtime，取消token和live标记防就绪回调继续运行。

**阶段性取舍。** 此提交只有模块和单元验收，**尚未接三种server**。不能把“新类编译了”当实际路径已替换。Feature无需锁来自owner保证；共享factory捕获可变配置需自己同步。send仍委托transport有界并发入口。

**证据。** ConnectionFeature.h、ConnectionRuntime.h/.cpp、ConnectionSchedulers.h/.cpp；ConnectionRuntimeTest.cpp的hook顺序/冻结/暂停叠加/取消/异常/close验收。三阶段笔记: ArenaServer/changes/2026-10-10-connection-runtime-features.md (Obsidian)。

## 16 · 8bf665c · 2026-10-10 · 三种transport真正接Runtime，统一背压

原题：Share runtime backpressure across epoll Asio and WebSocket transports。

**原因。** Runtime接线后才能减少实际重复。原TCP/Asio水位回调及慢客户端策略独立，WS只有硬队列界；统一BackpressureFeature让三者执行同一策略。

**路径。** server registry Entry强持Runtime，Runtime强持transport；transport read/output/close callbacks弱持Runtime。输入→Runtime.received→handler；输出变化→Runtime.outputChanged→BackpressureFeature。高512KiB暂停Backpressure理由并安排5秒timer；低256KiB取消timer/清该理由；持续拥塞关闭。原生worker慢连接巡检移除，改为连接级调度，底层共用loop timerfd，无每连接FD。

**线程与寿命。** epoll scheduler绑定owner EventLoop，Asio/WS绑定transport strand。native resume可能同步读/触发新输出，Runtime合并重入output通知；读交付防重入，保护view。WS新增有界readReady和send drain合并。异步完成仍保活active buffer，逻辑close后registry在owner延后erase。

**失败修复与取舍。** Asio async_accept直接输出endpoint，避免RST后getpeername的ENOTCONN竞态丢失peer。软策略迁走，1MiB硬界仍在transport，不能被Feature配置取消。空factory装默认Backpressure；**非空factory负责完整策略集**，不是自动追加到默认集。idle管理仍在既有transport/server。

**证据。** 三个server/transport、BackpressureFeature.cpp、Runtime/Schedulers；ConnectionRuntimeTest、AsioConnectTest、ConnectionLifecycleTest与真实slow/RST回归。历史联合100/102项结果见 笔记: ArenaServer/changes/2026-10-10-connection-runtime-features.md (Obsidian)，本次没有重跑。

## 17 · 841acb0 · 2026-10-10 · 完整包后、protobuf前早拒

原题：Apply reusable token bucket and whitelist features after packet framing。

**原因。** 令牌不能按read块扣：半包/粘包会让相同业务请求因网络分片得到不同额度。过滤要在codec确认完整有界帧后，昂贵protobuf和业务前运行。

**路径。** PacketCodec callback→dynamic_cast PacketFeatureHost（实际Runtime）→inspectPacket→有序Feature→Continue进入Handler、Drop消费当前帧继续下一帧、Close关门并停decoder。TokenBucket保存tokens/last，steady_clock惰性补充：min(capacity,tokens+elapsed×rate)，每包扣1；白名单排序去重后匹配外层type。

**线程与失败。** 每连接独立桶，owner事件无需锁；clock可注入做确定性测试。容量/rate必须有限且有效，超限只能Drop/Close；空白名单拒绝所有。没有为补令牌额外开timer。Feature丢包不是PacketHandler的Reject，后者默认关连接。

**取舍与扩展。** 不限制原始字节流、不跨连接共享、重连获得新突发。白名单检查PacketHeader::m_type，不是Envelope.msg_id/oneof。此提交只提供可选策略及测试profile，main还没有启用100包/s规则，下一阶段才接入。

**证据。** TokenBucketFeature.h/.cpp、PacketWhitelistFeature.h/.cpp、PacketPipeline.cpp；ConnectionFeatureTest.cpp/ConnectionFeatureIntegrationTest.py测桶、白名单、Drop/Close、半粘包、连接隔离及五传输配置。笔记: ArenaServer/changes/2026-10-10-connection-runtime-features.md (Obsidian)。

## 18 · d1bdc4a · 2026-10-10 · 鉴权门与固定拒绝原因

原题：Guard unauthenticated packet routing and count policy rejections。

**原因。** 仅白名单type1无法阻止未登录业务，因为不同业务共用该外层type。业务路由前需检查Session；拒绝也须有固定语义计数，不能只靠stderr文本。

**机制。** 当时默认链decoder→AuthenticationGateHandler→PingHandler。Pending允许LoginRequest/Ping继续，Authenticated放行，Expired拒绝；未解码Envelope fail closed。Login允许经过gate不等于已有Login handler，尾部仍Reject。Context增加共享RejectionCounters，八固定reason槽atomic relaxed record/snapshot；codec/decoder/Feature/异常路径分别记录原因。

**线程与边界。** Session仍owner；跨worker聚合计数用atomic，不用可增长的字符串map。relaxed足够统计，不用于发布身份状态；运行中snapshot不是所有槽同一时刻的事务快照。原因事件不等于去重连接数。

**取舍与后续。** 只提供鉴权状态与门，没有校验凭据，也没有把Ping当登录成功。v0.2默认链已改由MessageRouter按route检查requiresAuthentication；这个旧gate仍可组合，不能说两者默认同时运行。不同错误边界可能关闭或typed响应。

**证据。** AuthenticationGateHandler.cpp、PacketPipeline.h/.cpp、RejectionCounters.h/.cpp、ConnectionContext与Feature记录点；SecurityPolicyTest.cpp验证状态/早拒/精确原因。笔记: ArenaServer/changes/2026-10-10-authenticated-routing-rejections.md (Obsidian)。

## 19 · 0cfc94c · 2026-10-10 · 装配策略并以真实传输验证

原题：Enable handshake admission policies and verify rejection counts across transports。

**原因。** 有Feature类不代表应用开启防护。main显式装配完整策略集，网络测试核对行为和原因，完成“模块→产品配置”的接线。

**机制。** handshake factory顺序Backpressure→TokenBucket(200,100包/s)→PacketWhitelist({1})，同时传给Asio TCP与WS；main运行结束输出八reason snapshot。raw/packet空factory仍只默认背压。真实宿主security profile使用同一策略接口、合成认证操作及可观测counter。

**线程与失败。** 每连接fresh Feature，Session owner变更；共享counter原子计数。非法帧/解码/未认证/桶/白名单/slow分别定位层次。测试认证动作只属于宿主，生产不接受这种测试快捷登录。

**取舍与扩展。** 标题中的admission在这里是包处理准入，**没有共享IP并发准入**。生产main仍只有asio可选handshake，epoll通过测试验证能力。新增规则先实现，再在正确模式装factory，再跑跨传输验收。

**证据。** main.cpp、SecurityPolicyTestProfile.h、SecurityPolicyIntegrationTest.py、三个TestHost、CMakeLists.txt；历史普通/ASan各118项结果见 笔记: ArenaServer/changes/2026-10-10-authenticated-routing-rejections.md (Obsidian)。

## 20 · 6a1362b · 2026-10-10 · Router管理一次请求的全部生命周期

原题：Add typed MsgId dispatch with request correlation and owner deadlines。

**原因。** 串联Ping/Login Handler适合过滤和少量匹配，但业务增多需要注册表查找；异步完成还需关联ID、限时、重复/迟到处理及稳定错误。新增MessageRegistry/MessageRouter并用spdlog记录请求链。

**技术与路径。** Envelope加msg_id tag4及MsgId/ErrorCode枚举，tag2继续reserved。msg_id=0从已知oneof推导；显式未知ID允许到router返回Unknown1001。规范requestId优先Envelope非零uint64，否则header uint32，两者0 Invalid1002。sealed registry共享只读，每连接State独立pending≤64。route默认requiresAuthentication=true、timeout5s。

owner handle→validate/lookup/auth/budget→pending{id,generation,owned request,steady deadline,timer}→Handler(request,completion)→finish匹配id+generation→erase/cancel→响应编码→Runtime.send；timer走同一finish。BusinessError/expected错误稳定映射，未知异常Internal9000；结束前再次检查deadline，防到期但timer尚未调度时“成功”。

**寿命与取舍。** RequestMessage拥有Envelope，异步不会借用PacketView；completion/timer弱State。完成必须owner，后台线程须自行交还owner，接口未自动post。generation防旧回调完成复用ID的新调用；重复活动ID终结原调用，重复Handler不执行。日志用connection/request/msg/code/outcome，避免凭据及what()泄漏。owner timer不能抢占阻塞Handler。

**阶段性边界。** 此提交核心和单元完成，默认握手仍旧链，下一提交接入。当前成功响应验证仍仅认可LoginResponse/Pong；自定义MsgId的成功payload并非任意protobuf，需要扩展schema与respond验证。

**证据。** MessageRouter.h/.cpp、shared/proto/envelope.proto、EnvelopeCodec.cpp、gateway CMake spdlog依赖；MessageDispatchTest.cpp和EnvelopeCodecTest.cpp新增契约兼容测试。请求笔记: ArenaServer/changes/2026-10-10-msgid-request-dispatch.md (Obsidian)。

## 21 · be7d216 · 2026-10-10 · 默认链切到Router并输出协议表

原题：Route default handshake requests and verify typed replies across transports。

**原因。** 核心router须成为实际默认路径；登录前未知消息/业务错误不应混成关连接。协议表让客户端知道ID、方向和错误契约。

**机制。** makePreLoginHandshakeHandler(context,registry)默认decoder→MessageRouter；defaultMessageRegistry注册公开Ping返回Pong、公开Login返回Unavailable2002并seal。旧PingHandler保留。docs/protocol.md列消息、字段、错误和注册/期限规则。测试rpc profile引入合成业务与认证，真实TCP/WS逐请求核对响应及日志。

**并发与错误。** registry进程共享只读，Handler捕获若有可变数据仍要自己同步。State归连接owner，malformed framing/protobuf仍关闭；可关联的unknown/auth/business/timeout通常typed Error且保留连接；日志reply只说明进入发送路径，不等于peer已收到。

**取舍与扩展。** 默认Login没有真实凭据校验，不会markAuthenticated。请求deadline不是零输入登录deadline。WS/TCP握手业务共用一注册模块，新增业务不应改transport。允许所有自定义route成功payload仍需明确协议扩展。

**证据。** PreLoginHandshake.h/.cpp、docs/protocol.md；MessageDispatchTestProfile.h/IntegrationTest.py、三个host/CMake：epoll/Asio×0/4与WS/TCP共六网络配置。笔记: ArenaServer/changes/2026-10-10-msgid-request-dispatch.md (Obsidian)。

## 22 · 68906b1 · 2026-10-10 · 机械整理有独立理由

原题：Clean up dispatch sources and remove redundant host setup。

**实际差异。** 为protocol文档、Router及测试文件补末尾newline；移除WebSocketTestHost在context.run返回后重新makeRpcTestFactory的两行。此时server已构造、业务已结束，重新赋局部factory没有运行用途。

**原因与取舍。** 删除无效接线避免读者误以为运行后会重新安装handler；统一文本格式便于审查。**未增加模块、消息、状态机、锁或新的运行链**，不用为机械提交编造架构突破。

**线程与验证面。** 没有新的跨线程操作/所有权变化；应看git show此提交，不能因标题含dispatch就重复算前两提交的实现。沿用RPC网络测试，没有新增用例。同任务笔记: ArenaServer/changes/2026-10-10-msgid-request-dispatch.md (Obsidian)。

## 23 · 7ff856d · 2026-10-10 · 多层协议预算要相减

原题：Bound routed responses by the gateway packet budget。

**原因。** EnvelopeCodec允许protobuf≤256KiB；gateway PacketCodec限制的是body≤256KiB，body还含7字节type/id/version。直接用shared的4MiB encoder可能生成本gateway不能接受的响应。

**机制与失败。** State.respond序列化后额外要求payload.size≤kMaxPacketSize−PacketHeader::kMetadataSize；超限/编码失败把响应换Internal9000固定小Error，再编码。公共EnvelopeCodec通用上限不改，gateway在自己的组合层负责扣metadata。

**线程与所有权。** 全在原owner respond路径，不新增状态、锁或异步边界。失败仍关联原request id，避免连接无可解释大响应。transport的1MiB queue界解决积压，不能替代单帧body界。

**扩展原则。** 修改外层metadata或配置预算时，同时检查入站与出站组合边界；“内层合法”不保证组合合法。这是审查发现后真实修复，不是性能优化。

**证据。** MessageRouter.cpp::State::respond、PacketBuffer.h、PacketHeader.hpp；MessageDispatchTest.cpp新增边界响应测试。笔记: ArenaServer/changes/2026-10-10-msgid-request-dispatch.md (Obsidian)。
