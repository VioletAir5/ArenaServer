# 05 Context、Runtime、Feature、责任链与MsgId路由

固定v0.2。这里覆盖gateway中所有非Reactor、非transport的代码；[完整源码](source-index.md)保留每个头和实现。需要理解的三个Seam是IConnection（transport操作）、IConnectionControl/IConnectionFeature（连接政策）、PacketHandler/MessageRegistry（完整包与业务请求）。

## 1. 文件地图

| 文件（services/gateway下） | 行为入口 |
| --- | --- |
| `include/net/ConnectionContext.h`、`src/net/ConnectionContext.cpp` | accept元信息、ConnectionSession状态 |
| `include/net/ConnectionFeature.h` | Timer/Scheduler/Control、Feature hooks、ConnectionPipeline；Timer实现位于Runtime.cpp |
| `include/net/ConnectionRuntime.h`、`src/net/ConnectionRuntime.cpp` | 生命周期/事件深度、输出通知合并、暂停原因、close门、统一装配 |
| `include/net/ConnectionSchedulers.h`、`src/net/ConnectionSchedulers.cpp` | EventLoop或Asio执行器adapter |
| `include/net/ConnectionHandler.h`、`src/net/ConnectionHandler.cpp` | 每连接工厂、Context寿命wrapper、raw echo默认 |
| `include/net/PacketPipeline.h`、`src/net/PacketPipeline.cpp` | 帧到责任链，EnvelopeDecodeHandler |
| `src/net/AuthenticationGateHandler.cpp` | 可选的链节点式认证门，声明在PacketPipeline.h |
| `include/net/PacketEcho.h`、`src/net/PacketEcho.cpp` | Packet echo节点与装配 |
| `include/net/PreLoginHandshake.h`、`src/net/PreLoginHandshake.cpp` | 历史PingHandler、当前默认registry与decoder→Router装配 |
| `include/net/BackpressureFeature.h`、`src/net/BackpressureFeature.cpp` | 输出高低水位与慢读期限 |
| `include/net/TokenBucketFeature.h`、`src/net/TokenBucketFeature.cpp` | 完整包令牌桶 |
| `include/net/PacketWhitelistFeature.h`、`src/net/PacketWhitelistFeature.cpp` | outer type白名单 |
| `include/net/RejectionCounters.h`、`src/net/RejectionCounters.cpp` | 固定8原因原子计数 |
| `include/net/MessageRouter.h`、`src/net/MessageRouter.cpp` | registry、拥有请求、expected回复、generation和deadline |
| `include/net/PacketBuffer.h`、`src/net/PacketBuffer.cpp` | gateway Buffer adapter，详见[协议章](03-protocol.md) |
| `include/net/Session.h`、`src/net/Session.cpp`、`include/net/SessionManager.h`、`src/net/SessionManager.cpp` | 旧的SessionID＋Socket及map；未接入当前鉴权Runtime |

## 2. ConnectionContext与两种Session不要混淆

Context构造要求id非0、counters非空；connectionId/peer/acceptedAt/session指针/rejections指针自身const。accept时从真实socket取得IPv4 peer与steady_clock acceptedAt，worker创建Context；WS acceptedAt早于HTTP Upgrade。Context不持transport、FD、timer或Runtime反向引用。

const Context不代表其指向的Session内容永远不可变。ConnectionSession记录构造线程，loginState/markAuthenticated/expireLogin都requireOwner；Pending只能转Authenticated或Expired，重复/另一终态返回false。没有锁，因为所有状态读取与修改在同一owner；Context shared ownership只延长寿命。

旧Session拥有SessionID和Socket，构造move FD、禁止copy、允许move、getters借用Socket；没有凭据判断。SessionManager拥有unordered_map<ID,unique_ptr<Session>>，add拒null或重复，find返回借用const指针，remove/destructor释放Session及Socket；没有同步或owner检查。源码检索只有其实现和SessionManagerTest使用它，当前ConnectionRuntime未依赖它。文件brief的“authenticated session”不能证明真实认证流程已经存在。

## 3. Runtime所有权和事件规则

~~~text
server owner registry → strong Runtime
 Runtime → strong transport / Context / handler(Codec+chain) / unique Features
 transport callbacks → weak Runtime
 scheduled callback → weak Runtime
 entry局部strong guard → 本次Runtime事件
~~~

Runtime构造验证transport/context/scheduler/handler均有效，且transport.id=context.id。makeConnectionRuntime先用makeConnectionHandler建每连接handler，再创建Runtime、配置Features；**非空FeatureFactory定义完整集合**，空factory才自动加Backpressure，不能误以为自定义桶会自动保留默认背压。

features()只取pipeline；ConnectionPipeline.add在sealed后抛logic_error，每个node unique_ptr，禁止复制/移动，不能热删除。start要求shared ownership、幂等、封pipeline，依序onConnected；首个close就短路。received持strong guard并调用m_handler(Runtime本身,bytes)，因此业务看到的IConnection其实是Runtime facade，dynamic_cast可以取得FeatureHost/Control。

start/received/outputChanged/inspectPacket/scheduleAt回调有异常屏障，计CallbackFailure并请求close。eventDepth用于“回调里发生实际close”的重入场景：transportClosed先标closed；depth归零后notifyClosed，再逐Feature onClosed，一次且noexcept。析构也notifyClosed，但必须owner；成员销毁顺序让Features、handler timers取消时transport/context仍存在，外部scheduler必须更久。

### 输出通知合并与多暂停原因

outputChanged先写nextOutput；未start/closing/正在通知则返回。外层while把reported更新为next，给Features(before,after)；回调里的send可能再次改变next，外层下一轮补通知而非递归遍历。中间值可能合并，这不是每次write的逐事件审计日志。

setReadPaused使用Backpressure=1、RateLimit=2、Authenticating=4、Application=8的bitmask。只在总mask从0→非0时pause，非0→0时resume。low水位只清自己的Backpressure位，不能解除Application。native resume可同步读并重入，先提交mask再调用transport。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/ConnectionRuntime.cpp 223-237 -->
原文：`services/gateway/src/net/ConnectionRuntime.cpp:L223–L237`；[完整文件](source/services/gateway/src/net/ConnectionRuntime.cpp.md)。

~~~cpp
    requireOwner();
    if (m_closing)
        return;
    const auto bit = static_cast<std::uint32_t>(reason);
    const auto before = m_pauseReasons;
    if (paused)
        m_pauseReasons |= bit;
    else
        m_pauseReasons &= ~bit;
    if (!before && m_pauseReasons)
        m_transport->pauseRead();
    // Native resume may synchronously read and reenter
    // received/outputChanged.
    if (before && !m_pauseReasons)
        m_transport->resumeRead();
~~~

### requestClose与IConnection facade

requestClose owner-only，atomic exchange closing使多次关闭请求只排一次。立即pause读，实际transport.close通过scheduler.defer weak回调；投递异常则直接close兜底。它先使Runtime.connected=false，阻止本批次PacketCodec继续业务，transport的具体close/reclamation见[04](04-transports.md)。m_closing原子服务foreign send检查；transport入口自己的门仍是最终send/close竞争的同步点。

send只在!closing时转transport（foreign caller要保证Runtime和transport寿命）；shutdown转底层，不自动设置Runtime.closing；pendingBytes/id转底层；pause/resume映射Application理由。connected读取transport，其线程契约仍由底层约束。

## 4. ConnectionTimer与Schedulers

ConnectionTimer保存cancel闭包、move用exchange取走；move赋值先cancel自己旧token；reset先取出/清空再执行，所以重复reset不再次取消。cancel抛异常会terminate，token析构要在owner，不能把它移动到后台线程随意销毁。

EventLoop adapter：defer=queueInLoop；at计算max(when-now,0)，runAfter，再返回cancelTimer token。shared<bool> live不是atomic，读写都由owner，已进入ready的timer也能因live=false跳过。Asio adapter：defer=post(executor)；steady_timer expires_at，completion仅!error且live才action；token cancel取消timer，并由timer/shared captures维护寿命。

**当前差异：**原生TimerQueue::addAfter要求delay>0，而此adapter把过期when转0，所以past/极短deadline可能抛invalid_argument；Asio expires_at允许已经到期的deadline。此为静态源码观察，本任务没有复现或修复。规划登录timeout时，应在owner先判断acceptedAt+期限是否已经过去，过期直接expire/close，未过期再schedule，不能假定这个原生adapter总会把past期限当即刻callback。

Runtime.scheduleAt只在未closing时注册，weak Runtime限制寿命，回调包eventDepth/异常屏障；Feature要自己保存token，并onClosed reset，否则销毁Runtime前定时器可能仍在外部scheduler中留存。

## 5. 三个Feature：只描述政策，不接管FD

### BackpressureFeature

构造必须low<high<1MiB且timeout>0，默认256/512KiB与5s。输出≥high且未paused时，置paused、安装慢client timer、设置Backpressure暂停；降到≤low时清paused、取消timer、解除这个理由。高低水位滞回防止在单阈值附近反复启停。

慢client期限从首次跨高水位开始，期间轻微进展不重置；timer到期计SlowClient并requestClose。它不按每条请求计业务timeout，也不替代transport复制前的1MiB硬界。onClosed只取消token。

### TokenBucketFeature

构造检查capacity有限且≥1、rate有限且>0、Now有效、exhausted只能Drop或Close。初始满桶；onConnected记录时间；onPacket按elapsed秒补rate*elapsed、截到capacity，再有≥1就扣一枚Continue，否则计PacketRateExceeded并返回配置决策。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/TokenBucketFeature.cpp 31-53 -->
原文：`services/gateway/src/net/TokenBucketFeature.cpp:L31–L53`；[完整文件](source/services/gateway/src/net/TokenBucketFeature.cpp.md)。

~~~cpp
FeatureResult
TokenBucketFeature::onPacket(IConnectionControl& control, PacketView)
{
    auto now = m_now();
    if (now > m_last)
    {
        const double elapsed = std::chrono::duration<double>(
                                   now - m_last
        )
                                   .count();
        m_tokens = std::min(m_capacity, m_tokens + elapsed * m_rate);
        m_last = now;
    }
    if (m_tokens >= 1)
    {
        m_tokens -= 1;
        return FeatureResult::Continue;
    }
    control.context().rejections->record(RejectionReason::PacketRateExceeded);
    if (m_exhausted == FeatureResult::Close)
        std::cerr << "connection packet rate exceeded id="
                  << control.context().connectionId << '\n';
    return m_exhausted;
~~~

Now可注入确定性测试时钟；时间倒退不补令牌也不回退last。double表示分数令牌，不是简单整数QPS窗口。按**完整合法帧**扣一次，不按read、字节或MsgId；一次512帧粘包照样512次。非法header在这之前由codec早拒。新连接重新满桶，因此没有跨连接/IP限流保证。

### PacketWhitelistFeature

构造sort/unique配置，onPacket binary_search outer type；匹配Continue，否则计PacketTypeNotAllowed并Close。空vector拒所有；它不读peer/IP，也不是业务MsgId权限表。owner独有node且配置封存，不需要锁。

## 6. 两条pipeline：连接事件与Packet责任链

ConnectionPipeline维护Feature hooks的有序vector；PacketPipeline维护unique节点链。PacketHandler.setNext转移unique所有权；基类handle转next、tail无节点Reject。过滤通过时调用base，业务已经处理返回Handled，拒绝设PacketContext.rejectionReason再Reject。不是每个节点必须返回Continue enum。

PacketContext只有一次完整帧的寿命：借用connection/header/payload，optional Envelope解析后拥有数据；默认拒绝原因HandlerRejected。PacketPipeline构造拒null head，process直接head.handle。

makePacketHandler拥有独立shared PacketCodec与chain，检查gateway limit≤256KiB（下限由Codec检查）。每次feed完整帧**先dynamic_cast PacketFeatureHost做policy**，Close→close/停止decoder，Drop→消费此帧但不进业务；Continue才新建PacketContext进链。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/PacketPipeline.cpp 93-120 -->
原文：`services/gateway/src/net/PacketPipeline.cpp:L93–L120`；[完整文件](source/services/gateway/src/net/PacketPipeline.cpp.md)。

~~~cpp
                    if (auto* features = dynamic_cast<
                            PacketFeatureHost*>(&connection))
                    {
                        const auto decision = features->inspectPacket(
                            PacketView{
                                header.m_type,
                                std::as_bytes(std::span(payload))}
                        );
                        if (decision == FeatureResult::Close)
                        {
                            connection.close();
                            return false;
                        }
                        if (decision == FeatureResult::Drop)
                            return connection.connected();
                    }
                    PacketContext context{
                        connection,
                        header,
                        payload};
                    if (chain->process(context) ==
                        PacketHandleResult::Reject)
                    {
                        counters->record(context.rejectionReason);
                        connection.close();
                        return false;
                    }
                    return connection.connected();
~~~

InvalidFrame单独计数关闭；节点Reject计其reason；异常计CallbackFailure关闭。Feature自己计限流/白名单原因，返回Close时不再记HandlerRejected，避免这里重复统计。直接对裸transport安装handler而不经过Runtime，dynamic_cast失败就没有这些Feature；raw默认handler也不做framing/inspectPacket，不能把桶factory挂上去就声称raw流量已限速。

EnvelopeDecodeHandler先安全decode，失败设置InvalidEnvelope；成功emplace拥有Envelope再转链。AuthenticationGateHandler是可选节点，缺Envelope fail closed，Pending只放outer request的LoginRequest/Ping，Authenticated放行，Expired拒。它的Reject会关闭连接。**当前默认握手不装这个节点，认证政策在Router按route执行**，其Typed Unauthenticated回包通常保持连接。

EchoHandler仅outer request1匹配，PacketCodec encode response2/原outer id/同payload，发送后看connected；其他forward。makePacketEchoHandler组装它且使用默认counter。它不要求Envelope，用于packet demo和慢读echo测试。

## 7. PreLoginHandshake两代入口

历史PingHandler在Envelope后匹配request＋ping，回Pong/原inner id/原outer id，不走Registry/pending日志。它仍编译和有测试，但makePreLoginHandshakeHandler当前不使用它。

当前defaultMessageRegistry静态创建并seal：Ping公开，复制nonce到Pong；Login公开，但complete unexpected Unavailable2002。每连接makePreLoginHandshakeHandler创建fresh EnvelopeDecodeHandler→MessageRouter，再makePacketHandler并使用Context.rejections。Ping不markAuthenticated；schema里LoginResponse.accepted存在不等于验证credential成功。

## 8. MessageRegistry、RequestMessage与Typed Error

Registry unordered_map<MsgId,handler+options>，register拒seal后写入、id≤0、回复类MsgId(LoginResponse/Pong/Error)、空handler、timeout≤0或>1h、重复；seal后多worker只读，同步由“启动前配置、启动后不写”保证。新业务注册仍要增加schema和成功response校验，当前返回payload只有Pong/LoginResponse被支持。

RequestMessage拥有Envelope副本和shared<const Context>，其shared<const request>可给非阻塞任务持有；PacketContext/header/payload view不能异步保存。MessageReply=expected<Envelope,ErrorCode>；BusinessError规范化非法或OK错误到Internal，固定error文本，不把异常what()发送给客户端。Completion必须owner调用，foreign会抛logic_error；框架不自动投递回owner。

### 两层request id的实际规则

handle以inner request_id非零优先，否则outer uint32；保存wireRequestId独立回复外层。**当前不要求inner=outer，也不要求inner≤UINT32_MAX**；测试明确使用1234567890123ULL的inner并保留outer7。有效id不能0。日志/pending以有效uint64 id为键，网线外层仍回复原uint32。

msg_id非零优先，否则从oneof推导。known Ping必须has_ping、Login必须has_login_request；未注册msg_id先UnknownMessage。outer非request先InvalidRequest。不要把这些局部形状判断描述成所有新MsgId的通用payload验证。

## 9. Router.handle的实际先后

1. owner校验、Envelope存在、connection id一致；Router仅绑一个connection地址。
2. 复制RequestMessage，产生generation/临时Pending。
3. 若有效id已有pending：**结束旧pending**并给旧请求回DuplicateRequest；不启动新handler，不给新输入另一个start/end。旧请求若已到deadline，finish会改成Timeout。
4. log request_start；检查nonzero id、outer方向、route存在、已知payload匹配。
5. session Expired一律拒；requiresAuthentication=true必须Authenticated（默认true）。
6. pending≥64拒TooManyRequests；connection必须有IConnectionControl，否则Internal。
7. steady deadline=now+route timeout；先insert pending，再安排timer，再log request_handler并调用handler。
8. BusinessError映射其code；其他异常Internal；completion结束pending。若handler同步complete后又抛，第二次complete因pending已不存在而忽略。

### 迟到、重复completion与id复用

pending每条包含request、wire id、explicitMsgId、generation、deadline、RAII timer。finish先找到(id,generation)，无或generation不同直接return；匹配则move entry/erase、reset timer，然后检查now≥deadline，最后respond。旧completion不能把同id的新请求结束。generation是uint64自增，理论回绕未另加保护，不应写成无限次绝不复用。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/MessageRouter.cpp 249-263 -->
原文：`services/gateway/src/net/MessageRouter.cpp:L249–L263`；[完整文件](source/services/gateway/src/net/MessageRouter.cpp.md)。

~~~cpp
    void finish(
        std::uint64_t id, std::uint64_t ticket, MessageReply result
    )
    {
        auto found = pending.find(id);
        if (found == pending.end() ||
            found->second.generation != ticket)
            return;
        auto entry = std::move(found->second);
        pending.erase(found);
        entry.timer.reset();
        if (ConnectionClock::now() >= entry.deadline)
            result = std::unexpected(arena::protocol::ERR_TIMEOUT);
        respond(entry, std::move(result));
    }
~~~

timer和Completion都capture weak State，Router销毁时State的pending析构取消token并记录cancelled。State的connection指针借用Runtime，不应被外部长期强持State；外部只拿request/weak completion。request可被外部任务继续持有，因此pending界和timeout不能强行回收外部业务工作内存。

owner timer不能抢占阻塞handler；finish的二次deadline比较避免owner被阻塞后“晚完成成功”，但仍需业务非阻塞，否则整个loop的其他连接都受影响。

## 10. respond、错误计数及spdlog

respond若connection已断开，记录cancelled不send；正常Envelope成功必须匹配请求响应种类，否则Internal。覆盖reply.request_id有效id，explicit请求回复显式msg_id，旧msg_id=0继续0；outer type2及原wire id。

EnvelopeCodec encode失败或payload>256KiB-7会退化为小的Internal Error；若仍无法成帧则close并log encode_failure。Typed错误Unauthenticated计对应原因，其他计HandlerRejected；它们是拒绝事件，不一定close。send后request_end表示已提交回复，**不是对端已经收到**。

spdlog输出event/connection_id/request_id/msg_id/code/outcome，start→handler→end串起请求；早拒没有handler阶段，cancelled也可只有结束记录。log内部catch防止日志sink异常破坏close/业务；不记录account/credential/完整payload/任意exception.what。transport及Feature部分仍用cerr，不能称全仓日志已统一spdlog。

RejectionCounters固定8槽，record/count越界忽略/0，snapshot逐槽relaxed load，不是跨槽事务一致快照；default是同进程共享实例，测试可注入隔离对象。它记录packet/policy事件，不是所有EOF/RST/stop、去重连接或IP数。

## 11. 当前可扩展处与未实现处

新增连接规则放Feature hooks，新增包过滤放PacketHandler链，新增业务放sealed Registry；这分别保持政策、包处理和请求生命周期的Locality。shared可变factory捕获仍要自己同步；不能靠每连接node的owner契约保护跨worker共享数据。

LoginTimeoutFeature、共享IP Admission表、全局内存预算、真实凭据鉴权/授权、TLS尚未实现。expireLogin是状态接口，scheduleAt是能力，不等于已经接好登录期限；旧SessionManager也不能提供共享IP或新登录流程。

源码相关测试：ConnectionContextTest、ConnectionRuntimeTest、ConnectionFeatureTest、SecurityPolicyTest、PacketPipelineTest、MessageDispatchTest及各IntegrationTest；测试覆盖和测试support的作用见[07](07-tests.md)。继续阅读[构建与main](06-build-and-entry.md)了解哪些路径实际装配，避免把可选节点当默认运行链。
