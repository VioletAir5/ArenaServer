# 04 三种 transport：连接、发送队列与服务器回收

固定版本v0.2。相关文件全部在[源码索引](source-index.md)；本章逐组覆盖IConnection、ITcpServer、ConnectionId以及TcpConnection/TcpServer、AsioConnect/AsioTcpServer、WebSocketServer的头和实现。Reactor、Socket、Acceptor底层见[02](02-reactor.md)，通用策略见[05](05-runtime-routing.md)。

## 1. Interface：共有操作与具体契约

`services/gateway/include/net/IConnection.h`定义id/send/shutdown/close/connected/pauseRead/resumeRead/pendingBytes。shared_ptr只共享寿命，不自动让所有方法可并发。id不可变；TcpConnection/AsioConnect的send明确支持并发，其余操作及最终释放由owner负责。span输入在send返回后可以释放，因为transport立即写入或复制。

pendingBytes统计“已接受、尚未观察到成功写入”的应用字节，包括入口队列、owner输出和异步in-flight，排除成功write后已进入内核的字节。它不是TCP对端已确认量，也不是内核发送队列长度。connected表示连接是否逻辑存活，不保证还能接受新send。

`services/gateway/include/net/ITcpServer.h`定义stop/stopped/count/max/workers；负grace先抛异常；count包含等待worker注册和等待回收ack的accepted socket；Stopped要求自有workers已经cleanup/join，但不quit外部base context。具体启动方式保留在TcpServer/AsioTcpServer构造中。

**WS差异必须保留：**WebSocketServer没有实现ITcpServer，没有grace/stopped/workerCount；stop直接abort所有连接。内部WebSocketConnection虽然实现IConnection，但connected仅在Open为true，shutdown启动WS close并丢弃未开始输出，仅保留正在写的front；不遵循TCP“排空所有输出再FIN”的完整语义。调用者不能把两个shutdown当成同一个网络协议保证。

### ConnectionId的两种身份

`services/gateway/src/net/ConnectionId.h/.cpp`用进程级atomic uint64分配从1开始的id，compare_exchange_weak重试，relaxed只用于唯一数值，不发布其他内存。到UINT64_MAX后successor设0，后续调用抛overflow_error，避免回绕复用。它被三transport共用。

TCP server另有本实例的admission ticket用于base统计和worker回收。ticket不是transport.id、request id或SessionID。FD也不能当身份，因为Linux会复用已关闭FD号。

## 2. TcpConnection：跨线程入口与owner输出

文件：`services/gateway/include/net/TcpConnection.h`、`services/gateway/src/net/TcpConnection.cpp`。

### 构造和所有权

构造检查EventLoop有效且当前是owner，移动Socket所有权，Channel借用socket FD并启用IN/RDHUP/ET；事件回调路由到本对象。Server随后Channel.tie(connection)以weak_ptr在事件期间取得强引用。析构在错误线程会terminate；正确顺序是owner先DEL Channel，再最终释放Socket。close callback不应在当前事件栈同步销毁连接。

### send/drainSendQueue/sendInLoop

跨线程send要求调用者持有shared ownership；只修改mutex保护的m_sendQueue、pendingBytes、acceptingSends及drainPosted。检查采用data.size>1MiB-pending；超界关入口门，再由owner逻辑close。复制成功才加pending。尚未被owner借用的入口tail可合并到64KiB，减少小send分配。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/TcpConnection.cpp 99-129 -->
原文：`services/gateway/src/net/TcpConnection.cpp:L99–L129`；[完整文件](source/services/gateway/src/net/TcpConnection.cpp.md)。

~~~cpp
        std::lock_guard lock(m_sendMutex);
        if (!m_acceptingSends)
        {
            return;
        }
        if (data.size() > kHardOutputLimit - m_pendingBytes)
        {
            m_acceptingSends = false;
            overflow = true;
        }
        else
        {
            // Only ingress buffers are mutable; the owner has not
            // borrowed this tail for a socket write.
            if (!m_sendQueue.empty() &&
                m_sendQueue.back().size() + data.size() <= 64 * 1024)
            {
                auto& tail = m_sendQueue.back();
                tail.insert(tail.end(), data.begin(), data.end());
            }
            else
            {
                m_sendQueue.emplace_back(data.begin(), data.end());
            }
            m_pendingBytes += data.size();
            if (!owner && !m_sendDrainPosted)
            {
                m_sendDrainPosted = true;
                scheduleDrain = true;
            }
        }
~~~

foreign sender只在没有已排drain时queueInLoop一个weak回调；owner send直接drain，并临时保活避免output回调释放调用者。`m_drainingSends`防止同步回调再次send时递归drain；guard退出复位。drain在锁内swap入口和清posted，锁外依序sendInLoop，重复swap直到empty，因此回调产生的新send也能继续被处理。

sendInLoop只有owner进入：已有output先追加维持顺序；为空则尝试非阻塞send(MSG_NOSIGNAL)。成功按实际n扣pending/update activity，EINTR重试，EAGAIN将剩余追加到PacketBuffer并启用EPOLLOUT，其他错误close。MSG_NOSIGNAL防止断开peer的写触发SIGPIPE结束进程。

一次send是进入串行字节流的一次复制，不意味着对端一次recv或一个TCP packet；不同线程的先后由取得入口mutex的顺序决定，调用者不应期待墙钟调用顺序。m_outputBuffer不需要锁，因为foreign只碰入口，唯一owner写它。

### handleWrite与半关闭

handleWrite循环冲刷output，部分成功跳过已写字节并扣pending；EAGAIN保留interest等下一次就绪。output空且入口pending仍非零时禁用EPOLLOUT，等待已排drain，不提前FIN。pending全0后按state完成SHUT_WR、关闭已EOF peer或禁用写interest。

状态分别描述：Connected、PeerHalfClosed、WriteDraining、WriteDrainingPeerClosed、WriteHalfClosed、Disconnected。m_readPaused/m_reading/drainingSends是正交的操作状态，不能挤成单一“连接已连接”位。shutdownWrite先关send入口，排空已接受send，再切换draining；finishShutdownWrite仅pending=0时SHUT_WR，peer还未EOF就继续读。

### handleRead/pause/resume/error/close

handleRead的m_reading防止resume引起嵌套读取破坏message视图。8192字节栈缓冲读到inputBuffer，再同步消息回调，回调结束消费；每次检查close/pause。ET通常读到EAGAIN；读到0处理EOF并保留已接受的待发送量，不能简单把RDHUP直接当全关闭。

pauseRead/pauseReading禁用IN；resumeRead/resumeReading重新启用，并主动handleRead，因为ET下旧数据不保证再产生一个新边沿。m_reading使回调里的resume安全落回当前读取循环。

handleError读取SO_ERROR；error=0尝试读尽当前数据，实际errno由handleIoError统一日志和close。`handleClose`Disconnected guard确保只通知一次；关send入口、清入口队列、pending归零。**此时不DEL、不closeFD，m_outputBuffer仍可能保留存储**；Server在事件后移除注册、销毁Runtime/transport时才释放它，故队列释放的验收必须包括最终owner回收。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/TcpConnection.cpp 504-526 -->
原文：`services/gateway/src/net/TcpConnection.cpp:L504–L526`；[完整文件](source/services/gateway/src/net/TcpConnection.cpp.md)。

~~~cpp
void TcpConnection::handleClose()
{
    if (m_state == TcpConnectionState::Disconnected)
    {
        return;
    }

    m_state = TcpConnectionState::Disconnected;
    {
        std::lock_guard lock(m_sendMutex);
        m_acceptingSends = false;
        m_sendQueue.clear();
        m_sendDrainPosted = false;
        m_pendingBytes = 0;
    }

    // The close callback arranges owner-side Channel removal and final
    // release after the active event dispatch. Do not close the fd here.

    if (m_closeCallback)
    {
        m_closeCallback(this);
    }
~~~

id/fd/channel/lastActivity/closing/pendingOutputBytes和三个callback setter用于owner集成；pendingBytes虽内部加锁，IConnection仍不承诺所有getter跨线程可用。

## 3. TcpServer：控制线程不触碰worker连接

文件：`services/gateway/include/net/TcpServer.h`、`services/gateway/src/net/TcpServer.cpp`。

base owner拥有Acceptor、admissions、stop timer和EventLoopThreadPool。每个Worker拥有自己的connections(Runtime强引用＋transport借用指针)、pendingRemove、idleTimers，在自己的loop上修改。0workers使用base；其他worker通过pool round robin分配。

~~~text
base accept(Socket,peer,acceptedAt)
 → 容量/ticket检查，先登记admission
 → 携带Socket到worker queue
 → 创建TcpConnection/Context/Runtime，绑weak Runtime callbacks
 → 注册connections，Runtime.start
 → read/output/close在worker执行
 → close callback只deferRemoval
 → owner队列删除Channel、erase Runtime/Socket
 → base收到ticket回收ack → admissions.erase
~~~

handleNewConnection先登记ticket再排任务，避免快速失败/关闭比base登记更早；shared<Socket>仅使任务capture可复制，FD所有权仍由Socket的move转移，提交失败回滚admission。Worker setup失败取消timer、DEL已注册Channel、移除/释放连接，并通知base。

deferRemoval用set去重，queue一次removeClosedConnections。当前epoll事件分发结束后才DEL并erase；EventLoop还会将已复制ready列表中的该Channel置空，阻止稍后悬空事件。关闭期间不是只看closeOnce guard，还要看这条完整回收链。

idle timer根据lastActivity计算剩余，未到期重排；owner上取消，不把跨线程socket查询塞到base。drain遍历request shutdown，forceClose遍历close；遍历中不立即erase。

stop为Running→Draining：停止accept、给owners排drain，admissions空则finish；否则deadline forceClose。cleanupWorkers先给所有owners排cleanup/promise，全部提交后再wait，最后stop/join pool，清worker/admissions。Worker不等待base回收，因此base同步等待不会形成循环等待。不能以EventLoop.quit会执行所有剩余任务为前提。

destructor的callbackGuard使迟到的base通知不会使用已销毁this；先停止/取消/cleanup，owner错误或无法完成noexcept析构会terminate。stop/stopped/count/max/workers遵循base线程契约，查询没有额外自动dispatch。

## 4. AsioConnect：同一个owner，两条异步操作状态

文件：`services/gateway/include/net/AsioConnect.h`、`services/gateway/src/net/AsioConnect.cpp`。

构造保存owner线程、进程id和strand，检查socket已open且属于传入context，设置nonblocking/CLOEXEC，idle期限正值。**context仍要求恰好一个owner线程运行**；strand串行回调不能让所有普通方法随意跨线程执行。start在callback安装后调用、要求shared ownership、幂等；启动idle/read/write并drain此前send。

### 两级队列与in-flight存储

send入口mutex、门、1MiB预算及合并策略与原生TCP相似。drain把shared<vector<byte>>入口swap到owner的m_output。只允许合并尚未被Asio借用的tail：若正在写且output只有一个，它就是active front，不能insert造成vector重新分配。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/AsioConnect.cpp 181-197 -->
原文：`services/gateway/src/net/AsioConnect.cpp:L181–L197`；[完整文件](source/services/gateway/src/net/AsioConnect.cpp.md)。

~~~cpp
    for (auto& buffer : incoming)
    {
        // The active front buffer must not move while Asio uses it.
        if (!m_output.empty() &&
            (!m_writing || m_output.size() > 1) &&
            m_output.back()->size() + buffer->size() <= 64 * 1024)
        {
            auto& tail = *m_output.back();
            tail.insert(tail.end(), buffer->begin(), buffer->end());
        }
        else
        {
            m_output.push_back(std::move(buffer));
        }
    }
    notifyOutputChanged();
    writeNext();
~~~

writeNext在started、非Closed、非writing、output非空时才开始async_write_some；设置writing后再发起。frontOffset处理部分完成，按实际bytes扣pending；front完成才pop。completion先验证weak连接并处理operation_aborted和Closed，随后才能访问当前队列。下一次write只从这个完成路径启动，故多个send不产生重叠写。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/AsioConnect.cpp 463-503 -->
原文：`services/gateway/src/net/AsioConnect.cpp:L463–L503`；[完整文件](source/services/gateway/src/net/AsioConnect.cpp.md)。

~~~cpp
void AsioConnect::writeNext()
{
    if (!m_started || m_state == State::Closed || m_writing ||
        m_output.empty())
    {
        return;
    }
    m_writing = true;
    auto buffer = m_output.front();
    const auto offset = m_frontOffset;
    m_socket.async_write_some(
        boost::asio::buffer(
            buffer->data() + offset,
            buffer->size() - offset
        ),
        boost::asio::bind_executor(
            m_strand,
            [weak = weak_from_this(), buffer](
                boost::system::error_code error,
                std::size_t bytes
            )
            {
                auto self = weak.lock();
                if (!self)
                {
                    return;
                }
                if (error == boost::asio::error::operation_aborted)
                {
                    self->closeOnce();
                    return;
                }
                if (self->m_state == State::Closed)
                {
                    return;
                }
                self->m_writing = false;
                self->m_frontOffset += bytes;
                {
                    std::lock_guard lock(self->m_sendMutex);
                    self->m_pendingBytes -= bytes;
~~~

handler capture强buffer＋weak self：关闭清queue后，Asio仍借用的地址活到操作完成；连接可以被Server回收，弱锁失败时只释放buffer。不要把“队列已清”理解为操作内存能在取消completion前立即释放。

### read状态和暂停

ReadState为Idle→InFlight→Ready→Delivering→Idle。async_read_some用独立shared<array<byte,8192>>，并绑定strand。如果完成时readPaused，保留Ready数据与error而不启动下一次读；resume先deliverRead再readNext。Delivering防止消息回调同步resume覆写借用缓冲。

一次completion同时有bytes和EOF时，先交付bytes，再处理peer half-close；其他错误走ioError。读operation_aborted调用closeOnce，idle timer的operation_aborted直接return，因为重排/关闭时取消timer是常规路径。两类取消分支不能统称为一个业务错误。

### closeOnce、shutdown与idle

closeOnce先Closed gate，再关入口/clear/pending0，socket.close取消I/O、cancel idle、clear owner output/resetoffset/readstate；复制close callback并捕获异常，只通知一次。shutdown关入口、切换WriteDraining或WriteDrainingPeerClosed、drain，然后finishWrite：pending0时shutdown_send，not_connected容忍，peer已EOF则close，否则WriteHalfClosed保留读。

notifyOutputChanged在owner执行，回调异常导致close；idle按照lastActivity+timeout到期后再次比较真实空闲时长，未到期则rearm。FD getter为适配Boost1.74的非const native_handle使用const_cast，不转移handle。析构也严格owner，持shared_ptr不是任意线程最终释放的许可。

## 5. AsioTcpServer：把FD从base context转给worker

文件：`services/gateway/include/net/AsioTcpServer.h`、`services/gateway/src/net/AsioTcpServer.cpp`。wrapper拥有shared Impl；stop委托，五个查询读Impl状态；它们按base owner契约使用，不加查询锁。

Impl拥有base acceptor/work guard/retryTimer/stopTimer、admissions和workers。AsioWorker可拥有独立context/thread/guard；0workers借用base。worker启动前prime一个timer服务以建立reactor内部FD，READY后才统计稳定FD基线。run异常记录后继续run，不能推导异常中的业务状态一定能恢复。

accepted socket先在base；AcceptedFd RAII holder在release前分配，避免“先release再分配失败”丢失FD。worker socket.assign接管后holder.take设-1；Context保存真实peer/acceptedAt，创建Runtime绑定callbacks，登记/start。draining或forceClosing标记也检查新排队setup，避免stop期间漏掉半建连接。

async_accept传入独立endpoint输出，accept成功便有peer，不依赖RST后remote_endpoint还能查询。达到capacity直接关闭。EMFILE/ENFILE处理reserve FD、accept并丢弃一个积压peer、重开reserve；accept错误通过100ms retry避免忙循环。stop关闭acceptor/pending accept/重试并排drain或forceClose，ticket回收后才Stopped。

deferRemove post weak Worker并去重，离开completion栈后erase Runtime，再通知base。cleanup取消callbacks/close全部/清registry并reset guard，**没有context.stop直接丢弃未运行completion**；work自然排空后thread join，in-flight capture的buffer得到释放。cleanup同样先post所有promise再等，worker不等base。Impl.destroy标记destroying、停accept/cancel/cleanup；外部context需活到析构后。

## 6. WebSocketServer与内部WebSocketConnection

文件：`services/gateway/include/net/WebSocketServer.h`、`services/gateway/src/net/WebSocketServer.cpp`。内部连接不暴露单独头；shared Impl的registry强持Runtime，transport只是借用指针。整个server在一个context owner运行，connection的异步回调绑定各自strand。

### HTTP/Upgrade阶段

State依序ReadingHttp→Upgrading→Open；HTTP header和flat_buffer上限8KiB、body_limit=0，握手timer5s；请求必须Upgrade且target=/ws、Origin与配置字符串完全相等。拒绝是403（upgrade但policy不符）或400，HTTP response通过shared_ptr保活到async_write结束再abort。Origin不是身份，原生客户端可以伪造。

WS read_message_max=256KiB+8，binary(true)配置输出。`Runtime.start`在Upgrade成功后才运行，但Context.acceptedAt仍为TCP accept时刻。容量registry也包括握手中的连接；WebSocketServer默认1000，但测试宿主可设100，不能把宿主profile值写成类默认值。

### 收发路径

async_read由Beast重组WS frame成message。m_reading/readReady/delivering阻止重入、支持pause期间留存结果；deliverRead同步借用flat_buffer bytes，回调后consume。text以1003关闭，too_big的Beast路径记录1009，peer closed日志包含其code；应用close用1008。

send在mutex内复制string并入同一m_output，维护pending≤1MiB，post到strand只排一次drain。m_writing保证仅一个Beast async_write进行；capture强front维持storage。onWrite完成整条message才扣front长度/pop、通知及next。WS保留message边界，但PacketCodec按应用流处理；传输fragment不能当业务Packet。

### close和abort不同

startClose只从Open进入ClosePendingWrite，关闭send门、删除未开始的tail，只保留in-flight front；启动5s close timer。无write则performClose，有write则等onWrite，随后CloseHandshake并async_close。timer到期或completion都调用abort。避免async_close与另一个正在写的操作同时写WS stream。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/WebSocketServer.cpp 475-501 -->
原文：`services/gateway/src/net/WebSocketServer.cpp:L475–L501`；[完整文件](source/services/gateway/src/net/WebSocketServer.cpp.md)。

~~~cpp
        m_state = State::ClosePendingWrite;
        m_closeCode = code;
        m_closeReason = std::move(reason);
        {
            std::lock_guard lock(m_sendMutex);
            m_acceptingSends = false;
            // Preserve an in-flight write until its completion
            // callback.
            const auto keep = m_writing ? 1u : 0u;
            while (m_output.size() > keep)
            {
                m_pendingBytes -= m_output.back()->size();
                m_output.pop_back();
            }
        }
        auto self = shared_from_this();
        m_closeTimer.expires_after(kCloseTimeout);
        m_closeTimer.async_wait(asio::bind_executor(
            m_strand,
            [self](Error error)
            {
                if (!error)
                    self->abort();
            }
        ));
        if (!m_writing)
            performClose();
~~~

abort用atomic exchange Closed去重，关入口/clear/pending0，cancel两timer及底层socket，再closeFD，通知Runtime.transportClosed及registry.remove。取消回调仍可能持self，执行时Closed检查退出，最终buffer/self释放；原子state用于connected读，普通read/write标志仍owner专属，原子不能代替整体线程契约。

Impl.acceptNext容量检查后建立Context/Connection/Runtime，set四callbacks、入registry再start；异常记录setup日志。Impl.stop移动registry到局部后逐一abort，避免遍历原map时removed callback改变它；停accept后不会再acceptNext。Public destructor/stop复用这个幂等路径，没有TCP的graceful drain保证。

### 源码观察与改进机会（本任务未改生产代码）

WS generic error没有把operation_aborted单独分类，只走abort/Closed guard；它有取消收敛，但日志归因与AsioConnect不同。transport日志仍不少使用cerr，Router请求日志才使用spdlog。abort noexcept调用闭包且不设本地异常屏障；当前绑定Runtime封装路径要保持不抛。setup在registry.emplace后session.start若抛，catch没有显式erase新Entry；这是静态异常路径观察，未通过故障注入证明，本章不把它报告成已复现漏洞。

## 7. 放在一起理解的保证与限制

| 项目 | epoll TCP | Asio TCP | WS |
| --- | --- | --- | --- |
| 入口并发send | mutex入口→owner drain | mutex入口→strand drain | mutex输出→strand启动 |
| 写串行依据 | owner独占send/Channel | writing＋completion链 | writing＋Beast completion链 |
| 输出预算 | queued＋buffer≤1MiB | ingress＋queued＋in-flight≤1MiB | queued＋in-flight≤1MiB |
| close回收 | 逻辑close→事件后DEL/erase | close socket→defer erase→取消completion | close handshake/abort→registry erase→剩余callback释放 |
| 暂停后resume | 主动读尽ET旧数据 | Ready先deliver | readReady先deliver |
| 空闲策略 | Worker timer | 连接idle timer | HTTP/close timer；通用慢读Feature |
| server关闭 | FIN排空＋deadline | FIN排空＋deadline | abort全部 |

连接级预算不等于全局内存预算；三个Server没有共享IP计数/共享容量。共享策略通过Runtime Feature接入，不是往三个transport复制鉴权/限流。设计中的Seam确实有三个Adapter，但Interface仍包含owner、半关闭差异、视图与缓冲寿命这些使用条件。

测试入口：TcpServerWorkerTest/ConnectionOwnershipTest/ConnectionLifecycleTest、AsioConnectTest/AsioTcpServerTest、各TCP integration、WebSocketIntegrationTest、跨transport Feature/Security/Dispatch integration和ChaosRun。具体断言与验证限制见[07](07-tests.md)；[30分钟报告](../../reports/v0.2-chaos-20261011.md)不能代替所有并发/取消调度的证明。
