# 02 Reactor、Socket、线程发布与定时器

固定v0.2；覆盖整个Linux I/O底座，包含v0.1已经存在的代码。完整原文见[源码索引](source-index.md)。

## Socket与InetAddress：FD所有权与地址值分开

文件：`services/gateway/include/net/Socket.h`、`services/gateway/src/net/Socket.cpp`、`services/gateway/include/net/InetAddress.h`、`services/gateway/src/net/InetAddress.cpp`。

Socket拥有FileDescriptor；createTcp使用CLOEXEC，createTcpNonBlocking额外NONBLOCK；两者都是IPv4。bind/listen及setsockopt失败抛system_error，不吞掉errno。accept4在EINTR重试，EAGAIN/EWOULDBLOCK返回nullopt，其他错误抛；成功把peer sockaddr_in和值型Socket返回。accept不把“当前没有连接”与“资源失败”混为一种结果。

close通过空FileDescriptor赋值；shutdownWrite仅SHUT_WR，读方向仍有效。setReuseAddr/setReusePort/setTcpNoDelay只提供机制，是否调用看上层：提供TCP_NODELAY setter不意味着全部业务已默认启用。

InetAddress保存sockaddr_in，字符串构造inet_pton校验，port用htons/ntohs；默认构造用于accept填peer。sockaddrPtr只借用内存；ip用inet_ntop，失败返回空字符串；toString组合ip:port。当前不是IPv6通用地址接口。扩展IPv6需要socket族、储存结构和accept/client代码一起调整。

## EpollPoller：内核事件与借用Channel指针

文件：`services/gateway/include/net/EpollPoller.h`、`services/gateway/src/net/EpollPoller.cpp`。

构造epoll_create1(CLOEXEC)，RAII owns epoll FD；add/update/remove分别ADD/MOD/DEL，data.ptr是借用的Channel*，失败抛system_error。poll清active vector→epoll_wait→EINTR当空轮次→把revents写回Channel并放active；返回数量填满event buffer则容量翻倍。

关键点：epoll不拥有C++ Channel，DEL也撤不回已经复制给用户态本轮vector的事件；生命周期要由EventLoop+连接owner保证。events数组容量增长不是连接数量限制，容量准入在server。

## Channel：事件路由没有Socket所有权

文件：`services/gateway/include/net/Channel.h`、`services/gateway/src/net/Channel.cpp`。

m_fd只是int，没有close；头中的“Owned file descriptor”注释容易误导，以构造/析构实现为准。Socket或FileDescriptor才是真owner。m_events是兴趣，m_revents是本轮结果，必须分开。enable/disable只改bit，通过m_update可回调更新epoll；setEvents更新失败恢复previous，避免本地兴趣和内核登记含义分叉。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/Channel.cpp 129-159 -->
原文：`services/gateway/src/net/Channel.cpp:L129–L159`；[完整文件](source/services/gateway/src/net/Channel.cpp.md)。

~~~cpp
void Channel::handleEvent()
{
    const std::shared_ptr<void> guard = m_owner.lock();
    if (m_tied && !guard)
    {
        return;
    }
    if (m_revents & (EPOLLERR | EPOLLHUP))
    {
        if (m_errorCallback)
            m_errorCallback();
    }

    if (m_revents & EPOLLOUT)
    {
        if (m_writeCallback)
            m_writeCallback();
    }

    if (m_revents & EPOLLIN)
    {
        if (m_readCallback)
            m_readCallback();
    }

    if (m_revents & EPOLLRDHUP)
    {
        if (m_closeCallback)
            m_closeCallback();
    }
}
~~~

tie保存weak owner，dispatch先lock为guard，使本轮所有callback期间对象仍活着。ERR/HUP→write→read→RDHUP顺序可能在同一轮连续执行；error callback关闭后，后续连接callback须检查状态，不能假设每轮只有一个事件。RDHUP是对端写半关提示，不等于直接销毁连接。

## EventLoop：一个owner，两个执行入口

文件：`services/gateway/include/net/EventLoop.h`、`services/gateway/src/net/EventLoop.cpp`。

构造记线程ID，建eventfd NONBLOCK/CLOEXEC，注册wakeup Channel，创建TimerQueue。loop仅owner：poll(-1)→逐一dispatch→clear→pending functors。entry先exchange null，在removeChannel时将本轮对应指针替换为null；vector形状不变，callback移除其他已ready Channel也不会解引用它。

runInLoop在owner立即调用，外线程转queueInLoop；queueInLoop任何线程都加mutex入队。doPendingFunctors在锁内swap出批次、锁外执行，所以任务可继续投递，不会持队列锁执行用户函数。空→非空时必须唤醒，包括owner在loop前或callback中投递；otherwise poll可能无期限睡眠。

wakeup写eventfd uint64，EINTR重试、EAGAIN说明已有可读唤醒；handleWakeup读计数，EINTR重试/EAGAIN返回。eventfd是通知，实际任务在mutex队列，不用通知次数决定执行多少任务。

quit atomic gate；外线程quit唤醒，owner会在下一轮条件退出。通用functor队列目前无界，只有peak统计，没有背压或任务公平配额；任意callback异常可逃出loop，上层EventLoopThread捕获，应用直接loop则没有这层包装。不是所有异常都能继续服务。

add/update/remove Channel、runAfter/runEvery/cancelTimer/pendingTimerCount都owner检查；member逆序析构要求activeChannels活得比TimerQueue久，因为后者析构removeChannel会访问本轮缓存。

## Acceptor：LT监听、资源错误退避

文件：`services/gateway/include/net/Acceptor.h`、`services/gateway/src/net/Accptor.cpp`（实际文件名拼写就是Accptor）。

构造NONBLOCK listener、备用/dev/null FD、reuseaddr、bind/listen、EPOLLIN注册；默认LT。handleRead循环accept到nullopt，成功move Socket给callback；资源错EMFILE/ENFILE/ENOBUFS/ENOMEM禁读→关备用FD→accept4丢弃一个peer→重建备用FD→100ms retry。禁兴趣让LT backlog不持续唤醒忙循环。

retryListening若备用FD还失败再安排retry，成功恢复读兴趣；LT会再次报告现有accept queue，无需新连接触发。scheduleRetry失败stop listener后重抛。stop幂等、cancel retry、先DEL再close；析构stop。Channel这里未绑定通用m_update自动更新，资源流程显式loop.updateChannel。

## EventLoopThread：发布栈内EventLoop不是发布所有权

文件：`services/gateway/include/net/EventLoopThread.h`、`services/gateway/src/net/EventLoopThread.cpp`。

threadFunc在worker栈建loop，用mutex发布m_loop并设置m_startComplete，notify cv；正常运行或异常退出都先清m_loop，再销毁栈对象。startLoop等的是startComplete，不是仅“指针非空”，否则构造失败/快速退出会永等或返回悬空指针。

requestStop在publication mutex下等待start完成，再调用仍活的loop.quit；stopMutex串行join，但不能拿publication mutex等待join，否则worker无法清指针。stop先检查不能worker自join，join后显式重抛存储异常；析构在worker上terminate，不能detach后让thread继续访问this。它不是可restart线程：旧thread已退出时startLoop报已退出。

## EventLoopThreadPool：网络worker与普通ThreadPool不同

文件：`services/gateway/include/net/EventLoopThreadPool.h`、`services/gateway/src/net/EventLoopThreadPool.cpp`。

构造/查询/start/stop/析构要求base线程。start临时vector建全部workers，失败先停/join成功者再抛，成功才发布m_threads/m_started。getNextLoop round-robin；0 workers返回base；每次通过thread.startLoop验证publication，不从缓存指针盲拿已退出loop。

stopWorkers先requestStop所有，再逐个stop/join，保留firstError仍完成其他清理。显式stop重抛，析构只清理，不把worker业务异常从noexcept析构抛出。pool.stop后可重建新worker；这不意味着单个EventLoopThread可以restart。线程池不负责连接表，TcpServer worker再包装其owner registry。

## Timer与TimerQueue：一个timerfd托住多连接期限

文件：`services/gateway/include/net/Timer.h`、`services/gateway/include/net/TimerQueue.h`、`services/gateway/src/net/TimerQueue.cpp`。

Timer仅值对象：id/deadline/interval/callback，run调用，repeat判断interval非0，restart(now)为now+interval，是回调之后相对重排，不是固定墙钟节拍。

TimerQueue owning CLOCK_MONOTONIC timerfd+Channel；堆按(expiration,id)排序，indices map使任意cancel可定位下标。siftUp/Down和swapEntries必须同步维护索引；removeAt移末项、修堆，平均定位O(1)+调整O(log n)，不是每次线性扫所有timer。

addAfter/addEvery要求严格正delay/interval与有效callback；newEarliest才arm。cancel最早任务时重设最早期限；若当前任务已从堆移出，id==runningId用cancelRunning禁止重复重插。cancel不是移除正在执行C++栈，也不能抢占callback。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/TimerQueue.cpp 281-305 -->
原文：`services/gateway/src/net/TimerQueue.cpp:L281–L305`；[完整文件](source/services/gateway/src/net/TimerQueue.cpp.md)。

~~~cpp
    while (!m_heap.empty() &&
           m_heap.front().expiration() <= Timer::Clock::now())
    {
        Timer timer = removeAt(0);
        m_runningId = timer.id();
        m_cancelRunning = false;
        try
        {
            timer.run();
        }
        catch (...)
        {
            m_runningId = 0;
            arm();
            throw;
        }
        const bool repeat = timer.repeat() && !m_cancelRunning;
        m_runningId = 0;
        if (repeat)
        {
            timer.restart(Timer::Clock::now());
            insert(std::move(timer));
        }
    }
    arm();
~~~

到期先从堆删除，设置runningId，运行用户函数；异常时清runningId/重新arm并重抛；重复且未取消才按callback后now插回。callback可修改queue，下一轮仍从最新heap取。arm将已到期剩余钳到至少1ns，空heap设0解除timerfd；read计数只是唤醒，真正到期任务按steady时间判断。

**源码边界：** addAfter拒绝0，而native ConnectionScheduler::at对过期deadline算max(0,remaining)。若以后实现登录期限，要先处理已经过期的情况；不要假设scheduleAt(past)在两种scheduler都自动立即成功。该边界在05章解释，属于代码分析观察，本任务不改行为。

## 本章回到测试

SocketTest、ChannelTest、EpollPollerTest、EventLoopTest、EventLoopThreadTest、EventLoopThreadPoolTest、TimerQueueTest覆盖资源/事件/发布/取消；所有测试原文在07章索引。EpollPollerPerformanceTest只是性能target。重入、ET恢复及实际半关闭还要看04章连接实现，不能仅靠Reactor类假设业务已正确处理。
