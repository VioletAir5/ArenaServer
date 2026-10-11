# 01 共享基础设施：文件描述符、队列与线程池

固定源码为v0.2。本文逐模块解释实际代码，完整原文见[文件索引](source-index.md)。源码附件保留全部行；本文按逻辑分组解释，不把逐行复述注释当设计分析。

## FileDescriptor：资源只允许一个所有者

文件：`shared/include/os/FileDescriptor.h`、`shared/src/os/FileDescriptor.cpp`。

这个Module只隐藏FD所有权，不隐藏Socket行为。_fd=-1是空状态；get/valid是观察，不转移所有权。删除copy避免两个对象close同一个编号，move取走编号并把来源设-1。赋值必须先关旧FD，自赋值必须提前返回：

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e shared/src/os/FileDescriptor.cpp 21-45 -->
原文：`shared/src/os/FileDescriptor.cpp:L21–L45`；[完整文件](source/shared/src/os/FileDescriptor.cpp.md)。

~~~cpp
FileDescriptor& FileDescriptor::operator=(FileDescriptor&& other) noexcept
{
    if (&other == this)
    {
        return *this;
    }
    reset();
    this->_fd = other._fd;
    other._fd = -1;
    return *this;
}

FileDescriptor::FileDescriptor(FileDescriptor&& other) noexcept
    : _fd(other._fd)
{
    other._fd = -1;
}

void FileDescriptor::reset()
{
    if (_fd >= 0)
    {
        close(_fd);
    }
    _fd = -1;
~~~

按顺序看：身份检查→reset旧资源→偷取other._fd→other清空。析构同样reset。若source移走后仍close来源，内核复用编号时会误关另一资源，故-1不是装饰。reset没有重试close；这里是Linux项目的既有实现，不把它推广成其他OS统一规则。

调用者：Socket、epoll/eventfd/timerfd与备用FD；RAII覆盖异常返回，不需要每个分支手工close。FileDescriptor本身没有锁：归属线程/互斥由外层所有者决定，不能两个线程同时移动同一对象。没有公开release，Asio FD移交另用其socket.release/AcceptedFd，而不是靠get“拿走”。

验证入口：tests/shared/os/FileDescriptorTest.cpp中的移动、自赋值、析构/FD关闭。扩展其他句柄时保留“来源失效、赋值先释放旧值”的不变量，别把copy重新打开。

## BlockingQueue：条件变量守护谓词，而非守护通知次数

文件：`shared/include/concurrent/BlockingQueue.h`（模板定义/实现都在此）。

字段：capacity不可变，queue/closed受同一mutex；not_full给producer，not_empty给consumer。构造容量0拒绝，copy/move均禁用，避免等待线程仍引用旧mutex/cv。

- push用unique_lock等待“有位置或closed”；closed后返回false；成功完美转发进deque，解锁后notify_one consumer。
- pop等待“有数据或closed”；即使closed，只要还有数据仍取出，移走front并唤醒producer；只有closed且empty才返回nullopt。
- close幂等，设置closed后同时notify_all两个方向，阻塞producer也必须被唤醒。
- size/closed/empty/full是上锁快照，不保证调用后仍如此；capacity无需锁因为不变。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e shared/include/concurrent/BlockingQueue.h 91-118 -->
原文：`shared/include/concurrent/BlockingQueue.h:L91–L118`；[完整文件](source/shared/include/concurrent/BlockingQueue.h.md)。

~~~cpp
template <typename T> std::optional<T> BlockingQueue<T>::pop()
{
    std::unique_lock<std::mutex> lock(_mtx);
    _not_empty.wait(lock, [this] { return !(this->_queue.empty()) || this->_closed; });
    std::optional<T> task;
    if (!_queue.empty())
    {
        task = std::move(_queue.front());
        _queue.pop_front();
        lock.unlock();
        _not_full.notify_one();
    }
    return task;
}

template <typename T> void BlockingQueue<T>::close()
{
    std::unique_lock<std::mutex> lock(_mtx);
    if (_closed)
    {
        return;
    }
    _closed = true;
    lock.unlock();

    _not_empty.notify_all();
    _not_full.notify_all();
}
~~~

为什么用wait(lock,predicate)：伪唤醒、多个consumer竞争、关闭与生产并发都必须重新验证queue条件；通知本身不存一张“消费票”。close不是丢弃现有任务，它是拒绝新输入并让consumer排空。因此ThreadPool shutdown可依赖pop直到nullopt退出。

接口代价：push可能永久等待空位，队列有界不意味着调用非阻塞。**不要在connection owner里无条件调用可能阻塞的push**，否则所有网络/定时器都停。队列析构不自动确保所有waiter退出/join；使用方必须先close并回收线程。

验证入口：BlockingQueueTest.cpp，性能面另在BlockingQueuePerformanceTest.cpp；性能程序和语义测试分目标，不把benchmark取代关闭/唤醒断言。

## ThreadPool：通用计算任务池，不是网络Reactor线程池

文件：`shared/include/concurrent/ThreadPool.h`、`shared/src/concurrent/ThreadPool.cpp`。

区别：它执行Task函数队列；EventLoopThreadPool执行各线程独立I/O loop。gateway的TCP worker使用后者/Asio自有worker，没有把共享ThreadPool当网络owner。

submit的模板值得从返回值反向读：

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e shared/include/concurrent/ThreadPool.h 93-115 -->
原文：`shared/include/concurrent/ThreadPool.h:L93–L115`；[完整文件](source/shared/include/concurrent/ThreadPool.h.md)。

~~~cpp
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            [func = std::forward<F>(f),
             arguments = std::make_tuple(std::forward<Args>(args)...)]() mutable
            {
                return std::apply(
                    [&func](auto&&... unpacked) -> ReturnType
                    { return std::invoke(func, std::forward<decltype(unpacked)>(unpacked)...); },
                    std::move(arguments));
            });

        std::future<ReturnType> future = task->get_future();

        if (_state.load() != State::Running)
        {
            throw std::runtime_error("thread pool is not accepting tasks");
        }

        if (!_tasks.push([task] { (*task)(); }))
        {
            throw std::runtime_error("thread pool is shutting down");
        }

        return future;
~~~

1. invoke_result推导返回类型；func/arguments按值捕获并移动tuple，std::apply/std::invoke支持不同可调用对象。
2. packaged_task把结果/异常装入future。shared_ptr是为了让它能放入可复制std::function<void()>，不是让业务数据自动线程安全。
3. state先检查Running；检查和push之间可能shutdown，queue.close使push再返回false，所以有第二次检查。
4. 返回future只证明成功接纳，不证明完成；get可阻塞并抛任务异常。

生命周期函数：

| 函数 | 实际动作 | 约束 |
| --- | --- | --- |
| constructor | 验threadCount>0，queueSize由BlockingQueue验证，reserve workers | 未启动 |
| start | stateMutex；Created中逐个建线程，成功才Running | Running/Stopped再start均抛 |
| start失败回滚 | close queue→join已建线程→Stopped→重抛 | 不留下部分运行pool |
| shutdown | stateMutex；Running先Stopped→close queue→join | Created直接Stopped，重复Stopped无事 |
| workerLoop | hooks→pop→beforeExecute→Task→afterExecute，nullopt退出→stop hook | queue关闭后排空 |
| destructor | shutdown | 调用线程必须能够join workers |

hooks为virtual noexcept；throw导致terminate，不会变成业务future错误。普通提交函数由packaged_task捕获异常；这一点不同于直接任意Task被调用。源码没有“worker内自join保护”及自动owner回投接口，所以不要在自己的任务里shutdown该pool，也不要将route completion直接在计算worker执行。若以后接异步业务，需要显式owner handoff和外部任务取消契约。

## 本章完整方法与状态覆盖

FileDescriptor：construct/get/valid/move assignment/move constructor/reset/destructor。BlockingQueue：constructor/push/pop/close/closed/size/capacity/empty/full。ThreadPool：constructor/start/shutdown/destructor/workerLoop/submit及四hook，所有均在本文或完整原文中定位。无自动连接迁移、线程恢复或重新start保证。

关联历史：这些基础大多在v0.1之前；03/06/07/15以后会消费类似所有权/入口排队原则，但并非28次提交里新造了整个shared。继续读[Reactor](02-reactor.md)。
