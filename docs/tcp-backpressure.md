# TCP 输出水位与慢客户端策略

## 目标与边界

`TcpConnection` 负责统计待发送字节、检测水位跨越、执行硬上限与持续拥塞超时；它通过回调通知上层，由上层决定暂停哪个数据生产者。连接本身提供 `pauseReading()` 和 `resumeReading()` 供需要限制本连接输入的业务使用。当前 echo 服务在 `TcpServer` 中注册这两个动作，其他业务可以改为暂停上游流、任务生产者或订阅推送。

`send()`、水位回调和连接回调要求在所属 EventLoop 线程运行。跨线程调用者可用 `EventLoop::queueInLoop` 或 `runInLoop` 将操作投递到所属线程。

## 修改位置与原因

| 位置 | 修改 | 原因 |
| --- | --- | --- |
| `services/gateway/include/net/TcpConnection.h`、`services/gateway/src/net/TcpConnection.cpp` | 增加高、低水位回调注册接口与只读 `pendingOutputBytes()`；高水位达到 512 KiB、低水位降到 256 KiB 时各通知一次。 | 把水位检测与具体的暂停对象分开，并让调用方观测待发送队列是否有界。 |
| `services/gateway/src/net/TcpConnection.cpp` | 将 `pauseReading()`、`resumeReading()` 设为显式操作。恢复 `EPOLLIN` 后主动排空已有 socket 数据。 | ET 模式下恢复读兴趣不应依赖下一个边沿；同时允许业务自行决定是否限制连接读取。 |
| `services/gateway/src/net/TcpServer.cpp` | echo 连接注册高水位暂停本连接读取、低水位恢复读取的回调。 | echo 响应只依赖同一连接的请求输入，因此限制该输入源是合适的业务策略。 |
| `services/gateway/src/net/TcpConnection.cpp`、`services/gateway/src/net/TcpServer.cpp` | 保留 1 MiB 输出硬上限和持续高水位 5 秒断开；服务器通过 `EventLoop::runEvery` 每秒巡检；该任务和连接 idle 定时器共享一个 `timerfd` 与最小堆。 | 即使业务不注册暂停回调或客户端一直不可写，也能限制内存与占用时间。 |
| `tests/gateway/net/ConnectionLifecycleTest.cpp` | 验证回调可选择暂停恢复，也可只观察水位而不改变 `EPOLLIN`。 | 锁定传输层不内置 echo 读策略的接口契约。 |
| `tests/gateway/net/TcpServerIntegrationTest.py` | 继续验证千连接、超限和真实慢读客户端断开。 | 确认 echo 服务装配回调后，原有端到端行为仍成立。 |

## 运行路径和不变量

1. `send()` 的直接非阻塞写入遇到 `EAGAIN` 后，把剩余字节放入输出队列。队列首次达到 512 KiB 时记录时间并调用高水位回调。
2. echo 服务的高水位回调调用 `pauseReading()`，移除本连接的 `EPOLLIN`。不注册回调时，水位跨越本身不会修改读兴趣。
3. `handleWrite()` 排空队列到 256 KiB 或以下时调用低水位回调，并清除持续高水位计时。echo 服务的低水位回调调用 `resumeReading()`；它重新启用 `EPOLLIN` 并立即读取一次，防止 ET 下已有字节停住。
4. 若队列需要超过 1 MiB，立即关闭连接。若连续 5 秒未降到低水位，服务器定时器关闭连接。这两条保护规则不依赖水位回调。
5. 一次高水位阶段只发出一次高水位通知；下降到低水位后才允许下一次高水位通知。关闭中的连接不再恢复读取。

消息回调按每次读取到的字节块调用，单次最多 8192 字节。TCP 是字节流，业务协议必须自行处理拆包和粘包。

## 字节完整性与内存上界验收

- `TcpServerPreservesRandomByteStream` 生成固定种子的长度前缀逻辑记录，以随机发送块跨越记录边界，并用不同的随机接收块逐字节比对回显；同时检查没有额外字节、提前 EOF 或发送失败。这验证 TCP 字节流完整性，不表示服务端解析了记录边界。
- `TcpConnectionTest.SlowPeerPendingOutputIsBounded` 使用不读响应的非阻塞 socket 连续写入，每次断言 `pendingOutputBytes() <= 1 MiB`，并确认超过硬上限时关闭、关闭后不能继续排队。该上界针对用户态待发送队列；内核 TCP 缓冲区和进程其他内存另计。
- 原有真实慢客户端集成测试验证 5 秒持续高水位后断开并回收 FD。

## 验证

- 普通构建与 125 项非性能测试通过；其中 TCP 连接测试覆盖水位策略、慢客户端与 idle 超时，以及随机跨记录边界的字节流完整性。
- ASan/UBSan 下 TimerQueue、EventLoop、TcpConnection 的 17 项定向单元测试和慢客户端、idle 超时的 2 项 TCP 集成测试通过。

512 KiB、256 KiB、1 MiB 和 5 秒是当前初始策略，尚未按真实业务流量和内存预算调优。

## 当前连接 idle timeout

`TcpServer` 默认在连续 60 秒没有成功读取或写入时关闭连接；构造函数可配置时长。每条连接在定时堆中有一个预约项，但不单独占用 `timerfd`。实际读写只更新 `TcpConnection::m_lastActivity`；预约到期时若尚未空闲满阈值，按剩余时间重新预约。关闭回调取消预约，避免持有已删除连接的回调运行。真实 TCP 集成测试 `TcpServerClosesIdleConnection` 使用 1 秒配置验证活动续期、静默断连与 FD 回收。
