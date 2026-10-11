# 08 观测脚本与 dashboard：数据怎样产生、能证明什么

固定分析版本：v0.2（b6dd2835a050f3b118e2739ff65f992ed44bca5e）。本章是源码分析，没有重新执行 GDB、Valgrind、strace 或负载。历史测量与本章静态结论分开。

## 文件和阅读路径

| 文件 | 职责 | 完整源码 |
| --- | --- | --- |
| tools/gateway_inspect/capture.py | 四阶段宿主启动、短 echo 负载、输出解析、校验、静态 HTML 生成 | [原文](source/tools/gateway_inspect/capture.py.md) |
| tools/gateway_inspect/capture_gdb.py | GDB 进程内的字段读取、对象图、线程栈、maps/fd 快照 | [原文](source/tools/gateway_inspect/capture_gdb.py.md) |
| tools/gateway_inspect/dashboard.html | 地址/对象/调用/分配/系统调用的离线浏览 | [原文](source/tools/gateway_inspect/dashboard.html.md) |
| tests/gateway/net/TcpServerChurnMonitor.py | raw TCP churn 驱动、/proc 采样、HTTP 实时状态和报告 | [原文](source/tests/gateway/net/TcpServerChurnMonitor.py.md) |
| tests/gateway/net/TcpServerChurnDashboard.html | 实时图表、状态、窗口延迟、线程和 epoll 展示 | [原文](source/tests/gateway/net/TcpServerChurnDashboard.html.md) |

后两文件也属于[测试章](07-tests.md)：那里解释验收范围，这里解释指标、并发和 UI。六宿主 TCP/WS 的畸形包 chaos 驱动是另一套 [ChaosRun.py 分析](07-tests.md)，不能用 raw churn 代替它。

## 1. capture.py：四个独立进程实验的编排

### 1.1 负载与进程生命周期

调用链：

~~~text
main → capture
  → GDB + capture_gdb.py → 内部 --workload 客户端 → snapshot
  → run_profile(Massif) → workload → parse_massif
  → run_profile(Callgrind) → workload → parse_callgrind
  → run_profile(strace) → workload → parse_strace
  → validate → write_report / data.json / index.html
~~~

`dump` 是 JSON 写文件辅助；`port_number` 用临时 socket 绑定端口0取可用端口，再关闭它。关闭与宿主 bind 之间仍有端口竞争窗口，取号不是持续占有。

`workload` 打开 N 个连接，ConnectionRefused 在40秒期限内重试，随后等待0.5秒让 accept 分发。每个 peer 发送 bytes(range(256))*64，即16KiB；8轮逐连接收足、逐字节比较，再以 SHUT_WR 发 FIN 并要求 EOF。成功字节量是 N×16KiB×8。异常记录失败；finally 关闭所有客户端并向这个实验宿主发 SIGTERM。因此函数不仅读取指标，还创建流量、触发停止。

`target_pid` 只为 strace 寻找其第一个子进程；Valgrind 使用自身 PID 对应的受测进程。10秒找不到或 profiler 提前退出即失败。它依赖此处“一个 profiler 启动一个宿主”的进程结构，不是通用进程树发现器。

`run_profile` 以新 session 启动 profiler，将输出写阶段日志；运行相同负载后要求45秒内退出且结果 passed。finally 先终止宿主，3秒未结束才杀整个实验进程组。该清理规则只适用于它刚创建的测试进程。

### 1.2 capture 的契约和失败路径

要求普通 Debug 测试宿主、gdb/valgrind/strace、全新输出目录、4..128条连接，worker 固定为4。路径含 build-asan/build-tsan 会被拒绝；这是路径约定检查，不能识别重命名后的插桩二进制。

GDB batch 保留 ASLR（disable-randomization off），加载脚本后等待100秒。脚本保存 PID 和 /proc/stat start_ticks；超时和残留进程清理比较两者，降低 PID 重用后杀错进程的风险。GDB、snapshot、completion、客户端结果缺一或非成功即停止，不会把后续几种 profile 的成功掩盖为整体成功。

每阶段独立 PID 和 ASLR：不能把 Massif 的分配归属、strace 的 syscall 地址直接覆盖到 GDB 快照的绝对地址上。普通 Debug + 插桩实验也不能给出无插桩生产 P99。

输出解析与最终验证的 Interface：

| 函数 | 输入/输出机制 | 解释边界 |
| --- | --- | --- |
| parse_massif | snapshot 标记划分记录，读取 time/heap/extra/stacks，nK 行组成带深度和子节点数的分配树；取最大堆峰与带树的最大 detailed 峰 | 后者可能不是总峰对应的时刻；堆请求量、额外量、估计栈不是全部 RSS 或内核 socket 内存 |
| parse_callgrind/name | fn/cfn 压缩编号查表，calls 累计 caller→callee；只保留端点包含 service:: 的边 | 是动态调用次数，不是函数耗时；跨线程汇总，名称前缀过滤也不是全仓全部函数 |
| parse_strace | 每 TID 文件解析 -ff/-ttt/-T 格式，合并 unfinished/resumed，累计完成数、-1/errno、耗时和线程次数 | 未完成项不算完成，原文件仍保留；事件文本截断700字符；EAGAIN 常是正常非阻塞路径；跨线程耗时可重叠 |
| validate/mapped | 要求5线程/4worker，节点地址和 SP 在 maps，连接计数等于负载数，调用边/系统调用/分配树非空 | 校验自洽性，不证明对象图完整、并发无竞态、所有权释放无误 |
| write_report | 写实验范围、PID、字节量、RSS、调用/分配概况和原始证据路径 | 文本硬写“存在未提交工作树改动”，即使工作树干净也会出现；不能用这一句断定状态 |
| capture 尾段 | 记录 binary SHA256、HEAD、git diff HEAD 的 SHA256；JSON 替换 HTML 占位符 | diff hash 仅覆盖 tracked diff，不纳入 untracked 文件；完整输入身份还需要二进制和构建配置 |

JSON 的 `<` 转义防止输入构造 </script> 提前结束内嵌脚本；生成后的 index.html 无需 API。原 dashboard.html 仍有 __CAPTURE_DATA__ 占位符，不能当作已经注入数据的成果直接打开。

## 2. capture_gdb.py：为什么读取字段也需要维护

### 2.1 从调试符号到对象图

`address` 读对象地址；`children` 请求 pretty printer。`items` 手动遍历 libstdc++ unordered_map 的哈希节点，清理 GDB 的 struct/class 类型名前缀，再按 _Hash_node 的模板参数转换存储。`shared` 读取 _M_ptr；`vector` 读取 _M_start/_M_finish；`storage` 读取 Buffer 的 vector 地址、size/capacity 和读写索引。这些都是实现布局知识，不属于标准 C++ 容器的可移植 Interface。

`proc_snapshot` 导出 maps/smaps/status；FD 链接和 fdinfo 允许个别对象消失。smaps 中数字统一×1024适合 Rss/Size/Pss 的 kB 数据，但无单位计数字段也会被转换，不能将这些字段全解释成字节。

`take_snapshot` 先检查 N 个连接是否全部进入 worker，再读取最多40层线程栈和 SP。节点包括 server/base loop/pool/acceptor、线程 wrapper、worker loop、connection/channel/socket/buffer/storage。所有权边的 member/shared/weak/borrowed 分类是脚本按源码指定；GDB 提供地址和字段，并不会自动推导所有权。

worker 的线程归属用 loop 对象与线程 SP 是否位于同一映射判断。这里依赖 EventLoop 位于工作线程栈上的实现；比按 FD 顺序猜测强，但不能泛化到 loop 分配在堆上的结构。

### 2.2 与 v0.2 连接注册表存在静态不兼容

采集脚本仍这样解引用：

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e tools/gateway_inspect/capture_gdb.py 168-175 -->
原文：`tools/gateway_inspect/capture_gdb.py:L168–L175`；[完整文件](source/tools/gateway_inspect/capture_gdb.py.md)。

~~~python
        for cid,sptr in items(w["connections"]):
            conn=shared(sptr)
            count+=1
            c=node(conn,"connection","connection #"+str(int(cid)),
                tid=owner,connection_id=int(cid),state=str(conn["m_state"]),
                fd=int(conn["m_socket"]["fd_"]["_fd"]))
            edge(wid,c,"shared","connections")
            edge(c,lid,"borrowed","m_loop")
~~~

而本版 worker 实际保存：

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e services/gateway/src/net/TcpServer.cpp 272-281 -->
原文：`services/gateway/src/net/TcpServer.cpp:L272–L281`；[完整文件](source/services/gateway/src/net/TcpServer.cpp.md)。

~~~cpp
    struct Entry
    {
        std::shared_ptr<ConnectionRuntime> runtime;
        TcpConnection* transport; // Owned by runtime.
        TcpConnection* operator->() const noexcept
        {
            return transport;
        }
    };
    std::unordered_map<Id, Entry> connections;
~~~

Entry 没有 _M_ptr，传给 shared 会按错误类型读取字段。当前源码说明该路径不兼容；本次没有运行 GDB 复现具体错误文本。旧 HTML 成功生成只说明对应旧版本和当时二进制的采集，不能证明 v0.2 脚本继续可用。

正确的当前所有权阅读路径应是 worker Entry → shared Runtime → owned transport，Entry.transport 为借用指针，详见[三 transport 章](04-transports.md)。脚本图中直接 worker→shared connection 也已不足以表达 v0.2；Context/Feature 不在这套图里。学习资料标记此差异，不在本任务顺便改工具代码。

### 2.3 断点、停止与结束

`ReadyBreakpoint.stop` 在宿主 loop.loop() 源行取 &server，记录 PID/start_ticks，启动独立客户端，并安装 TcpConnection::send 断点。`IoBreakpoint.stop` 累计 N×3次命中才返回 true，借此等待连接已分发且有活动。Ready 断点禁用后返回 false 继续运行。

顶层 run → take_snapshot → 删除全部断点 → continue → 检查 $_exitcode 和 captured；失败保存 gdb-snapshot-error.txt。源码注释说由 inspect.py 加载，但实际加载入口是 capture.py，这只是旧文字。

“read-only”指不调用 inferior C++ 函数来取值，不表示运行时无影响。GDB all-stop 会暂停整个宿主，断点、pretty printer、符号布局、/proc 读取和观测开销均改变调度；不能从这个快照证明延迟、锁公平性或竞态不存在。

## 3. 离线 dashboard.html：每张图的真实单位

CSS/HTML 提供七个页签、侧栏、详情框、响应式布局；无外部图表库。初始化从 DATA 构造 nodes/map 索引；tab/hash 和事件委托只是切换视图，不重新采集。

| 代码组 | UI 行为 | 阅读注意 |
| --- | --- | --- |
| esc、bytes/num、contains/mapFor、category/label | 动态字符串转义；十六进制地址用 BigInt 比较，映射区间 [start,end)；SP 对应栈、无权限为 guard、[heap]/可执行/匿名/文件分组 | 64位地址不能用 JS Number 精确保存；匿名段不强称 glibc arena |
| renderMaps/selectMap 与过滤/地址查询 | 映射目录排序、概览条、选定映射的局部地址缩放、落入此段的对象 | 概览各段等宽，不按映射大小；aria 中“65段”是固定旧文字，实际段数来自 DATA |
| detail/selectNode/box/line | 展示对象字段及相邻关系；点击对象联动详情 | 嵌套成员大小可与父对象重叠，不能把每节点 sizeof 相加当进程内存 |
| ownerGraph、connectionOptions/connectionGraph | server/pool/acceptor/base/4worker 布局；按 TID 选择连接和相关成员 | 只是脚本识别的部分图，继承旧 registry 模型的问题；不是任意 C++ heap 扫描 |
| renderStack/线程选择/hideStd | 捕获的最多40帧反转成外层到内层，可隐藏部分非项目帧，显示 PC/源码位置 | 丢失帧和无符号帧不能凭 UI 补齐；优化和调试符号会影响结果 |
| callGraph/callBox/renderCalls | 以选定函数展开最多5条上/下游，表格最多250条匹配边 | 边权是 calls；截断是展示，不是剩余函数不存在 |
| heapChart/heapFlame/分配详情 | 堆、额外量、估计栈折线；按 children 数递归构建树，宽度按父子 bytes，点击看分配路径 | 父统计包含子统计，不能重复相加；火焰宽度不是 CPU 时间；不是 RSS 图 |
| 系统调用筛选、时间线/分页/原文 | 按 TID/name 筛选，显示完成数/errno/耗时，事件80条分页 | -1包括正常 EAGAIN；跨线程 total seconds 不等于墙钟时长；原文用 textContent |
| captureRecords/manifest/__inspectCheck | 展示阶段 PID、二进制/版本与原始依据；提供渲染自检入口并标记 rendered | DOM 渲染成功只验证页面有对应元素，不能验证采集内容正确 |

这些函数共同构成“固定证据浏览”的 Module，Interface 是已生成的数据结构和浏览动作。Depth 在于从同一组原始输出联动查看映射、线程和对象；它没有实时查询服务端状态的 Interface。

## 4. TcpServerChurnMonitor.py：采样器和负载同属一个进程

### 4.1 /proc 数据采集

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e tests/gateway/net/TcpServerChurnMonitor.py 38-77 -->
原文：`tests/gateway/net/TcpServerChurnMonitor.py:L38–L77`；[完整文件](source/tests/gateway/net/TcpServerChurnMonitor.py.md)。

~~~python
def ticks(path):
    fields = path.read_text().rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def proc_metrics(pid):
    root = Path(f"/proc/{pid}")
    status = {}
    for line in (root / "status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            status[key] = value.strip()
    epolls = []
    descriptors = list((root / "fd").iterdir())
    for fd in descriptors:
        try:
            if os.readlink(fd) == "anon_inode:[eventpoll]":
                data = (root / "fdinfo" / fd.name).read_text()
                epolls.append({"fd": int(fd.name), "watched": data.count("tfd:")})
        except FileNotFoundError:
            pass  # A connection can close between enumeration and readlink.
    threads = []
    for task in (root / "task").iterdir():
        try:
            threads.append({
                "tid": int(task.name),
                "ticks": ticks(task / "stat"),
                "name": (task / "comm").read_text().strip(),
            })
        except FileNotFoundError:
            pass
    kb = lambda key: int(status.get(key, "0 kB").split()[0])
    return {
        "fd": len(descriptors), "rss_mib": round(kb("VmRSS") / 1024, 2),
        "rss_peak_mib": round(kb("VmHWM") / 1024, 2),
        "ticks": ticks(root / "stat"),
        "threads": sorted(threads, key=lambda item: item["tid"]),
        "epolls": sorted(epolls, key=lambda item: item["fd"]),
    }

~~~

`ticks` 从 stat 的最后一个右括号切开，避免 comm 中空格打乱列；utime+stime 用 SC_CLK_TCK 换算 CPU 时间。`proc_metrics` 数 /proc/PID/fd、读取 VmRSS/VmHWM、枚举 task，按 eventpoll FD 的 fdinfo 中 tfd: 数统计注册量。列目录和读取并非原子快照，关闭中的 FD/TID 允许消失。

FD 数不是精确活动连接数：其中包含监听、epoll、eventfd、timerfd等。稳定基线+已知 peer 平台让差值可作为此实验的连接近似；短期竞态与采样间隔可能漏峰。epoll 注册数也不是 send queue 长度。本章脚本没有直接导出每连接 pendingOutput；该字段应通过运行时回调/测试接口核对。

`percentile` 对有界延迟 deque 排序，取 floor((n-1)*fraction)，不是 ChaosRun 的全程直方图上界算法。UI P99 属于最多100000次最近测量窗口，不能混入完整 chaos 报告的统计口径。

### 4.2 Monitor 全部方法分组

| 方法 | 实现和保证 | 失败/限制 |
| --- | --- | --- |
| __init__ | 全新输出目录；Lock、done/failure Event；CSV；80条事件、两个100000延迟窗口、sample/round 集合和状态 | rounds 保留全部；samples 最多7200，不是无限长实时历史 |
| event | 锁内追加事件、错误数、last_error，设置 failure | 事件窗口截断不清除累计错误 |
| snapshot | 同锁复制状态/sample/最近60轮/事件，计算窗口P50/P99 | 复制和排序期间占锁，读请求不是无成本 |
| write_snapshot | 写 name.tmp，再 replace name | 避免读者看半写 JSON；与 CSV 不组成跨文件事务 |
| read_logs | 独立线程读取宿主 stdout；3×5MiB轮转；READY/RST/诊断/异常分类，计数持久化 | raw churn 把连接上限、慢读等视异常；chaos 中这些可为预期拒绝，两脚本规则不同 |
| sample | /proc 差分 CPU、每TID CPU、完成连接速率；锁内更新sample/state/CSV并刷新，写live.json | 100%表示一核，进程多线程可>100%；connections_per_s 是已完成周期/秒，不是业务QPS |
| sample_loop | 约1Hz直到 done；采样失败记录 error | 1Hz峰值不是内核完整历史峰；VmHWM另为RSS历史峰 |
| start_http | ThreadingHTTPServer 绑定127.0.0.1；/提供HTML，/api/state给snapshot，其余404；no-store | 不暴露任意文件读取；多 HTTP 线程与采样/负载共享锁；服务不是外网监控部署 |
| wait_fd/check_health | 20秒等目标FD并持续检查 failure 和宿主存活 | FD数等于基线不能独自证明无堆泄漏 |
| churn_round | semaphore64控制建连；随机1..2048字节raw echo；平台等待；1/5 RST其余FIN；全任务settle；基线确认两次 | 是 raw echo，不发Envelope畸形包或WS；finally中断残余peer |
| workload | 宿主 idle60000/workers 参数；ASan/LSan/UBSan环境；READY后基线/采样；逐轮到期限；SIGUSR1停止，要求退出0且无诊断 | 失败清理可以TERM/KILL，但不能把强杀当LSan通过 |
| write_report | 连接/FIN/RST/窗口延迟/FD/RSS/诊断/环境与产物路径 | 此脚本完成时elapsed包含关闭阶段，不能原样解释成严格chaos工作窗口 |
| run | 启动HTTP，asyncio.run驱动，写报告；完成后默认继续展示3600秒，再关HTTP | dashboard仍在线不表示宿主仍活跃；宿主已经退出 |
| main | 参数解析：duration默认1800，connections默认1000，workers4，httpport0，keep3600 | 正时长、1..1000连接、非负workers/展示时长；新目录保护旧结果 |

`churn_round` 内部 open_peer/echo_peer/close_peer 是协程，settle 用 gather(return_exceptions=True) 等本批全部结束后再抛失败，避免第一个失败留下其他协程仍在修改轮次计数。echo 以137字节片段发送并 readexactly 检验 TCP 是流；close 分 FIN 与 linger{1,0}+abort 的 RST，finally兜底清理。

### 4.3 所有权、锁与期限

Monitor 拥有宿主 Popen、CSV、HTTP实例；asyncio 驱动客户端，日志/采样线程和 HTTP请求线程跨线程访问 state，由 Lock 同步。done/failure 用 Event发布停止/失败。/proc 输出不会因 Monitor 锁而变成宿主的原子状态。

成功路径在宿主正常退出后 join日志，结束采样并关闭输出；失败路径负责终止残余实验进程。计时采用 monotonic，展示 started_at 用墙钟，两者不能互相替代。该工具既制造网络事件又读取遥测，对测量有负载与 Python 调度开销。

## 5. 实时 dashboard：暂停到底暂停什么

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e tests/gateway/net/TcpServerChurnDashboard.html 105-116 -->
原文：`tests/gateway/net/TcpServerChurnDashboard.html:L105–L116`；[完整文件](source/tests/gateway/net/TcpServerChurnDashboard.html.md)。

~~~cpp
async function poll(){
if(paused||inFlight)return;inFlight=true;
try{var response=await fetch("/api/state",{cache:"no-store"});if(!response.ok)throw new Error("HTTP "+response.status);render(await response.json())}
catch(error){put("updated","监控连接中断："+error.message)}
finally{inFlight=false}
}
document.getElementById("pause").addEventListener("click",function(){
paused=!paused;this.textContent=paused?"恢复图表刷新":"暂停图表刷新";this.setAttribute("aria-pressed",String(paused));if(!paused)poll();
});
window.addEventListener("resize",function(){if(latest)render(latest)});
poll();setInterval(poll,1000);
})();
~~~

暂停只改变浏览器的 paused 变量并停止 fetch；没有 POST、没有向 Monitor 发暂停命令，也没有暂停宿主或负载。inFlight 避免轮询重叠；失败显示“监控连接中断”，不据此宣告宿主 crash。每秒 fetch no-store，resize用最新快照重绘。

其余 JS 也有明确分工：

- put/fmt/time/rows：textContent更新文字/表格，格式化数字、时长；服务器字符串不当HTML解析。
- svgElement/text/draw：原生SVG绘图、坐标/网格；采样很多时约1200点降采样并保留尾点。pointer工具提示查原始最近点，图中缺线段不等于无原始sample。
- render：显示状态、阶段、轮次、PID、FD/RSS/HWM/CPU、窗口P99、完成周期速率、FIN/RST/诊断、线程、epoll、最后10轮/14事件。进度可达上限，但 passed 来自 Monitor最终状态，不由进度条推断。
- epoll展示：按排序后的FD顺序标注base/worker，这是明确的启发式；与GDB按实际loop/SP归属不同，不能据此证明owner线程。
- labels/phases：包含 paused_by_user 等历史快照标签；当前按钮只暂停图表刷新。原始HTML需要 Monitor的HTTP/API，直接以本地文件打开不能生成实时数据。

## 6. 这组工具提供的知识和未提供的证明

| 要回答的问题 | 合适证据 | 本版限制 |
| --- | --- | --- |
| loop在哪个线程、哪些节点借用FD | 固定源码 + 同一GDB快照的SP/对象/字段 | v0.2 registry使采集脚本过期，当前先以源码为准 |
| 活跃堆来自哪些分配路径 | Massif树及原始massif.out | 与RSS、kernel TCP内存分开；短实验不证明长期无泄漏 |
| 实际动态调用关系 | Callgrind calls边 | 前缀过滤、跨线程聚合；不是源码的全路径或墙钟热点 |
| 系统调用如何等待/失败 | strace TID事件、errno、原始trace | EAGAIN正常；跟踪改变时序，不能替代性能基准 |
| churn完成了多少周期、FD是否回收 | Monitor客户端计数、两次FD基线确认、退出/诊断 | 窗口P99不是全程P99，原始FD不是所有C++资源 |
| chaos的受测稳定性 | [已归档完整报告](../../reports/v0.2-chaos-20261011.md)的工作窗口、六宿主正常退出、FD恢复、sanitizer | 不是本章重新测试；长期RSS稳定/所有竞态仍需独立证据 |

学习顺序建议：先读[Reactor](02-reactor.md)和[transport](04-transports.md)确认对象与线程模型，再读采集脚本；否则“图里有一条 shared 边”容易被误解成当前真实所有权。工具布局和生产布局之间是需要维护的内部 Seam，不可因为工具自身 validate 通过就免去版本比对。
