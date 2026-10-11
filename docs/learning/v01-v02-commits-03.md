# v0.1 → v0.2：逐提交设计讲解（三）

范围：24–28。入口：[导航](v01-v02-index.md)，前一章：[Runtime与请求路由](v01-v02-commits-02.md)。这一组包含注释、测试工具、修复测试客户端和证据提交；不把每一个提交都说成生产架构变更。

## 24 · d5cf131 · 2026-10-10 · 英文源码注释：给已有机制命名

[该提交代码对照](code-analysis/09-commit-code.md#commit-24).

原题：docs: add English source comments。

**原因与差异。** 给CMake、gateway/shared头文件及实现、工具脚本加file/brief/member注释，便于顺着owner、socket、queue、runtime和handler导航。核心机制来自之前提交；该提交没有新增send queue、认证或业务route。

**技术与架构作用。** Doxygen式注释描述文件、类、生命周期和成员用途，提高阅读定位能力。它是“解释接口”的工作，不是改变接口。Stores the…一类注释只能告诉变量叫什么，仍需实现/不变量解释为什么存在；这正是本轮逐提交讲解补足的部分。

**线程与取舍。** 未新增锁、原子、线程移交或寿命关系。注释可能滞后，遇到冲突以源码及测试为准；例如connected在不同transport中的阶段语义，不能靠统一英文短句自行推断。

**扩展与证据。** 用git show d5cf131检查添加内容；从ConnectionRuntime.h/AsioConnect.h/MessageRouter.h跳到对应.cpp核对具体机制。没有新增测试面，本次不为纯注释编造性能或安全结论。

## 25 · 8217f36 · 2026-10-11 · Chaos工具：验收也要有明确契约

[该提交代码对照](code-analysis/09-commit-code.md#commit-25).

原题：Add seeded TCP and WebSocket chaos runner with sanitizer telemetry。

**原因。** 原短程集成与raw churn没有同时混合Envelope、WS控制协议、异常连接和慢读的完整30分钟证据。新增独立显式长程工具，不改变生产I/O。

**机制。** ChaosRun.py启动epoll/Asio/WS×Envelope/Packet echo六宿主；每TCP四worker，WS base owner。随机打乱健康、半包RST/截断FIN、magic/长度/版本/type、protobuf/深度、burst、慢读与重连；WS另测Origin/text/message界。seed固定选择/数据，调度仍非确定。每host显式64接受连接峰值、逐轮等待FD回基线、外部/proc VmRSS/VmHWM和约1Hz CSV采样、全程成功RTT的0.1ms固定直方图。

**并发与证据寿命。** Python线程池驱动独立peer，指标Counter/snapshot有锁；日志reader持续排空避免管道堵住server，每host轮转3×5MiB，诊断与原子计数独立保留。结束发SIGUSR1走宿主既有正常stop，读完退出日志；强杀/提前退出不能算leak pass。C++仅加test profile与八原因汇总。

**取舍。** RTT含Python、TCP/WS、插桩及六进程竞争；不是server内部P99或生产容量。0 leak契约是受测FD回收与LSan正常退出无诊断，不是RSS必须回初始。慢读必须实际命中SlowClient，不能只声称“客户端睡过”。

**证据。** tests/gateway/net/ChaosRun.py、ChaosTestProfile.h和三个TestHost；脚本中正常/失败判断及cleanup路径。验收笔记: ArenaServer/changes/2026-10-11-v02-chaos-acceptance.md (Obsidian)、工具说明: ArenaServer/tool/chaos-run.md (Obsidian)。

## 26 · 51aaeff · 2026-10-11 · 修复WS测试客户端的边界误解

[该提交代码对照](code-analysis/09-commit-code.md#commit-26).

原题：Reassemble WebSocket continuation frames in the chaos client。

**原因与定位。** 初次正式运行约56秒出现WS echo mismatch，不能直接认定server坏。最小真实探针发现4096-byte payload对应4111-byte Packet，Beast合法切成binary 4096＋continuation 15；拼接完全正确。旧helper只读一个frame，却当成完整message。

**机制。** ChaosRun.read_ws_message重组opcode2与后续opcode0到FIN，处理ping/pong并检查RSV/masking/总量1MiB。新增ChaosRunFramingTest.py用真实Beast server核对4KiB/64KiB输出，CTest只注册这项短程回归，30分钟仍opt-in。

**寿命与取舍。** 重组状态由客户端actor局部拥有，不改变server socket、strand、backpressure或状态机。无需关闭Beast自动fragmentation来配合错误客户端。WS frame、WS message、Arena Packet三个边界各自有职责，message内部仍交给流式PacketCodec。

**验证与后续。** 修复后短程预检通过，重新从零启动完整窗口，失败尝试不拼接计时。真实网络回归覆盖当次缺陷，不能借一个用例宣称所有WS控制序列都fuzz通过。

**证据。** ChaosRun.py::read_ws_message、ChaosRunFramingTest.py、tests/gateway/net/CMakeLists.txt；失败和修复记录: ArenaServer/changes/2026-10-11-v02-chaos-acceptance.md (Obsidian)。

## 27 · 5705fcb · 2026-10-11 · 威胁清单：写清信任边界和开放项

[该提交代码对照](code-analysis/09-commit-code.md#commit-27).

原题：Document gateway v0.2 threats and chaos acceptance boundaries。

**原因。** “长时间没crash”不能代替完整安全设计。将可信peer、客户端Origin/request id/时间、Packet/protobuf/route等边界逐层说明，把已有机制、当次测试和未完成项分开。

**文档机制。** docs/security/threats-v0.2.md列14项：帧校验、解析放大、速率、慢读、关闭寿命、WS规则、认证、慢输入、共享总资源、请求错配、日志、兼容、FD耗尽、TLS。每项锚定真实代码/验证范围，不把测试认证器当生产认证。

**架构与取舍。** 信任边界从TCP/HTTP/WS到framing到protobuf再到route；Origin不是身份，魔数不是安全签名，pending限制不是全进程内存预算。开放真实认证、登录/解析期限、共享IP/全局预算、TLS及日志策略。源码存在和历史测试不自动算本轮实测。

**状态/并发。** 本提交无新的运行状态、所有权或同步，仅为已实现架构及风险建立可审查描述；不能把风险清单当已实现防护。

**证据与扩展。** 仅新建上述威胁文档，下一提交补最终报告；新业务加入时回看T07/T10/T11，资源策略回看T03/T08/T09。笔记: ArenaServer/changes/2026-10-11-v02-chaos-acceptance.md (Obsidian)。

## 28 · b6dd283 · 2026-10-11 · 归档结果，v0.2成为可定位版本

[该提交代码对照](code-analysis/09-commit-code.md#commit-28).

原题：Record v0.2 thirty-minute chaos acceptance and raw metrics。

**原因。** 验收需要可核对数值和受测版本，不能只留下聊天“测试通过”。提交Markdown报告、完整统计JSON、9990行采样CSV及环境/二进制SHA256 manifest。

**实际证据。** 最终monotonic工作窗口1802.626秒、231轮；六进程正常退出0，ASan/UBSan/LSan诊断0，六宿主各231次FD回基线。健康响应33284、重连5544、峰值384、服务端拒绝事件11088；P99与RSS逐host列出。随后18/18真实网络短程回归独立通过。受测源码51aaeff；后两提交仅文档，生产路径不变。

**取舍与边界。** epoll/WS Envelope尾段RSS中位增长51.24/47.52MiB，不能说长期RSS稳定；ASan quarantine是可能来源，未另测证明。WSL ISO墙钟差1706秒与monotonic相差96.626秒，原因未定位，持续时间按monotonic。报告保留满FD/UBSan专项、TSan、真实认证、外网等未覆盖项。计数是事件，不是唯一拒绝连接；WS403/1003/1009另列。

**版本与架构。** 本地annotated v0.2指向本提交，tag是Git引用创建，不是第29个commit；本轮学习文档提交在tag之后，不移动它。CSV保留原始CRLF和SHA256。文档不变更线程/资源状态；重现同源码仍不保证相同调度和P99。

**证据。** docs/reports/v0.2-chaos-20261011.{md,json}、*-metrics.csv、*-manifest.json；验收笔记: ArenaServer/changes/2026-10-11-v02-chaos-acceptance.md (Obsidian)。查看完整命令和数据应读报告，而不是凭本章概括做新性能承诺。
