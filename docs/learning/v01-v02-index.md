# ArenaServer：v0.1 → v0.2 设计学习导航

日期：2026-10-11（Asia/Shanghai）。目的：把每个提交的变化、为什么需要它、采用的技术、线程/所有权和扩展位置串起来，帮助自己继续开发。

## 范围与如何读

- 基线v0.1：7fcf8b516dd586dc28e23c0adc9ca0a377aa1afc。
- 终点v0.2：b6dd2835a050f3b118e2739ff65f992ed44bca5e。
- `v0.1..v0.2` 共28个提交，包含v0.2，不包含v0.1本身。单独说明v0.1已有机制，不算其前的提交。
- 每个提交有独立编号/标题/原题及设计理由、机制、并发寿命、取舍/后续、源码与验证面；纯注释/整理/报告明确标记无运行行为变化。
- 设计理由从历史diff、契约和原Change Note还原，不把推测写成作者口述。逐提交章节讲“当时”；当前导读讲“v0.2”。
- 文档任务没有重跑历史测试。历史测试结果看对应Change Note；运行机制通过tag源码核对。已有chaos不等于每次commit都在同一条件下测过。

**推荐阅读顺序：** 先读[当前架构与扩展入口](v02-current-design.md)的职责表/调用链；再按下表查最不理解的提交；最后从第一章到第三章完整读一遍，沿每节源码锚点看实现。

正文存入Obsidian Vault/ArenaServer/architecture；仓库docs/learning保留可Git审查的同步副本。副本中Change/Knowledge/Tool Note用vault路径引用，避免伪造仓库中不存在的相对链接。Obsidian正文使用实际相对链接。commit-manifest.json保留全SHA、日期、subject和所有改动文件，供查漏与定位。

## 完整提交索引：28/28

| 序号 | SHA | 改变了什么 | 阅读 |
| --- | --- | --- | --- |
| 01 | 698ca5c | 连接使用契约、半关闭与逻辑关闭 | [第一章](v01-v02-commits-01.md) |
| 02 | babcc33 | 服务器管理契约与停止完成屏障 | 第一章 |
| 03 | f6749f3 | 第二种I/O Adapter、FD移交和Buffer寿命 | 第一章 |
| 04 | 146518c | 字节流→完整Packet，跨transport复用codec | 第一章 |
| 05 | a9eb1da | 互斥生命周期与正交读阶段 | 第一章 |
| 06 | c01f0aa | Asio并发send入口、strand、取消/close once | 第一章 |
| 07 | c91409a | 原生TCP入口队列、owner独占Buffer | 第一章 |
| 08 | 84e2ef8 | Envelope、字段兼容和解析资源预算 | 第一章 |
| 09 | fd13a53 | magic及破坏性外层wire升级 | 第一章 |
| 10 | 992a42b | 第一版完整包函数stage列表 | 第一章 |
| 11 | 3942122 | WS封装、预登录Ping、Origin与队列界 | 第一章 |
| 12 | fc55741 | unique_ptr PacketHandler责任链 | [第二章](v01-v02-commits-02.md) |
| 13 | b40ed33 | WS阶段状态机与connected语义 | 第二章 |
| 14 | 9fa7fda | Context/Session，metadata不等于全局表 | 第二章 |
| 15 | 3022736 | Runtime/Feature/Scheduler核心，尚未接server | 第二章 |
| 16 | 8bf665c | 三transport接线，共享背压与重入控制 | 第二章 |
| 17 | 841acb0 | 完整包后限流/白名单，尚未main启用 | 第二章 |
| 18 | d1bdc4a | Session鉴权门和八固定拒绝reason | 第二章 |
| 19 | 0cfc94c | main装配handshake策略与跨传输验证 | 第二章 |
| 20 | 6a1362b | MsgId注册、Typed Error、ID/期限/generation | 第二章 |
| 21 | be7d216 | 默认链切到Router，协议表与真实网络验证 | 第二章 |
| 22 | 68906b1 | 换行/无效宿主代码整理，无新业务路径 | 第二章 |
| 23 | 7ff856d | 出站payload扣metadata预算 | 第二章 |
| 24 | d5cf131 | 英文注释，解释已有代码 | [第三章](v01-v02-commits-03.md) |
| 25 | 8217f36 | seeded chaos负载与外部资源采样 | 第三章 |
| 26 | 51aaeff | 客户端WS continuation重组回归 | 第三章 |
| 27 | 5705fcb | 威胁边界和开放项文档 | 第三章 |
| 28 | b6dd283 | 完整验收归档，v0.2 tag终点 | 第三章 |

## 从v0.1到v0.2的五次职责变化

1. **已有原生Reactor → 可替换transport。** v0.1已有EventLoop/epoll、eventfd任务、timerfd、worker、TCP半关闭、水位及输出界。01/02建立连接与管理接口，03增加真实Asio实现。接口里线程和寿命是契约的一部分。
2. **raw bytes → 有边界的应用协议。** 04分帧，08引入protobuf业务模型，09识别外层协议；10/12把业务过滤从echo中拆出，11让WS复用同一应用协议。
3. **策略散落transport → 统一连接协调。** 14提供metadata/Session，15造宿主，16接真实路径，17加包事件策略。只有16之后Runtime才是三server的实际路径。
4. **链式单消息匹配 → 请求生命周期模块。** 18/19先有门禁/计数并启用策略；20/21注册表与Router统一业务准入、完成/超时和日志。默认链现在decoder→Router，旧gate/Ping链保留用于自定义。
5. **实现存在 → 验收范围有证据。** 23修组合预算，25–28建设chaos、修客户端并归档风险/数值；测试提交不自动改变生产架构。24/22分别是注释/整理。

## 常见理解偏差：去哪里校正

| 困惑 | 正确区分 | 对应节 |
| --- | --- | --- |
| “一个status应该够” | 生命周期互斥，读阶段/暂停/活动写正交 | 05、13 |
| “shared_ptr就是线程安全” | 生命周期共享≠状态访问/最终析构线程许可 | 01、03、14–16 |
| “原子写位置就能并发Buffer” | 还要处理扩容/发布/关闭；当前入口mutex+owner | 06、07 |
| “有Feature就已经开启规则” | 模块实现、server接线、main配置是三步 | 15–19 |
| “Chain与Feature是同一套” | 前者Packet语义，后者连接事件策略 | 12、15–17 |
| “白名单就是鉴权” | outer type粗过滤，route才根据Session鉴权 | 17–21 |
| “Context应保存IP计数map” | metadata是每连接，共享准入跨连接 | 14及当前导读6D |
| “protobuf兼容说明旧客户端不用改” | 09外层magic破坏无magic旧wire | 08、09 |
| “请求timeout等于登录timeout” | 前者已进入route，后者要覆盖零输入连接 | 20/21及当前导读6C |
| “register一个MsgId就能回任意消息” | codec/oneof/response验证仍需扩展 | 20/21及当前导读6A |
| “chaos没有crash就证明生产安全” | 插桩检测/FD证据与未覆盖风险分开 | 25–28 |

## 代码证据怎样自己复查

在WSL项目根目录用只读Git命令：

~~~bash
git log --reverse --format='%h %ad %s' --date=short v0.1..v0.2
git rev-list --count v0.1..v0.2
git show c91409a -- services/gateway/src/net/TcpConnection.cpp
git show 992a42b:services/gateway/include/net/PacketPipeline.h
git show v0.2:services/gateway/src/net/PreLoginHandshake.cpp
git diff 3022736^ 3022736 -- services/gateway/include/net/ConnectionRuntime.h
~~~

先看同commit parent diff，再看该版本完整实现，最后看v0.2；直接看HEAD会把后续Feature/Router/注释混进早期解释。无需checkout旧版本，也不用破坏当前工作区。表中的日期与Change Note命名日期可能不同，以Git提交记录为准。

## 与已有知识库连接

- owner与多reactor: ArenaServer/knowledge/network/multi-reactor-connection-ownership.md (Obsidian)
- Asio Buffer完成寿命: ArenaServer/knowledge/network/asio-buffer-lifetime.md (Obsidian)
- Runtime与Feature: ArenaServer/knowledge/network/connection-runtime-features.md (Obsidian)
- Context与Session: ArenaServer/knowledge/network/connection-context-and-owner-session.md (Obsidian)
- 完整包处理链: ArenaServer/knowledge/network/packet-pipeline-after-framing.md (Obsidian)
- 令牌桶: ArenaServer/knowledge/network/packet-token-bucket.md (Obsidian)
- protobuf演进与界: ArenaServer/knowledge/network/protobuf-schema-evolution-and-bounds.md (Obsidian)
- request id与deadline: ArenaServer/knowledge/network/request-dispatch-and-deadlines.md (Obsidian)
- WS应用协议: ArenaServer/knowledge/network/websocket-packet-transport.md (Obsidian)
- chaos观测原则: ArenaServer/knowledge/network/churn-soak-observability.md (Obsidian)

不重复新造相同Knowledge Note。既有architecture/gateway-runtime.md继续作为源码机制长篇说明；本组文档提供版本顺序和扩展位置。本次入库/核对记录见changes/2026-10-11-v01-v02-design-learning.md。
