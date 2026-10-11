# v0.2 全代码分析

固定版本：**b6dd2835a050f3b118e2739ff65f992ed44bca5e**（v0.2）。tag之后的学习文档不修改这个版本。

这套资料把 **152个作者维护文件、26296行原文** 按实际模块和方法组解释，涵盖生产代码、测试、构建、工具及目标架构DSL；另为 **v0.1..v0.2全部28次提交** 配上当时的代码diff和机制分析。原文附件帮助定位，机制正文解释为什么这样写、线程/所有权、失败路径与扩展限制。

## 从哪里读

1. 先读[当前架构导读](../v02-current-design.md)，建立职责与调用链。
2. 按模块读下面01–08章，再从代码节选跳到完整原文。
3. 对照[28次commit代码分析](09-commit-code.md)和[逐提交设计讲解](../v01-v02-index.md)，区分“当时加入什么”和“当前实际路径”。
4. 新增功能前查测试章、当前未实现项和静态观察，不把历史测试结果当本次执行结果。

## 全部章节

| 章节 | 解释内容 |
| --- | --- |
| [01 共享基础设施](01-shared.md) | FileDescriptor、BlockingQueue、ThreadPool；资源转移、谓词等待、关闭/排空、future和worker寿命 |
| [02 Reactor](02-reactor.md) | Socket、地址、Channel、epoll、EventLoop、Acceptor、线程发布、timerfd/索引堆 |
| [03 协议与Buffer](03-protocol.md) | 每个Buffer操作、magic/长度/metadata、增量分帧、protobuf字段/未知字段/大小深度、组合预算 |
| [04 三种transport](04-transports.md) | TCP/Asio/WS状态、并发send入口、写串行、半关闭、取消、buffer寿命、registry和停止屏障 |
| [05 Runtime与路由](05-runtime-routing.md) | Context/Session、Feature事件、背压/桶/白名单、责任链、MsgId/Typed Error/request id/generation/deadline/spdlog |
| [06 构建与入口](06-build-and-entry.md) | 所有CMake/preset/editor/gitignore、main模式装配、退出；DSL明确标作目标提案 |
| [07 全部测试与宿主](07-tests.md) | 每文件的激励、断言和限制；profile/真实网络/chaos/性能工具，合成鉴权与生产分开 |
| [08 观测脚本与页面](08-observability-tools.md) | capture/GDB/Massif/Callgrind/strace、/proc、两dashboard、统计口径及布局过期问题 |
| [09 历史代码对照](09-commit-code.md) | 28次提交的实际diff片段、代码机制、当前关联章、完整diff附件 |

## 查漏与定位

- [全文件索引](source-index.md)：每个文件的章节及完整源码。
- [覆盖与核验说明](coverage.md)：范围、所有文件的解释入口及核验边界。
- [测试入口/断言索引](test-case-index.md)：43个C++/Python文件中的378个宏/函数入口，含helper；不是378个已执行用例。
- [固定源码清单](inventory.json)：全SHA、blob、行数与SHA256。
- [历史diff清单](history-inventory.json)：提交/父提交/节选行/原始diff SHA256。
- [核验结果](verification.json)：源码、节选、历史、入口定位和双份链接的静态检查记录。

覆盖范围是固定tag的全部作者维护C++/头、Python、proto、HTML、CMake/构建/编辑设置及目标设计DSL。生成protobuf/第三方/build代码、渲染PNG/PUML/SVG、测量CSV/JSON和纯说明文档不当作待解释代码。它们需要时作为协议、目标架构或历史证据引用。

说明按Module和相关方法组组织，头与实现一起读；没有在每行原文后重复注释。全部源码保留不等于全部路径已测试，也不自动保证解释正确。静态检查能确认定位和原文一致；机制解释仍依据实际函数、调用方、测试和线程/寿命审查。

## 阅读时保留的实际限制

- 真实认证、零输入登录期限、共享IP/总资源准入、TLS仍是开放工作。
- 原生scheduler将past deadline变成runAfter(0)，与TimerQueue要求正delay存在差异；本任务只记录静态观察。
- GDB脚本仍假设worker直接持shared connection，与v0.2的Entry{Runtime, borrowed transport}不兼容；旧图不是当前可用性证明。
- 内外request id未要求相等，inner uint64不自动限制到outer uint32；WS connected只在Open，与TCP半关闭契约有区别。
- 本任务只增加/校正学习资料，没有修复这些行为，也没有重新运行生产构建、历史测试或profiler。

## 文档生成与核验

在WSL仓库目录执行：

~~~bash
python3 -B docs/learning/generate-code-snapshot.py
python3 -B docs/learning/generate-test-index.py
python3 -B docs/learning/generate-history-analysis.py
python3 -B docs/learning/verify-code-analysis.py
~~~

前三个生成器管理证据附件：snapshot从固定tag保存原文/清单，可用--render替换人工章节的code占位符；test-index按宏/Python AST索引入口和静态断言；history读取人工history-excerpts.json中的选段与解释，核对Git顺序后生成28份历史diff。

verify对Git对象逐文件核对来源、hash和原文，核对27段当前节选与28段历史节选、测试入口位置、152文件的正文定位、仓库/Obsidian链接及镜像内容。它不会自动判断文字机制正确，不会执行网络或负载。默认要求项目指定Obsidian镜像存在；--write可更新verification.json。

四个脚本和人工配置属于tag之后的文档维护，未计入v0.2的152文件。Obsidian保留同样学习正文和附件；报告链接适配镜像references目录。其他镜像正文、源码、diff由核验比较。
