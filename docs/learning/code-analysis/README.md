# v0.2 全代码分析（编写中）

固定版本：b6dd2835a050f3b118e2739ff65f992ed44bca5e。

已建立152个作者维护的代码/构建/测试/工具文件、26296行原文的固定快照与SHA256清单。完整源码附件用于逐项定位，不能代替机制分析；当前已完成共享基础设施、Reactor、协议、三transport、Runtime/Router、构建、测试和观测工具八章，其余分析继续补齐。

已完成：

- [共享基础设施](01-shared.md)
- [Reactor/Socket/线程/定时器](02-reactor.md)
- [协议与Buffer](03-protocol.md)
- [三transport与服务器](04-transports.md)
- [Runtime/Feature/Router](05-runtime-routing.md)
- [构建与入口](06-build-and-entry.md)
- [全部测试与宿主](07-tests.md)
- [观测脚本与 dashboard](08-observability-tools.md)
- [测试入口/断言索引](test-case-index.md)
- [全文件索引](source-index.md)

待完成：28次commit代码对照、覆盖/链接/源码节选校验。正文未完成前不称全部代码分析完成。

范围：v0.2所有作者维护的C++/头、Python、proto、HTML、CMake/构建/编辑设置及目标设计DSL。第三方/构建生成代码、渲染图片/PUML/SVG、报告CSV/JSON和纯说明文档不算待分析代码；报告/README在上下文引用。各独立模块是否接入gateway另行说明。
