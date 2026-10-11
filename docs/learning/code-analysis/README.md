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
- [28次commit代码对照与完整diff](09-commit-code.md)
- [测试入口/断言索引](test-case-index.md)
- [全文件索引](source-index.md)

待完成：覆盖/链接/源码节选校验。正文未完成前不称全部代码分析完成。

范围：v0.2所有作者维护的C++/头、Python、proto、HTML、CMake/构建/编辑设置及目标设计DSL。第三方/构建生成代码、渲染图片/PUML/SVG、报告CSV/JSON和纯说明文档不算待分析代码；报告/README在上下文引用。各独立模块是否接入gateway另行说明。

## 文档生成器与人工解释

三个生成器只管理证据附件：generate-code-snapshot.py从固定tag保存原文/清单，并把人工章节中的code占位符替换为精确源码；generate-test-index.py按宏或Python AST索引测试入口及静态断言，计数不代表执行结果；generate-history-analysis.py读取人工history-excerpts.json中的选段和解释，核对Git顺序后生成历史节选/28份完整diff。正文的机制、限制与评价来自人工源码阅读，不由文件行数或生成器推断。学习工具位于docs/learning，属于tag之后的文档维护，未计入v0.2的152文件。
