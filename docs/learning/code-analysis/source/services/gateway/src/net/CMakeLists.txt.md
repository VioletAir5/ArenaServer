# services/gateway/src/net/CMakeLists.txt

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；3行。

[对应分析](../../../../../06-build-and-entry.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cmake
# Adds the gateway networking implementation to the service library.
file(GLOB sources CONFIGURE_DEPENDS "*.cpp")
target_sources(arena_gateway PRIVATE ${sources})
~~~
