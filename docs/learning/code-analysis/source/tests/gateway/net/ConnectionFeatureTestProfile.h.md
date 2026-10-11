# tests/gateway/net/ConnectionFeatureTestProfile.h

固定源码：b6dd2835a050f3b118e2739ff65f992ed44bca5e；20行。

[对应分析](../../../../07-tests.md)。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。

## 完整源码（保留原始内容）

~~~cpp
#pragma once
#include <iostream>

#include "net/BackpressureFeature.h"
#include "net/PacketWhitelistFeature.h"
#include "net/TokenBucketFeature.h"
namespace service::gateway
{
inline ConnectionFeatureFactory makeFeatureTestProfile()
{
    return [](ConnectionPipeline& pipeline, const ConnectionContext&)
    {
        pipeline.add<BackpressureFeature>();
        pipeline.add<PacketWhitelistFeature>(
            std::vector<std::uint16_t>{1}
        );
        pipeline.add<TokenBucketFeature>(2.0, 0.1);
    };
}
} // namespace service::gateway
~~~
