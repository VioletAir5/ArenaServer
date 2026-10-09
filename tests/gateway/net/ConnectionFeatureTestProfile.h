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
