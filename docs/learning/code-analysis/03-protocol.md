# 03 协议、Buffer 与增量解析

固定源码：`v0.2^{commit}=b6dd2835a050f3b118e2739ff65f992ed44bca5e`。本章分析共享协议模块的所有接口与实现，并解释 gateway 的 Buffer adapter。全文件原文见[索引](source-index.md)；测试证据见[测试章](07-tests.md)，这里分析代码，不将源码存在当作测试已通过。

## 阅读范围

| 文件（相对仓库根） | 实际职责 |
| --- | --- |
| `shared/include/protocol/Buffer.hpp`、`shared/src/protocol/Buffer.cpp` | 有容量界的连续字节存储、读写位置、prepend、压缩和扩容 |
| `shared/include/protocol/PacketHeader.hpp` | 外层线格式、字段尺寸、magic、类型与版本常量 |
| `shared/include/protocol/PacketHeaderCodec.hpp`、`shared/src/protocol/PacketHeaderCodec.cpp` | 不完整/非法/完整头的分类、头编码、已有Buffer中逐帧回调 |
| `shared/include/protocol/PacketCodec.hpp`、`shared/src/protocol/PacketCodec.cpp` | 每个字节流一个有状态 decoder；限制未完成帧；完整帧编码 |
| `shared/include/protocol/EnvelopeCodec.hpp`、`shared/src/protocol/EnvelopeCodec.cpp` | protobuf载荷的字节、深度、有效形状限制 |
| `shared/proto/envelope.proto` | Envelope/login/ping/pong/error字段、MsgId和稳定错误码 |
| `services/gateway/include/net/PacketBuffer.h`、`services/gateway/src/net/PacketBuffer.cpp` | gateway容量配置及readPtr/skip旧接口；也在[Runtime章](05-runtime-routing.md)登记 |

## 1. Buffer：索引含义与每个方法

`m_storage`是vector的实际字节区，`m_readIndex`指向可读区起点，`m_writeIndex`指向可写区起点。通常保持 `readIndex <= writeIndex <= storage.size()`。构造时两者等于prependReserve；prepend可以把readIndex减到reserve之前，因此不能把“readIndex永远≥reserve”写成不变量。

`readableBytes`是write-read，`writableBytes`是storage.size-write，`prependableBytes`是readIndex。`peek/writePtr`返回借用指针，不转移所有权；append扩容、压缩、prepend/retrieve后不能继续假设旧视图仍表示同一段有效数据。模块没有锁，由调用者的owner串行使用。

- 构造检查initial非零、initial≥reserve、initial≤max；不合法抛invalid_argument。
- `append`先允许零长度，再检查null/容量，memcpy后推进writeIndex。`hasWritten`用于调用者先写入writePtr，再提交实际写入的长度；不能超过writable。
- `prepend`只在前方已有空间时成功，先减readIndex再复制；不自动扩容。HeaderCodec因此可以承诺校验失败不改变Buffer。
- `retrieve`只消费现有可读长度；读完时两索引恢复reserve。它不缩容，故空Buffer可能保留此前的峰值容量。
- `ensureWritable`依次选择现有尾部、把已消费的前部回收、重新分配。前部回收保留prependReserve，因此compact后还可以prepend一个头。
- 压缩可能有重叠，使用memmove；新vector与旧区不重叠，使用memcpy。容量运算先用减法检查余量，避免readable+length溢出后绕过限制。增长倾向倍增，但最终不超过max。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e shared/src/protocol/Buffer.cpp 110-155 -->
原文：`shared/src/protocol/Buffer.cpp:L110–L155`；[完整文件](source/shared/src/protocol/Buffer.cpp.md)。

~~~cpp
bool Buffer::ensureWritable(std::size_t length)
{
    if (length <= writableBytes())
    {
        return true;
    }

    const std::size_t readable = readableBytes();
    const std::size_t usableFront = m_readIndex > m_prependReserve
                                        ? m_readIndex -
                                              m_prependReserve
                                        : 0;
    if (length <= usableFront + writableBytes())
    {
        std::memmove(
            m_storage.data() + m_prependReserve,
            peek(),
            readable
        );
        m_readIndex = m_prependReserve;
        m_writeIndex = m_readIndex + readable;
        return true;
    }

    // Calculate after compaction, not from the old write index.
    if (readable > m_maxCapacity - m_prependReserve ||
        length > m_maxCapacity - m_prependReserve - readable)
    {
        return false;
    }
    const std::size_t required = m_prependReserve + readable + length;
    const std::size_t capacity = std::min(
        m_maxCapacity,
        std::max(
            required,
            m_storage.size() > m_maxCapacity / 2
                ? m_maxCapacity
                : m_storage.size() * 2
        )
    );
    std::vector<char> enlarged(capacity);
    std::memcpy(enlarged.data() + m_prependReserve, peek(), readable);
    m_storage.swap(enlarged);
    m_readIndex = m_prependReserve;
    m_writeIndex = m_readIndex + readable;
    return true;
~~~

这里限制的是单个Buffer存储，不是进程RSS；vector分配仍可能抛bad_alloc。append的输入不能是会被压缩/扩容失效的自身借用指针：实现没有专门处理自引用追加，调用者要保证源地址在操作中有效。

`PacketBuffer`是薄adapter：默认4KiB初始/1MiB最大、prependReserve=0；另一构造允许传入容量；readPtr转peek，skip转retrieve。它既不解析Packet，也不让写位置变成原子。gateway的`kMaxPacketSize=256KiB`是包体政策，不能误读成所有Buffer都是256KiB。

## 2. PacketHeader：按线格式编码，不能直接发送C++结构体

| 字节偏移 | 尺寸 | 字段 | 约束 |
| --- | ---: | --- | --- |
| 0 | 4 | magic | 0x4152454E，ASCII AREN |
| 4 | 4 | body length | 包括后续7字节metadata及payload；不包括前8字节 |
| 8 | 2 | outer type | 当前只允许1(request)/2(response) |
| 10 | 4 | outer request id | uint32，大端 |
| 14 | 1 | version | 当前1 |
| 15 | 可变 | payload | 可以是Envelope，也可以是测试echo数据 |

`kPrefixSize=8`，`kMetadataSize=7`，`kHeaderSize=15`，总帧长度=8+bodyLength=15+payloadLength。shared包体上限4MiB；gateway的PacketPipeline通常把decoder包体限制到256KiB。Envelope自身上限256KiB，**能装进gateway帧的Envelope最大值实际还要减去7字节metadata**，Router发送前已做这一检查。

C++结构体布局含padding，也使用主机字节序，不能直接memcpy整个struct到网线。HeaderCodec按偏移逐字段用htonl/htons和memcpy，避免未对齐reinterpret_cast读取引发未定义行为；一字节version不用转序。

## 3. PacketHeaderCodec：早拒与输出提交点

私有`valid`检查magic、[7,4MiB]长度、version=1和type=1/2。`encode`再核对length-metadata与Buffer现有payload一致、prepend空间足够，全部成立才组装15字节并prepend；失败返回false且Buffer不变。

`decode`在四个阶段处理：

1. <4字节：Incomplete。
2. 达到4字节：检查magic，非法立即Invalid，不等完整包。
3. 达到8字节：检查bodyLength，非法立即Invalid。
4. 达到15字节：解析type/request/version并valid；最后才`header=candidate`。不完整或非法不会污染调用者的旧header。

这里只证明头有效，不证明body已经到齐。`decodeFrames`另外检查8+length是否已可读，然后把payload借用为string_view。回调结束后retrieve整帧；回调false返回Stopped，true继续读下一帧。回调不能修改或销毁input，也不能保留payload视图用于异步任务；要异步处理就先复制/解析成拥有对象。

没有“扫描下一个AREN”自动重同步逻辑；非法意味着调用者应该关闭连接。这使攻击字节不能被跳过后继续解释成另一种合法请求，也避免不明确的恢复规则。

## 4. PacketCodec：只保留当前未完成帧

`PacketCodec`每连接一个、不可复制、owner串行调用feed。构造检查自定义包体上限[7,4MiB]，内部Buffer最大=maxBody+8且reserve=0。这里没有把调用者整个粘包输入一口气append。

核心是目标尺寸：先收4字节，再8字节，再15字节，再8+bodyLength；每轮只复制达到当前目标所需的字节。一帧完成后回调并清除，剩余span推进到下一帧。因此任意多的coalesced frames不要求内部同时保存它们；峰值留存受单帧限制。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e shared/src/protocol/PacketCodec.cpp 65-123 -->
原文：`shared/src/protocol/PacketCodec.cpp:L65–L123`；[完整文件](source/shared/src/protocol/PacketCodec.cpp.md)。

~~~cpp
            const auto size = m_input.readableBytes();
            std::size_t target = PacketHeader::kMagicSize;
            if (size >= PacketHeader::kMagicSize)
            {
                PacketHeader header;
                auto status = PacketHeaderCodec::decode(
                    m_input.peek(),
                    size,
                    header
                );
                if (status == HeaderDecodeStatus::Invalid)
                {
                    return m_status = FrameDecodeStatus::Invalid;
                }
                if (size < PacketHeader::kPrefixSize)
                {
                    target = PacketHeader::kPrefixSize;
                }
                else
                {
                    std::uint32_t networkLength;
                    std::memcpy(
                        &networkLength,
                        m_input.peek() + PacketHeader::kMagicSize,
                        sizeof(networkLength)
                    );
                    const auto bodyLength = ::ntohl(networkLength);
                    if (bodyLength > m_maxBodySize)
                    {
                        return m_status = FrameDecodeStatus::Invalid;
                    }
                    target = size < PacketHeader::kHeaderSize
                                 ? PacketHeader::kHeaderSize
                                 : PacketHeader::kPrefixSize + bodyLength;
                }
                if (size == target &&
                    status == HeaderDecodeStatus::Complete)
                {
                    auto result = PacketHeaderCodec::decodeFrames(
                        m_input,
                        onFrame
                    );
                    if (result != FrameDecodeStatus::NeedMore)
                    {
                        return m_status = result;
                    }
                    continue;
                }
            }
            if (bytes.empty())
            {
                return FrameDecodeStatus::NeedMore;
            }
            const auto count = std::min(target - size, bytes.size());
            if (!m_input.append(bytes.data(), count))
            {
                return m_status = FrameDecodeStatus::Invalid;
            }
            bytes = bytes.subspan(count);
~~~

`pendingBytes`仅代表内部未消费字节。输入完整但没有下一帧时返回NeedMore，这是正常流式结束，不代表刚才的帧没有被处理。

`Invalid`、回调false产生的`Stopped`都是终态；再feed只返回原状态，剩余输入不处理。`m_feeding`检测重入；Guard确保正常或异常返回都恢复flag。回调抛异常时catch把decoder设Invalid再重抛，因此调用方不能从已经部分处理的帧重新尝试。终态未主动清空m_input，存储跟decoder/Runtime一起销毁，连接close后不应继续持有Runtime造成滞留。

静态`encode`先检查payload≤4MiB-7，构造header和容量恰能容纳整帧的Buffer，append载荷，再HeaderCodec::encode prepend。返回optional使不合法输入与成功编码可区分；分配异常仍由外层处理。

## 5. EnvelopeCodec：解析与业务路由分开

decode拒绝空输入与>256KiB。CodedInputStream设置total bytes和16层recursion；临时Envelope执行ParseFromCodedStream、ConsumedEntireMessage、validEnvelope，成功才Swap到output。因此失败不会留下半解析消息。未知字段仍遵守wire字节/解析深度限制，并由protobuf保留，不会因为业务不认识就绕过预算。

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e shared/src/protocol/EnvelopeCodec.cpp 51-75 -->
原文：`shared/src/protocol/EnvelopeCodec.cpp:L51–L75`；[完整文件](source/shared/src/protocol/EnvelopeCodec.cpp.md)。

~~~cpp
bool EnvelopeCodec::decode(
    std::span<const std::byte> bytes,
    arena::protocol::Envelope& output
)
{
    if (bytes.empty() || bytes.size() > kMaxBytes)
    {
        return false;
    }
    google::protobuf::io::CodedInputStream input(
        reinterpret_cast<const std::uint8_t*>(bytes.data()),
        static_cast<int>(bytes.size())
    );
    input.SetTotalBytesLimit(static_cast<int>(kMaxBytes));
    input.SetRecursionLimit(kMaxDepth);

    arena::protocol::Envelope parsed;
    if (!parsed.ParseFromCodedStream(&input) ||
        !input.ConsumedEntireMessage() || !validEnvelope(parsed))
    {
        return false;
    }
    output.Swap(&parsed);
    return true;
}
~~~

`validEnvelope`接受已知login/ping/pong；Error检查递归causes深度；LoginResponse内error从深度3开始，顶层Error从2开始，以计入Envelope及payload层。没有已知payload时只有显式非零msg_id可接受，目的是把“有效protobuf但未知消息”交给Router映射UnknownMessage，而不是统一当格式破坏关闭。

encode先验证形状/深度，再ByteSizeLong≤256KiB，SerializeToString返回optional。它不会检查account、credential、权限或消息方向；这些属于Handler/Router。对已知Error树先检查深度可阻止任意深对象继续进入序列化；这里不是一般protobuf对象图的完整安全审查器。

## 6. envelope.proto：字段演进和两层标识

<!-- verified-excerpt b6dd2835a050f3b118e2739ff65f992ed44bca5e shared/proto/envelope.proto 33-47 -->
原文：`shared/proto/envelope.proto:L33–L47`；[完整文件](source/shared/proto/envelope.proto.md)。

~~~protobuf
message Envelope {
  uint64 request_id = 1;
  reserved 2;
  reserved "legacy_client_tag";
  uint64 sent_at_unix_ms = 3;
  // Zero means a v1 peer: infer from the known payload. Never reuse this tag.
  uint32 msg_id = 4;

  oneof payload {
    LoginRequest login_request = 10;
    LoginResponse login_response = 11;
    Ping ping = 12;
    Pong pong = 13;
    Error error = 14;
  }
~~~

request_id是uint64，outer header是uint32；Router有一致性/范围规则，不能假定两个字段任意各写一个值。msg_id=0是旧客户端，Router从已知oneof推导MsgId；非零代表显式消息标识，需与payload匹配。sent_at_unix_ms是客户端提供的时间，不参与服务端steady_clock超时计算。

Envelope字段1、3、4及oneof10–14固定；退休字段2及名称legacy_client_tag均reserved。Error.code保留uint32，enum列出1001未知、1002非法、1003未认证、1004超时、1005过载、1006重复、2001业务失败、2002不可用、9000内部错误。可读新字段/保留未知字段与理解新业务语义是两个要求。

LoginRequest的credential是bytes，LoginResponse有accepted/session_token/error；**schema存在不代表真实鉴权已实现**。默认Login仍返回Unavailable。Ping/Pong用nonce往返；Error.causes提供嵌套原因，因此必须限制深度。

旧/新schema兼容测试仅证明protobuf字段演进。header magic修改改变外层线格式，不能用“旧客户端能读新Envelope”推导旧TCP客户端也能连接新服务器。

## 7. 实际接收与发送路径

~~~text
transport借用bytes → Runtime.received
  → PacketPipeline的PacketCodec.feed
  → 完整帧政策(桶/白名单)
  → EnvelopeDecoderHandler(EnvelopeCodec.decode)
  → AuthenticationGateHandler/MessageDispatchHandler
  → Router → handler → Completion
  → EnvelopeCodec.encode → PacketCodec.encode → transport.send
~~~

分层的Depth体现在调用者只学习feed/encode及视图寿命、容量和终态契约；三种transport都复用这些规则。OS读取一次、WS message一次、业务Packet一次并不等价；此decoder可以跨读与跨message保留半帧，也可以在一次输入拆出多个帧。Beast重组WS frame为message，而PacketCodec继续处理应用Packet。

并发保证来自owner，不是shared::Buffer内部锁。跨线程send必须先复制到transport入口队列；接收视图只活到同步回调结束。相关失败会记录RejectionCounters或Typed Error，映射详见[Runtime/Router](05-runtime-routing.md)。

## 8. 验证与阅读入口

- `tests/shared/protocol/PacketHeaderCodecTest.cpp`：头阶段、字节序、magic和不修改输出。
- `PacketCodecTest.cpp`：分片/粘包、早拒、自定义limit、终态/回调异常/重入。
- `EnvelopeCodecTest.cpp`、`compat/legacy_envelope.proto`：双向字段兼容、unknown、reserved、字节/深度。
- `tests/gateway/net/PacketBufferTest.cpp`及`PacketCodecIntegrationTest.py`：gateway政策及真实网络帧。
- `MessageDispatchTest.cpp`：两层request id、payload/msg_id匹配及回复预算。

这里只引用各测试的职责；v0.2实测执行范围以[chaos报告](../../reports/v0.2-chaos-20261011.md)为准。后续阅读：[连接实现](04-transports.md)、[Runtime和Router](05-runtime-routing.md)。
