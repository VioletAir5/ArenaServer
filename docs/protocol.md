# Gateway 请求协议表

当前实现：2026-10-10。TCP 与二进制 WebSocket 消息共用 PacketCodec/Envelope。
入口：`ArenaServer asio handshake [ws-origin]`。默认注册 Ping 和 LoginRequest；
真实凭据认证仍待接入，LoginRequest 当前返回 ServiceUnavailable。

## Packet 帧

所有多字节头字段采用大端；TCP read 和 WS message 边界都不等于 Packet 边界。

| 字段 | 字节数 | 语义 |
| --- | ---: | --- |
| magic | 4 | `0x4152454E`（AREN） |
| body length | 4 | 7 字节元数据加 protobuf payload，不包含 magic/length |
| type | 2 | 1=request，2=response |
| request id | 4 | 旧传输层请求标识；响应原样回传 |
| version | 1 | 当前 1 |
| payload | 可变 | Envelope；gateway body 上限 256 KiB |

完整头为 15 字节。非法 magic、长度、version/type 或 malformed protobuf 走既有拒绝关闭；
无法可靠识别的畸形字节不尝试构造带请求 id 的业务 Error。

## Envelope 字段

| 字段 | protobuf 字段号 | 类型 | 语义 |
| --- | ---: | --- | --- |
| request_id | 1 | uint64 | 当前调用的关联 id；非零时优先于包头 id |
| retired tag | 2 | reserved | 原 legacy_client_tag；禁止复用 |
| sent_at_unix_ms | 3 | uint64 | 客户端时间元数据，不用于服务端超时计算 |
| msg_id | 4 | uint32 | 新消息的 MsgId；0 表示旧客户端，从已知 oneof 推导 |
| login_request / login_response | 10 / 11 | message | 登录请求/结果 |
| ping / pong | 12 / 13 | message | 应用心跳 |
| error | 14 | message | Typed Error 的稳定线格式 |

有效调用需要非零关联 id：Envelope.request_id 非零时采用它，否则回退包头 request id；
两者均为0返回 InvalidRequest。包头 id 始终原样回传，不要求等于64位 Envelope id。
响应 Envelope.request_id 由路由器统一赋值，业务 Handler 不能覆盖关联 id。

旧消息的 msg_id=0 通过已知 oneof 推导，响应仍省略 msg_id，避免改变旧 Ping/Pong 字节。
显式 msg_id 的请求，响应携带相应响应 MsgId。旧客户端可忽略/保留新字段4。
显式未知 MsgId 可没有已知 oneof（用于未来消息），仍受 protobuf 大小/深度限制；
路由器回复 UnknownMessage。已知 MsgId 与 oneof 不匹配则 InvalidRequest。

## MsgId → Handler

MsgId 数值由 shared/proto/envelope.proto 定义；已发布值禁止改号或复用。

| MsgId | 名称 / payload | 方向 | 默认 Handler / 行为 | 默认认证要求 | 默认超时 |
| ---: | --- | --- | --- | --- | --- |
| 0 | MSG_UNSPECIFIED | 兼容入口 | 从已知 oneof 推导；无已知 payload 不路由 | 按推导后的注册项 | 同注册项 |
| 10 | MSG_LOGIN_REQUEST / LoginRequest | C→S | 当前返回 Error 2002；可注册真实认证 Handler | 无；收到请求不会自动认证 | 5s |
| 11 | MSG_LOGIN_RESPONSE / LoginResponse | S→C | 登录结果，不注册为请求路由 | — | — |
| 12 | MSG_PING / Ping | C→S | 返回 Pong，并回传 nonce | 无；不改变登录状态 | 5s |
| 13 | MSG_PONG / Pong | S→C | 心跳结果，不注册为请求路由 | — | — |
| 14 | MSG_ERROR / Error | S→C | 失败响应，不注册为请求路由 | — | — |
| 其他 | 未注册 MsgId | C→S | Error 1001，不执行业务 Handler | 不执行业务 | 立即响应 |

MessageRegistry::registerHandler 默认 requiresAuthentication=true、timeout=5s；
只有明确公开的登录/心跳路由配置 false。Expired 会话拒绝所有已注册路由，
Pending 不能执行要求认证的业务路由。未知消息先映射 UnknownMessage。
原 AuthenticationGateHandler/PingHandler 可用于自定义对象链；默认握手链已改为
EnvelopeDecodeHandler → MessageRouter，认证策略由注册项在业务调用前统一检查。

注册示例（在共享注册表发布给 worker 前完成）：
~~~cpp
auto routes = std::make_shared<MessageRegistry>();
routes->registerHandler(arena::protocol::MSG_PING,
    [](std::shared_ptr<const RequestMessage> request,
       RequestCompletion complete) {
        arena::protocol::Envelope reply;
        reply.mutable_pong()->set_nonce(request->envelope.ping().nonce());
        complete(std::move(reply));
    },
    MessageRouteOptions{false, std::chrono::seconds(5)});
routes->seal();
ConnectionHandlerFactory handlers = [routes](ConnectionContext::Ptr context) {
    return makePreLoginHandshakeHandler(std::move(context), routes);
};
~~~

seal 后禁止注册/覆盖；重复 MsgId、空 Handler、响应方向的内置 MsgId、
非正超时或大于1小时的超时在配置时拒绝。注册表只读地共享给 worker；
共享 Handler 的捕获不得包含未同步的跨连接可变状态。

## Typed Error

Error 的字段保持兼容：code=1(uint32)、message=2(string)、causes=3(repeated Error)。
C++ 业务使用 ErrorCode 枚举、MessageReply（std::expected<Envelope, ErrorCode>）和
BusinessError。客户端依据 code 判断，不解析 message。响应文本固定，不发送
未知异常的 what()、账户、凭据或原始请求 payload。

| code | ErrorCode | 固定 message | 触发条件 |
| ---: | --- | --- | --- |
| 0 | ERR_OK | 非错误响应 | 正常完成；不会用作失败 Error |
| 1001 | ERR_UNKNOWN_MESSAGE | unknown message | MsgId 未注册，包括服务端响应 MsgId 被当请求发送 |
| 1002 | ERR_INVALID_REQUEST | invalid request | 无有效请求 id、已知 MsgId/payload 不匹配或请求方向错误 |
| 1003 | ERR_UNAUTHENTICATED | authentication required | 会话未认证却调用业务路由，或会话已 Expired |
| 1004 | ERR_TIMEOUT | request timed out | owner deadline 到期，或完成回调到达时已过 deadline |
| 1005 | ERR_TOO_MANY_REQUESTS | too many requests | 每连接64个待完成请求已满 |
| 1006 | ERR_DUPLICATE_REQUEST | duplicate request id | 重复活动 id：终止原调用，回复一次；重复包不执行 |
| 2001 | ERR_BUSINESS_FAILURE | business failure | Handler 返回/抛出该 Typed Error |
| 2002 | ERR_UNAVAILABLE | service unavailable | 服务尚未接入或 Handler 明确返回；默认 Login |
| 9000 | ERR_INTERNAL | internal error | 非业务异常、非法错误枚举、不合法/超限业务响应、缺少调度依赖 |

业务失败用 `complete(std::unexpected(code))` 或抛 BusinessError(code)；
不要把失败伪装成成功 Envelope。已知 Ping/Login 的成功响应必须分别是
Pong/LoginResponse。业务返回的 request_id/msg_id 会由路由器统一设置。

Typed Error 表示调用完成，正常情况下连接继续可用；传输输出硬界、关闭或
不可恢复编码失败仍可终止连接。现有包限流/白名单在解码之前执行并关闭，
不会保证先发一个业务 Error；它们的拒绝计数与业务错误日志分开解释。

## 请求生命周期、超时与日志

每连接最多64个未完成调用。路由前挂 owner steady_clock deadline，
成功/失败完成时取消 timer。超时、重复完成、迟到 timer/业务回调最多回复一次。
请求 id 可以在完成后复用，内部 generation 阻止旧完成影响新请求。

RequestMessage 拥有 protobuf 数据与 ConnectionContext，不借用 PacketCodec 缓冲；
异步 Handler 保留 shared_ptr<const RequestMessage>。完成回调必须回到连接 owner，
不能在工作线程直接调用。Handler 应非阻塞；owner timer 不会抢占阻塞 Handler，
但同步完成时仍检查实际 deadline。关闭后不发送响应，回收取消 timer，
completion 弱引用路由状态，不保活 Runtime/socket。

请求链使用 spdlog，统一事件：
~~~text
event=request_start connection_id=123 request_id=456 msg_id=12 code=0 outcome=reply
event=request_handler connection_id=123 request_id=456 msg_id=12 code=0 outcome=reply
event=request_end connection_id=123 request_id=456 msg_id=12 code=0 outcome=reply
~~~
用 connection_id + request_id 检索同一次调用；客户端应使用独立调用 id。
unknown/typed business/timeout 的结束事件使用对应错误码。关闭回收的待完成
调用记录 outcome=cancelled，不当作成功响应；不记录凭据或原始 payload。

## 验收入口

- tests/gateway/net/MessageDispatchTest.cpp：注册、typed映射、id、日志、超时/迟到/取消与上限。
- tests/gateway/net/MessageDispatchIntegrationTest.py：真实TCP/WS响应及按request id核对日志。
- tests/shared/protocol/EnvelopeCodecTest.cpp：旧/新字段兼容、tag4与retired tag2。
- MessageDispatchTestProfile.h 的固定账户动作和合成业务 MsgId100只属于测试宿主。