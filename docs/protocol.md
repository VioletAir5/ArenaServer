# Gateway 请求协议

当前实现：2026-10-21。TCP 与二进制 WebSocket 共用 PacketCodec/Envelope。
入口为 ArenaServer asio handshake [ws-origin]。默认路由有 Ping 和返回不可用的 Login；
ARENA_JWT_KEY 选择认证/在线注册表。密码账户/KDF 适配器仍需应用提供，main 中为
Unavailable；测试宿主的固定账户只用于验收。

## Packet 与 Envelope

Packet 头为15字节，所有多字节整数为大端：magic4=0x4152454E、body length4、
type2（请求1/响应2）、request id4、version1=1；body length 含7字节元数据和
protobuf payload。body 上限256KiB。TCP read/WS message 边界不替代 Packet framing。
畸形 magic/长度/版本/不可解析 protobuf 会关闭连接，不尝试猜测请求ID。

Envelope 保持 request_id=1、reserved2/legacy_client_tag、sent_at_unix_ms=3、msg_id=4。
规范请求ID优先采用非零 Envelope.request_id，否则回退 Packet request id；两者均0
返回1002。响应的 Packet id 原样保留，Envelope id 由协议统一赋值。
msg_id=0 时由已知 oneof 推导，响应仍省略 msg_id；显式 msg_id 的响应带对应响应ID。
未知显式ID允许未知payload并返回1001；已知ID/payload不匹配返回1002。

## 消息表

MsgId 与对应 Envelope oneof 字段号相同，已有10–14和reserved2不得重用。

| 请求 / 响应 | payload | 认证要求 | 行为 |
| --- | --- | --- | --- |
| 10 / 11 | LoginRequest / LoginResponse | Pending连接 | 密码新建逻辑会话，或JWT恢复 |
| 12 / 13 | Ping / Pong | 公开；失效旧绑定仍拒绝 | 回传nonce |
| — / 14 | Error | — | 固定typed错误 |
| 20 / 21 | SessionStateRequest / SessionStateResponse | 当前已认证绑定 | 查询自身及当前房间成员 |
| 30 / 31 | RoomJoinRequest / RoomJoinResponse | 当前已认证绑定 | 原子加入/移动到房间 |
| 32 / 33 | RoomLeaveRequest / RoomLeaveResponse | 当前已认证绑定 | 解除房间关系，保留会话 |

20/30/32 在 makeLoginMessageRegistry 认证 profile 注册；默认未配置认证的 profile
没有这些路由。新内置响应ID不可注册为请求。每个新请求都支持显式或推导ID模式。
MessageRegistry 发布前 seal；默认路由期限5秒、每连接最多64 pending。

## 登录与重连入口

LoginRequest：account1、credential2(bytes)、client_version3、credential_kind4。
kind0=CREDENTIAL_PASSWORD，kind1=CREDENTIAL_JWT。JWT模式 account 必须为空；
主体身份来自验证后的JWT，客户端不能替换 PlayerId。

LoginResponse 兼容保留 accepted1、session_token2、error3、player_id4、session_epoch5，
追加 session_id6、room_id7、resumed8。room_id=0 表示未绑定；密码登录 resumed=false，
JWT恢复=true。SessionId 是稳定逻辑身份，session_epoch 是当前连接权限栅栏，不能互用。
JWT profile 的7项claims、签名/issuer/audience/expiry校验及精确epoch/expiry匹配不变。

恢复步骤：

1. 初次密码登录，保存session_token及session_id，按需调用RoomJoin。
2. 断线后服务端保留同一个逻辑会话/room slot，presence变为Detached。
3. 新连接发送LoginRequest(kind=JWT, credential=旧token, account空)。
4. grace内验证成功，返回相同session_id/room_id、更新epoch/token；旧token及旧连接失效。
5. 到达或超过grace deadline，旧token即使签名/有效期仍正确，也不能恢复该记录。
   此时可重新验证密码建立新逻辑会话，原房间不自动带入。

密码登录会替换旧逻辑会话并移除其原房间成员；它不等同于JWT恢复。
恢复不重放旧连接已取消的RPC，不保证恢复之前业务副作用的幂等。

## Presence 与房间 roster

Presence：UNSPECIFIED0、ONLINE1、DETACHED2。
SessionStateRequest为空；SessionStateResponse字段为：

| 字段 | 号 | 含义 |
| --- | ---: | --- |
| session_id | 1 | 当前稳定SessionId |
| player_id | 2 | 已认证PlayerId |
| room_id | 3 | 当前房间，0表示无 |
| presence | 4 | 当前连接正常查询时为ONLINE |
| members | 5 | repeated RoomMember，同一事务快照 |

RoomMember：player_id1、session_id2、presence3。房间成员可为ONLINE或DETACHED；
无房间时members为空。查询入口只返回调用者自身和所属房间，不支持任意玩家枚举。
RoomJoinRequest含room_id1，必须非零；RoomJoinResponse、RoomLeaveResponse都以
state1 包含 SessionStateResponse。RoomLeaveRequest为空；重复leave和同房间join幂等。

首次join可按ID创建受限gateway roster，默认10000房间、每房64成员、100000保留玩家。
这不是战斗RoomActor、邀请系统或房间访问权限策略。加入/移动、leave、会话替换及
到期删除同时维护player→room和room→members，失败不能破坏原有绑定。
Detached成员在grace内仍占房间slot；到期移除，空房间回收。

ARENA_DISCONNECT_GRACE_MS 默认30000，允许1..3600000毫秒。
第一次匹配的断线使用steady_clock建立deadline；重复清理不延长。
独立管理timer每100ms sweep，无新登录/查询也能回收；owner阻塞可能推迟物理清理，
但 now>=deadline 仍拒绝恢复。旧epoch/connection的close或业务调用不能删除新绑定。

## 错误与请求生命周期

Error字段保持code1(uint32)、message2(string)、causes3(repeated Error)。

| code | 固定message | 主要条件 |
| ---: | --- | --- |
| 1001 | unknown message | 未注册消息，包括把响应作为请求 |
| 1002 | invalid request | 无ID、已知payload错配、room_id=0 |
| 1003 | authentication required | 未认证/旧绑定/无效JWT/过期grace |
| 1004 | request timed out | deadline或完成点实际超时 |
| 1005 | too many requests | 64 pending上限 |
| 1006 | duplicate request id | 结束原活动调用，回复一次 |
| 2001 | business failure | 房间成员满或typed业务失败 |
| 2002 | service unavailable | 未接入认证、玩家/房间总容量满 |
| 9000 | internal error | 未知异常、错误回复类型或编码预算失败 |

客户端按code判断，不解析message。异常文本、密码、token不写响应或日志。
业务成功回复的payload必须匹配请求；协议统一填写关联字段。
错误响应通常不关闭连接，帧/包策略、输出硬界或不可恢复编码失败可关闭。

RequestDispatcher 管 pending/generation/deadline，ArenaProtocol 管wire转换。
RequestMessage拥有Envelope/context，完成回调可跨线程经有界poster回owner。
最多256个未消费foreign action；满载拒绝后原deadline收尾。迟到/重复/关闭后完成无效。
Handler仍在owner同步调用，耗时工作需主动交给executor；timer不抢占阻塞Handler。
停机先quiesce新输入，等待已接收请求/期限，再排空输出，最后由server grace强制兜底。

spdlog保留event=request_start/request_handler/request_end，含connection_id、request_id、
msg_id、code与outcome；reply表示进入发送路径，不代表peer已接收。
新增event=session_install含player/connection/epoch/session_id/resumed；
event=online_sweep含active/retained/rooms，只有真正回收时输出。

## 验收入口

- tests/auth/OnlineRegistryTest.cpp：clock边界、竞态、索引、容量与清理。
- tests/auth/OnlineRegistryMaintenanceTest.cpp：独立timer、取消/退役。
- tests/auth/OnlineProtocolTest.cpp：真实生产routes、认证、恢复、快照与错误。
- tests/auth/OnlinePresenceIntegrationTest.py：epoll/Asio0/4、TCP↔WS、observer与无流量回收。
- tests/auth/AccountLoginIntegrationTest.py：100并发登录、旧token/旧连接隔离。
- tests/shared/protocol/EnvelopeCodecTest.cpp：旧schema/tag兼容及新增payload。
