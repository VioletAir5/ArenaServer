# Player / Session / Ticket / Room / Battle / Settlement 生命周期

日期：2026-10-12（Asia/Shanghai）。本页是本次实现的状态契约及合法迁移表，术语见[CONTEXT](../../CONTEXT.md)。

## 实现范围和位置

已实现独立 `arena_domain` 业务模型库：六种强类型非零ID、不可变关联ID、owner约束、带版本号的合法迁移、类型化拒绝和表驱动测试。通用算法位于 `shared/include/state/OwnedStateMachine.h`，业务图位于 `domain/include/arena/domain/*.h`；业务概念不放入shared。

这不是一个新部署服务。Gateway依旧运行既有传输/协议，Auth/Lobby/Match/Room/Storage服务器、持久化、恢复凭据/租约timer和actor消息接线仍属后续实现。构建模型和测试不使这些业务服务已经运行。

## 术语与关系

| 对象 | 不变身份/引用 | 初态 | 权威 owner |
| --- | --- | --- | --- |
| Player | PlayerId | Active | Lobby.PlayerDirectory |
| Session | SessionId、PlayerId | Online | Gateway.SessionRegistry |
| Ticket | TicketId、PlayerId | Queued | Match.MatchQueue |
| Room | RoomId | Forming | Room.RoomActor |
| Battle | BattleId、RoomId | Prepared | 所属Room.RoomActor |
| Settlement | SettlementId、BattleId | Pending | Storage.SettlementCoordinator |

Player是稳定身份；在线/离线属于Session，对局状态属于Battle。它们不是一个巨大Player枚举。Ticket只代表一次申请；终态后新申请使用新TicketId。Room可承载多场Battle，每场有新BattleId；结算重试保持SettlementId。

已确认采用可恢复Session：断线只使其Online→Detached，不自动取消Ticket、离开Room或中止Battle。这些独立对象需要各自owner接收对应业务事件。

`arena::domain::Session` 是认证成功后的业务会话，初态Online。`service::gateway::ConnectionSession` 是现有连接内的Pending/Authenticated/Expired认证门；旧 `service::gateway::Session` 是未接入当前运行链的Socket容器。三者没有隐式互相转换，本次没有改两个旧类型。

## 统一 Interface 与拒绝规则

~~~cpp
Session session(SessionId{10}, PlayerId{20}); // 必须在Session owner线程构造
auto before = session.snapshot();            // expected<{state, revision}, TransitionError>
if (before) {
    auto result = session.apply(Session::Event::TransportLost, before->revision);
    // 成功：Detached、revision+1；失败：state/revision都不变
}
~~~

所有实体：

- 构造于自己的owner executor；捕获当前thread id，所有状态读/写先检查它。
- `id()`、关联ID和 `ownerThread()` 是不可变数据，可跨线程读取；`snapshot()`/`apply()` 只允许owner。
- `apply(event, expectedRevision)` 是唯一状态修改入口，无setState，实体禁copy/move，不能靠搬到新线程重绑owner。
- 成功恰好递增revision一次；无合法表项就拒绝，包括终态复活和同状态重复命令。
- 错误优先级：WrongThread → InvalidEvent → StaleRevision → IllegalTransition → RevisionExhausted（最后一项仅对合法边检查）。
- 未声明的Event字节值全部InvalidEvent；revision不匹配拒绝迟到/重放命令；最大revision不回绕。
- ID为同类中的非零身份，不负责分配全局唯一ID、查找引用对象或数据库唯一约束；这些由未来registry/存储负责。

| 错误 | 含义 | 状态变化 |
| --- | --- | --- |
| WrongThread | 非owner调用，包括只读snapshot | 无；拒绝前不读取可变字段 |
| InvalidEvent | 不在事件集合内的枚举值 | 无 |
| StaleRevision | 命令针对的旧版本与当前版本不同 | 无 |
| IllegalTransition | 事件已声明，但当前状态没有该边 | 无 |
| RevisionExhausted | 合法迁移会使uint64版本溢出 | 无 |

事件是**owner已经验证的业务事实**，不是把客户端发来的任意枚举直接用于apply。此模型只强制状态图、版本、线程、身份类型；下表“业务事实”列说明接线时须验证的条件，并不宣称本版已有凭据校验、期限计算、房间成员管理或数据库事务。

## 每状态的 owner、线程与合法表

每个对象从初态到终态使用同一个owner/thread，没有终态线程迁移。表中“executor”是目标绑定职责；当前模型把它具体绑定为构造线程，运行服务的executor尚未接线。

### Player

[实现](../../domain/include/arena/domain/Player.h)。

| 状态 | 含义 | owner | 读/写线程 |
| --- | --- | --- | --- |
| Active | 身份可参与业务；不表示当前在线 | Lobby.PlayerDirectory | PlayerDirectory shard executor，构造线程 |
| Suspended | 参与资格暂停，可恢复 | Lobby.PlayerDirectory | PlayerDirectory shard executor，构造线程 |
| Retired | 身份生命周期结束，终态 | Lobby.PlayerDirectory | PlayerDirectory shard executor，构造线程 |

| 当前状态 | 事实事件 | 下一状态 | owner提交事件前须验证 |
| --- | --- | --- | --- |
| Active | Suspend | Suspended | 资格暂停事实已由权限方确认 |
| Suspended | Reinstate | Active | 恢复资格事实已确认 |
| Active | Retire | Retired | 删除/退休事实已确认 |
| Suspended | Retire | Retired | 删除/退休事实已确认 |

**表外转换全部拒绝。**

### Session

[实现](../../domain/include/arena/domain/Session.h)。

| 状态 | 含义 | owner | 读/写线程 |
| --- | --- | --- | --- |
| Online | 已认证会话绑定有效连接 | Gateway.SessionRegistry | SessionRegistry shard executor，构造线程 |
| Detached | 会话保留，等待租约内授权恢复 | Gateway.SessionRegistry | SessionRegistry shard executor，构造线程 |
| Expired | 有效租约已结束，终态 | Gateway.SessionRegistry | SessionRegistry shard executor，构造线程 |
| Revoked | 主动撤销，终态 | Gateway.SessionRegistry | SessionRegistry shard executor，构造线程 |

| 当前状态 | 事实事件 | 下一状态 | owner提交事件前须验证 |
| --- | --- | --- | --- |
| Online | TransportLost | Detached | 属于当前绑定的连接代 |
| Detached | ResumeAuthorized | Online | 身份/恢复凭据与有效租约已校验 |
| Online | LeaseExpired | Expired | 确认当前租约确已到期 |
| Detached | LeaseExpired | Expired | 确认当前租约确已到期 |
| Online | Revoke | Revoked | 撤销事实已确认 |
| Detached | Revoke | Revoked | 撤销事实已确认 |

**表外转换全部拒绝。**

### Ticket

[实现](../../domain/include/arena/domain/Ticket.h)。

| 状态 | 含义 | owner | 读/写线程 |
| --- | --- | --- | --- |
| Queued | 等待匹配 | Match.MatchQueue | MatchQueue shard executor，构造线程 |
| Reserved | 持有临时分配预留 | Match.MatchQueue | MatchQueue shard executor，构造线程 |
| Assigned | 成功交付分配结果，终态 | Match.MatchQueue | MatchQueue shard executor，构造线程 |
| Cancelled | 主动取消，终态 | Match.MatchQueue | MatchQueue shard executor，构造线程 |
| Expired | 申请/预留期限结束，终态 | Match.MatchQueue | MatchQueue shard executor，构造线程 |
| Failed | 不可恢复的分配失败，终态 | Match.MatchQueue | MatchQueue shard executor，构造线程 |

| 当前状态 | 事实事件 | 下一状态 | owner提交事件前须验证 |
| --- | --- | --- | --- |
| Queued | Reserve | Reserved | 匹配队列接受有效预留 |
| Reserved | ReleaseReservation | Queued | 当前预留已释放，原申请仍有效 |
| Reserved | Assign | Assigned | 房间分配成功且对应本预留 |
| Queued | Cancel | Cancelled | 申请方有取消权，取消事实有效 |
| Reserved | Cancel | Cancelled | 申请方有取消权，取消事实有效 |
| Queued | DeadlineExpired | Expired | 当前申请/预留期限确已到期 |
| Reserved | DeadlineExpired | Expired | 当前申请/预留期限确已到期 |
| Reserved | AllocationFailed | Failed | 当前分配发生不可恢复失败 |

**表外转换全部拒绝。**

### Room

[实现](../../domain/include/arena/domain/Room.h)。

| 状态 | 含义 | owner | 读/写线程 |
| --- | --- | --- | --- |
| Forming | 成员/准备状态尚未就绪 | Room.RoomActor | RoomActor owner / tick executor，构造线程 |
| Ready | 已准备，可创建新对局 | Room.RoomActor | RoomActor owner / tick executor，构造线程 |
| Running | 承载当前对局 | Room.RoomActor | RoomActor owner / tick executor，构造线程 |
| Closing | 停止新操作并回收房间资源 | Room.RoomActor | RoomActor owner / tick executor，构造线程 |
| Closed | 房间已回收，终态 | Room.RoomActor | RoomActor owner / tick executor，构造线程 |

| 当前状态 | 事实事件 | 下一状态 | owner提交事件前须验证 |
| --- | --- | --- | --- |
| Forming | MembersReady | Ready | 成员、容量与准备条件已验证 |
| Ready | ReadinessLost | Forming | 当前成员/准备条件失效 |
| Ready | StartBattle | Running | 资源就绪且新Battle已准备 |
| Running | BattleEnded | Forming | 当前Battle已终结，清理完本轮准备状态 |
| Forming | RequestClose | Closing | 关闭已被owner批准 |
| Ready | RequestClose | Closing | 关闭已被owner批准 |
| Running | RequestClose | Closing | 关闭已被owner批准 |
| Closing | CloseCompleted | Closed | 全部房间资源/待处理操作已清理 |

**表外转换全部拒绝。**

### Battle

[实现](../../domain/include/arena/domain/Battle.h)。

| 状态 | 含义 | owner | 读/写线程 |
| --- | --- | --- | --- |
| Prepared | 对局资源已准备，尚未开始 | 所属 Room.RoomActor | Owning RoomActor / same tick executor，构造线程 |
| Running | 权威模拟进行中 | 所属 Room.RoomActor | Owning RoomActor / same tick executor，构造线程 |
| Finished | 结果已固定，终态 | 所属 Room.RoomActor | Owning RoomActor / same tick executor，构造线程 |
| Aborted | 中止，无正常胜负结果，终态 | 所属 Room.RoomActor | Owning RoomActor / same tick executor，构造线程 |

| 当前状态 | 事实事件 | 下一状态 | owner提交事件前须验证 |
| --- | --- | --- | --- |
| Prepared | Start | Running | 所属Room接受本对局开始 |
| Running | Finish | Finished | 权威模拟固定最终结果 |
| Prepared | Abort | Aborted | 中止已由Room owner批准 |
| Running | Abort | Aborted | 中止已由Room owner批准 |

**表外转换全部拒绝。**

### Settlement

[实现](../../domain/include/arena/domain/Settlement.h)。

| 状态 | 含义 | owner | 读/写线程 |
| --- | --- | --- | --- |
| Pending | 已接收可结算结果，尚未应用 | Storage.SettlementCoordinator | SettlementCoordinator shard executor，构造线程 |
| Applying | 执行结果应用/等待提交确认 | Storage.SettlementCoordinator | SettlementCoordinator shard executor，构造线程 |
| RetryWait | 可重试失败，等待下一次尝试 | Storage.SettlementCoordinator | SettlementCoordinator shard executor，构造线程 |
| Committed | 结果应用得到持久化提交确认，终态 | Storage.SettlementCoordinator | SettlementCoordinator shard executor，构造线程 |
| Failed | 不可恢复失败/重试预算耗尽，终态 | Storage.SettlementCoordinator | SettlementCoordinator shard executor，构造线程 |

| 当前状态 | 事实事件 | 下一状态 | owner提交事件前须验证 |
| --- | --- | --- | --- |
| Pending | BeginApply | Applying | 结算结果与幂等键已校验 |
| Applying | CommitConfirmed | Committed | 存储事务确认已提交（不是只发出写请求） |
| Applying | RetryableFailure | RetryWait | 当前应用发生可重试失败 |
| RetryWait | RetryDue | Applying | 退避已到期，重试预算仍有效 |
| Applying | PermanentFailure | Failed | 不可恢复失败/预算耗尽已确认 |
| RetryWait | PermanentFailure | Failed | 不可恢复失败/预算耗尽已确认 |

**表外转换全部拒绝。**

## 跨 owner 调用与线程规则

~~~text
调用方/网络线程
  → 不可变命令 {entityId, event, expectedRevision, 业务证明}
  → 目标registry按entityId定位并投递到固定owner
  → owner验证业务事实 → entity.apply
  → 复制Snapshot/结果返回调用方
~~~

这是接线契约，当前没有实现消息总线/registry或跨服务RPC。禁止外线程持实体指针读取状态；对象寿命也由owner注册表管理，任务不得持悬空指针。销毁应在owner确认任务结束后发生，模型本身没有socket、timer或回调资源，也不会自动取消外部任务。

Session恢复到新连接时不迁移Session owner；新连接可以有另一个I/O worker，应把恢复请求交回原SessionRegistry owner。迟到旧连接回调携带旧绑定时取得的revision，不能在收到时改成当前版本来绕过栅栏。未来租约timer必须验证当前租约及deadline；版本变化后重建timer或重新验证有效到期事实，不能凭这个状态机自行产生到期事件。

Room与它的Battle必须由同一RoomActor串行推进；Storage回调不能直接改Battle，Room tick也不能直接改Settlement。共享线程不让两对象迁移自动成为事务：未来RoomActor需要先验证整体前置条件，再推进二者，处理中途失败。模型当前不保证跨实体/跨进程原子性。

没有state mutex/atomic，因为合法读写限定在同一thread；外线程只读不可变身份或使用已复制snapshot。shared_ptr延长寿命不授权跨线程访问，普通线程池如果切换执行线程也不满足固定owner契约。

## 必须能解释的边界场景

1. **旧连接在恢复后才报告断开**：Session已Online rev2，旧TransportLost rev0返回StaleRevision，不能再次Detach。Expired/Revoked即使携带新revision也不能恢复。
2. **已取消匹配，分配回调才到达**：旧Assign被版本拒绝；用新版本重试仍IllegalTransition。Assigned后不Release回Queued；新匹配使用新Ticket。
3. **准备阶段成员变化**：Ready→Forming；Running不能再StartBattle。Battle结束后Room→Forming重置本轮准备，但已Finished的Battle不能重启。
4. **关闭与新命令竞争**：owner确定排序；Closing只能CloseCompleted，不能恢复Ready/Running；Closed无出口。
5. **结算暂时失败**：Applying→RetryWait→Applying，原SettlementId保持。只有持久化提交确认才能Committed；Committed不允许再次提交或失败。
6. **结算失败/重复回调**：终态不复活；revision拒绝旧调用。状态图不能代替数据库幂等键/事务，不能从本测试宣称“奖励写库exactly once”。
7. **Player暂停不等于所有对象自动关闭**：是否撤销Session/取消Ticket/驱逐Room由策略明确发事件；不能暗含跨对象副作用。

## 测试与硬验收

`tests/domain/LifecycleTest.cpp` 的Spec保存独立字面量矩阵；预期值不读取生产rules数组。reach按规格找路径，通过公共apply到每个状态，不能靠测试setState。测试枚举完整状态×声明事件集，而不是只挑几条成功路径。

- 六对象共27个状态、36条合法边。
- 128个状态/事件组合：36个应接受、92个应拒绝；拒绝后snapshot和revision不变。
- 每个状态遍历0..255事件值，共6784个未声明值检查，均拒绝。
- 每状态验证foreign snapshot/apply、stale version；每条成功边验证旧命令不能重放。
- 初态/owner/ID类型/引用/禁止copy-move、实体隔离，以及重连、取消、房间多局、结算重试和版本耗尽均有测试。
- 50项GTest入口，CTest按独立 `arena_domain_tests` target发现，不把这些模型放入网络负载测试。

~~~bash
cmake --preset gcc15
cmake --build build --target arena_domain_tests -j 2
ctest --test-dir build -R '^(Domain|OwnedStateMachine)' --output-on-failure
~~~

Sanitizer验证使用现有gcc15-asan preset构建相同target，并带detect_leaks=1/halt_on_error执行。实际运行结果记录在本次Obsidian Change Note；本表没有把尚未实现的业务事实检查算成已通过。
