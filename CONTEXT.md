# ArenaServer Domain

ArenaServer 的玩家身份、可恢复会话、匹配、房间、战斗和结算使用以下术语。网络连接与这些业务对象分别具有自己的生命周期。

## Language

**Account**:
用于证明登录凭据归属的账户；当前一个账户只关联一个 Player，账户与玩家分别具有身份。
_Avoid_: Player、Session、某次连接中的登录状态。

**Item**:
有稳定标识和定义的可持有物品种类；同类物品可分别由不同 Player 持有。
_Avoid_: 某玩家背包中的数量记录、一次资产变更。

**Inventory**:
某个 Player 当前持有的物品数量集合；每种 Item 只有一个当前数量。
_Avoid_: 资产流水、奖励请求、结算本身。

**Player**:
稳定的玩家身份；连接更换、会话恢复或房间切换不产生新的 Player。
_Avoid_: 把 Socket、Connection、房间内角色快照称为 Player。

**Session**:
认证成功后建立、关联一个 Player 的登录会话；断线后可在有效租约内恢复，过期或撤销后不能复活。
_Avoid_: ConnectionSession、TCP 连接、凭据本身。

**Ticket**:
一个 Player 的一次匹配申请；预留尚可释放，分配成功或终止后不再重新排队。
_Avoid_: Session、Room、可无限复用的匹配请求。

**Room**:
组织成员、准备状态和战斗的逻辑房间；一个 Room 可依次承载多场 Battle。
_Avoid_: Battle、单个玩家角色、网络线程。

**Battle**:
某个 Room 内的一场权威对局；正常结束与中止是不同结果，结束的 Battle 不重新启动。
_Avoid_: Room、一次 tick、Settlement。

**Settlement**:
一场已结束 Battle 的结果应用过程；重试沿用同一个 Settlement 身份，提交确认是最终完成。
_Avoid_: Battle 结束事件、每次数据库重试新建的结算。

**Match**:
一次成功配对确定的对局及其固定参与者；当前每次 Match 对应一场 Battle。
_Avoid_: Ticket、可复用的 Room、匹配算法的一次扫描。

**SettlementEvent**:
某次结算已经提交的业务事实；发布或投递重试沿用同一事件身份。
_Avoid_: 一次发布尝试、尚未提交的结算意图。
