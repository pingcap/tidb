# Lease Lock Renewal Bounded Write Proof 设计

## 状态

初版设计草案，用于后续讨论。

这份文档收窄 60 分钟 lease TTL 调整之后的后续工作。目标是保障业务临界区安全，
而不是证明对象存储中永远不会出现 delayed renewal zombie。

## 背景

当前分支已经使用 generation-based 物理 lock 实例，并且让 BR 正确性路径使用共享
`LeaseClock`。持有者会 acquire 一个具体 lock 对象，例如
`v1/LOCK.WRIT.<generation>`，renewal 会在同一个物理对象上原地刷新。

已经接受的 TTL 策略是：

```text
LeaseTTL = 60 * time.Minute
renewInterval = LeaseTTL / 3
staleReclaimGrace = 30 * time.Minute
stale cleanup threshold = ExpireAt + staleReclaimGrace
```

在 60 分钟 TTL 下，普通的慢 PD 取时或慢对象存储写入不太容易耗尽整个 lease
窗口。剩下最重要的风险不是普通慢请求，而是无上界或非常延迟的 renewal write。
renewal write 可能长时间阻塞，也可能在持有者已经没有有意义 lease 窗口之后才返回。
持有者在这种情况下不能继续执行受 lock 保护的业务操作。

## 目标

保障 **业务临界区安全**：

```text
holder 只有在能证明自己的 lease window 仍然有效时，才可以继续执行受该 lock
保护的业务写入或删除。若无法证明有效 lease window，holder 必须停止受保护的业务临界区。
```

设计应该通过已有的 `onLeaseLost` 回调，以及 BR 调用点已经使用的 child context
cancel，把 renewal 失效暴露给业务层。

## 非目标

- 本阶段不实现 cleanup tombstone。
- 本阶段不增加 strict strong-consistency lock option。
- 不证明 delayed renewal zombie 永远不会出现在对象存储里。
- 本阶段不把手工命令 `br operator migrate-to` 迁移到 PD-backed lease time。
- 不改变 acquire 冲突语义，不让 acquire 忽略 tombstoned 或 zombie lock 文件。
- 不给所有对象存储操作全局套一层新的 timeout 策略。

## 安全模型

Delayed renewal zombie 在这里被视为可用性和清理延迟问题，而不是主要的互斥安全问题。

如果一次 renewal write 在本地超时，但之后仍然落到了对象存储里，发起它的进程必须
已经进入终止态丢锁状态，并且必须已经 cancel 掉业务临界区。后续 acquire 可能会保守地
看到这个旧物理对象并阻塞，直到 stale cleanup 能 reclaim 它。按照当前 cleanup 规则，
这可能把进展延迟到 `ExpireAt + staleReclaimGrace` 之后，但不应该让旧 holder 继续写受保护数据。

因此，这里的安全边界是进程本地的：

- 一旦 holder 不能再证明有效 lease 窗口，它就进入丢锁状态。
- 丢锁状态对这个 `RemoteLock` 是终止态。
- 晚到的 storage success 不能把 lock 恢复成 healthy。
- 丢锁后的 cleanup 是 best-effort，不改变已经丢锁的结论。

## 阶段划分

本设计拆成两个阶段，避免第一阶段被 `RemoteLock` lifecycle 重构拖大。

第一阶段只实现收益明确、局部可验证的 bounded renewal proof：

- renewal `WriteFile` 使用 bounded context；
- renewal 成功返回后做 post-write proof；
- post-write proof 后要求至少 1 分钟 remaining lease；
- 下一轮 renewal delay 基于已证明的 remaining lease；
- stale cleanup grace 从 `LeaseTTL` 拆出，设为 30 分钟。

第二阶段再评估 `RemoteLock` lifecycle 和 cleanup 语义：

- lost 后是否主动 best-effort 删除自己的物理 lock 实例；
- 是否统一正常 `Unlock` 和 lease lost 的终止流程；
- 是否引入 `first terminal reason wins`；
- 是否拆分 terminal outcome 和 renewal loop control。

第一阶段的实现计划不应依赖第二阶段完成。

## 第一阶段：Bounded Renewal Proof

### 1. Renewal Write 使用 Bounded Context

每次 renewal attempt 都应该为 storage `WriteFile` 派生一个有上界的 context。
这个 deadline 应该安全地落在当前已经证明的 lease window 内，而不是一个和 lease
无关的固定 wall-clock timeout。

第一版使用简单保守的公式：

```text
remainingOldLease = oldExpireAt - nowBeforeWrite
renewWriteTimeoutCap = 5 * time.Minute
writeTimeout = min(renewWriteTimeoutCap, remainingOldLease / 2)
```

如果没有正数 timeout window，renewal 应该立刻报告 lease lost，而不是尝试一次无法
在可信窗口内完成的写入。

5 分钟 cap 相对 60 分钟 TTL 足够小，不允许一次很小的 lock metadata write 消耗大部分
lease window；它也大于当前 renewal transient retry backoff 总量，避免普通短暂抖动过于
容易触发丢锁。`remainingOldLease / 2` 则保证越接近旧 lease 结束，write timeout 越保守。

如果 bounded write context 超时，这次 renewal 不应该被当成普通 transient retry。
它应该让这个 `RemoteLock` 进入终止态 lease lost。

### 2. 成功返回的 Renewal Write 需要 Post-Write Proof

`WriteFile` 成功返回之后，renewal 应该立刻再次从同一个 `LeaseClock` 读取 lease time。

刷新后的 metadata 使用：

```text
newExpireAt = nowBeforeWrite + LeaseTTL
```

post-write proof 检查：

```text
nowAfterWrite <= newExpireAt
remainingNewLease = newExpireAt - nowAfterWrite
minRenewRemainingLease = 1 * time.Minute
```

如果 post-write 取时失败，或者刷新后的 lease 已经过期，lock 进入终止态丢锁状态。
如果 `remainingNewLease <= minRenewRemainingLease`，也进入终止态丢锁状态。1 分钟相对
60 分钟 TTL 很小，正常路径不会触发；但它避免 holder 在极薄的已证明窗口里继续执行业务
临界区。

### 3. Renewal Scheduling 使用已证明的剩余窗口

一次经过证明的 renewal 成功后，下一轮 delay 必须基于 `remainingNewLease`，不能盲目
使用固定的 `renewInterval`。

一个简单规则是：

```text
nextDelay = min(renewInterval, remainingNewLease / 3)
```

如果 `nextDelay <= 0`，renewal loop 应该按照上面选定的阈值策略立刻重试或声明丢锁。

这可以避免某次 renewal 返回时只剩很小的已证明 lease 窗口，holder 却仍然 sleep 固定
20 分钟。

### 4. Cleanup Grace

Cleanup reclaim grace 和 lease TTL 分开建模：

```text
staleReclaimGrace = 30 * time.Minute
cleanup threshold = ExpireAt + staleReclaimGrace
```

`LeaseTTL` 表示 holder 可以证明自己仍然持有 lock 的有效窗口；`staleReclaimGrace`
表示 lease 已过之后，cleanup 额外等待多久再自动 reclaim 物理 lock 实例。两者不需要相等。

30 分钟是安全性和可用性的折中：它比继续等待完整 60 分钟 `LeaseTTL` 更快释放 crash 或
zombie lock，又比很短的 grace 更保守，给异常调度、对象存储延迟和 delayed write 留出余量。

### 5. 第一阶段范围

Bounded storage operation timeout 第一阶段只覆盖 renewal `WriteFile`。

`Unlock` cleanup 和 acquire proof cleanup 暂不纳入本阶段的 timeout 语义：

- renewal write 是持有者仍在临界区期间的续约路径，直接关系到业务临界区安全；
- `Unlock` 发生在退出临界区后的释放路径，阻塞主要是 cleanup / availability 问题；
- acquire proof cleanup 发生在 acquire 失败路径，失败不会让 holder 进入临界区。

后续可以单独设计 cleanup operation timeout，但不把它混入第一阶段的业务安全目标。

## 第二阶段候选：RemoteLock Lifecycle 重构

下面内容是后续候选设计，不属于第一阶段必须实现的范围。实现第一阶段时可以参考这些语义，
但不应为了它们扩大第一阶段 scope。

### 1. 统一 RemoteLock 终止流程

本阶段不应该在现有 `Unlock` / `renewalLoop` 旁边再补一条独立的 lost cleanup 分支。
更好的方向是重构 `RemoteLock` 的终止流程，让正常释放和 lease lost 共用同一套 lifecycle
收口逻辑。

可以把终止原因建模为两类：

```text
finishNormalUnlock
finishLeaseLost
```

统一的终止流程负责：

- 状态转换：running -> stopped 或 running -> lost。
- 确保终止动作只执行一次。
- 关闭或通知 renewal loop。
- 根据终止原因决定是否调用 `onLeaseLost`。
- 根据终止原因决定是否删除自己的物理 lock 实例。
- 根据终止原因决定错误是返回给 caller，还是只记录日志。

终止流程采用 `first terminal reason wins`：

```text
terminal outcome:
    none
    released
    lost

renewal loop control:
    idle
    running
    stopping/exited
```

`terminal outcome` 是业务语义，表示这个 `RemoteLock` 是否仍然代表持锁状态，以及最终是正常
释放还是丢锁。`renewal loop control` 是 goroutine lifecycle，表示 renewal loop 是否正在
运行、是否被要求停止、是否已经退出。两者不应该混在一个 enum 里。

如果 `finishNormalUnlock` 先赢，后续 renewal attempt 因 stop/cancel 返回，只能被解释为正常
shutdown signal，不能再升级成 lease lost。如果 `finishLeaseLost` 先赢，后续 `Unlock` 只能观察
到已经 terminal，不再走 strict unlock 错误路径。

外层流程变成：

```text
Unlock(ctx):
    return finish(normal unlock, ctx)

renewalLoop detects lost:
    finish(lease lost, background cleanup ctx)
    return
```

更具体的流程是：

```text
Unlock(ctx):
    outcome := finish(normal unlock)
    if outcome already lost:
        return nil or log only
    request renewal stop
    wait renewal loop exits
    strict delete own physical lock

renewalLoop:
    if stop requested:
        exit without lost
    if renewal proof fails / write timeout / retry exhausted:
        finish(lease lost)
        exit
```

实现阶段需要重新评估重构粒度。如果完整拆分 `terminal outcome` 和 `renewal loop control` 让
scope 过大，可以先做最小可行改造，但必须保留上面的语义：只有第一个终止原因生效，normal
unlock 的 stop/cancel 不能被误解释成 lease lost，lease lost 先发生时后续 unlock 不能再按正常
strict unlock 报噪音错误。

### 2. 终止态丢锁结果

`RemoteLock` 应该在现有 idle/running/stopped renewal state 之外，增加一个明确的终止态
lost result。lost 是 `finishLeaseLost` 的结果，而不是一条独立 cleanup 路径。

下面任一情况都应该触发 `finishLeaseLost`，并让 lock 恰好进入一次 lost result：

- renewal write 之前发现旧 lease 已经过期；
- bounded renewal write 超时；
- post-write lease proof 失败；
- 现有 retry policy 下 renewal retry 耗尽；
- 观察到物理 lock 对象里的 TxnID 和本地不一致。

进入 lost result 应该只调用一次 `onLeaseLost`。之后即使 storage 调用成功返回，或者
goroutine 晚些时候醒来，也不能把 lock 从 lost 恢复成 running 或 stopped，仿佛它仍然
healthy。

### 3. 终止流程中的 Own-Lock 删除

正常释放和 lease lost 都可以复用同一个底层删除 primitive，例如：

```text
deleteOwnPhysicalLock(ctx, mode)
```

不同终止原因决定不同 mode：

```text
finishNormalUnlock:
    strict verify/delete; error returned to caller

finishLeaseLost:
    best-effort verify/delete; error logged only
```

这应该被建模为 cleanup，而不是安全证明本身：

- 先进入终止态 lost。
- 先调用 `onLeaseLost` / cancel 业务。
- 再 best-effort 删除 `l.path`。
- 删除失败只记录日志，不改变 lost state。
- 本地超时之后，如果 delayed renewal write 仍然成功落盘，它可能重新制造 zombie；
  这个 zombie 后续可以由 family cleanup reclaim。

这个 cleanup 不会误删后续 holder 的 lock：当前协议下，每次 acquire 都创建唯一的物理
lock 实例，后续 acquire 不会复用旧 holder 的 `l.path`。因此 lost holder 删除自己的
exact physical path 只影响自己的实例；如果对象已经不存在，删除可以视为 cleanup 已完成。

正常 unlock 和 lease lost 共用删除 primitive，但不直接从 renewal goroutine 调用 public
`Unlock`：

```text
Unlock:
    finish(normal unlock) -> strict deleteOwnPhysicalLock

renewal lease lost:
    finish(lease lost) -> onLeaseLost -> best-effort deleteOwnPhysicalLock
```

`finishLeaseLost` 的顺序固定为：

```text
mark lost -> call onLeaseLost -> best-effort delete own physical lock
```

业务临界区安全优先于 cleanup。删除自己的物理 lock 实例是 availability cleanup，不能挡在
`onLeaseLost` 前面；即使 cleanup 慢、失败或之后出现 delayed renewal zombie，业务也已经先
收到 lease lost。

lease lost 删除前应尽量读取 lock metadata 并确认 TxnID 仍然属于自己；如果读取
失败、对象不存在、或 TxnID 不匹配，则跳过删除并记录日志。跳过删除不改变 lost state，
也不影响业务临界区安全。

## 残余风险

这个设计不消除 delayed renewal zombie。它有意接受 zombie 作为 holder 停止业务之后的
cleanup-delay artifact。

残余行为是：

- 本地超时的 renewal write 之后仍可能落到对象存储。
- 后续 acquire 可能看到这个物理 lock 并保守冲突。
- stale cleanup 应该在它超过 `ExpireAt + staleReclaimGrace` 后最终删除它。
- 进展可能被延迟，但旧 holder 不应该继续修改受保护的 migration 状态。

这个取舍只有在目标是业务临界区安全时才成立。如果目标变成完整的 storage-protocol
证明，要求 delayed renewal zombie 不影响后续 acquire，那么 cleanup tombstone 和更强的
visibility 假设仍然会重新变得重要。

## 建议验证

第一阶段验证：

- 单元测试：renewal `WriteFile` 阻塞超过 bounded context 后进入 lost state，并且
  `onLeaseLost` 只调用一次。
- 单元测试：renewal write 在刷新后的 `ExpireAt` 之后才返回时进入 lost state。
- 单元测试：一次成功 renewal 如果只剩很小 lease window，下一轮 renewal 根据剩余窗口
  调度，而不是固定 `renewInterval`。
- 单元测试：cleanup threshold 使用 `ExpireAt + staleReclaimGrace`，而不是
  `ExpireAt + LeaseTTL`。
- 现有 BR stream migration 测试应继续证明 lease loss 会 cancel 受保护的 child context。

第二阶段候选验证：

- 单元测试：terminal loss 之后的 late success 不能 revive lock。
- 单元测试：best-effort own-lock cleanup 失败会被记录或暴露给测试 hook，但不阻止 lost
  state。
