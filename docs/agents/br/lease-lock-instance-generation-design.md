# Lease Lock 实例 Generation 设计

## 背景

lease-lock 改动引入了自动 stale lock 清理。固定路径 lock 文件在这种清理模型下并不安全，因为 stale cleanup 会先读取 lock metadata，之后在没有 compare-and-delete 原语的情况下删除同一个路径。

对于固定路径 lock，两个等待者可能同时观察到同一个旧的 stale 文件。其中一个等待者可以删除这个 stale 文件，并在同一路径获取一个新的 lock。另一个等待者随后继续执行自己的 stale-cleanup 流程，就可能把这个新的 lock 删除掉。

下面的设计让业务调用方继续使用逻辑 lock path，同时把实际物理 lock 文件改成基于实例的文件。

## 术语

- **逻辑锁路径 / logical lock path**：业务代码传入的锁命名空间。本设计只讨论当前已有的 `truncating.lock`、`v1/LOCK`、`v1/APPEND_LOCK`。
- **物理锁实例路径 / physical lock instance path**：object store 中实际写入的 lock object key，例如 `truncating.lock.<generation>`、`v1/LOCK.WRIT.<generation>`。它不是新的业务锁类型，只是已有逻辑锁的一次 acquire 实例。renewal 原地刷新该实例的 `ExpireAt`，不生成新的 generation。`RemoteLock.path` 保存的是当前物理锁实例路径。
- **锁族 / lock family**：同一个逻辑锁路径下所有参与冲突判断的物理文件集合，包括新实例文件以及需要兼容的旧格式固定路径文件。是否允许 stale cleanup 删除，由 cleanup eligibility 单独决定；旧格式 lock 只参与冲突判断，不自动删除。

## 目标

- 业务侧 lock API 尽量保持不变。
- 防止 stale cleanup 删除刚刚获取到的固定路径 lock。
- 让 read-lock 文件命名尽量接近现有格式。
- 覆盖 `truncating.lock`，而不只覆盖 `v1/LOCK` / `v1/APPEND_LOCK` 的 `READ` 和 `WRIT` lock。
- 兼容旧 lock 文件。
- 避免引入 compare-and-delete 这类新的 storage API。

## 迁移范围

本设计应一次覆盖当前已有、会复用固定写路径的 lock writer：

- `TryLockRemoteTruncate(ctx, storage, hint)` 写入 `truncating.lock.<generation>`。
- `TryLockRemoteWrite(ctx, storage, "v1/LOCK", hint)` 写入 `v1/LOCK.WRIT.<generation>`。
- `TryLockRemoteRead(ctx, storage, "v1/LOCK", hint)` 写入 `v1/LOCK.READ.<generation>`。
- `TryLockRemoteWrite(ctx, storage, "v1/APPEND_LOCK", hint)` 写入 `v1/APPEND_LOCK.WRIT.<generation>`。

旧格式 `truncating.lock`、`v1/LOCK.WRIT`、`v1/APPEND_LOCK.WRIT`、已有旧格式 `v1/LOCK.READ.<16hex>` 必须继续参与冲突判断，但不参与自动 cleanup。

## 非目标

- 不重新设计 restore、truncate 或 append 的业务语义。
- 不要求 object store 提供比当前 `Storage` interface 更强的原语。
- 本次改动不解决跨主机 clock skew 语义。当前 lease 判断使用本地进程时间；后续 review 可以考虑使用 PD time 或其他共享时间源。

## 文件命名

调用方继续传入逻辑 lock path：

- `v1/LOCK`
- `v1/APPEND_LOCK`
- `truncating.lock`

lock 层通过追加一个 generation suffix 字段来写入物理实例文件。

建议的 generation 格式：

```text
<unix_nano_hex><short_random_hex>
```

例如，UnixNano 使用 16 个 hex 字符，强随机 bits 使用 16 个 hex 字符：

```text
%016x%016x
```

这样 suffix 仍然是一个字段，同时对人工排查来说大致可按时间排序，对并发进程来说也有足够的抗冲突能力。随机部分应来自 `crypto/rand` 或等价强随机源，不应依赖 `math/rand`。

所有新写入的 `truncating.lock`、`READ`、`WRIT` 物理实例都应使用同一种 generation 格式。generation 只用于生成唯一物理实例路径和提升可观测性，不参与 lease 有效性判断；lease 是否有效仍以 lock metadata 中的 `ExpireAt` 为准。

物理文件名：

```text
Migration read lock    v1/LOCK.READ.<generation>
Migration write lock   v1/LOCK.WRIT.<generation>
Append write lock      v1/APPEND_LOCK.WRIT.<generation>
Truncate lock          truncating.lock.<generation>
```

`truncating.lock` 不需要额外的 `EXCL` 字段。`truncating.lock` 本身已经表达 truncate 业务锁，只追加 `<generation>` 可以让文件名变化保持最小。

## 业务 API 表面

大多数业务代码应继续使用现有 API：

- `TryLockRemoteRead(ctx, storage, logicalPath, hint)`
- `TryLockRemoteWrite(ctx, storage, logicalPath, hint)`
- `LockWithRetry(ctx, locker, storage, logicalPath, hint)`
- `RemoteLock.StartRenewal(ctx, onLeaseLost)`
- `RemoteLock.Unlock(ctx)`
- `RemoteLock.UnlockOnCleanUp(ctx)`

`RemoteLock.path` 应保存当前物理实例路径。获取 lock 之后，renewal 和 unlock 都操作这个物理路径，业务调用方不需要知道生成出来的 suffix。

`TryLockRemoteRead` / `TryLockRemoteWrite` 虽然保留现有签名，但实现上只支持本设计定义的 logical family：

- `TryLockRemoteRead` 只支持 `v1/LOCK`。
- `TryLockRemoteWrite` 只支持 `v1/LOCK` 和 `v1/APPEND_LOCK`。

未知 logical path 应返回错误，不应静默创建未定义 lock family。

`TryLockRemote` 和 `CleanUpStaleLock` 是本分支引入 lease cleanup 过程中留下的通用 exported API。当前业务侧 `TryLockRemote` 只用于 truncate lock；替换 truncate 调用后，应移除这两个 exported API，避免后续业务代码继续使用 fixed-path exclusive lock 或通用 stale cleanup 入口。必要的能力可以保留为 unexported helper：

- `tryLockRemoteExact(...)`：仅供内部测试或底层实现使用的 exact-path primitive。
- `cleanUpStaleLockInstance(ctx, storage, physicalPath)`：清理一个已经由 family classifier 确认的具体物理 lock 文件。

唯一需要暴露给业务侧的 stale cleanup API 应是 truncate 专用入口：

```go
CleanUpStaleTruncateLock(ctx, storage)
```

truncate acquire 也应使用不接收 path 的专用入口：

```go
TryLockRemoteTruncate(ctx, storage, hint)
```

这两个 API 内部固定使用既有 `truncating.lock` 逻辑锁路径，不接受任意 path，避免重新暴露通用 fixed-path exclusive lock。

当前 truncate 路径会在 `TryLockRemoteTruncate(ctx, storage, hint)` 前主动清理 stale truncate lock。`CleanUpStaleTruncateLock` 只应清理：

- `truncating.lock.<generation>`

`CleanUpStaleTruncateLock` 不清理旧固定路径 `truncating.lock`。旧固定路径是否 stale 无法由当前分支安全判断；当前分支尚未发布，也不存在需要兼容的中间态 lease fixed-path truncate lock。

`v1/LOCK` 和 `v1/APPEND_LOCK` 的 stale cleanup 是 `LockWithRetry` 的内部职责，不作为公开 API。实现上可以复用内部 helper 来列出 lock family 并清理一个具体物理 lock 文件，但不要把 migration / append lock family cleanup 暴露成业务侧 API。

内部 helper 仍然可以把实现拆成“列出 lock family”和“清理一个物理 lock 文件”两个操作。

## Lock Family 规则

lock family 是表示同一个逻辑 lock 的一组物理文件。本设计只定义 `truncating.lock`、`v1/LOCK`、`v1/APPEND_LOCK` 的 family。

`truncating.lock` 的 lock family：

```text
truncating.lock              旧固定路径 truncate lock
truncating.lock.<generation> 新 truncate lock 物理实例
```

`v1/LOCK` 的 lock family：

```text
v1/LOCK.WRIT                旧固定路径 migration write lock
v1/LOCK.WRIT.<generation>   新 migration write lock 物理实例
v1/LOCK.READ.<generation>   migration read lock 物理实例
```

`v1/APPEND_LOCK` 的 lock family：

```text
v1/APPEND_LOCK.WRIT                旧固定路径 append write lock
v1/APPEND_LOCK.WRIT.<generation>   新 append write lock 物理实例
```

family classifier、`conditionalPut` intent、acquire conflict check 的协议细节仍在细化中。该部分不要在本文中继续扩展；统一在 [Lease Lock Family Acquire 协议](./lease-lock-family-acquire-protocol.md) 中讨论和落地。

## 获取 Lock 流程

`truncating.lock` acquire：

1. 调用方调用 `TryLockRemoteTruncate(ctx, storage, hint)`。
2. lock 层生成 `truncating.lock.<generation>`。
3. `conditionalPut` 写入这个物理目标。
4. 它的 verify callback 扫描逻辑 family，并拒绝有冲突的 member。verify 不做 stale 判断。
5. 返回的 `RemoteLock.path` 是生成出的物理目标。

`v1/LOCK` 的 read / write acquire 使用相同模式，只是物理名分别使用 `READ.<generation>` 和 `WRIT.<generation>`。`v1/APPEND_LOCK` 只用于 append write lock，物理名使用 `WRIT.<generation>`。

## Stale Cleanup

stale cleanup 应只删除它已经读取并判定为可 reclaim 的物理实例路径。因为新的 holder 会使用新路径，stale cleanup 不再以新 holder 刚获取到的同一路径为目标。

兼容规则：

- 没有 `ExpireAt` 的旧 lock 文件永远不会被自动 reclaim。
- 旧固定路径 lock 文件不自动 reclaim，即使包含 `ExpireAt`。本分支尚未上线，不需要兼容中间态 lease fixed-path 文件；避免继续保留 fixed-path cleanup race 的过渡语义。
- 新实例文件使用同样的 stale threshold。

`LockWithRetry` 应该对它的逻辑 lock path 使用内部 family-aware cleanup。`RunStreamTruncate` 保持现有 acquire 行为：先调用 `CleanUpStaleTruncateLock(ctx, storage)` 主动清理一次 stale truncate instance，再调用 `TryLockRemoteTruncate(ctx, storage, hint)` acquire 一次；不要改成 `LockWithRetry` 的长重试语义。

对新的 instance lock，cleanup 保持当前 `ExpireAt + LeaseTTL` reclaim 规则即可，不引入额外 double-read 或 compare-and-delete 需求。合法 holder 的 renewal 在发现远端 `ExpireAt` 已过期时应判定 lease lost，不应继续刷新该 instance；在当前 `Storage` API 没有 compare-and-delete 的前提下，删除前再读一次也只能缩小窗口，不能从根本上消除删除竞态。instance path 的职责是避免 cleanup 删除后来新 acquire 的不同实例。

具体 cleanup eligibility 和错误处理规则以 [Lease Lock Family Acquire 协议](./lease-lock-family-acquire-protocol.md) 为准，避免在两个文档中重复维护。

## Renewal 流程

renewal 不迁移 generation。获取 lock 时生成的物理锁实例路径在该 `RemoteLock` 生命周期内保持不变；renewal 只原地刷新该物理实例中的 `ExpireAt`。

这是最终设计约束：generation 只在 acquire 时生成，不在 renewal 时生成。不要通过 renewal path migration 来解决 stale cleanup race。

基本流程：

1. 读取当前 `RemoteLock.path` 对应的 lock metadata。
2. 校验远端 `TxnID` 与本地 lock 匹配。
3. 校验远端 `ExpireAt` 尚未过期。
4. 写回同一个 `RemoteLock.path`，只刷新 `ExpireAt`。

这样 generation 只用于区分不同 acquire 实例，避免 stale cleanup 删除后来新 acquire 的 lock；renewal 不再引入新旧 generation 双实例、旧实例删除失败、`RemoteLock.path` 切换等复杂状态。更多 acquire / classifier / intent 细节见 [Lease Lock Family Acquire 协议](./lease-lock-family-acquire-protocol.md)。

## S3 和 Object Store 影响

这个设计只改变 object key，不需要新的 S3 操作。

预期变化：

- 更多操作会依赖 prefix listing 来做 lock-family 冲突检查和 stale cleanup。
- `truncating.lock` 会从一个精确 key 变成生成出来的实例 key。
- 人工排查时应按逻辑 prefix 搜索，而不是只查一个精确文件名。

`v1/LOCK` 的 read lock 已经使用生成出的 `READ` 文件，所以该 lock family 已经依赖 prefix-style listing。主要新增的 listing 影响来自 `truncating.lock`。

## 兼容性

lock 层必须继续识别旧 lock 文件：

- `truncating.lock`
- `v1/LOCK.WRIT`
- `v1/APPEND_LOCK.WRIT`
- 任何已有的旧格式 `v1/LOCK.READ.<16hex>`

没有 `ExpireAt` 的旧文件会被视为 old-client lock，并且在人工删除前保持有效。新的 cleanup 不应自动删除它们。

旧固定路径文件和旧格式 `v1/LOCK.READ.<16hex>` 只参与冲突判断，不参与自动 stale cleanup。本分支尚未上线，不需要兼容 instance-generation 改造前的中间态 lease fixed-path 文件。cleanup 只 reclaim 新的 32 hex instance lock。

遇到旧固定路径 lock 时，应尽量保持现有行为：把它作为有效冲突返回，并通过 blocker path / manual unlock 提示引导用户确认后清理，而不是自动按 lease 删除。family verifier 不读取 `LockMeta`。

## 需要新增或更新的测试

- `TryLockRemoteTruncate(ctx, storage, hint)` 应创建 `truncating.lock.<32hex>`，并 unlock 该物理路径。
- `TryLockRemoteWrite(ctx, storage, "v1/LOCK", hint)` 应创建 `v1/LOCK.WRIT.<32hex>`。
- `TryLockRemoteRead(ctx, storage, "v1/LOCK", hint)` 应创建 `v1/LOCK.READ.<32hex>`，不再生成新的 16 hex read lock。
- `TryLockRemoteWrite(ctx, storage, "v1/APPEND_LOCK", hint)` 应创建 `v1/APPEND_LOCK.WRIT.<32hex>`。
- acquire verifier 不做 stale 判断：即使冲突 member 已过期，也应先返回 family conflict，由 `LockWithRetry` cleanup 后再 retry。
- acquire verifier 应按现有语义处理 intent：write acquire 被 read/write intent 阻塞；read acquire 被 write intent 阻塞，但不被 read intent 阻塞；当前事务自己的 intent 必须被忽略。
- 旧固定路径 lock（`truncating.lock`、`v1/LOCK.WRIT`、`v1/APPEND_LOCK.WRIT`）和旧 `v1/LOCK.READ.<16hex>` 应参与冲突判断，但不被 stale cleanup 自动删除。
- 现有期望 stale fixed-path `v1/LOCK.WRIT` 或旧 `v1/LOCK.READ.<16hex>` 被删除的 cleanup 测试应反转或替换为“返回冲突、保留文件、提示 manual unlock”的测试。
- `CleanUpStaleTruncateLock(ctx, storage)` 只应 reclaim stale 的 `truncating.lock.<32hex>`，不删除旧 `truncating.lock`。
- `LockWithRetry` cleanup 只应 reclaim stale 的新 32 hex instance lock，不删除旧 fixed-path lock、旧 16 hex read lock、intent、unknown protected-prefix object；应覆盖 `v1/LOCK.WRIT.<32hex>`、`v1/LOCK.READ.<32hex>`、`v1/APPEND_LOCK.WRIT.<32hex>` 的 positive cleanup case。
- unknown protected-prefix object（例如 `v1/LOCK.WRITER_NOTES`、`truncating.lock.backup`）应保持 undeleted，并在 acquire 中产生带 blocker path 的 family conflict。
- 新 32 hex instance 缺 `ExpireAt`、metadata 读失败、JSON parse 失败应作为 candidate cleanup error 记录，但不进入 `LockWithRetry` 最终返回错误。
- mixed cleanup candidate 场景应验证 cleanup 遇到 malformed 32 hex instance 后继续处理后续 eligible stale instance；malformed object 保留并继续阻塞后续 acquire。
- stale cleanup 删除旧 instance 后，后来新 acquire 的不同 instance 不应被删除。
- `RunStreamTruncate` call site 应验证执行一次 `CleanUpStaleTruncateLock` 后单次调用 `TryLockRemoteTruncate`，不进入 `LockWithRetry` 长重试。
- `v1/APPEND_LOCK` 应覆盖 append write lock 的兼容和冲突测试。
- renewal 应原地刷新当前 `RemoteLock.path` 的 `ExpireAt`，并保持 generation 不变。
- renewal 在 `TxnID` mismatch、实例缺失、或 `ExpireAt` 已过期时应失败并触发现有 retry / lease-lost 逻辑。
