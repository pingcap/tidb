# Lease Lock Family Acquire 协议

## 背景

`lease-lock-instance-generation-design.md` 已经确定要把当前已有的业务锁从固定路径改成物理实例路径：

- `truncating.lock.<generation>`
- `v1/LOCK.WRIT.<generation>`
- `v1/LOCK.READ.<generation>`
- `v1/APPEND_LOCK.WRIT.<generation>`

但是 acquire 不是单纯换文件名。它依赖 `conditionalPut` 的 intent 协议，而现有 `conditionalPut` 只理解单个 `Target` 的写入过程，不理解 lock family 的 read/write/truncate 语义。这个文档专门定义 acquire 阶段的 family classifier、intent 阻塞规则、冲突错误和 cleanup 分层。renewal 不属于本文协议；它不迁移 generation，只原地刷新当前物理实例的 `ExpireAt`。

基础设计参考：`docs/design/2024-10-11-put-and-verify-transactions-for-external-storages.md`。

## 已确认事实

- `conditionalPut` 的 intent 文件名是 `<Target>.INTENT.<txnID>`。
- `conditionalPut` 会在写 intent 前后调用 `Verify()`，并通过 `assertOnlyMyIntent()` 检查同一个 `Target` 前缀下没有其他 object，除了当前事务自己的 intent。
- `assertOnlyMyIntent()` 只保护当前 exact `Target` 前缀，不能表达 `truncating.lock`、`v1/LOCK`、`v1/APPEND_LOCK` 的 family 关系。
- 现有 write acquire 的 verify 已经会扫描可能冲突的 prefix；read 阻塞 write 不是新行为。
- 现有错误补充逻辑会在 `TryLockRemoteWrite("v1/LOCK")` 失败后尝试读取固定 `v1/LOCK.WRIT` 的 meta。这只是诊断信息逻辑，不决定互斥正确性；在 family instance 模型下不应继续依赖固定 target 读取 meta。
- `v1/LOCK` exact key 历史上不存在，未来也不应作为 migration lock member。
- `v1/APPEND_LOCK` 只有 write lock；不存在 `v1/APPEND_LOCK.READ.*`。

## 目标

新的协议需要同时满足：

- prefix listing 只作为候选收集，不能定义 family 语义。
- family classifier 必须决定候选 object 是否属于当前 lock family。
- stale cleanup 只清理 cleanup-eligible 的新 32 hex committed instance member，不把 intent 当 `LockMeta` 清理。
- acquire 不能绕过按现有 read/write 语义应当冲突的 `conditionalPut`。
- conflict error 只需要 family-level 信息；可以记录 first observed member 做诊断，但不能赋予“唯一阻塞者”语义。

## 协议模型

### Conditional Put 分层

`conditionalPut.assertOnlyMyIntent()` 保持当前 exact-target 语义，不升级为 family-aware 检查。instance-generation 会让 write/truncate lock 的 `Target` 从逻辑锁的固定写入点变成某次 acquire 的物理实例路径，因此 exact-target 检查不再承担逻辑锁互斥职责。

分层规则：

- `conditionalPut.Target` 表示本次事务要写入的物理 object key。
- `assertOnlyMyIntent()` 只保护这个 exact physical target 前缀的 put-and-verify 过程，例如同 target 并发写入、generation 碰撞、或同 target 下的 unknown object。
- logical lock 的互斥正确性必须由各 acquire 的 `Verify()` callback 通过 family classifier 和 conflict rule 表达。

因此，instance-generation 改造安全的前提是：`TryLockRemoteWrite`、`TryLockRemoteRead`、`TryLockRemoteTruncate` 的 family verifier 必须覆盖当前语义下会冲突的 committed lock member 和 intent member。

### Family Classifier

classifier 输入：

- family：`truncating.lock`、`v1/LOCK`、`v1/APPEND_LOCK`
- object key：listing 返回的 key

classifier 输出应至少区分：

- committed lock member
- intent member
- non-member

prefix listing 只用于收集候选 object key，不能直接定义 stale cleanup 的删除边界。cleanup 必须只删除被 classifier 明确判定为 cleanup-eligible 的 committed instance member 的 object；共享 prefix 但不符合 family 格式的 object 不能被 cleanup 删除。

acquire conflict check 可以比 cleanup 更保守：如果候选 object 共享当前 family 的受保护前缀，但 classifier 无法确认它是安全 non-member，应宁可让 acquire 失败并重试/返回冲突，也不要错误放行。换句话说，cleanup 需要严格避免误删；acquire 允许 false positive，但不能有 false negative。

当前受保护前缀定义为：

```text
truncating.lock family: truncating.lock
v1/LOCK family:        v1/LOCK.WRIT, v1/LOCK.READ
v1/APPEND_LOCK family: v1/APPEND_LOCK.WRIT
```

例如，`v1/LOCKXYZ` 不属于 `v1/LOCK` 的受保护前缀；`v1/LOCK.WRITER_NOTES` 落在 `v1/LOCK.WRIT` 受保护前缀下，acquire 可以保守视为冲突，但 cleanup 不能删除它。

`truncating.lock` family 保守占用 `truncating.lock` 前缀。正常业务不应在该命名空间下写入 `truncating.lock.backup` 这类 unrelated object；如果出现，acquire 可以保守失败，cleanup 仍只能删除合法 truncate lock member。

当前已确认的 committed member：

```text
truncating.lock
truncating.lock.<32hex generation>

v1/LOCK.WRIT
v1/LOCK.WRIT.<32hex generation>
v1/LOCK.READ.<16hex suffix>
v1/LOCK.READ.<32hex generation>

v1/APPEND_LOCK.WRIT
v1/APPEND_LOCK.WRIT.<32hex generation>
```

新 generation 固定为 32 hex：`16 hex unix_nano + 16 hex crypto random`。`truncating.lock` 和 `WRIT` lock 不接受 16 hex suffix；只有 `READ` lock 需要兼容当前 `math/rand.Int63()` 生成的 16 hex suffix。新的 `READ` lock 也应使用 32 hex generation，16 hex 仅作为旧格式兼容。

intent 文件名格式继续保持当前 `conditionalPut` 的 `<target>.INTENT.<txnID>`，不引入新的 intent 文件名。对于当前 family，intent member 来自对应 committed target 的 intent 文件：

```text
truncating.lock.INTENT.<txnID>                   old fixed-path truncate intent
truncating.lock.<32hex>.INTENT.<txnID>           new truncate instance intent

v1/LOCK.WRIT.INTENT.<txnID>                      old fixed-path migration write intent
v1/LOCK.WRIT.<32hex>.INTENT.<txnID>              new migration write instance intent
v1/LOCK.READ.<16hex>.INTENT.<txnID>              old migration read intent
v1/LOCK.READ.<32hex>.INTENT.<txnID>              new migration read instance intent

v1/APPEND_LOCK.WRIT.INTENT.<txnID>               old fixed-path append write intent
v1/APPEND_LOCK.WRIT.<32hex>.INTENT.<txnID>       new append write instance intent
```

acquire verifier 可以保守处理共享受保护前缀的 unknown intent-like object；cleanup 仍然跳过 intent-like object，不把它当 `LockMeta` 删除。

协议应保持当前 intent 兼容语义，不因为物理实例路径改造而额外收紧 read/read 并发：

- write acquire 当前通过 `Verify()` 扫描逻辑前缀，因此会被 read lock、read intent、write lock、write intent 阻塞。
- read acquire 当前只扫描 write target 前缀，因此会被 write lock 和 write intent 阻塞，但不会被其他 read lock 或 read intent 阻塞。
- standalone acquire 当前只检查同一 target 前缀下的 lock 和 intent；迁移到 `truncating.lock.<generation>` 后，应保持 truncate lock 互斥语义，但不引入与本问题无关的新 intent 规则。

### Acquire Conflict Check

规则：

1. 通过 prefix listing 收集候选 object key。
2. 用 family classifier 过滤和分类。
3. acquire verifier 不做 stale 判断，不根据 `ExpireAt` 放行，也不删除任何 object。只要候选 member 按业务语义构成冲突，就返回 conflict；stale 判断和 reclaim 只发生在 cleanup 层。
4. committed lock member 按已有业务语义判断冲突：
   - `truncating.lock` family 中任何 committed member 都阻塞 truncate acquire。
   - `v1/LOCK.WRIT` acquire 被 `v1/LOCK` family 中 read/write committed member 阻塞。
   - `v1/LOCK.READ` acquire 被 `v1/LOCK` family 中 write committed member 阻塞。
   - `v1/APPEND_LOCK.WRIT` acquire 被 `v1/APPEND_LOCK` family 中 write committed member 阻塞。
5. intent member 按当前 read/write 兼容语义参与冲突判断：write acquire 被 read/write intent 阻塞；read acquire 只被 write intent 阻塞，不被 read intent 阻塞。当前事务自己的 `ctx.IntentFileName()` 必须被忽略，否则 `conditionalPut` 写入 intent 后的第二次 `Verify()` 会自冲突。不要为了 instance-generation 改造改变 read/read 并发行为。
6. conflict error 返回 family-level 信息，例如 “conflicting member exists in `v1/LOCK` family while acquiring write lock”。不要在 `CommitTo` 失败后读取固定 target 来补充 lock meta。

### Conflict Error

family conflict 不应强行复用只携带单个 `LockMeta` 的 `ErrLocked` 模型。新增 family-level conflict error，用于提供 first observed blocker 诊断信息。

建议字段：

```text
Family       logical lock family, such as v1/LOCK
AcquireKind  read / write / truncate
MemberPath   first observed blocker path, for diagnostics only
MemberKind   read / write / truncate / unknown
IsIntent     whether the blocker is an intent
```

`MemberPath` 只是 first observed blocker，不表示唯一阻塞者。family verifier 不读取 `LockMeta`；对于 intent、unknown protected-prefix object 等情况，error 仍可作为 conflict 分类使用，但不应伪造 `LockMeta`。

未知 logical path 是调用方错误，不是 family conflict。`TryLockRemoteRead` / `TryLockRemoteWrite` 遇到未定义 family 时应返回普通 invalid/internal error；family cleanup 遇到未定义 family 时应 no-op，不应尝试按任意 prefix 清理。`LockWithRetry` 的 retry/backoff 行为应尽量保持现状。

### Cleanup

cleanup 与 acquire conflict check 分离：

- `LockWithRetry` 保持现有行为：acquire 返回错误后可以无条件尝试一次 family cleanup，然后进入现有等待/重试流程。cleanup 自身必须先确认 logical family 是已定义 family，并且只 reclaim cleanup-eligible 的新 32 hex committed instance；未知 logical path 的 cleanup 应 no-op。
- `CleanUpStaleTruncateLock(ctx, storage)` 只清理 `truncating.lock.<32hex>` 新 instance member，不清理旧固定路径 `truncating.lock`。
- 旧固定路径 lock 和旧格式 `v1/LOCK.READ.<16hex>` 只参与冲突判断，不自动 stale cleanup。本分支尚未上线，不需要兼容中间态 lease fixed-path 文件；cleanup 应只 reclaim 新的 32 hex instance lock member。
- 旧 lock 的冲突处理应尽量保持现有行为：返回冲突，并通过 blocker path / manual unlock 提示引导人工确认清理。family verifier 不读取 `LockMeta`。
- 例如，旧 `v1/LOCK.READ.<16hex>` 阻塞 write acquire 时，只返回冲突并引导 manual unlock；不要因为它看起来像 instance path 就自动 reclaim。
- cleanup 遇到 unknown protected-prefix object 时跳过并记录 warning，不删除它，也不把它作为 cleanup error 返回。后续 acquire conflict error 应暴露 blocker path，供用户人工判断。
- cleanup 遇到新 32 hex instance 但 metadata 缺少 `ExpireAt` 时，应按 metadata 读失败 / JSON parse 失败同级别处理：返回该 candidate 的 cleanup error，因为这种文件不应由新实现产生；family cleanup / `LockWithRetry` 只记录该错误并继续处理其它 candidate，不中断主流程。后续 acquire 仍会被该 blocker 阻塞，并通过 conflict error 暴露。
- family cleanup 的 candidate error 不进入 `LockWithRetry` 的最终返回错误。最终返回应保留 acquire / retry exhausted 的主错误；cleanup error 只作为日志诊断，避免掩盖真正的 blocker。
- cleanup 跳过 intent member。本次改造不扩大到 orphan intent 残留问题；intent 没有 `LockMeta` / `ExpireAt`，独立 intent expiry 或 cleanup 需要单独设计。

cleanup eligibility:

```text
can cleanup if stale:
  truncating.lock.<32hex>
  v1/LOCK.WRIT.<32hex>
  v1/LOCK.READ.<32hex>
  v1/APPEND_LOCK.WRIT.<32hex>

never auto cleanup:
  truncating.lock
  v1/LOCK.WRIT
  v1/APPEND_LOCK.WRIT
  v1/LOCK.READ.<16hex>
  *.INTENT.*
  unknown protected-prefix object
```

### Suggested Internal Structure

实现上应保持 acquire/verify 流程和 cleanup 流程分离。两者可以复用 listing 和 classifier helper，但 `Verify()` 不能调用 cleanup，也不能删除 object。

Acquire / verify flow:

```text
listLockFamilyCandidates
  -> classifyLockFamilyMember
  -> isConflictForAcquire
  -> return nil or family-level conflict error
```

Cleanup flow:

```text
listLockFamilyCandidates
  -> classifyLockFamilyMember
  -> filter cleanup-eligible 32 hex committed instance
  -> cleanupStaleInstance
```

helper 责任边界：

- `listLockFamilyCandidates` 只做 prefix listing，收集候选 object key，不判断语义。
- acquire / verify 使用的 listing 应保持当前 `assertNoOtherOfPrefixExpect` 的保守行为，设置 `IncludeTombstone: true`。如果 tombstone 导致短暂 false positive，按冲突处理并重试。
- cleanup 使用的 listing 保持当前行为，不额外设置 `IncludeTombstone: true`。
- `classifyLockFamilyMember` 把候选 key 归类为 committed member、intent member、unknown protected-prefix object、或 non-member。
- `isConflictForAcquire` 只按 acquire kind 和 member kind 判断是否冲突，并忽略当前事务自己的 intent；它不读 `ExpireAt`，不删除 object。
- `cleanupStaleInstance` 只处理已经确认 cleanup-eligible 的 32 hex committed instance，负责读取 `LockMeta`、检查 `ExpireAt + LeaseTTL`、并在满足条件时删除该 instance。

## 已确认决策摘要

- `conditionalPut.assertOnlyMyIntent()` 保持 exact-target 语义，不升级为 family-aware；logical lock mutual exclusion 由 acquire-specific `Verify()` 负责。
- acquire conflict check 可以保守失败，宁可 false positive，不允许 false negative；cleanup 必须严格避免误删。
- acquire verifier 不做 stale 判断，不读取 `ExpireAt` 放行，也不删除 object。
- intent 文件名格式保持 `<target>.INTENT.<txnID>`；本次不设计 orphan intent cleanup。
- cleanup 只 reclaim 新的 32 hex instance lock；旧 fixed-path lock、旧 `v1/LOCK.READ.<16hex>`、intent、unknown protected-prefix object 都不自动删除。
- 新 32 hex instance 缺 `ExpireAt`、metadata 读失败、JSON parse 失败都作为 candidate cleanup error 记录，但不进入 `LockWithRetry` 的最终返回错误。
- 新增 family-level conflict error，用于提供 first observed blocker 诊断信息；family verifier 不读取 `LockMeta`，`LockWithRetry` 也不需要依赖该错误类型来决定是否调用 cleanup。
