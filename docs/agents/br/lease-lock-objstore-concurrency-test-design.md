# Lease Lock Objstore 并发测试设计

## 状态

第一阶段测试设计，用于把 `lease-lock-concurrency-and-ha-test-direction.md` 中的
`pkg/objstore` correctness proof 方向展开成可实现的 deterministic concurrency tests。

本文档只覆盖第一阶段：`pkg/objstore` 层的确定性并发与 HA 语义测试。不展开固定 seed
随机乱序测试，也不展开 BR shell / 真实集群 HA 测试。

## 背景

当前 lease lock 已经具备以下测试基础：

- 单次 acquire / conflict / unlock 的基础语义；
- read/write/truncate/append lock family 的兼容性和 instance path 语义；
- lease clock 驱动的 acquire、renewal、stale cleanup；
- renewal bounded write timeout、post-write proof、retry/backoff、lease lost；
- stale cleanup 只 reclaim eligible 32 hex instance lock，不删除 legacy / intent /
  unknown protected-prefix object；
- BR 集成测试覆盖真实 PD clock、真实 BR command 和 `onLeaseLost` cancel 路径。

这些测试仍然缺少一类证明：当多个 owner 同时 acquire、renew、unlock、lose lease 和 cleanup
交错时，lock 层是否仍然维持临界区 safety invariant。

第一阶段应优先在 `pkg/objstore` 层补齐这个 proof。BR 集成测试成本更高、变量更多，适合证明
真实链路可用，不适合承担主要并发算法证明。

## 术语约束

本文档沿用根目录 `CONTEXT.md` 中的 lease-lock 术语：

- **业务临界区安全** 指 holder 只有在能证明自己的 lease window 仍然有效时，才可以继续执行受
  lock 保护的业务写入或删除。
- **物理 lock 实例** 指一次 acquire 创建并由一个 holder 持有的具体 lock 身份。
- **truncate lock** 专指 BR log truncate 流程外层的 `truncating.lock.<generation>` 物理
  lock 实例，不包含 truncate 流程内部可能再次获取的 migration write lock。

因此，第一阶段的主目标是业务临界区安全和物理 lock 实例生命周期安全，不把“对象存储中永远不会
出现 zombie object”提升成全局保证。`Unlock` / renewal 交错测试里的 zombie 只指一个具体风险：
正常释放删除自己的物理 lock 实例后，已经被要求停止的 renewal goroutine 不能再把同一路径写回来。

## 目标

第一阶段测试需要证明：

1. 互斥类 lock 的受保护临界区同时最多只有一个 owner。
2. migration read/read 可以并存，read/write 不能同时进入冲突临界区。
3. truncate lock 与同类 truncate protected action 不能并存。
4. append migration write lock 同时最多只有一个 writer。
5. owner 丢失 lease 后不会继续执行受保护动作。
6. `Unlock` 与 renewal 交错时不会重新写出 zombie physical lock。
7. stale cleanup 与 live holder 并发时不会误删 live lock。
8. stale cleanup 只 reclaim past `ExpireAt + staleReclaimGrace` eligible 32 hex instance。
9. 并发失败路径不留下会阻塞后续 acquire 的 `.INTENT.` lock。

## 非目标

- 不在第一阶段实现固定 seed 随机乱序测试。
- 不在第一阶段增加 BR shell / 真实 TiKV / 真实 PD HA 场景。
- 不覆盖真实 S3/GCS/Azure eventual consistency 的所有组合。
- 不做 N 个 owner 全排列式 exhaustive interleaving。
- 不用真实生产 TTL 做等待测试。
- 不为了测试并发而重构生产 lock API。

## 测试位置

主要测试文件：

```text
pkg/objstore/locking_test.go
```

如果 helper 明显膨胀，可以后续拆出同 package 测试 helper 文件，例如：

```text
pkg/objstore/locking_concurrency_test.go
```

第一阶段优先复用现有 helper：

- `createMockStorage`
- `sequenceLeaseClock`
- `controlledWriteStorage`
- `failWriteAfterStorage`
- `requireListedPathsWithPrefix`
- `requireSinglePathWithPrefix`
- `getLockMetaForTest`
- `writeLockMeta`
- lease constants test setters
- existing failpoint hooks around `conditionalPut` and renewal write

## 测试模型

### Owner

测试中的 owner 是一个轻量状态机：

```text
owner = goroutine + ownerID + acquired RemoteLock + protected action state
```

owner 只有在 acquire 成功后才能进入受保护动作。acquire 失败的 owner 不允许进入临界区。

### Critical Section Audit

测试使用隔离的内存状态表示受保护业务区，而不是依赖真实 BR side effect：

```text
ownerHistory
```

建议用一个小 helper 只记录受保护动作的 audit log：

```text
recordEnter(ownerID, family, lockInfo, detail)
recordExit(ownerID, family, lockInfo, detail)
recordStep(ownerID, family, lockInfo, detail)
recordLost(ownerID, family, lockInfo, detail)
snapshot()
```

实际实现时，audit helper 不引入不同于生产 acquire 的测试接口。测试仍直接调用
`TryLockRemoteRead`、`TryLockRemoteWrite`、`TryLockRemoteTruncate`、`LockWithRetry` 或
`LockRemoteTruncate`。acquire 成功后，调用方自己决定传入什么诊断文本，例如
`RemoteLock.String()`、storage listing 得到的 physical path、或空字符串。audit 不接收
`*RemoteLock`，不读取 unexported path，也不把 storage listing 封装进 audit。

建议接口形状：

```text
recordEnter(ownerID, family, lockInfo, detail)
recordExit(ownerID, family, lockInfo, detail)
recordStep(ownerID, family, lockInfo, detail)
recordLost(ownerID, family, lockInfo, detail)
snapshot()
```

helper 内部只负责并发安全地追加事件，不定义任何互斥规则。互斥和 lease-lost 语义由每个具体
testcase 基于 audit snapshot 断言。每条 audit event 必须包含：

- owner id；
- lock family；
- lock diagnostic string, provided by caller；
- action；
- detail；
- deterministic interleaving 名称。

已确认边界：

- audit helper 是事实记录器，不是 lock 规则裁判。
- audit helper 不判断 read/write/truncate/append 是否互斥。
- audit helper 不阻止任何 owner 记录事件。
- audit helper 不提供替代 acquire API，不包装生产 lock API。
- audit helper 不接收 `*RemoteLock`，不依赖 test-only accessor 读取 `RemoteLock.path`。
- audit helper 只保存调用方传入的信息，不主动发现 lock 信息。
- 每个 testcase 自己决定如何解释 audit snapshot。
- 若后续实现发现事件扫描逻辑重复，再补只读 assertion helper；第一阶段设计先不展开。

### Assertion Sources

并发测试不能单独依赖 audit log。audit 只能证明测试代码观察到并记录了哪些受保护动作，不能证明
object storage 的真实状态，也不能证明失败路径没有残留。

每个 testcase 应组合三类证据：

1. acquire result：哪些 owner 成功、哪些 owner 失败，错误是否符合预期；
2. audit log：成功 owner 是否进入 / 退出 / 停止 protected action，lost 后是否还有 step；
3. storage state：physical lock、`.INTENT.`、stale/live object 是否符合预期。

示例口径：

- read/write 并发不能只断言 audit 中没有 writer enter；还要断言 acquire result 和
  `.INTENT.` 残留状态。
- renewal lost 不能只断言 lost 后没有 step；还要断言 `onLeaseLost` 被调用、protected loop
  的 context 被 cancel，stop reason 是 `lease_lost`，并避免主动 `Unlock` 掩盖 lost 状态。
- cleanup live holder 不能只断言 audit 中 holder 仍在 step；还要断言 cleanup 运行过、live
  physical lock 仍存在、holder 后续 unlock / acquire 行为符合预期。

cleanup 本身不是 holder 的 protected action，所以 cleanup case 的主要 oracle 是 storage
state。audit 只用于证明 live holder 在 cleanup 发生前后仍处于受保护动作中。

### Holder Progress Evidence

测试需要明确证明 holder 的 protected work 是仍在推进，还是已经停止。不要只用“没有看到更多
日志”这类弱证据。

建议用一个测试专用 `protectedWorker` 模拟受保护动作。它不执行真实 BR side effect，只在测试
显式请求时记录 monotonic step。不要使用 `time.Ticker` 自动推进，避免测试依赖 sleep 和调度。

建议模型：

```text
holder worker goroutine:
  loop until context is canceled
  each requested step records a monotonic step event
  on exit records a stopped event and closes stopped channel
```

建议接口：

```text
startProtectedWorker(ctx, audit, ownerID, family, lockInfo) -> worker
worker.requestStep() -> step number / ok
worker.stop(reason)
worker.lostCh()
worker.waitStopped()
worker.stopReason()
```

`requestStep` 由测试显式调用，worker 在收到请求后记录 `step-N` 并返回 N。若 worker 已经停止，
`requestStep` 应返回 not ok，而不是阻塞。

worker stop reason 必须区分：

```text
test_stop
lease_lost
```

stop reason 采用 first-wins 语义，避免测试 cleanup stop 覆盖真实 lease lost，或真实 lease lost
覆盖已经开始的测试收尾。`onLeaseLost` callback 必须先记录 lost evidence，再 cancel worker：

```text
onLeaseLost:
  worker.stop(lease_lost)
    record lost event
    close lostCh
    cancel worker context
```

测试主动结束 worker 时使用 `worker.stop(test_stop)`。renewal-lost case 只有在观察到 `lostCh`
关闭且 `worker.stopReason() == lease_lost` 后，才能接受 worker stopped 作为 lease lost 证据。

证明 holder 仍在工作：

```text
holder enter protected loop
holder records step N
test triggers cleanup and waits for cleanup attempt to return
holder records step N+1 after cleanup attempt returns
test stops holder loop
```

只有出现 cleanup 返回后的 `N+1` step，才说明 holder 在 cleanup 期间没有被误删 lock 后停掉。
如果 case 只需要证明 cleanup 没删除 live physical lock，可以不要求 `N+1` step，但必须明确该
case 的 oracle 是 storage state，而不是 protected action progress。

证明 holder 已停止 protected work：

```text
holder records step N
lease lost or test stop signal fires
if lease lost: lostCh closes and stop reason is lease_lost
holder records stopped event
stopped channel closes
requestStep returns not ok after stopped
test captures final snapshot and verifies no step after stopped event
```

`stopped` channel 是停止的正向证据；“一段时间内没有新 step”只能作为补充的稳定性检查，不能单独
作为 holder 已停止的证明。

### Clock

第一阶段使用 deterministic lease clock：

- 正常 acquire / renewal 使用 local clock 或 `sequenceLeaseClock`；
- stale cleanup 使用固定 lease time；
- lease lost 测试使用缩短的 lease constants；
- 不依赖真实 30 分钟或 60 分钟 TTL。

### Storage Wrapper

并发交错通过以下方式控制：

- `exclusive-write-commit-to-1` 和 `exclusive-write-commit-to-2` failpoint 控制 acquire
  intent / commit 顺序；
- storage wrapper 卡住 renewal `WriteFile`；
- storage wrapper 或 failpoint 注入 renewal write error；
- channel / barrier 控制 owner 进入、停留、退出临界区；
- local object storage 检查 physical lock 和 `.INTENT.` 残留。

### Cleanup Entry Points

当前代码中的 cleanup 入口分两类：

- `CleanUpStaleTruncateLock` 直接暴露 truncate family cleanup；
- migration write / migration read / append write family cleanup 通过 `LockWithRetry` 在 acquire
  失败后内部触发 `tryCleanUpStaleLockFamily`。

第一阶段测试默认优先使用现有 public / exported-for-test API，不为了测试直接扩大生产 API。若某个
case 必须精确断言 migration / append cleanup 的返回值或 candidate error，再考虑在
`export_test.go` 中新增 `TEST...` helper，而不是改 production API。

已确认边界：

- 第一阶段不新增 migration / append family cleanup 的 test-only direct helper。
- migration / append cleanup 通过第二个 owner 的 `LockWithRetry` 触发，并断言 object side
  effect 与后续 acquire 行为。
- 如果要触发 live / stale `v1/LOCK.READ.<32hex>` 的 cleanup，第二个 owner 必须尝试 migration
  write lock；migration read/read 可以并存，不会触发 cleanup。
- 对 migration / append family，第一阶段只证明 cleanup 的 observable side effect，不精确证明
  cleanup attempt 的返回时刻或内部 candidate error。
- 只有当后续实现必须精确断言内部 `candidate error` 或 cleanup 返回值时，才补充
  `export_test.go` 中的 `TEST...` helper。

## Deterministic Test Matrix

### 1. Concurrent Acquire Exclusion

建议测试名：

```text
TestLeaseLockConcurrentAcquireExclusion
```

目的：

- 证明 family verifier 在并发 acquire 下没有 false negative。
- 证明冲突组合不会同时进入受保护临界区。

覆盖矩阵：

| 组合 | 预期 |
| --- | --- |
| truncate vs truncate | 最多一个成功 |
| migration write vs write | 最多一个成功 |
| append write vs write | 最多一个成功 |
| migration read vs read | 两个都可成功 |
| migration read vs write | 不能同时进入冲突临界区 |

实现要点：

- 新写 `TestLeaseLockConcurrentAcquireExclusion`，不继续扩展现有 `TestConcurrentLock`。
- 复用现有 `TestConcurrentLock` 的 failpoint barrier 思路。
- 两个 acquire attempt 分别卡在 `exclusive-write-commit-to-1` / `exclusive-write-commit-to-2`。
  现有 failpoint 是全局 phase barrier，不能保证固定 owner 一定命中特定 phase。
- 固定释放顺序，让测试稳定覆盖 intent before commit 和 commit before verify 的交错。
- 成功 owner 通过 `criticalSectionAudit` 和 `protectedWorker` 记录受保护动作。
- 冲突组合断言最多一个 owner 成功；read/read 断言两个 reader 都成功。
- 测试结束后检查对应 family 下没有 `.INTENT.` 残留。

已确认边界：

- `TestConcurrentLock` 保留现有历史覆盖，不继续塞 audit、protected worker 和 storage oracle。
- 第一阶段 proof 使用新的 `TestLeaseLockConcurrentAcquireExclusion` 承载。

### 2. Critical Section Audit Self-check

建议测试名：

```text
TestLeaseLockCriticalSectionAuditSelfCheck
```

目的：

- 验证测试专用 audit helper 在并发 record / snapshot 下不丢事件。
- 让后续并发测试可以复用同一个极小的业务临界区观测模型。

覆盖行为：

- record enter / step / exit 会按调用写入 history；
- snapshot 返回稳定副本，不暴露内部 slice；
- 并发 record 不丢失事件；
- helper 不判断 read/write/truncate/append 互斥关系。

实现要点：

- 这个测试不访问 object storage，不使用 failpoint；可以使用少量 goroutine 只测试 audit 的并发
  record 能力。
- 直接调用 `recordEnter` / `recordStep` / `recordExit` 验证事件字段完整。
- 使用少量 goroutine 并发 record，断言 snapshot 事件数和 owner set 正确。
- 不在 helper self-check 中断言任何 lock-family 互斥规则。

这个 self-check 只证明测试工具自身记录能力正确，不替代 lock 行为测试。lock 行为仍由具体
deterministic interleaving case 基于 audit log 证明。

### 3. Renewal Lost Stops Protected Action

建议测试名：

```text
TestLeaseLockRenewalLostStopsCriticalSection
```

目的：

- 证明持锁 owner 在 renewal 永久失败后会停止受保护动作。
- 证明 `onLeaseLost` 的 cancel 语义在 lock 层可被测试到，不依赖 BR CLI。

覆盖故障：

| 故障 | 预期 |
| --- | --- |
| renewal `WriteFile` block | bounded timeout 后触发 lease lost |
| renewal `WriteFile` error | retry/backoff 结束或 proven window 耗尽后触发 lease lost |

第一阶段覆盖 family / entrypoint：

| family | entrypoint |
| --- | --- |
| migration write | `LockWithRetry(..., TryLockRemoteWrite, "v1/LOCK", ...)` |
| truncate | `LockRemoteTruncate` |

migration read 和 append migration write 不单独覆盖 renewal lost。它们复用同一个 `RemoteLock`
renewal 机制；第一阶段用 migration write 覆盖 retry acquire 入口，用 truncate 覆盖 direct truncate
入口。

实现要点：

- 使用 `LockWithRetry` 或 `LockRemoteTruncate` 启动 renewal。
- owner acquire 成功后进入 protected loop。
- `onLeaseLost` 通过 `worker.stop(lease_lost)` 记录 lost event、关闭 `lostCh`，再 cancel owner
  context。
- protected loop 每一步先检查 context，再记录 protected action step。
- 注入 renewal write block / error。
- 等待 `onLeaseLost` 被调用。
- 等待 `lostCh` 关闭，并断言 `worker.stopReason() == lease_lost`。
- 记录 lost 时的 protected action step 数。
- 断言 lost 后 step 数不再增加。

注意：

- 不应通过主动 `Unlock` 掩盖 lost 后的 lock 文件状态。
- lock 文件是否留给后续 stale reclaim 是另一个语义，不应在本 case 里强行清理成成功状态。

### 4. Unlock Waits For In-flight Renewal

建议测试名：

```text
TestLeaseLockUnlockWaitsForInFlightRenewal
```

目的：

- 证明 `Unlock` 会等待 renewal goroutine 退出。
- 证明 delete 后不会被 in-flight renewal write 重新写出 zombie physical lock。

现有 `TestUnlockWaitsForRenewalGoroutine` 已覆盖相近语义。第一阶段应先评估它是否已经满足下列
断言；如果不足，再扩展该测试。

目标交错：

```text
renewal reads old meta
renewal write is blocked by storage wrapper
main goroutine calls Unlock
Unlock waits for renewal goroutine to stop
DeleteFile removes physical lock
blocked renewal cannot write the same path back after DeleteFile
later acquire is not blocked by zombie lock
```

该 case 必须显式缩短 renewal write bound，例如通过 `TESTSetRenewalProofConstants` 或等价 helper
降低 write timeout cap / min remaining lease，避免 in-flight renewal write 使用生产级 timeout。

关键断言：

- `Unlock` 不会在 renewal goroutine 未退出时返回；
- original physical lock path 最终不存在；
- 同 family 后续 acquire 成功或至少不被同一 zombie path 阻塞；
- family 下没有 `.INTENT.` 残留。

### 5. Stale Cleanup Does Not Delete Live Holder

建议测试名：

```text
TestLeaseLockStaleCleanupDoesNotDeleteLiveHolder
```

目的：

- 证明 stale cleanup 与 live holder 并发时不会误删 live lock。

覆盖 family：

- truncate instance；
- migration write instance；
- migration read instance；
- append migration write instance。

实现要点：

- acquire 一个真实 live lock，保持 `ExpireAt` 在 lease clock 看来仍然 live。
- 对 truncate family，直接并发运行 `CleanUpStaleTruncateLock`。
- 对 migration write / append family，通过第二个 owner 的 `LockWithRetry` 触发内部 cleanup。
- 对 migration read instance，通过第二个 owner 尝试 migration write lock 触发 cleanup；不能用
  migration read contender，因为 read/read 兼容。
- cleanup 返回后检查 live physical path 仍存在。
- cleanup 返回后重新读取原 physical path 的 metadata，断言 `TxnID` 仍匹配 cleanup 前记录的 holder
  `TxnID`。
- cleanup 返回后 holder 的 `Unlock` 必须成功，证明原 holder 仍能释放自己的物理 lock 实例。
- holder 随后正常 unlock。
- 后续 acquire 能成功，证明没有 cleanup / unlock 交错造成残留 blocker。

如果当前 cleanup 没有会等待后再二次确认的流程，本 case 不虚构不存在的“双读等待”语义；只证明当前
协议下最重要的 live-lock non-deletion。

注意：cleanup live-holder 的 primary oracle 是 storage state，不是 worker progress。cleanup
返回后的 `N+1` step 只能证明 protected worker 仍可推进，不能证明 live lock 没被删除。必须同时
断言：

```text
original physical path still exists
metadata TxnID still equals the holder's original TxnID
holder Unlock succeeds
```

### 6. Stale Cleanup Reclaims Only Expired Eligible Instance

建议测试名：

```text
TestLeaseLockStaleCleanupReclaimsOnlyExpiredEligibleInstance
```

目的：

- 证明 cleanup 的删除边界严格遵守 family acquire 协议。

覆盖对象：

| 对象 | 预期 |
| --- | --- |
| past `ExpireAt + staleReclaimGrace` 32 hex instance | 可 reclaim |
| live 32 hex instance | 不删除 |
| legacy fixed path | 不删除 |
| legacy 16 hex read lock | 不删除 |
| `.INTENT.` object | 不删除 |
| unknown protected-prefix object | 不删除 |
| malformed JSON instance | 不删除 |
| zero `ExpireAt` instance | 不删除 |

实现要点：

- 可以复用并收敛现有 cleanup case，避免重复宽表测试。
- truncate family 的删除边界可由 `CleanUpStaleTruncateLock` 直接断言。
- migration / append family 的删除边界可先通过 `LockWithRetry` 触发 cleanup 并断言 object
  side effect；只有在需要区分 candidate error 语义时才新增 test-only helper。
- 新增重点是同时放置多个 candidate，证明 cleanup 不因为遇到 malformed / unknown 对象而停止后续
  eligible reclaim。
- cleanup 后再尝试 acquire，确认剩余 blocker 行为符合预期：该 blocked 就 blocked，不应被错误放行。
- malformed JSON / zero `ExpireAt` 的 precise candidate error 不通过当前 public truncate cleanup
  API 断言；当前阶段只断言 object side effect。如需精确断言 candidate error，需补 test-only helper。

### 7. Intent Cleanup Postcondition

建议 helper：

```text
requireNoIntentWithPrefix(t, storage, prefixes...)
```

目的：

- 证明并发 acquire 失败路径不会留下 `.INTENT.` lock，避免后续 acquire 被测试制造的 orphan intent
  阻塞。

覆盖：

- same physical target conflict；
- write/write conflict；
- read/write conflict；
- truncate/truncate conflict；
- append write/write conflict。

实现要点：

- 不作为独立第一批 testcase。
- 作为每个 concurrent acquire testcase 的 common postcondition。
- 等待并发 acquire goroutine 返回后，对相关 protected prefix 做 listing。
- helper 应使用 `WalkDir` 且设置 `IncludeTombstone: true`，与 acquire verifier 的 listing 语义一致。
- 断言没有 path 包含 `.INTENT.`。
- 如果 committed lock 存在，只能是成功 owner 的 physical lock。

## 第一阶段测试顺序

推荐按以下顺序实现，便于 review 和定位失败：

1. `criticalSectionAudit`、`protectedWorker` 和 intent listing helper。
2. `TestLeaseLockCriticalSectionAuditSelfCheck`。
3. `TestLeaseLockConcurrentAcquireExclusion`。
4. `TestLeaseLockRenewalLostStopsCriticalSection`。
5. `TestLeaseLockUnlockWaitsForInFlightRenewal` 的现有覆盖评估与必要扩展。
6. `TestLeaseLockStaleCleanupDoesNotDeleteLiveHolder`。
7. `TestLeaseLockStaleCleanupReclaimsOnlyExpiredEligibleInstance` 的收敛或补强。

## 验收口径

第一阶段完成后，应能明确回答：

1. 多 owner 并发 acquire 不会让互斥临界区重入。
2. read/read 与 read/write 兼容矩阵在并发 acquire 下仍成立。
3. owner 丢失 lease 后不会继续推进受保护动作。
4. unlock / renewal 交错不会留下 zombie physical lock。
5. cleanup 与 live holder 并发不会误删 live lock。
6. cleanup 只 reclaim past `ExpireAt + staleReclaimGrace` eligible 32 hex instance。
7. 并发失败路径不会留下 `.INTENT.` 残留。

## 验证命令

第一阶段实现后，优先运行 scoped package tests。由于 `pkg/objstore` 使用 failpoints，运行单测前后
需要按 repository testing flow 启用 / 禁用 failpoints。

建议 WIP 验证：

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock|TestConcurrentLock|TestRWLock|TestConflictLock|TestUnlockWaitsForRenewalGoroutine|TestCleanUpStaleTruncateLock|TestLockWithRetry' -count=1
```

WIP 时应优先运行实际新增或修改的 test names；上面的 regex 只是覆盖本设计相关测试族的示例。

完成前使用 Ready profile。若有代码变更，Ready profile 必须运行：

```bash
make lint
```

若实现新增 top-level Go test function、添加 / 移动 / 重命名 / 删除 Go 文件、修改 Bazel metadata，
或修改 `go.mod` / `go.sum`，按仓库规则需要运行：

```bash
make bazel_prepare
```

## 与后续阶段的关系

第一阶段只补 deterministic correctness proof。固定 seed 随机乱序测试应在这些 deterministic
case 稳定后再设计，作为 coverage amplifier。BR HA / 真实压力测试应在 lock 层 proof 补齐后再挑选
少量高价值场景，作为 system confidence，而不是替代 lock 层 correctness proof。
