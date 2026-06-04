# Lease Lock Model Concurrency Test Design

## 状态

Phase 2 测试设计，承接第一阶段
`docs/agents/br/lease-lock-objstore-concurrency-test-design.md` 已落地的 deterministic
`pkg/objstore` 并发测试。

本文档只设计 `pkg/objstore` 层的 mock / model-based concurrency tests。不展开 BR shell
HA 测试。BR HA 测试作为后续 phase 3，根据本阶段发现和成本收益单独设计。

Renewal operation hang / failure 相关测试的执行计划见
`docs/agents/br/lease-lock-renewal-operation-tests-implementation-plan.md`。
Terminal reason stability / normal unlock race 相关测试的执行计划见
`docs/agents/br/lease-lock-terminal-tests-implementation-plan.md`。

## 背景

当前分支已经有三类 lease-lock 测试基础：

- `pkg/objstore/locking_test.go` 覆盖 acquire、renewal、stale cleanup、backward compatibility
  等细粒度语义；
- `pkg/objstore/locking_concurrency_test.go` 覆盖第一阶段确定性交错，包括 concurrent acquire
  exclusion、renewal lost stop、unlock / renewal lifecycle ordering、live cleanup 和 expired
  cleanup boundaries；
- `br/tests/br_lease_lock` 覆盖真实 BR command、真实 PD-backed lease clock、真实业务
  `onLeaseLost` cancel、正常 unlock 和 stale reclaim。

这些测试已经覆盖已知主路径和若干关键 interleaving。下一阶段的目标不是重复这些 case，而是用更强的
mock / model harness 扩大 interleaving 空间，尤其覆盖多个事件几乎同时发生时的终止语义和 storage
side-effect 边界。

## 目标

Phase 2 需要回答：

1. `Unlock`、renewal permanent loss、业务 `onLeaseLost` callback 竞态时，holder 是否仍遵守业务
   临界区安全。
2. stale cleanup 观察到旧 stale instance 后，新的 physical instance 被 acquire 时，不会被旧
   cleanup 决策误删。
3. 多 contender 同时 `LockWithRetry`、cleanup、acquire、fail 时，互斥类 lock 不会同时进入受保护
   临界区。
4. `.INTENT.` 创建、提交、失败、listing、cleanup 乱序时，不会留下阻塞后续 acquire 的 orphan
   intent。
5. 固定 seed 的小模型乱序测试可以在失败日志里提供足够信息来复现和定位。
6. holder 受保护业务循环死亡或长时间挂起时，测试能区分 safety 语义和 availability 语义：
   死亡且 renewal 停止后应由 stale reclaim 恢复可用；挂起但 renewal 仍存活时应阻塞 contender，
   不能伪装成 stale reclaim 成功。
7. holder 受保护业务循环正常推进但 renewal loop 长时间挂起时，protected work 不能无限越过已经证明的
   lease window；一旦无法继续证明有效 lease，必须触发 lease lost 或停止 protected work。
8. renewal proof 相关 operation 使用统一的 lease-bounded protection 口径，单次 operation timeout
   cap 设为 10 分钟，并继续受当前 proven lease window 约束。覆盖 lock metadata read、lease clock
   read、lock metadata write 和 post-write proof clock read。

## 非目标

- 不在本阶段新增 BR shell HA case。
- 不模拟真实 S3/GCS/Azure 的全部 consistency model。
- 不把随机测试作为唯一 correctness proof；随机乱序只作为 deterministic tests 的补强。
- 不改变生产 lock API。
- 不为了测试便利新增 migration / append cleanup 的 production API。
- 不追求 exhaustive state-space exploration；CI 默认规模必须短小稳定。

## 测试位置

主要放在：

```text
pkg/objstore/locking_model_concurrency_test.go
```

如果实现时发现 helper 可以与第一阶段共用，应优先复用
`pkg/objstore/locking_concurrency_test.go` 中的 audit / protected worker / intent listing helper。
若 helper 明显变成 model harness 专属，则放在新文件内，避免继续膨胀第一阶段测试文件。

由于新增 Go test file 和新的顶层 `TestXxx`，实现后需要运行 `make bazel_prepare` 并纳入
`pkg/objstore/BUILD.bazel` 变更。

## Harness 设计

### Model Owner

每个 owner 表示一个可能持锁的业务 actor：

```text
owner id
target family: truncate | migration-write | migration-read | append-write
lock handle: nil or *RemoteLock
terminal reason: none | normal_unlock | lease_lost | acquire_failed
protected work state: running | hung | dead
renewal loop state: idle | running | hung | stopped
protected steps: monotonic counter
```

owner 只有 acquire 成功后才能记录 protected step。若 terminal reason 已经设置，后续 step 必须失败。
若 protected work state 是 `hung`，测试不自动记录 protected step，只允许显式 release 后继续；若是
`dead`，owner 不再续约、不再 unlock，后续只能通过 lease expiry + stale cleanup 恢复可用。
若 renewal loop state 是 `hung` 且 protected work state 仍是 `running`，测试必须检查 protected step
是否仍处于可证明 lease window 内；不能只因为业务 goroutine 还活着就继续允许 protected work。

### Renewal Proof Operation Bound

当前代码基线（2026-06-04，renewal operation tests 已落地后）：

- renewal proof operation cap 已提高到 10 分钟。`renewalLoop` 使用 renewal-owned context 将 normal
  unlock cancellation 传入 `tryRenew`；各阶段 timeout 由 `tryRenew` 内部按 operation 语义负责。
- renewal `ReadFile` 和 pre-write lease clock `Now` 已有 bounded context。它们 timeout 后仍属于
  mutation 前 observation failure，由 `renewalLoop` 在 proven lease window 内重试。
- renewal `WriteFile` 已有 `renewalWriteTimeout` 保护；触发 `errRenewWriteTimeout` 后属于
  permanent renewal loss，`renewalLoop` 会直接调用 `onLeaseLost` 并退出。
- post-write lease proof clock read 失败或证明 `nowAfterWrite > newExpireAt` 时属于
  permanent renewal loss。
- renewal `WriteFile` 返回的非 timeout 普通错误也属于 transient error；只有 context deadline 已经超时
  时，当前代码才把它归类为 `errRenewWriteTimeout`。

renewal proof 相关 operation 应使用同一类 timeout policy：

```text
timeout = min(10min, remaining proven lease window / 2)
```

覆盖对象：

- lock metadata `ReadFile`；
- pre-write lease clock `Now`；
- lock metadata `WriteFile`；
- post-write proof lease clock `Now`。

如果 remaining proven lease window 已经不足以给 operation 建立正数 timeout，应进入 lease-lost
路径，而不是继续等待 storage / clock operation。

新增 bounded timeout 后，错误分类应继续区分 ordinary failure、non-mutating timeout 和 ambiguous
mutating timeout：

- ordinary read / pre-clock / write error 保持当前 transient retry 语义；
- lock metadata `ReadFile` 和 pre-write lease clock `Now` timeout 没有产生写入副作用，应作为
  transient renewal failure，由 `renewalLoop` 继续受 proven lease window / backoff 约束；单次读失败
  或读超时不能直接触发 `onLeaseLost`；
- lock metadata `WriteFile` timeout 保持当前 permanent renewal loss 语义，因为写入可能已经发生但
  holder 没有 proof；
- post-write proof clock read error / timeout 保持 permanent proof failure 语义，因为写入已经返回，
  但 holder 无法证明新 lease 窗口；这类失败必须停止 protected work。但 normal `Unlock`
  已经关闭 renewal stop signal 后导致的 `context.Canceled` 属于 shutdown artifact，不应重新触发
  业务 `onLeaseLost`。

读失败与写 timeout 的风险级别不同：读失败不会制造迟到写、不会复活旧 physical lock path，也不会改变
远端 `ExpireAt`；它只表示 holder 这一次没能观察到 lock metadata。因此读失败应该消耗 retry budget
和 proven lease window，而不是单次失败就判定 permanent lease loss。

pre-write lease clock `Now` 失败或 timeout 也属于 mutation 前的 observation failure：它只表示 holder
这一次没能获得可用于 renewal write 的 lease time，不应直接触发 `onLeaseLost`。

这个 policy 已在当前实现中落地：read / pre-clock / write / post-clock 采用统一
lease-bounded protection，operation cap 为 10 分钟，同时保持“一次普通读写失败不直接 lease lost”的
retry 语义。

测试上只需要验证统一 lease-bounded protection 的外部行为：

- read / pre-clock hang 到 timeout 后不会单次直接触发 `onLeaseLost`；
- read / pre-clock hang 会消耗 retry budget 和 proven lease window；
- write timeout 会触发 lease lost，并停止 protected work；
- post-write proof clock hang / failure 会触发 lease lost，并停止 protected work；
- 所有 operation 的单次等待上限都按 10 分钟 cap 与 remaining proven lease window 共同约束。

### Controlled Storage

新增测试专用 storage wrapper，包裹 local object storage，并支持在指定 operation / path 上设置 barrier：

```text
ReadFile(path) before/after barrier
WriteFile(path) before/after barrier
DeleteFile(path) before/after barrier
WalkDir(prefix) before/after barrier
```

barrier 不改变 storage 语义，只控制可复现 interleaving。每个 barrier 必须有：

- signal channel：证明测试到达该点；
- release channel：测试显式放行；
- timeout：避免测试永久挂住；
- event log entry：失败时能看到哪个 owner / operation / path 卡住。

### Model Event Log

记录每个可观察事件：

```text
seq
seed
owner id
action
family
physical path if known
terminal reason if set
storage operation if any
error summary
```

失败时打印：

- seed；
- step index；
- owner state table；
- object storage listing for protected prefixes；
- event log tail。

### Invariants

每个 deterministic case 和 model random step 都检查这些 invariant：

- truncate 同类临界区最多一个 active owner；
- migration write 与 migration write / migration read 不能同时 active；
- migration read 与 migration read 可以同时 active；
- append write 同类最多一个 active owner；
- terminal owner 不能继续 protected step；
- normal unlock 删除的只能是自己的 physical path；
- stale cleanup 只能删除 expired eligible 32-hex committed instance；
- cleanup 不删除 live holder 的 current physical path；
- failed acquire / canceled acquire 不留下 `.INTENT.` 残留；
- old-format fixed path 和 zero-`ExpireAt` instance 不被自动 reclaim。
- dead holder 停止续约后，contender 在 `ExpireAt + staleReclaimGrace` 前不能进入互斥临界区；
- dead holder 过期后，eligible physical instance 可以被 reclaim，后续 acquire 可以成功；
- hung holder 仍在续约时，contender 不能把它当 stale 删除；这属于 availability blocker，不是
  stale cleanup 的成功条件。
- renewal proof operation 长时间挂起时，受保护业务循环的 protected step 只能发生在最后一次
  proven lease window 内；超过该窗口后必须停止或进入 `lease_lost` terminal reason。

## 第一批 Deterministic 用例

### 1. Terminal Reason Stability

建议测试名：

```text
TestLeaseLockTerminalReasonStableAfterLeaseLost
```

目标：

验证 `lease_lost` 已经成为业务终止结果后，后续 `Unlock` 只是 cleanup / diagnostic 行为，不会把
终止结果覆盖为 normal unlock，也不会让受保护业务循环恢复。

这组用例是顺序语义测试，不需要构造复杂乱序。建议用表驱动覆盖三种代表性 lease-lost trigger：

1. `TxnID` mismatch：代表远端 lock ownership 已经不是当前 holder。
2. renewal `WriteFile` timeout：代表 mutation outcome ambiguous，late write 可能随后落盘。
3. post-write proof failed：代表 renewal write 已经返回成功，但 holder 无法证明新 lease window 安全。

统一流程：

```text
holder acquire
protected worker records one step
renewal triggers lease_lost
onLeaseLost stops protected worker
business calls Unlock after lease_lost
```

断言：

- `lease_lost` first terminal reason 不被后续 `Unlock` 覆盖；
- `Unlock` 返回值不等同于业务终止结果；
- `lease_lost` 后 protected step 返回 false；
- audit 中 `lost` 之后没有 `step`；
- 不能删除其他 holder 的 physical lock instance；
- 无 `.INTENT.` 残留。

按 trigger 分别补充断言：

- `TxnID` mismatch：后续 `Unlock` 可以返回 mismatch error；远端其他 `TxnID` 的 metadata 仍存在，
  不能被旧 holder 删除。
- renewal `WriteFile` timeout：late write 若落盘且仍是自己的 `TxnID`，后续 `Unlock` 可以成功清理
  自己的 physical path；但 protected worker 的终止结果仍是 `lease_lost`。
- post-write proof failed：后续 `Unlock` 可以成功清理自己的 physical path；但 cleanup success 不表示
  holder 重新获得有效 lease。

### 2. Normal Unlock Wins In-Flight Renewal

建议测试名：

```text
TestLeaseLockNormalUnlockWinsInFlightRenewal
```

目标：

验证 normal unlock 已经开始并成为业务终止结果时，in-flight renewal operation 后续返回 cancellation /
timeout 不应再触发业务 `lease_lost` 语义。

目标交错：

```text
holder acquire
renewal WriteFile enters storage operation and blocks
business starts normal Unlock
test harness records normal_unlock terminal reason and stops protected worker
renewal operation returns after Unlock has won
Unlock waits for renewal goroutine before deleting physical path
```

断言：

- first terminal reason 是 `normal_unlock`；
- `onLeaseLost` 不应在 normal unlock 已经获胜后被调用；
- protected step 在 `normal_unlock` 后返回 false；
- `Unlock` 必须等待 in-flight renewal write 退出后才删除 physical path；
- holder 自己的 physical path 最终被删除；
- 后续 acquire 可以成功；
- 无 `.INTENT.` 残留。

注意：这个测试最初要暴露的风险是，`Unlock` 只关闭 renewal `stopCh` 但不能取消已进入
`tryRenew` 的 operation，导致 normal unlock 已经赢了之后仍可能触发 `onLeaseLost`。当前实现通过
renewal-owned cancellable context 把 normal unlock cancellation 传入 `tryRenew`。这个测试仍应保持严格
验收标准：normal unlock 先赢时，后续 renewal shutdown signal 不应覆盖业务终止结果。

### 3. Cleanup Does Not Delete Reacquired Instance

建议测试名：

```text
TestLeaseLockCleanupDoesNotDeleteReacquiredInstance
```

目标交错：

```text
cleanup reads old stale instance A
contender acquires new instance B in same family
cleanup resumes and deletes only A
holder B records protected step and unlocks
```

覆盖 family：

- truncate；
- migration write；
- migration read, triggered by migration write contender cleanup；
- append write。

断言：

- A 被删除；
- B 仍存在且 TxnID 不变；
- holder B 可以继续 protected step；
- holder B unlock 后 B 被删除；
- 后续 acquire 成功。

### 4. Contender Retry Cleanup Interleaving

建议测试名：

```text
TestLeaseLockContenderRetryCleanupInterleaving
```

目标：

多个 contender 同时 `LockWithRetry`，其中一个清理 stale blocker，另一个正在 acquire 或 verify。

覆盖组合：

- migration write vs migration write；
- migration read vs migration write；
- append write vs append write。

断言：

- 互斥组合最多一个 owner 进入 protected step；
- read/read 兼容仍可由第一阶段测试覆盖，本 case 不重复；
- cleanup 后没有 orphan `.INTENT.`；
- 未成功 owner 不记录 protected step。

### 5. Intent Lifecycle Interleavings

建议测试名：

```text
TestLeaseLockIntentLifecycleInterleavings
```

目标：

专门覆盖 conditional put 的 `.INTENT.` 生命周期乱序，而不是业务 renewal。

子场景：

- intent 创建后 commit 前，conflicting contender listing 看到 intent；
- intent 创建后 commit 失败，cleanup intent；
- contender listing 包含 tombstone / intent 时不会误判 own committed lock；
- cleanup 与 failed intent 同时发生时，不把 intent 当成 eligible stale committed instance 删除。

断言：

- 冲突 acquire 返回 conflict，不进入 protected step；
- failed acquire 清理自己的 intent；
- unknown / foreign intent 保守阻塞 acquire，但不被 stale cleanup 删除；
- 后续手动删除 foreign intent 后 acquire 成功。

### 6. Model Fixed Seeds

建议测试名：

```text
TestLeaseLockModelFixedSeeds
```

默认 CI 规模：

```text
seeds: 3
owners: 4
steps per seed: 40
```

本地放大通过环境变量：

```text
TIDB_LEASE_LOCK_MODEL_SEEDS
TIDB_LEASE_LOCK_MODEL_STEPS
TIDB_LEASE_LOCK_MODEL_OWNERS
```

动作集合：

- acquire read / write / append / truncate；
- protected step；
- unlock；
- inject renewal transient error；
- inject renewal permanent loss by TxnID mismatch；
- inject renewal hang while protected work continues；
- stale cleanup；
- contender retry acquire；
- manual delete of foreign intent only when the model intentionally simulates operator cleanup。

随机测试只使用 production lock APIs 和 test-only controlled storage / clock。每一步后检查 invariants。

### 7. Protected Work Death and Hang Semantics

建议测试名：

```text
TestLeaseLockProtectedWorkDeathAndHangSemantics
```

目标：

区分 holder 受保护业务循环死亡和受保护业务循环挂起这两类故障。死亡表示 holder 不再续约、不再 unlock；挂起表示
业务 protected loop 不推进，但 renewal goroutine 仍可能持续刷新 lease。

子场景：

1. protected work died and renewal stopped:
   - owner acquire lock 后模拟进程死亡：不再 protected step，不调用 `Unlock`，也不再 renewal；
   - contender 在 `ExpireAt + staleReclaimGrace` 前尝试 acquire，应被旧 physical instance 阻塞；
   - lease clock 推进到 `ExpireAt + staleReclaimGrace` 后，cleanup reclaim 旧 physical instance；
   - 后续 contender acquire 成功。
2. protected work hung while renewal alive:
   - owner acquire lock 并启动 renewal，但 protected worker 停在测试控制的 barrier；
   - 等待 renewal 推进 `ExpireAt`；
   - contender 尝试 acquire / cleanup，应被 live holder 阻塞，不能删除 holder physical path；
   - release hung worker 后，holder 可以正常 unlock；
   - 后续 acquire 成功。
3. hung protected work then renewal lost:
   - owner protected worker 挂在 barrier，renewal write 被注入 permanent loss；
   - `onLeaseLost` 必须记录 terminal reason；
   - release worker 后不得继续 protected step，只能观察 terminal state。

断言：

- 死亡且 renewal 停止的 holder 只能在 stale boundary 后被 reclaim；
- 挂起但仍续约的 holder 不被 stale cleanup 删除；
- 挂起后丢锁的 holder 在 barrier release 后也不能继续业务 step；
- event log 明确标出 `dead`、`hung`、`lease_lost`、`normal_unlock`，避免把 availability blocker
  误判成 lock safety failure。

### 8. Renewal Observation Hang Is Retried Within Proven Window

建议测试名：

```text
TestLeaseLockRenewalObservationHangIsTransient
```

目标：

覆盖受保护业务循环仍然正常、但 renewal loop 在 mutation 前 observation operation 长时间挂起或失败的
场景。`ReadFile` 和 pre-write lease clock `Now` 不产生写入副作用，单次 failure / timeout 不应直接
触发 `onLeaseLost`；但它们必须消耗 retry budget 和最后一次 proven lease window。

建议 harness：

- 复用 `locking_concurrency_test.go` 里的 `protectedWorker` / `criticalSectionAudit`，用 worker step
  表示受保护业务循环仍在推进。
- 复用 `locking_test.go` 里的 `createMockStorage`、`sequenceLeaseClock`、lease timing override helper。
- 新增测试专用 `operationBlockingStorage`，至少支持按 physical lock path 阻塞 renewal `ReadFile`：
  - `started` channel 证明 renewal loop 已进入 read；
  - 阻塞时等待 `ctx.Done()`，以证明 production code 给 read 传入了 bounded context；
  - 可配置 `release`，让下一次 retry 成功；
  - 记录 operation、path、ctx error，供失败日志打印。
- 新增测试专用 `blockingLeaseClock`，按 call index 阻塞 pre-write `Now`：
  - acquire 阶段和 post-acquire proof 阶段返回固定 time；
  - renewal pre-write call 可以阻塞到 `ctx.Done()` 或返回 transient error；
  - 下一次 retry 可返回成功 time；
  - 记录 call index，避免测试误把 acquire clock call 当成 renewal clock call。
- 测试时间常量应缩短到毫秒级，例如 TTL / renew interval / operation timeout cap / base backoff 都使用
  test override；断言只看行为边界，不使用真实 10 分钟等待。

子场景：

1. renewal read timeout then retry succeeds:
   - owner acquire lock 并记录 protected step；
   - renewal loop 在 `ReadFile` 阶段被 controlled storage 阻塞到单次 operation timeout；
   - 本次 renewal failure 作为 transient 记录，不能立即调用 `onLeaseLost`；
   - 下一次 retry 放行 `ReadFile` 并 renewal 成功；
   - protected work 在 proven lease window 内可以继续，最终正常 unlock。
2. pre-write clock timeout then retry succeeds:
   - renewal loop 在 pre-write lease clock `Now` 阶段阻塞或持续返回 transient error；
   - 单次 timeout 不触发 `onLeaseLost`；
   - 后续 retry 成功后刷新 `ExpireAt`；
   - protected work 没有出现 terminal reason 后的 step。
3. repeated observation failures exhaust proven window:
   - renewal `ReadFile` 或 pre-write clock 持续 timeout / transient error；
   - retry/backoff 不允许越过最后一次 proven lease window；
   - 到达 loss boundary 后才触发 `lease_lost`；
   - release barrier 后 old holder 也不能继续 protected step。
4. observation hang before detecting hijack:
   - lock file 已被另一个 holder 通过 stale reclaim / new acquire 替换；
   - old holder 的 renewal loop 卡在 `ReadFile` 前或中间；
   - old holder 受保护业务循环不能在 lease window 之后继续 protected step；
   - release read 后应检测 TxnID mismatch 或 expired lease，并 terminal。

断言：

- 单次 read / pre-clock timeout 不直接调用 `onLeaseLost`；
- repeated observation failure 只能在 proven lease window 耗尽后触发 `lease_lost`；
- protected step 日志中不能出现 lease-lost event 之后的 step；
- failure log 必须显示 renewal hang point、最后一次 proven `ExpireAt`、retry attempt 和 protected
  work attempted step。

### 9. Renewal Write Timeout and Post-Write Proof Failure Stop Protected Work

建议测试名：

```text
TestLeaseLockRenewalAmbiguousWriteAndProofFailureStopProtectedWork
```

目标：

覆盖受保护业务循环仍然正常、但 renewal loop 在 mutation 或 mutation 后 proof 阶段无法证明结果的场景。
这类故障必须停止 protected work，因为 holder 不能再证明自己拥有一个足以覆盖后续业务操作的 lease
window。

建议 harness：

- 继续复用 `protectedWorker` / `criticalSectionAudit`，`onLeaseLost` 只负责停止 worker 并记录
  `lease_lost` terminal reason。
- 可以复用 `locking_concurrency_test.go` 里的 `renewalBlockingStorage` 来验证 write timeout 后
  `Unlock` 等待 in-flight write 退出；但本 case 还需要一个更强的测试 wrapper，例如
  `lateWriteStorage`：
  - 第一次 renewal `WriteFile` 到 physical lock path 时阻塞到 operation timeout；
  - 在 `ctx.Done()` 后先向 caller 返回 timeout error；
  - 测试显式 release 后，再把捕获的 renewal payload 写入底层 storage，模拟底层 object store late write；
  - 提供 `writeStarted`、`ctxDone`、`lateWriteCommitted` channel，失败日志打印 path 和 captured
    `ExpireAt`。
- post-write proof 场景使用 clock wrapper 控制 renewal pre-write 和 post-write proof 两个 call：
  - acquire / post-acquire proof 正常返回；
  - renewal pre-write `Now` 正常返回，让 renewal write 确实发生；
  - post-write proof `Now` 可配置为 hang、返回 error，或返回晚于 new `ExpireAt` 的 time；
  - 断言时只看 `onLeaseLost`、worker terminal state 和 protected step，不依赖内部函数名。
- ordinary write error 子场景可以复用现有 failpoint / storage wrapper，让 `WriteFile` 返回非 timeout
  error，并在下一次 retry 成功；它主要证明 “write error” 与 “write timeout” 的语义区别。
- 这些测试应优先作为 full renewal-loop test：通过 `TESTStartRenewal` 启动 goroutine，再观察
  `onLeaseLost` 和 protected worker。helper-level `TESTTryRenew` case 可作为补充，但不能替代
  “业务停止”断言。

子场景：

1. renewal write timeout:
   - owner acquire lock 并记录 protected step；
   - renewal loop 在 `WriteFile` 阶段被 controlled storage 阻塞到 operation timeout；
   - `onLeaseLost` 必须记录 terminal reason；
   - late write 之后即使被 release，也不能恢复 old holder 的 protected work；
   - 后续 protected step 返回 false。
2. renewal write ordinary error remains transient:
   - renewal `WriteFile` 返回非 timeout storage error；
   - 本次 failure 作为 transient 记录，不直接 `onLeaseLost`；
   - 下一次 retry 成功后 protected work 仍只在 proven lease window 内继续。
3. post-write proof clock hang or error:
   - renewal write 已返回成功，但 post-write proof 的 lease clock `Now` 长时间挂起；
   - 受保护业务循环不能因为 write 成功就无限推进；
   - 超过 bounded proof timeout / proven lease window 后触发 `lease_lost` 或停止 worker；
   - 后续 protected step 返回 false。
4. post-write proof says lease already unsafe:
   - renewal write 返回成功；
   - post-write proof clock 返回的 `nowAfterWrite` 已经晚于新 `ExpireAt`；
   - holder 立即进入 `lease_lost`；
   - 后续 protected step 返回 false。

断言：

- write timeout 和 post-write proof failure 都会停止 protected work；
- write ordinary error 不直接 lease lost，仍按 transient retry 处理；
- protected step 日志中不能出现 lease-lost event 之后的 step；
- 如果测试推进 fake lease clock 到最后一次 proven `ExpireAt` 之后，受保护业务循环必须停止；
- contender 只有在 old holder terminal 或 stale reclaim 成功后才能进入互斥临界区；
- failure log 必须显示 renewal hang point、operation timeout cap、最后一次 proven `ExpireAt`、
  protected work attempted step。

## BR HA 后续候选

本阶段完成后，再根据收益选择少量 phase 3 HA case：

- restore vs truncate 真实命令并发；
- truncate vs truncate 真实命令并发；
- 持锁 BR 进程被 kill 后由后续命令 stale reclaim；
- 长循环 acquire / renew / unlock，检查 lock 残留；
- 真实命令中注入 storage delay，验证 failure log 和 cleanup behavior。

这些 case 运行成本高、失败定位慢，不作为 phase 2 的主线。

## 验证建议

WIP 验证优先运行：

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalRace|CleanupDoesNotDeleteReacquiredInstance|ContenderRetryCleanupInterleaving|IntentLifecycle|ModelFixedSeeds|ProtectedWorkDeathAndHangSemantics|RenewalObservationHangIsTransient|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1
```

新增 Go file 或顶层 test 后运行：

```bash
make bazel_prepare
```

Ready 前按 repo policy 运行：

```bash
make lint
```

`br_lease_lock` integration 作为独立最终验证项保留：

```bash
make build_for_br_integration_test
TEST_NAME=br_lease_lock br/tests/run.sh
```

## 验收口径

Phase 2 完成后，应能明确回答：

1. 终止竞态下 holder 不会在 lost 或 unlock 后继续推进受保护业务。
2. cleanup 基于旧 stale observation 时不会删除 later-acquired new physical instance。
3. 多 contender cleanup / retry / acquire 交错下互斥临界区不会重入。
4. `.INTENT.` 生命周期乱序不会留下无法解释的 blocker。
5. 固定 seed model 测试失败时可以直接用 seed 和 event log 复现。
6. 受保护业务循环死亡和受保护业务循环挂起被区分处理：死亡依赖 stale reclaim 恢复，挂起但仍续约时保持互斥阻塞。
7. mutation 前 observation failure 不会单次直接 lease lost，但 repeated failure 不能让业务越过最后一次
   proven lease window。
8. write timeout 和 post-write proof failure 会停止 protected work，避免 ambiguous mutation 或无法证明
   lease window 后继续业务。
9. Renewal proof operation 的单次 bound 统一为 `min(10min, remaining proven lease window / 2)`，
   覆盖 read、write 和 lease clock proof。
