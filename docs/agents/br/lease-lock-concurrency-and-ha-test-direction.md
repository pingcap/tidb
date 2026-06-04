# Lease Lock 并发与 HA 测试方向

## 状态

方向记录，用于承接 `lease-lock-integration-test-design.md` 之后的下一阶段测试设计。

本文档只记录测试方向和优先级，不展开到具体 failpoint、脚本结构或实现步骤。后续如果进入
实现，应再拆成具体 implementation plan。

2026-06-04 状态更新：

- 第一阶段 deterministic `pkg/objstore` concurrency tests 已落地在
  `pkg/objstore/locking_concurrency_test.go`，覆盖 concurrent acquire exclusion、
  renewal lost stop、unlock / renewal lifecycle ordering、stale cleanup live-holder
  safety 和 expired-candidate reclaim boundaries。
- `br/tests/br_lease_lock` 已有第一批真实 BR command 集成测试。用户反馈此前已在可用环境中跑通；
  当前可把它视为独立的最终/回归验证项，而不是下一阶段测试设计的主线阻塞点。
- 下一阶段重点应从“补齐已知 deterministic case”转向“mock / model-based 并发竞态测试”，再按收益
  选择少量 BR HA 场景。
- Phase 2 具体设计见 `docs/agents/br/lease-lock-model-concurrency-test-design.md`。

## 背景

当前 BR lease lock 已经有三类测试基础：

- `pkg/objstore` 单测覆盖 lock/renewal/stale cleanup 的细粒度语义；
- `pkg/objstore/locking_concurrency_test.go` 覆盖第一阶段确定性交错下的并发 correctness proof；
- `br/tests/br_lease_lock` 集成测试覆盖真实 BR 命令、真实 PD clock、真实业务
  `onLeaseLost` cancel 路径、正常 unlock 和 stale lock reclaim。

这些测试能证明真实链路已经接通，但还不能充分回答一个核心问题：

> 在多个 owner 并发抢锁、续约、丢锁、unlock、stale cleanup 交错时，lease lock 是否仍然
> 满足临界区安全性？

这个问题不应该主要依赖 BR 集成测试回答。BR 集成测试成本高、变量多、失败定位慢，更适合证明
真实链路可用；锁算法本身的并发正确性应主要在 `pkg/objstore` 层通过可复现的并发测试证明。

## 测试目标分层

### 1. Mock / model-based 并发竞态测试

这是当前下一阶段最重要的测试方向。

目标是在可复现的 mock storage / model harness 中扩大 interleaving 覆盖，证明 lock 层在更多
acquire、renew、unlock、lease lost、stale cleanup 组合下仍满足 safety invariant。它回答的是
correctness proof 问题。

建议主要放在 `pkg/objstore` 测试中，使用：

- local object storage；
- fake / sequence lease clock；
- controlled storage wrapper；
- barrier、channel、failpoint 控制并发顺序；
- 隔离的测试临界区，例如原子计数器或 owner 状态表。

核心 invariant：

- 互斥类 lock 的临界区同时最多只有一个 owner；
- read/read 可以并存，read/write 必须冲突；
- truncate lock 与其他受保护写类操作不能并存；
- owner 丢失 lease 后不能继续执行受保护动作；
- unlock 只删除自己的 physical lock；
- stale cleanup 不能误删 live lock；
- 并发失败后不应留下 `.INTENT.` lock；
- stale reclaim 行为可解释、可复现、不会破坏活跃 holder。

优先覆盖的薄弱点：

1. `Unlock`、renewal permanent loss、业务 `onLeaseLost` callback 同时发生时的终止结果。
2. stale cleanup 与 owner re-acquire / contender retry 的竞态，特别是 cleanup 观察 stale 后另一个
   owner 已经 acquire 新 physical instance 的情况。
3. 多 owner 循环 acquire / protected step / unlock / cleanup 的 invariant 检查。
4. renewal transient error、bounded write timeout、retry backoff 与 manual unlock 的组合。
5. `.INTENT.` 创建、提交、失败清理和 contender listing 的乱序组合。
6. holder 受保护业务循环死亡或长时间挂起：死亡且续约停止后依赖 stale reclaim 恢复；挂起但仍续约时不能被
   contender 当作 stale lock 删除。
7. holder 受保护业务循环正常但 renewal loop 长时间挂起：业务 protected step 不能越过最后一次 proven
   lease window 继续推进；lock metadata read/write 和 lease clock proof 都应有统一 operation bound。
   单次 read / pre-clock observation failure 不应直接 lease lost；write timeout 和 post-write proof
   failure 才是必须停止 protected work 的高风险边界。

### 2. 固定 seed 随机乱序与错误注入

这是已知薄弱点测试之后的补强方向。

目标是在可复现的前提下扩大 interleaving 覆盖面，发现人工枚举遗漏的组合。它不应取代确定性测试，
也不应成为唯一安全证明。

建议模型：

- N 个 goroutine 代表 N 个 owner；
- 每个 owner 随机执行 acquire、renew、unlock、simulate lease lost、stale cleanup、
  retry acquire 等动作；
- 临界区只操作隔离状态，例如 `activeWriterCount`、`activeReaders`、`ownerHistory`；
- 每一步都检查 invariant；
- seed 固定并在失败日志中打印，保证失败可复现；
- CI 默认轮数较小，本地或 heavy profile 可通过环境变量放大 seed 数和 step 数。

建议默认规模：

- CI：少量固定 seed，例如 3 到 5 个 seed；
- 每个 seed：少量 owner 和 step，保证运行时间稳定；
- 本地压力：允许通过环境变量扩大到更多 seed / owner / step。

### 3. BR HA / 真实压力场景测试

这是更靠后的信心增强方向。

目标是验证真实 BR、PD/TiKV/TiDB、真实进程生命周期、调度延迟和对象存储操作组合起来时，
没有暴露新的系统级问题。它回答的是 confidence test 问题。

候选场景：

- restore vs truncate 真实命令并发；
- truncate vs truncate 真实命令并发；
- restore vs restore / migration metadata append 并发；
- 持锁命令被 kill 后 stale reclaim；
- 持锁命令受保护业务循环长时间挂起但 renewal 仍存活时，后续命令应被阻塞并给出可诊断状态；
- 持锁命令受保护业务循环仍推进但 renewal loop 的 storage / lease-clock proof operation 长时间挂起时，
  应停止受保护业务或触发 lease lost；
- 长时间循环 acquire / renew / unlock，观察残留 lock；
- 真实进程 restart 或存储操作延迟注入。

这类测试成本高、失败定位慢，不建议作为当前阶段的主线。只有在 lock 层 correctness proof
补齐后，再选择少量高价值 HA 场景进入 BR 集成测试。

## 推荐优先级

1. 先做 `pkg/objstore` 层 mock / model-based 并发竞态测试。
2. 再做固定 seed 随机乱序与错误注入，把 mock harness 作为 coverage amplifier。
3. 最后按收益选择少量 BR HA / 真实压力场景，并保留 `br_lease_lock` 作为独立最终验证项。

换句话说：

- 已知薄弱点测试是 correctness proof；
- 随机乱序测试是 coverage amplifier；
- BR HA 测试是 system confidence。

当前阶段最应该优先补齐 correctness proof。

## 非目标

- 不把大量随机压力测试放进 BR shell 集成测试。
- 不依赖不可复现的随机失败作为 CI 依据。
- 不用真实 30 分钟 TTL 做等待测试。
- 不在 HA 测试中重复所有 `pkg/objstore` lock 语义。
- 不为了测试并发而引入生产代码的大规模重构。

## 初步验收口径

下一阶段测试补齐后，应能明确回答：

1. 多 owner 并发下，互斥临界区不会重入。
2. 丢锁后，owner 不会继续执行受保护动作。
3. unlock / renewal / lost 交错不会误删其他 owner 的 live lock。
4. stale cleanup 并发下不会误删 live lock。
5. 随机乱序测试失败时可以通过 seed 复现。
6. BR 集成测试仍只承担真实链路证明，不承担全部并发算法证明。
