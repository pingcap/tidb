# Lease Lock 集成测试 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 BR lease lock 第一阶段实现一组真实集群集成测试，证明 renewal、lost cancel、normal unlock 和 stale reclaim 都能在真实 BR CLI 路径下工作。

**Architecture:** 在 `pkg/objstore` 增加测试专用 failpoint 来缩短 lease 参数并注入 renewal write block/error；在 BR 业务路径的持锁点后增加 signal/block failpoint；在 `br/tests/br_lease_lock` 下新增 shell integration case，复用真实 PD/TiKV/TiDB、真实 `br.test` 和 `local://` storage。测试只改变时序和故障注入，不用 helper 绕过真实 restore/truncate 数据路径。

**Tech Stack:** Go, failpoint, `pkg/objstore`, `br/pkg/restore/log_client`, `br/pkg/task`, BR shell integration harness, `local://` external storage, `./tools/check/failpoint-go-test.sh`, `br/tests/run.sh`, `make bazel_prepare`, `make lint`.

---

This plan implements `docs/agents/br/lease-lock-integration-test-design.md`.

## Progress

- [ ] 确认 failpoint 命名、输入格式和插入位置。
- [ ] 实现 `pkg/objstore` lease 参数缩短 failpoint。
- [ ] 实现 `pkg/objstore` renewal write block/error failpoint。
- [ ] 实现 BR 业务持锁后 signal/block failpoint。
- [ ] 新增 `br/tests/br_lease_lock` shell harness 和通用 helper。
- [ ] 实现 migration renewal success case，并检查 normal unlock。
- [ ] 实现 migration lost block/error 两个 case。
- [ ] 实现 truncate renewal success case，并检查 normal unlock。
- [ ] 实现 truncate lost block/error 两个 case。
- [ ] 实现 stale lock reclaim real-command case。
- [ ] 更新 BR integration test group。
- [ ] 运行 WIP 验证。
- [ ] 运行 Ready 验证并记录结果。
- [ ] 合并独立审查发现并同步更新本计划。

## Surprises & Discoveries

- Observation: 独立审查发现原计划缺少 failpoint-enabled `br.test` 构建步骤。
  Evidence: `Makefile` 的 `build_for_br_integration_test` 会执行 `make failpoint-enable` 并重新编译 `br.test`；`br/tests/run.sh` 只运行已有二进制。

- Observation: BR integration 的 `restart_services` 会清理 `$TEST_DIR` 下以 `br`、`tidb`、`tiflash`、`tikv`、`pd` 开头的顶层目录。
  Evidence: `tests/_utils/run_services` 中 `cleanup_data` 使用这些前缀删除目录；因此本测试的 storage、marker、log 顶层目录不能叫 `br_lease_lock`。

- Observation: migration read lock 残留不会阻塞后续 migration read acquire。
  Evidence: `pkg/objstore/locking_helper.go` 中 read/read 兼容；normal unlock 的后续阻塞验证需要使用 exact lock file deletion 和会与 read lock 冲突的 migration write 路径。

- Observation: BR integration 默认会启用 encryption wrapper 和 encryption check。
  Evidence: `br/tests/run.sh` 导出 `ENABLE_ENCRYPTION_CHECK=true` 和 `ENCRYPTION_ARGS`；本测试需要读取 lock JSON 并验证 expected-failure 语义，因此需要显式关闭无关加密检查。

- Observation: `run_br` 是 shell wrapper，后台 kill wrapper pid 不一定清理真正的 `br.test` 子进程。
  Evidence: `br/tests/run_br` 会继续调用 `tests/_utils/run_br`，后者再执行 `br.test`；后台 helper 必须清理进程树或独立进程组。

## Decision Log

- Decision: 第一版一共实现 7 个实际运行 case。
  Rationale: migration/truncate 各有 renewal success、lost by block、lost by error；normal unlock 嵌入两个 success case；stale reclaim 是独立兜底 case。
  Date/Author: 2026-06-01 / Codex 与用户讨论确认。

- Decision: truncate case 走真实 `br log truncate -s local://... --until ... -y` 数据路径。
  Rationale: 集成测试要证明真实 `RunStreamTruncate` 和 outer `LockRemoteTruncate` 接线正确，failpoint 只控制时序和故障。
  Date/Author: 2026-06-01 / Codex 与用户讨论确认。

- Decision: lost case 同时覆盖 renewal write block 和 renewal write error。
  Rationale: block 证明 bounded write timeout；error 证明快速失败后的 retry/backoff/lost/cancel。
  Date/Author: 2026-06-01 / Codex 与用户讨论确认。

- Decision: stale reclaim case 纳入第一版必做。
  Rationale: 它是 delayed renewal zombie 或历史 holder 停止续约后的最终可用性兜底。
  Date/Author: 2026-06-01 / Codex 与用户讨论确认。

- Decision: integration validation 必须先运行 `make build_for_br_integration_test`。
  Rationale: 新增 failpoint 需要被编进 `br.test`，否则 `GO_FAILPOINTS` 可能被旧二进制忽略。
  Date/Author: 2026-06-01 / 独立审查后确认。

- Decision: stale reclaim case 采用同一路径的两阶段验证。
  Rationale: 先前考虑用“未过期先阻塞”证明 exact path 会被扫描；第二轮审查后改为 stable reclaim marker + exact stale file deletion，避免把未过期不误删边界搬进 shell integration。
  Date/Author: 2026-06-01 / 独立审查后确认。

- Decision: stale reclaim case 不做未过期 lock 的必跑阻塞阶段。
  Rationale: `ExpireAt + staleReclaimGrace` 未到时不会误删由单测覆盖；shell integration 只证明真实命令能扫描并删除 exact stale lock。
  Date/Author: 2026-06-01 / 第二轮独立审查后确认。

## Outcomes & Retrospective

实现完成后，在这里总结实际覆盖的 case、命令输出、剩余风险和后续优化。

- 2026-06-01 计划审查结论：第三轮独立审查通过。可实施性审查结论为“可实施”，设计一致性审查结论为“符合设计”，证明力审查结论为“证明力充足”，BR integration 模式审查结论为“符合模式”。后续可以进入实现阶段。

## Context and Orientation

`pkg/objstore` 提供 remote lease lock。`RemoteLock.startRenewal` 后台刷新 lock metadata 的 `ExpireAt`；当 renewal 无法证明仍然持有安全 lease window 时，调用 `onLeaseLost`。BR restore/truncate 会把自己的业务 context cancel function 作为 `onLeaseLost`，从而停止受 lock 保护的临界区。

当前第一阶段已经实现：

- `LeaseTTL = time.Hour`。
- `staleReclaimGrace = 30 * time.Minute`。
- renewal write 使用 bounded context。
- renewal write 成功后使用 `LeaseClock.Now` 做 post-write proof。
- next renewal delay 来自已经证明的 remaining lease。
- `Unlock` 会先停止 renewal loop，再删除自己的 physical lock。

这份计划只增加测试与测试专用 failpoint，不改变生产语义。

### 关键文件

- `pkg/objstore/locking.go`: renewal loop、`tryRenew`、lease 参数、`LockWithRetry`、`LockRemoteTruncate`。
- `pkg/objstore/locking_helper.go`: lock family path、physical lock path 分类、stale cleanup helper。
- `br/pkg/restore/pd_lease_clock.go`: `PDLeaseClock.Now` 从 PD TSO 获取 lease time；success case 需要证明真实路径使用该 clock。
- `br/pkg/restore/log_client/client.go`: `LogClient.GetLockedMigrations` 在 PITR restore 中获取 migration read lock。
- `br/pkg/stream/stream_metas.go`: `MigrationExt.MergeAndMigrateTo` 获取 migration write lock；本计划的 migration restore case 优先使用 read lock 路径。
- `br/pkg/task/stream.go`: `RunStreamTruncate` 获取 outer `truncating.lock.<generation>`。
- `pkg/objstore/locking_helper.go`: stale cleanup 实际删除 stale physical lock；stale reclaim case 需要稳定 marker 证明删除分支处理了 exact path。
- `br/tests/run.sh`: BR integration harness，启动真实 PD/TiKV/TiDB 并运行选定 case。
- `br/tests/run_group_br_tests.sh`: CI group 列表；新增 `br_lease_lock` 后必须加入一个 group。

### 第一版 7 个 case

1. `migration renewal success`
2. `migration lost by renewal-write-block`
3. `migration lost by renewal-write-error`
4. `truncate renewal success`
5. `truncate lost by renewal-write-block`
6. `truncate lost by renewal-write-error`
7. `stale lock reclaim allows real command`

## Failpoint Contract

### `pkg/objstore/lease-lock-test-constants`

位置：`pkg/objstore/locking.go`。

用途：缩短 BR 子进程内的 lease 参数，避免等待真实 30/60 分钟。

输入格式：

    ttl=2s,interval=200ms,write-timeout-cap=100ms,min-remaining=200ms,stale-reclaim-grace=500ms,base-backoff=100ms

要求：

- 只在 failpoint build 下生效。
- 在 `tryRenew`、`LockWithRetry`、`LockRemoteTruncate` 和 stale cleanup 入口前都能被应用到。
- 对 `br.test` 单个命令进程内的全局变量生效即可；BR integration shell 每次 `run_br` 都是独立进程。
- 解析失败时返回明确错误，不静默使用生产默认值。
- 不改变 `renewMaxRetries` 来制造快速失败；error lost case 需要证明 transient retry/backoff 受 proven lease window 约束，而不是只靠耗尽较小 retry limit。

### `br/pkg/restore/lease-clock-pd-now-signal`

位置：`br/pkg/restore/pd_lease_clock.go`，`PDLeaseClock.Now` 成功从 PD TSO 转换出 physical time 后。

用途：证明 success case 的 lock metadata 时间来自真实 PD-backed `LeaseClock`，不是 local clock fallback。

输入格式：

    dir=/tmp/lease-lock/pd-clock

行为：

- 每次 `PDLeaseClock.Now` 成功后，在目录中创建递增 marker，例如 `now.1`、`now.2`。
- success case 先在 acquired signal 后记录 marker 数量，再等待 `ExpireAt` 推进；推进后断言 marker 数继续增长，最好至少增加 2 次，证明 renewal 读旧 lease 和 post-write proof 都经过 PD-backed clock。
- 不改变返回的 PD time。

### `pkg/objstore/lease-lock-renewal-write-block`

位置：`pkg/objstore/locking.go`，`tryRenew` 创建 bounded `writeCtx` 后、调用 `l.storage.WriteFile(writeCtx, l.path, newData)` 前。

用途：模拟 renewal storage write hang 住，证明 bounded context 会触发 timeout。

输入格式：

    signal=/tmp/lease-lock/fault

行为：

- 创建 `signal` 文件，证明故障发生在 renewal write 阶段。
- 阻塞直到 `writeCtx.Done()`。
- 返回一个普通 write error 或 `writeCtx.Err()`；生产 `tryRenew` 分支必须根据 `writeCtx.Err() == context.DeadlineExceeded` 转换成 `errRenewWriteTimeout`，集成测试不直接返回 sentinel。
- 不调用底层 `WriteFile`，避免测试真的卡住或写出不确定对象。

### `pkg/objstore/lease-lock-renewal-write-error`

位置：`pkg/objstore/locking.go`，同样在 renewal `WriteFile` 前。

用途：模拟 renewal storage write 快速失败，证明 transient retry/backoff 最终会受 proven lease window 约束并触发 lost。

输入格式：

    signal-dir=/tmp/lease-lock/faults

行为：

- 每次被触发时创建一个 attempt marker，例如 `attempt.1`、`attempt.2`。
- 返回普通 transient error，例如 `failpoint: renewal write error`。
- 不返回 permanent lease-lost sentinel。lost 应由 renewal loop 的 retry/backoff/proven-window 逻辑得出。
- 集成测试至少断言出现两个 attempt marker，检查日志包含 transient retry/backoff 信息，并最终命中 proven-window 相关分支，例如 `proven lease window elapsed` 或 `retry backoff would exceed proven lease window`。

### `pkg/objstore/lease-lock-stale-reclaim-signal`

位置：`pkg/objstore/locking_helper.go`，stale cleanup 确认某个 physical lock stale 并准备删除或刚删除成功后。

用途：证明真实 BR command 的 stale cleanup 分支扫描并处理了测试构造的 exact physical lock path。

输入格式：

    dir=/tmp/lease-lock/stale-reclaim

行为：

- 每次 stale cleanup 处理一个 stale physical lock 时，在目录中创建包含 path basename 的 marker。
- marker 只用于测试观测，不改变 reclaim 行为。
- stale reclaim integration case 断言 marker 指向手工写入的 exact `truncating.lock.<32hex>`。

### `br/pkg/restore/log_client/lease-lock-after-migration-lock-acquired`

位置：`br/pkg/restore/log_client/client.go`，`LogClient.GetLockedMigrations` 成功获取 `ext.GetReadLock` 后、`ext.Load(ctx)` 前。

用途：在 PITR restore 的 migration lock 保护区入口提供可控阻塞点。

输入格式：

    signal=/tmp/lease-lock/acquired,release=/tmp/lease-lock/release,after=/tmp/lease-lock/after

行为：

- 创建 `signal` 文件，证明 migration lock 已经 acquired。
- 等待 `release` 文件出现，或等待 `ctx.Done()`。
- 如果因为 `release` 放行而继续，创建 `after` 文件，然后返回 nil。
- 如果因为 lease lost cancel 导致 `ctx.Done()`，返回 `ctx.Err()`，且不创建 `after`。

### `br/pkg/task/lease-lock-after-truncate-lock-acquired`

位置：`br/pkg/task/stream.go`，`RunStreamTruncate` 成功获取 outer `objstore.LockRemoteTruncate` 后、读取 truncate safepoint 或推进 truncate metadata 前。

用途：在 outer truncate lock 保护区入口提供可控阻塞点。

输入格式和行为与 migration failpoint 相同。

### 推荐的 helper

为避免两个业务 failpoint 重复解析 `signal/release/after`，新增一个小 helper：

- Create: `br/pkg/utils/lease_lock_failpoint.go`

建议接口：

    type LeaseLockFailpointSpec struct {
        Signal  string
        Release string
        After   string
    }

    func ParseLeaseLockFailpointSpec(raw string) (LeaseLockFailpointSpec, error)
    func WaitLeaseLockFailpoint(ctx context.Context, spec LeaseLockFailpointSpec) error

`WaitLeaseLockFailpoint` 使用短 polling interval 检查 release 文件；如果 context 被 cancel，返回 `ctx.Err()`。这个 helper 只负责测试 failpoint 的文件信号，不引入生产 lock 语义。

## File Map

- Modify: `pkg/objstore/locking.go`
  增加 lease 参数 failpoint、renewal write block/error failpoint，并确保 failpoint 不影响 initial acquire 和普通业务写入。

- Modify: `pkg/objstore/locking_helper.go`
  增加 stale reclaim marker failpoint，用于集成测试证明 exact stale physical lock 被 cleanup 分支处理。

- Modify: `pkg/objstore/locking_test.go`
  增加 failpoint contract 的单测，至少覆盖参数解析、block 返回 `errRenewWriteTimeout`、error 返回 transient error 且创建多次 attempt marker。

- Modify: `br/pkg/restore/pd_lease_clock.go`
  增加 PD clock marker failpoint，用于证明真实 BR path 使用 PD-backed lease time。

- Modify: `pkg/objstore/export_test.go`
  如果单测需要访问 sentinel 或 helper，增加 test-only export。

- Create: `br/pkg/utils/lease_lock_failpoint.go`
  增加 signal/release/after 解析和等待 helper。

- Create: `br/pkg/utils/lease_lock_failpoint_test.go`
  验证 helper 的解析、release 放行、context cancel 不创建 after marker。

- Modify: `br/pkg/restore/log_client/client.go`
  在 `GetLockedMigrations` 获取 read lock 后注入 migration acquired failpoint。

- Modify: `br/pkg/task/stream.go`
  在 `RunStreamTruncate` 获取 outer truncate lock 后注入 truncate acquired failpoint。

- Create: `br/tests/br_lease_lock/run.sh`
  新增 7 个 integration case 和 shell helper。

- Modify: `br/tests/run_group_br_tests.sh`
  把 `br_lease_lock` 加入一个 BR integration group。优先选择已有 PiTR 相关 group，最终以 CI runtime 平衡为准。

- Modify: generated Bazel metadata
  如果新增 Go 文件或新的顶层 Go test function，按仓库规则运行 `make bazel_prepare` 并提交相关 `BUILD.bazel` 变化。

- Modify: `docs/agents/br/lease-lock-integration-test-implementation-plan.md`
  实现过程中持续更新 progress、discovery、validation evidence。

## Plan of Work

### Task 1: Objstore failpoint contract

Scope: 先用单测锁定 test-only failpoint 行为，避免 shell 集成测试直接调试底层时序。

Steps:

- [ ] 在 `pkg/objstore/locking_test.go` 增加参数解析测试，输入 `ttl=2s,interval=200ms,write-timeout-cap=100ms,min-remaining=200ms,stale-reclaim-grace=500ms,base-backoff=100ms`，断言所有字段被正确解析。
- [ ] 运行 red test：

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockTestConstantsFailpoint|TestRenewalWriteBlockFailpoint|TestRenewalWriteErrorFailpoint' -count=1

  期望：编译失败或测试失败，因为 failpoint/helper 尚未实现。

- [ ] 在 `pkg/objstore/locking.go` 实现 `lease-lock-test-constants` 解析和应用。
- [ ] 在 `tryRenew` 的 bounded `writeCtx` 创建后注入 `lease-lock-renewal-write-block`。
- [ ] 在同一位置注入 `lease-lock-renewal-write-error`。
- [ ] 确保 block/error failpoint 只作用于 renewal write，不影响 initial acquire 的 `conditionalPut.CommitTo`。
- [ ] 确保 block failpoint 不直接返回 `errRenewWriteTimeout`，而是让 `tryRenew` 通过 `writeCtx.Err()` 转换 timeout sentinel。
- [ ] 确保 error failpoint 能创建多次 attempt marker，单测断言至少两次 renewal write attempt 后才进入 lost。
- [ ] 运行 green test：

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockTestConstantsFailpoint|TestRenewalWriteBlockFailpoint|TestRenewalWriteErrorFailpoint' -count=1

  期望：新增 failpoint 单测通过。

### Task 2: Business acquired signal/block failpoint

Scope: 增加可复用的 signal/release/after helper，并把它接入 migration restore 和 truncate 真实路径；同时给 PD-backed lease clock 增加观测 marker。

Steps:

- [ ] 在 `br/pkg/utils/lease_lock_failpoint_test.go` 写 helper 测试：
  - parse `signal=<path>,release=<path>,after=<path>` 成功；
  - release 文件出现后创建 after marker；
  - context cancel 时返回 `context.Canceled` 且不创建 after marker。
- [ ] 运行 red test：

      ./tools/check/failpoint-go-test.sh br/pkg/utils -run 'TestLeaseLockFailpoint' -count=1

  期望：编译失败，因为 helper 尚未实现。

- [ ] 新增 `br/pkg/utils/lease_lock_failpoint.go`，实现 `ParseLeaseLockFailpointSpec` 和 `WaitLeaseLockFailpoint`。
- [ ] 在 `br/pkg/restore/log_client/client.go` 的 `GetLockedMigrations` 获取 `readLock` 后、`ext.Load(ctx)` 前注入 `lease-lock-after-migration-lock-acquired`。
- [ ] 在 `br/pkg/task/stream.go` 的 `RunStreamTruncate` 获取 `lock` 后、`stream.GetTSFromFile` 前注入 `lease-lock-after-truncate-lock-acquired`。
- [ ] 在 `br/pkg/restore/pd_lease_clock.go` 的 `PDLeaseClock.Now` 成功返回前注入 `lease-clock-pd-now-signal`。
- [ ] 在 `pkg/objstore/locking_helper.go` 的 stale cleanup 删除分支注入 `lease-lock-stale-reclaim-signal`。
- [ ] 如果 failpoint 返回错误，使用当前函数的自然错误返回路径，不吞掉 context cancellation。
- [ ] 运行 green test：

      ./tools/check/failpoint-go-test.sh br/pkg/utils -run 'TestLeaseLockFailpoint' -count=1

  期望：helper 单测通过。

### Task 3: Shell harness and shared helpers

Scope: 新增 `br/tests/br_lease_lock/run.sh`，先搭好独立运行和 helper，不急着一次写完所有 case。

Steps:

- [ ] 创建 `br/tests/br_lease_lock/run.sh`，包含 license header、`set -eu`、`. run_services`、`CUR=$(cd "$(dirname "$0")"; pwd)`。
- [ ] 在脚本顶部显式关闭无关加密包装：

      export ENCRYPTION_ARGS=""
      export ENABLE_ENCRYPTION_CHECK=false

  本测试需要读取 lock JSON、控制 expected-failure 退出语义，不覆盖加密路径。

- [ ] 添加通用变量：

      PREFIX="lease_lock"
      CASE_ROOT="$TEST_DIR/lease_lock"
      LOG_DIR="$CASE_ROOT/logs"
      MARKER_DIR="$CASE_ROOT/markers"

- [ ] 添加 helper：
  - `case_task_name <case_name>`：返回 `lease_lock_<case_name>`，每个子 case 使用独立 log backup task name。
  - `reset_case_dir <case_name>`：清理并创建 `$TEST_DIR/lease_lock/<case_name>`，顶层目录不能以 `br` 开头，避免 `restart_services` 清理误伤。
  - `wait_for_file <path> <timeout_seconds>`：polling 等待 marker。
  - `assert_file_exists <path>` 和 `assert_file_not_exists <path>`。
  - `find_single_lock_file <storage_dir> <kind>`：使用精确 pattern 查找 `v1/LOCK.READ.<32hex>` 或 `truncating.lock.<32hex>`，排除 `.INTENT.`、legacy path 和非 JSON 文件。
  - `read_expire_at_epoch_ns <lock_file>`：用 `python3` 读取 JSON `expire_at` 并转成纳秒。
  - `read_lock_txn_id <lock_file>`：保存当前 holder 的 exact path 和 txn id，后续 unlock 只检查该 holder。
  - `wait_expire_at_advanced <lock_file> <old_ns>`：等待 `ExpireAt` 推进。
  - `run_br_capture <log_file> <args...>`：调用 PATH 中的 `run_br` wrapper，保留 stdout/stderr，并尽量给 BR 命令传 `--log-file "$log_file.br"`。
  - `run_br_with_failpoints <failpoints> <log_file> <args...>`：使用命令级 `GO_FAILPOINTS="$failpoints" run_br ...`，避免 failpoint 泄漏到后续命令。
  - `run_br_bg_with_failpoints <failpoints> <log_file> <args...>`：后台运行 BR、记录 wrapper pid、进程组或子进程树、stdout/stderr 和完整 failpoint 字符串。
  - `wait_pid_with_timeout <pid> <timeout_seconds> <expected_exit>`：超时后打印日志，并清理整个进程组或递归 `pkill -P` 清理 `br.test` 子进程；expected_exit 支持 `success` 和 `failure`。
  - `cleanup_background_pids`：trap 中清理所有已登记进程树，并重置 `GO_FAILPOINTS=""`。
- [ ] 添加 `LEASE_FP` 字符串，统一设置缩短后的 lease 参数。
- [ ] 先让脚本只打印 case 列表并退出，验证 harness 能启动：

      make build_for_br_integration_test
      TEST_NAME=br_lease_lock br/tests/run.sh

  期望：failpoint-enabled `br.test` 被重新构建，真实集群启动，脚本成功退出。

### Task 4: PITR fixture for migration and truncate cases

Scope: 复用真实 BR 命令准备 full backup、log backup、restore point 和 truncate 输入。

Steps:

- [ ] 在 `run.sh` 中实现 `prepare_pitr_fixture <case_name>`：
  - 创建小库小表；
  - 使用 `TASK_NAME="$(case_task_name "$case_name")"`；
  - `run_br --pd "$PD_ADDR" log start --task-name "$TASK_NAME" -s "local://$log_storage"`；
  - `run_br --pd "$PD_ADDR" backup full -s "local://$full_storage"`；
  - 插入少量增量数据；
  - 等待 log checkpoint 前进；
  - 计算 `RESTORED_TS` 和 `TRUNCATE_TS`；
  - `run_br --pd "$PD_ADDR" log stop --task-name "$TASK_NAME"`，避免后续 restore/truncate 与仍运行的 log backup task 互相影响；
  - 对 restore case，明确清空目标：调用 `restart_services` 或 drop 目标库表后再运行 restore。所有 fixture storage、marker 和 log 都放在 `$TEST_DIR/lease_lock/<case_name>` 下，避免被 `restart_services` 清理。
- [ ] 尽量复用 `br/tests/br_test_utils.sh` 的 `wait_log_checkpoint_advance`。
- [ ] 确保每个 case 使用独立 `TASK_NAME`、database name、storage 子目录和 marker/log 子目录，避免跨 case lock 或 log backup 状态互相影响。
- [ ] 用一个临时 smoke path 验证 fixture：

      make build_for_br_integration_test
      TEST_NAME=br_lease_lock br/tests/run.sh

  期望：fixture 能完成 full backup、log backup checkpoint 等准备动作。

### Task 5: Migration renewal success with normal unlock

Scope: 实现第 1 个实际 case，证明 migration lock 在真实 PD clock 下能 renewal，且正常退出会 unlock。

Steps:

- [ ] 使用 `prepare_pitr_fixture migration_renewal_success` 准备数据。
- [ ] 后台启动真实 PITR restore 命令，设置 failpoint：

      github.com/pingcap/tidb/pkg/objstore/lease-lock-test-constants=return("ttl=2s,interval=200ms,write-timeout-cap=100ms,min-remaining=200ms,stale-reclaim-grace=500ms,base-backoff=100ms")
      github.com/pingcap/tidb/br/pkg/restore/lease-clock-pd-now-signal=return("dir=<pd-clock-dir>")
      github.com/pingcap/tidb/br/pkg/restore/log_client/lease-lock-after-migration-lock-acquired=return("signal=<signal>,release=<release>,after=<after>")

- [ ] 等待 `<signal>` 出现。
- [ ] 在 log storage 下找到唯一 `v1/LOCK.READ.*` physical lock。
- [ ] 保存 exact physical lock path 和 txn id。
- [ ] 读取第一次 `ExpireAt`，等待至少一次 renewal，再读取第二次，断言第二次更大。
- [ ] 在 acquired signal 后记录 `<pd-clock-dir>` 下已有 `now.*` marker 数量；等待 `ExpireAt` 推进后断言 marker 数继续增加，最好至少增加 2 次，证明 renewal 读旧 lease 和 post-write proof 都经过 PD-backed clock。
- [ ] 创建 `<release>` 放行 restore。
- [ ] 等待 BR 进程退出且 exit code 为 0。
- [ ] 断言 `<after>` 存在。
- [ ] 校验 restore 业务结果，例如目标表 row count 或 sum/checksum 与 fixture 预期一致。
- [ ] 断言 BR 日志不包含 `lease lost`、`onLeaseLost`、`context canceled`。
- [ ] 断言当前 holder 的 exact `v1/LOCK.READ.<32hex>` physical lock 不存在。
- [ ] 再运行一个会获取 migration write lock 的真实路径，例如 `br operator unsafe-migrate-to` 或带 `--clean-up-compactions` 的 truncate 路径；不能只看 exit code，还要断言日志不包含 `failed to get the lock, nothing will happen` 或 `the following errors happened`。如实现成本可控，优先给 migration write lock acquired 增加 marker 并断言 marker 出现。

### Task 6: Migration lost by block and error

Scope: 复用 migration fixture，分别覆盖 block 和 error 两种 renewal 故障。

Steps:

- [ ] 实现 `run_migration_lost_case <mode>`，`mode` 取 `block` 或 `error`。
- [ ] 两种模式都设置 `lease-lock-test-constants` 和 `lease-lock-after-migration-lock-acquired`。
- [ ] `block` 模式额外设置：

      github.com/pingcap/tidb/pkg/objstore/lease-lock-renewal-write-block=return("signal=<fault>")

- [ ] `error` 模式额外设置：

      github.com/pingcap/tidb/pkg/objstore/lease-lock-renewal-write-error=return("signal-dir=<fault-dir>")

- [ ] 后台启动真实 PITR restore，等待 acquired signal。
- [ ] block 模式等待 fault signal，记录 fault signal 到 BR 退出的 elapsed time，断言小于明确上界，例如 10 秒。
- [ ] error 模式等待 `<fault-dir>/attempt.1` 和 `<fault-dir>/attempt.2`，证明发生过至少两次 renewal write attempt。
- [ ] 不创建 release 文件。
- [ ] 等待 BR 进程在有限时间内失败退出。
- [ ] block 模式断言日志包含 renewal write timeout 或 `lease lost`。
- [ ] error 模式断言日志包含 transient retry/backoff 信息，最后包含 proven-window 相关分支，例如 `proven lease window elapsed` 或 `retry backoff would exceed proven lease window`，并包含 `lease lost` 或 context cancellation。
- [ ] 断言 after marker 不存在。
- [ ] 断言 restore 业务副作用没有发生，例如目标库表不存在或 row count 未变化。
- [ ] 对 `block` 和 `error` 各运行一次。

### Task 7: Truncate renewal success with normal unlock

Scope: 实现第 4 个实际 case，证明 outer truncate lock 在真实 `RunStreamTruncate` 路径下能 renewal，且正常退出会 unlock。

Steps:

- [ ] 使用 `prepare_pitr_fixture truncate_renewal_success` 准备 log backup metadata。
- [ ] 后台启动真实命令：

      run_br log truncate -s "local://<log_storage>" --until "<truncate_ts>" -y

- [ ] 设置 failpoint：

      github.com/pingcap/tidb/pkg/objstore/lease-lock-test-constants=return("ttl=2s,interval=200ms,write-timeout-cap=100ms,min-remaining=200ms,stale-reclaim-grace=500ms,base-backoff=100ms")
      github.com/pingcap/tidb/br/pkg/restore/lease-clock-pd-now-signal=return("dir=<pd-clock-dir>")
      github.com/pingcap/tidb/br/pkg/task/lease-lock-after-truncate-lock-acquired=return("signal=<signal>,release=<release>,after=<after>")

- [ ] 等待 `<signal>` 出现。
- [ ] 找到唯一 `truncating.lock.*` physical lock。
- [ ] 保存 exact physical lock path 和 txn id。
- [ ] 读取并断言 `ExpireAt` 推进。
- [ ] 在 acquired signal 后记录 `<pd-clock-dir>` 下已有 `now.*` marker 数量；等待 `ExpireAt` 推进后断言 marker 数继续增加，最好至少增加 2 次。
- [ ] 创建 `<release>` 放行 truncate。
- [ ] 等待命令 exit code 为 0。
- [ ] 断言 `<after>` 存在。
- [ ] fixture 必须包含至少一个确定 eligible 的待删 metadata/data object；断言 truncate safepoint 文件被推进到本 case 的 `truncate_ts`，并断言该目标 metadata/data file 被删除或 metadata 被 rewrite，不能只用 safepoint 推进作为唯一业务成功证据。
- [ ] 断言 BR 日志不包含 `lease lost`、`onLeaseLost`、`context canceled`。
- [ ] 断言当前 holder 的 exact `truncating.lock.<32hex>` 不存在。
- [ ] 再运行一次轻量 `br log truncate`，确认没有被残留 truncate lock 阻塞。

### Task 8: Truncate lost by block and error

Scope: 复用 truncate fixture，分别覆盖 block 和 error 两种 renewal 故障。

Steps:

- [ ] 实现 `run_truncate_lost_case <mode>`，`mode` 取 `block` 或 `error`。
- [ ] 启动真实 `br log truncate -s "local://<log_storage>" --until "<truncate_ts>" -y`。
- [ ] 两种模式都设置 `lease-lock-test-constants` 和 `lease-lock-after-truncate-lock-acquired`。
- [ ] `block` 模式设置 `lease-lock-renewal-write-block`。
- [ ] `error` 模式设置 `lease-lock-renewal-write-error`。
- [ ] block 模式等待 acquired signal 和 fault signal，并记录 fault signal 到 BR 退出的 elapsed time。
- [ ] error 模式等待 acquired signal，并等待至少两个 error attempt marker。
- [ ] 不创建 release 文件。
- [ ] 等待 BR 命令在有限时间内失败退出。
- [ ] block 模式断言日志包含 renewal write timeout 或 `lease lost`。
- [ ] error 模式断言日志包含 transient retry/backoff 信息，最后包含 proven-window 相关分支，例如 `proven lease window elapsed` 或 `retry backoff would exceed proven lease window`，并包含 `lease lost` 或 context cancellation。
- [ ] 断言 after marker 不存在。
- [ ] 断言 truncate safepoint 未推进，且 fixture 中那个确定 eligible 的关键待删 metadata 或 data file 仍存在。
- [ ] 对 `block` 和 `error` 各运行一次。

### Task 9: Stale lock reclaim real-command case

Scope: 实现第 7 个实际 case，证明超过 `ExpireAt + staleReclaimGrace` 的旧 lock 不会永久阻塞真实 BR 命令。

Steps:

- [ ] 使用 truncate fixture 准备真实 log storage。
- [ ] 选择一个确定属于 truncate lock family 的 exact path：`truncating.lock.<32hex>`，其中 `<32hex>` 满足 `pkg/objstore/locking_helper.go` 的 instance-form path 分类规则。
- [ ] 在 log storage 根目录手工写入该 exact path，内容为合法 `LockMeta` JSON。写入后立刻读回 JSON，打印 `expire_at` 和 `txn_id`，确认字段格式与 `pkg/objstore.LockMeta` 一致。

      {
        "locked_at": "2000-01-01T00:00:00Z",
        "locker_host": "stale-test",
        "locker_pid": 1,
        "txn_id": "AAAAAAAAAAAAAAAAAAAAAA==",
        "hint": "stale integration test",
        "expire_at": "2000-01-01T00:00:01Z"
      }

- [ ] 把 exact path 写成 stale lock，满足 `ExpireAt + staleReclaimGrace < current PD lease time`。运行真实 `br log truncate -s "local://<log_storage>" --until "<truncate_ts>" -y`，同时设置 `lease-lock-test-constants` 缩短 `stale-reclaim-grace`，并设置 `lease-lock-stale-reclaim-signal=return("dir=<stale-reclaim-dir>")`。
- [ ] 断言命令 exit code 为 0。
- [ ] 断言同一个 exact stale lock file 被删除；不能只用“后续不阻塞”替代删除断言。
- [ ] 断言 `<stale-reclaim-dir>` 中出现指向同一个 exact stale lock basename 的 marker。
- [ ] 断言日志包含 stale cleanup/reclaim 相关信息；如果现有日志文本不稳定，实现阶段需要补充稳定测试日志或依赖 reclaim marker，不能静默放弃 reclaim 分支观测。

### Task 10: Group, Bazel, and validation

Scope: 把新 case 接入 CI group，并运行必要验证。

Steps:

- [ ] 修改 `br/tests/run_group_br_tests.sh`，把 `br_lease_lock` 加到一个合适 group。
- [ ] 运行 group 漏加检查：

      br/tests/run_group_br_tests.sh others

  期望：输出 `All br integration test cases have been added to groups`。

- [ ] 如果新增 Go 文件或新增顶层 Go test function，运行：

      make bazel_prepare

  期望：Bazel metadata 更新完成；检查并提交相关 `BUILD.bazel`/`.bzl` 变化。

- [ ] 运行 objstore 相关 failpoint 单测：

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock|TestRenewal|TestCleanUpStale' -count=1

- [ ] 运行 BR utils helper 单测：

      ./tools/check/failpoint-go-test.sh br/pkg/utils -run 'TestLeaseLockFailpoint' -count=1

- [ ] 运行新增 BR integration case：

      make build_for_br_integration_test
      TEST_NAME=br_lease_lock br/tests/run.sh

- [ ] 记录 `TEST_NAME=br_lease_lock br/tests/run.sh` 的总耗时。选择 CI group 时不能只依赖 `others` 漏加检查；如果新增 group，需要同步修改 CI 配置。
- [ ] Ready 前运行：

      make lint

- [ ] 文档检查：

      git diff --check
      rg -n -P '\x60(MUST(?: NOT)?|SHOULD|MAY)\x60' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br/lease-lock-integration-test-design.md docs/agents/br/lease-lock-integration-test-implementation-plan.md CONTEXT.md
      rg -n 'TO(DO)|TB(D)' docs/agents/br/lease-lock-integration-test-design.md docs/agents/br/lease-lock-integration-test-implementation-plan.md CONTEXT.md

## Validation and Acceptance

实现完成后，必须能观察到：

- migration success case 中 exact `v1/LOCK.READ.<32hex>` 的 `ExpireAt` 推进，PD clock marker 在 acquired 后继续增加，BR restore 成功且数据校验通过，当前 holder lock 被删除，后续 migration write 路径不被阻塞且日志无 lock warning，成功日志无 lost/cancel。
- migration block/error lost case 中 block fault 或至少两个 error attempt marker 出现，BR restore 失败退出，日志显示 timeout 或 proven-window retry/lost/cancel，after marker 不存在，restore 业务副作用没有发生。
- truncate success case 中 exact `truncating.lock.<32hex>` 的 `ExpireAt` 推进，PD clock marker 在 acquired 后继续增加，真实 `br log truncate` 成功并推进 safepoint 且删除/rewrite 确定 eligible 的目标 metadata/data，当前 holder lock 被删除，后续 truncate 不被阻塞，日志无 lost/cancel。
- truncate block/error lost case 中 block fault 或至少两个 error attempt marker 出现，真实 `br log truncate` 失败退出，after marker 不存在，truncate safepoint 未推进且关键待删对象仍存在。
- stale reclaim case 中 exact `truncating.lock.<32hex>` 以 stale 状态被真实 truncate stale cleanup 处理，reclaim marker 指向该 exact path，文件被删除并允许命令成功。
- `TEST_NAME=br_lease_lock br/tests/run.sh` 单独通过。
- `br/tests/run_group_br_tests.sh others` 不报告漏加 case。

## Idempotence and Recovery

`br/tests/br_lease_lock/run.sh` 中每个 case 都使用独立 storage 子目录和 marker 目录。单个 case 失败后可以删除 `/tmp/backup_restore_test` 并重新运行 `TEST_NAME=br_lease_lock br/tests/run.sh`。

如果后台 BR 进程超时，shell helper 必须打印日志文件路径并 kill 对应进程，避免后续 case 被残留进程污染。

如果 failpoint 字符串解析失败，测试应立即失败并打印完整 `GO_FAILPOINTS`，不要退回生产默认 lease 参数。

所有 `GO_FAILPOINTS` 都应使用命令级作用域传给单个 `run_br`，或者在 helper/trap 中恢复为空。任何 case 失败时都要打印当前 failpoint 字符串和对应日志文件。

## Risks

- Runtime: 7 个真实 BR case 可能偏慢。通过复用小数据、缩短 lease 参数和独立 helper 控制时长。
- Flakiness: block case 依赖 timeout 和 CI 调度。通过 `signal` 确认故障已发生，并给 `min-remaining` 留抖动余量。
- Fixture complexity: PITR/log truncate 数据准备可能占主要时间。优先复用现有 BR integration 准备方式，不手工拼 metadata。
- Scope creep: 本计划只加测试和 test-only failpoint，不重构 `RemoteLock` lifecycle，也不实现 lost 后主动删除 own lock。
