# Lease Lock 集成测试设计

## 状态

初版测试设计，用于讨论第一版 BR 集成测试覆盖范围。

本文档描述 lease lock renewal bounded proof 在真实 BR 流程中的集成测试方案。目标不是
重复 `pkg/objstore` 单测里的所有边界，而是验证真实 PD/TiKV/TiDB、真实 BR CLI、真实
业务临界区取消路径组合起来以后，仍然满足安全预期。

## 背景

当前单测已经覆盖了 lock 层的细粒度语义：

- renewal `WriteFile` 使用 bounded context；
- renewal 成功写入后做 post-write proof；
- proof 后剩余 lease 小于阈值会进入 lease lost；
- 下一轮 renewal delay 来自已证明的 remaining lease；
- transient retry backoff 不会睡过已证明 lease window；
- stale cleanup 使用 `ExpireAt + staleReclaimGrace`。

这些单测能证明 `pkg/objstore` 状态机本身，但不能证明：

- `restore.NewPDLeaseClock` 在真实 PD/TSO 下可用；
- `br.test` CLI 的真实业务流程会正确接入 `onLeaseLost`；
- 持锁临界区被 lease lost cancel 后不会继续执行受保护动作；
- `br/tests` 里的真实 cluster harness 能稳定覆盖这些路径。

因此需要在 `br/tests` 下增加一套真实 BR 集成测试。

## 目标

第一版集成测试回答六个问题：

1. migration lock 路径在真实 PD clock 下能正常续约。
2. migration lock 路径在 renewal lost 后会停止 restore 临界区。
3. truncate lock 路径在真实 PD clock 下能正常续约。
4. truncate lock 路径在 renewal lost 后会停止 truncate 临界区。
5. 正常完成的持锁 BR 流程会走 `Unlock`，清理自己的物理 lock，不留下会阻塞后续
   操作的 lock 文件。
6. 真实 BR 命令能 reclaim 一个超过 `ExpireAt + staleReclaimGrace` 的旧 lock。

## 非目标

- 不测试真实 30 分钟或 60 分钟等待，所有时间参数都通过 failpoint 缩短。
- 不接入真实 S3/GCS/Azure bucket；第一版只使用 `local://` storage。
- 不把所有 lock 单测边界搬到 shell 集成测试里。
- 不测试对象存储 eventual consistency 的所有组合。
- 不证明 delayed renewal zombie 永远不会出现。

## 测试位置

新增目录：

```text
br/tests/br_lease_lock/
  run.sh
```

如果 helper 增多，可以拆出：

```text
br/tests/br_lease_lock/
  run.sh
  util.sh
```

运行方式沿用 BR integration test harness：

```bash
TEST_NAME=br_lease_lock br/tests/run.sh
```

该 harness 会启动真实 PD/TiKV/TiDB，并通过 `run_br` 执行 `br.test`。

## 第一版测试矩阵

### 共同测试语义

#### critical-section marker

lost case 的 `after-critical-section` marker 必须代表命令已经越过 lease 保护边界，
即将开始或已经开始执行受保护副作用。它不能只表示“拿到了 lock”，否则无法证明丢锁后
业务临界区停止。

第一版采用以下语义：

- `signal` marker：目标 lock 已经 acquire 成功，测试可以开始观察 renewal 或注入故障。
- `release` marker：成功 case 中由 shell 创建，用来放行被阻塞的 BR 命令。
- `after-critical-section` marker：BR 命令通过可取消阻塞点，并准备进入受保护副作用前
  创建。lost case 中如果 context 已经被 lease lost cancel，该 marker 不应出现。

migration lock 路径中，该阻塞点放在 migration lock acquired 之后、进入受 migration
lock 保护的 restore / migration metadata 关键动作之前。truncate lock 路径中，该阻塞点
放在外层 `LockRemoteTruncate` acquired 之后、推进 truncate safepoint 或删除 metadata /
data file 之前。

#### unlock 检查口径

正常 unlock 的集成检查只要求证明当前 holder 没有留下会阻塞后续操作的 lock：

- 当前 holder 的 physical lock file 不存在；
- 同类 lock family 中没有会阻塞后续同类命令的残留 lock；
- 后续轻量同类 acquire 或同类 BR 命令可以成功。

第一版不要求整个 lock family 目录完全为空，因为目录中可能存在旧版本兼容样本、其他 holder
样本或测试刻意构造的 stale lock。

#### 时间参数

文档中的 lease 参数是实现参考值，不作为必须固定的最终值。实现时需要保持这些关系：

- TTL 明显短于生产值，保证单个 case 能在几十秒内结束；
- renew interval 足够短，成功 case 能稳定观察至少一次 `ExpireAt` 推进；
- renewal write timeout cap 足够短，block 故障能稳定触发 bounded timeout；
- min remaining lease 需要留出 CI 抖动余量，避免真实 PD/TSO 和调度延迟导致误判。

#### truncate fixture

truncate case 优先复用现有 BR integration test 中能生成 log backup metadata 的流程，再执行
真实 `br log truncate -s local://... --until ... -y`。第一版不手工拼对象存储 metadata，
除非实现阶段发现现有 helper 无法稳定构造目标路径。

### 1. migration lock renewal with real PD clock

目的：

- 验证 PiTR restore / migration metadata 路径持有 migration lock 后，能用真实
  `PDLeaseClock` 完成至少一次 renewal。
- 验证 post-write proof 不会在真实 TSO 下误判。

测试步骤：

1. 创建小库小表，准备 full backup 和 log backup。
2. 启动需要读取或追加 migration metadata 的 restore 流程。
3. 通过 failpoint 在 migration lock acquired 后写入 signal file 并阻塞。
4. 通过 failpoint 缩短 lease 参数，例如：

   ```text
   LeaseTTL = 2s
   renewInterval = 200ms
   renewWriteTimeoutCap = 100ms
   minRenewRemainingLease = 200ms
   staleReclaimGrace = 500ms
   ```

5. shell 等待 signal file，读取 local storage 下对应 lock metadata 的 `ExpireAt`。
6. 等待至少一个 renewal interval，再次读取 `ExpireAt`。
7. 断言 `ExpireAt` 已推进。
8. 创建 release file 放行 restore。
9. 断言 BR 命令成功，数据校验成功。
10. 断言正常 unlock 后 migration lock family 不再残留当前 holder 的 physical lock。
11. 再跑一次轻量同类 acquire 或同类 BR 命令，确认后续操作不会被残留 lock 阻塞。

关键指标：

- lock acquired signal file 出现。
- 同一个 physical lock object 的 `ExpireAt` 单调推进。
- BR 进程退出码为 0。
- restore 后数据行数或 checksum 符合预期。
- 正常退出后没有残留当前 holder 的 migration physical lock。
- 后续同类操作能重新 acquire lock。
- 日志中没有 lease lost / context canceled。

能证明：

- 真实 PD clock 能驱动 migration lock renewal。
- BR CLI 正常流程和 bounded proof 不冲突。
- 缩短 lease 参数后 renewal loop 实际运行过，而不是只 acquire 后立即结束。
- 正常 `Unlock` 会等待 renewal loop 停止并删除自己的物理 lock，不会被 in-flight renewal
  重新写出 zombie lock。

不能证明：

- 真实云对象存储的一致性行为。
- 所有 migration lock 冲突组合。

### 2. migration lock lost cancels restore critical section

目的：

- 验证 migration lock 持有期间 renewal lost 会 cancel restore 工作。
- 验证业务临界区不会在丢锁后继续执行。

测试步骤：

1. 准备和 case 1 类似的 PiTR restore 输入。
2. 启动 restore，并在 migration lock acquired 后通过 failpoint 阻塞。
3. 分别注入两类只作用于 renewal write 的 failpoint：

   ```text
   lease-lock-renewal-write-block
   lease-lock-renewal-write-error
   ```

   `lease-lock-renewal-write-block` 覆盖 storage write hang 住时的 bounded context
   timeout；`lease-lock-renewal-write-error` 覆盖 storage write 快速返回错误时的
   retry/backoff/lost。两者都是第一版必测语义。

4. 等待 BR 进程退出。
5. 断言退出码非 0。
6. 断言日志包含 lease lost 相关信息。
7. 断言 failpoint 阻塞点之后的 after-critical-section marker 不存在。

关键指标：

- lock acquired signal file 出现。
- block 和 error 两类 renewal fault signal file 都出现过，证明故障发生在持锁后。
- BR 进程在有限时间内失败退出。
- 日志包含 `lease lost`、`onLeaseLost`、或由 context cancellation 派生的错误。
- after-critical-section marker 不存在。

能证明：

- `onLeaseLost` 在真实 BR restore 路径上会传递到业务 context cancel。
- 丢锁后不会继续执行 migration lock 保护的关键动作。
- bounded renewal write timeout 在真实 BR 路径上能打断 hang 住的 storage write。
- renewal write 快速失败时，retry/backoff/lost 路径在真实 BR 路径上生效。
- bounded renewal proof 的安全语义不只停留在 `pkg/objstore` 单元测试。

不能证明：

- lost 后对象存储中不会留下 delayed renewal zombie。
- 正常 unlock cleanup 的完整生命周期重构，这属于后续阶段。

### 3. truncate lock renewal with real PD clock

目的：

- 验证 log truncate / metadata cleanup 路径持有 truncate lock 后，能用真实 PD clock
  完成 renewal。
- 覆盖 `LockRemoteTruncate` 这条和 migration lock 不同的物理 lock 家族。
- 第一版中 “truncate lock” 专指外层 `truncating.lock.<generation>`。如果 truncate
  流程内部因为 `CleanUpCompactions` 再获取 migration write lock，该内层 lock 不作为
  本 case 的观测目标。

测试步骤：

1. 准备一组 log backup metadata 或可触发 truncate 的最小输入。
2. 启动真实 `br log truncate -s local://... --until ... -y` 命令。
3. 通过 failpoint 在 truncate lock acquired 后 signal 并阻塞。
4. 缩短 lease 参数。
5. 读取 `truncating.lock.<generation>` metadata 的 `ExpireAt`。
6. 等待 renewal 后再次读取。
7. 断言 `ExpireAt` 已推进。
8. 放行命令。
9. 断言命令按预期成功。
10. 断言正常 unlock 后不再残留 `truncating.lock.<generation>`。
11. 再跑一次轻量 truncate acquire 或同类 BR 命令，确认后续操作不会被残留 lock 阻塞。

关键指标：

- truncate lock acquired signal file 出现。
- `truncating.lock.<generation>` 的 `ExpireAt` 推进。
- BR 命令退出码为 0。
- 命令走过真实 `RunStreamTruncate` 数据路径，而不是只测试一个 lock helper。
- 正常退出后没有残留当前 holder 的 truncate physical lock。
- 后续同类操作能重新 acquire lock。
- 没有 lease lost 日志。

能证明：

- truncate lock 真实路径也接入了 PD-backed lease renewal。
- migration lock 测试没有遗漏另一类重要 lock family。
- truncate 路径正常 `Unlock` 会清理物理 lock，后续 truncate 类操作不会被自己留下的
  lock 阻塞。
- 外层 `LockRemoteTruncate` 的 lease 行为和正常释放路径是健康的。

不能证明：

- truncate 删除的所有业务数据组合。
- truncate 流程内部 migration write lock 的行为；该行为由 migration lock case 覆盖。
- 旧版本无 `ExpireAt` lock 的兼容处理，单测已覆盖。

### 4. truncate lock lost cancels truncate critical section

目的：

- 验证 truncate lock 丢失后，truncate / metadata cleanup 不会继续删除或更新受保护对象。
- 这里的 truncate lock 仍然只指外层 `truncating.lock.<generation>`；不把内部 migration
  write lock 的 lost 行为混入本 case。

测试步骤：

1. 准备最小可触发 truncate 的 storage 数据。
2. 启动真实 `br log truncate -s local://... --until ... -y` 命令。
3. 在 truncate lock acquired 后 signal 并阻塞。
4. 分别注入 renewal write block 和 renewal write error。
5. 等待 BR 进程退出。
6. 断言退出码非 0。
7. 断言日志包含 lease lost / context canceled。
8. 断言 after-critical-section marker 不存在。
9. 如可稳定判断，额外断言关键对象仍存在或 metadata 未被推进。

关键指标：

- truncate lock acquired signal file 出现。
- block 和 error 两类 renewal fault signal file 都出现过。
- BR 命令在有限时间内失败退出。
- 命令已经进入真实 `RunStreamTruncate` 数据路径，故障只改变 renewal 行为。
- after-critical-section marker 不存在。
- 关键受保护对象没有被删除。

能证明：

- truncate 临界区在 lease lost 后停止。
- truncate lock 和 migration lock 两条业务路径都受同一安全机制保护。
- bounded renewal write timeout 和 renewal write 快速失败都能触发 truncate 路径停止。

不能证明：

- 所有 cleanup 可用性问题。
- lost 后主动删除自己 lock 的行为，因为第一阶段没有实现该功能。

### 5. normal unlock cleanup assertions

目的：

- 把正常 `Unlock` 作为第一版集成测试的明确验收点，而不是只依赖单测。
- 覆盖 renewal loop 和 `Unlock` 同时存在时的真实 BR 退出路径。

测试方式：

- 不单独新增一个很重的 shell case。
- 将该断言嵌入两个成功 case：
  - `migration lock renewal with real PD clock`
  - `truncate lock renewal with real PD clock`

每个成功 case 都需要在 BR 命令完成后检查：

1. 当前 holder 的 physical lock file 不存在。
2. 同类 lock family 下没有会阻塞后续同类命令的残留 lock。
3. 后续轻量同类 acquire 或同类 BR 命令可以成功。

关键指标：

- BR 命令退出码为 0。
- after-critical-section marker 存在。
- lock file check 没有发现当前 holder 残留。
- 后续同类操作成功。

能证明：

- 正常退出路径会调用 `Unlock`。
- `Unlock` 会等待 renewal loop 停止后再删除物理 lock。
- 没有发生 renewal write 在 delete 后重新写回 lock 的集成级回归。

不能证明：

- lease lost 后主动删除自己的 lock。第一阶段没有实现该语义。
- 所有 delayed write zombie 情况。该类风险由 bounded proof 和 stale reclaim 控制。

### 6. stale lock reclaim allows real command

目的：

- 验证真实 BR 命令不会被超过 reclaim threshold 的旧 lock 永久卡住。
- 覆盖 bounded proof 和 delayed write zombie 风险之外的最终兜底路径。

测试步骤：

1. 在 `local://` storage 中手工写入一个目标 lock family 的物理 lock file。
2. lock metadata 设置为：

   ```text
   ExpireAt + staleReclaimGrace < current PD lease time
   ```

3. 启动需要 acquire 同类 lock 的 BR 命令。
4. 断言命令能够 reclaim stale lock 并继续。

关键指标：

- stale lock file 被删除或被新的 lock family 状态取代。
- BR 命令退出码为 0。
- 日志包含 reclaim stale lock。

能证明：

- `staleReclaimGrace = 30min` 的机制可以在真实 BR CLI 路径上解除旧 lock 阻塞。
- 如果历史 holder 已经不再续约，后续真实命令最终可以通过 stale reclaim 恢复可用性。

不能证明：

- `ExpireAt + staleReclaimGrace` 未到时不会误删。这个反面边界由单测覆盖，shell
  集成测试不重复等待。

## 需要新增的测试 failpoint

第一版需要少量 test-only failpoint，避免靠长 sleep 猜 timing。

### 1. 缩短 lease 参数

建议位置：

```text
pkg/objstore
```

建议名称：

```text
lease-lock-test-constants
```

建议输入格式：

```text
ttl=2s,interval=200ms,write-timeout-cap=100ms,min-remaining=200ms,stale-reclaim-grace=500ms
```

作用：

- 在 `br.test` 进程内缩短 lease 相关参数；
- 只在 failpoint build 下生效；
- 测试结束后进程退出，自然恢复生产默认值。

### 2. 持锁后 signal/block

建议位置：

```text
br/pkg/restore/log_client 或 br/pkg/stream
br/pkg/task/stream.go
```

建议名称：

```text
lease-lock-after-migration-lock-acquired
lease-lock-after-truncate-lock-acquired
```

建议输入格式：

```text
signal=<path>,release=<path>,after=<path>
```

行为：

- 持有目标 lock 后创建 `signal`；
- 阻塞直到 `release` 文件存在，或直到 context canceled；
- 如果正常通过阻塞点并继续执行受保护动作，创建 `after` marker。

lost 测试断言 `after` 不存在。

### 3. renewal write 故障

建议位置：

```text
pkg/objstore/locking.go
```

建议名称：

```text
lease-lock-renewal-write-block
lease-lock-renewal-write-error
```

作用：

- 只在 `tryRenew` 的 renewal `WriteFile` 路径触发；
- 不影响 initial acquire；
- 不影响普通 BR backup file 写入。

`lease-lock-renewal-write-block` 用于覆盖 bounded write timeout。
`lease-lock-renewal-write-error` 用于覆盖 transient retry/backoff 直到 proven window 不足或
retry exhausted。

第一版 lost 测试需要同时覆盖 block 和 error。测试组织上可以复用同一套业务准备和断言，
但不能只测其中一种故障。

## Shell 测试组织原则

- 所有等待都使用 signal file + polling，避免固定长 sleep。
- 每个 case 使用独立 storage 子目录。
- truncate case 第一版尽量走真实 `br log truncate -s local://... --until ... -y` 数据路径；
  failpoint 只负责 lock acquired 后的 signal/block 和 renewal 故障注入，不默认绕过
  `RunStreamTruncate` 后续逻辑。
- lost case 必须同时覆盖 renewal write block 和 renewal write error；两类故障可以复用
  业务准备和断言，但验收时都要实际跑到。
- lost case 必须同时检查退出码、日志和 after marker。
- 成功 case 必须检查业务结果，不只检查 BR 退出码。
- 成功 case 必须检查正常 unlock 后没有残留当前 holder 的 physical lock，并验证后续
  同类操作可重新 acquire。
- 测试时间参数保持小，但要留足本地 CI 抖动余量。
- 不把真实 S3/GCS 接入第一版。

## 第一版最低验收标准

第一版认为有效，需要满足：

- migration lock renewal 成功 case 通过；
- migration lock lost cancel case 通过；
- truncate lock renewal 成功 case 通过；
- truncate lock lost cancel case 通过；
- migration 和 truncate lost case 都覆盖 renewal write block 和 renewal write error；
- migration 和 truncate 成功 case 都证明正常 unlock 清理了自己的 physical lock；
- 每个 lost case 都能证明 after-critical-section marker 不存在；
- stale lock reclaim real-command case 通过；
- `br_lease_lock` 能通过 `TEST_NAME=br_lease_lock br/tests/run.sh` 单独运行；
- case 被加入 `br/tests/run_group_br_tests.sh`，避免 CI 漏跑。

如果实现成本需要拆分，建议第一批先实现 renewal success、lost cancel 和 stale reclaim
三个安全主线；normal unlock 断言嵌入 success case 中随第一批一起完成。

## 证明力总结

这套集成测试能证明：

- lease lock 第一阶段机制能在真实 BR CLI、真实 PD/TiKV/TiDB 环境下工作；
- migration lock 和 truncate lock 两条业务路径都能正常 renewal；
- migration lock 和 truncate lock 两条业务路径都能正常 unlock，不留下阻塞后续操作的
  lock 文件；
- renewal lost 会传递为业务 context cancel；
- 持锁临界区在丢锁后不会继续执行关键动作；
- 超过 reclaim threshold 的旧 lock 不会永久阻塞后续真实 BR 命令；
- 缩短时间参数下，测试仍覆盖真实 renewal loop，而不是纯单元函数。

这套集成测试不能证明：

- 真实云对象存储所有一致性和延迟行为；
- delayed renewal zombie 永不出现；
- lost 后主动删除自己 lock 的语义；
- 长达 60 分钟生产 TTL 的真实时间等待。

这些不能证明的部分需要通过单测、兼容性测试、nightly 测试或第二阶段 lifecycle 设计继续覆盖。
