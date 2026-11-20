# ADD INDEX 迟到 merge 可达性审查

状态：源码链路审查，WIP。尚未完成真实调度流程的确定性故障注入复现，不能宣称现有 ADD INDEX 已确认存在数据损坏漏洞。

## 正常成功路径

`pkg/dxf/framework/taskexecutor/task_executor.go:runSubtask` 等待 `stepExec.RunSubtask` 返回，再调用 finishSubtask。`pkg/dxf/framework/scheduler/scheduler.go:onRunning/isStepSucceed` 只在该 step 无 pending/running/failed/canceled 子任务时推进。`pkg/ddl/index.go` 等待 DXF task 结束，Merging reorg 完成后才设 BackfillStateInapplicable。因此，直接拿 Public 元数据调用 step 的已有反例不能证明正常成功路径可达。

## 迁移路径中的待证窗口

1. `scheduler/balancer.go:doBalanceSubtasks` 对正常可用节点只搬 pending；但节点不在 adjustedNodeMap（死亡或资源不足）时，将其全部活动 subtasks 加入重新分配，包括 running。
2. `storage/task_table.go:UpdateSubtasksExecIDs` 事务更新 exec_id，只校验 ID/state，没有等待旧执行器退出的同步确认。
3. `taskexecutor/task_executor.go:checkBalanceSubtask` 默认每 2 秒轮询归属；发现运行任务已不属于本机才取消，读取元数据出错则继续等待。取消不是与用户 KV 提交同事务的 fence。
4. `ddl/backfilling_dist_executor.go:IsIdempotent` 对这些 subtasks 返回 true，新执行器可接手 running 子任务。
5. `storage/subtask_state.go:FinishSubtask` 用 exec_id 约束成功状态更新。这能防旧执行器错误标记任务成功，不能阻止旧执行器此前或此后的用户索引 KV 写入。

## 需要确定性复现的具体时序

在捕获已开启、基线读 S 尚未取得时插入一行，使基线正常索引与 b-version temp put 同时存在。生产 `checkTempIndexKey` 对值相同的正常索引设置 skip；两个 worker 循环遇到 skip 都直接 continue，不删除对应 temp key。

暂停旧执行器 A，使其尚未开启下一次 merge 事务。通过真实 balancer/归属更新让 B 接手。B 跳过上述已存在的索引，保留 b temp，完成 step；DDL 进入 Inapplicable/Public。普通 DML 删除该行及正常索引，此时不再写 temp。若 A 在发现归属变化并取消之前恢复，使用新事务读到残留 b put 和缺失的正常索引，当前非唯一 merge 逻辑可以重建孤立索引。

关键是 A 的事务在前台 DELETE **之后**开始。若只是让一个旧快照事务晚提交，正常 MVCC write conflict 可能已经挡住，不能把两者混为一谈。旧 worker 的新事务是否实际能启动、框架是否还有其他同步屏障、两次 schema 同步/MDL 是否让窗口不可达，都需要真实调度测试确认。

## 已有证据与边界

手工越生命周期反例 `.codex-tmp/partition-reorg-prototype-ln761qgn/result.json` 在六种 batch/CPU 配置下重建了孤立索引，说明执行层本身不拒绝该输入。它没有覆盖上面的真实任务迁移及 schema 切换，不能作为完整漏洞复现。

本轮未修改生产代码、未运行新增调度集成测试。检索并阅读了 index.go、backfilling_dist_executor.go、index_merge_tmp.go、backfilling_operators.go，以及 DXF scheduler/balancer、taskexecutor、storage 的任务归属和成功条件。下一项验收是将上述时序接入真实任务生命周期的 failpoint 测试；测试必须证明执行了归属迁移和 DDL 状态切换，并区分旧快照冲突与切换后新事务。Go 构建/测试保持 -p 2、GOMAXPROCS=2。

## Targeted fix in progress


The user explicitly requests a TiDB fix. Consume a skipped, already-validated b temp key in the same merge transaction, in both legacy and DXF workers. The cleanup also establishes a write/write conflict with another attempt or concurrent temp writer. TestAddIndexSkippedTempCleanup covers matching baseline+b, cleanup, subsequent row/index deletion and late replay for both workers. Require failure before the fix and success afterward; keep the broader task-reassignment reachability audit distinct from this verified stale-temp defect. Go concurrency remains 2.

## Concurrency and regression correction


The user changed Go concurrency to 1: all subsequent Go build/test commands use -p 1, GOMAXPROCS=1 and run serially. The first regression selected a nonunique insert fixture, which does not enter batchCheckTemporaryUniqueKey's skip branch, and passed before any fix. This is not fail-before evidence. Select the unique insert fixture before rerunning. Let canonical failpoint cleanup finish before editing/rerunning; no production fix was applied by the aborted orchestration.

## Confirmed cleanup omission and local fix


TestAddIndexSkippedTempCleanup now selects a unique insert fixture (the original nonunique selection never entered skip). Both real legacy/DXF worker subtests fail before the fix and pass afterward. The fix adds temporary-key deletion inside each validated skip branch; normal index semantics are unchanged. Root production replay/codec matrix also passes with the fix.

TestAddIndexConsumesSkippedBackfillTemp drives normal SQL ALTER TABLE ADD UNIQUE INDEX, inserts after capture starts but before SnapshotVer is chosen, and observes the real merge batch. With both fixes temporarily removed it fails with skipped backfill temp remains after SQL ADD INDEX merge. The fixes were restored in a finally block; SQL regression and six existing corner cases pass afterward. Logs: .codex-tmp/skipped-temp-unique-before.log, skipped-temp-unique-after.log, skipped-temp-sql-before.log, skipped-temp-sql-after.log.

Important correction of scope: temporary ranges are eventually reclaimed by delete-range GC. The confirmed report concerns skipped-record cleanup and replayable stale history; no wrong query result through actual task migration has been demonstrated. The issue body explicitly preserves this limitation. An issue on this narrow verified behavior was authorized by the user; title/body/labels follow tidb-issue-metadata-guard and the bug-report template, after issue/PR duplicate searches. Do not describe it as a confirmed corruption vulnerability.

All new Go tests/builds use -p 1, GOMAXPROCS=1 after the user's concurrency update. Production changes are limited to the two skip branches. The broader reassignment/fresh-transaction reachability investigation remains open.

## Issue and validation outcome


Filed https://github.com/pingcap/tidb/issues/73115 with type/bug, component/ddl, severity/moderate after duplicate searches. The issue explicitly reports only the confirmed skip-temp cleanup omission, acknowledges eventual range GC and correct ordinary SQL results in the reproduction, and does not claim that task-migration corruption is proven. The code change remains a narrow cleanup/hardening fix; full migration reachability still requires separate work.

Validation commands (Go concurrency 1):

    GOMAXPROCS=1 ./tools/check/failpoint-go-test.sh pkg/ddl -p 1 -run '^TestAddIndexSkippedTempCleanup$' -count=1 -timeout=10m
    GOMAXPROCS=1 ./tools/check/failpoint-go-test.sh pkg/ddl -p 1 -run '^(TestAddIndexSkippedTempCleanup|TestPartitionPrototypeProductionMerge|TestPartitionPrototypeFixtureCodec)$' -count=1 -timeout=10m
    GOMAXPROCS=1 ./tools/check/failpoint-go-test.sh pkg/ddl/tests/indexmerge -p 1 -run '^TestAddIndexConsumesSkippedBackfillTemp$' -count=1 -timeout=10m
    GOMAXPROCS=1 ./tools/check/failpoint-go-test.sh pkg/ddl/tests/indexmerge -p 1 -run '^(TestAddIndexConsumesSkippedBackfillTemp|TestAddIndexMergeProcess|TestAddIndexMergeVersionIndexValue|TestAddIndexMergeDeleteNullUnique|TestAddIndexMergeReplaceDelete|TestAddIndexMergeDeleteDifferentHandle|TestAddUniqueIndexFalsePositiveDuplicate)$' -count=1 -timeout=10m
    GOMAXPROCS=1 GOFLAGS='-p=1' make bazel_prepare
    GOMAXPROCS=1 GOFLAGS='-p=1' make lint

Before-fix tests fail at the intended temp-retention assertions. After-fix worker/matrix/codec and SQL suites pass. The SQL before-run temporarily removed only our own two skip-branch changes; a finally block restored their exact bytes before the after-run. Canonical failpoint cleanup completed with refcount 0. Production diff is 12 insertions / 2 comment deletions across index_merge_tmp.go and backfilling_operators.go; regression tests and generated BUILD metadata accompany it. The standalone runner's Go build/runtime defaults now follow the user preference of 1.

Ready profile applies to this narrow fix after final lint/diff checks. No claim is made about full REORGANIZE integration, scheduler failover safety, or a confirmed existing wrong-result bug. Full current-source real TiKV replays and scheduling-reassignment tests have not been run for this patch; the production worker matrix and SQL tests use embedded stores.
