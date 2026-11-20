# 分区 global sort：已核实事实与证据边界

当前唯一实施规格为 [最终设计](../../design/2026-11-02-partition-global-sort.md)，后续实施按 [ExecPlan](10-partition-fast-reorg-execplan.md)。[历史可行性](09-partition-fast-reorg-feasibility-history.md)和[审查记录](../../design/2026-11-03-partition-global-sort-review-notes.md)保留被否定方案，不能继续按其旧章节执行。

## 源码基线

TiDB 工作区 commit `44facb0f374a16d39d5a7267714a9c5d0d7f17a4`，有用户其他未提交修改，本工作未修改产品 Go/Bazel。以下事实由当前源文件核实，不靠 agent runbook 推断。

| 事实 | 入口 |
| --- | --- |
| 普通 partition reorg 强制 txn | `pkg/ddl/partition.go:doPartitionReorgWork` |
| 行复制、新分区索引、未变更分区 global 的三阶段 | `reorgPartitionDataAndIndex` |
| nonclustered 冲突可重新分配源行 hidden handle | `reorgPartitionWorker.BackfillData` |
| 新/旧分区按状态双写；目标不存在时当前 nonclustered 删除可跳过 | `pkg/table/tables/partition.go:AddRecord/RemoveRecord/partitionedTableUpdateRecord` |
| SQL 可指定多个源；RANGE 要求相邻 | `pkg/ddl/executor.go:ReorganizePartitions/getReplacedPartitionIDs` |
| 末尾 RANGE 校验允许改变末端 | `checkReorgPartitionDefs`；fast 需另核对外范围 |
| 现有索引扫描按任务读事务 TS，不是天然共享 job.SnapshotVer | `pkg/ddl/backfilling_operators.go:tableScanWorker.scanRecords` |
| 普通 RR 首次访问可能更新 latest MDL table，新增 public index 有特殊隐藏处理 | `pkg/planner/core/preprocess.go:tryLockMDLAndUpdateSchemaIfNecessary` |
| 历史 schema/meta 接口存在 | `pkg/domain/domain.go:GetSnapshotInfoSchema/GetSnapshotMeta` |
| schema cache 的 TS 是 schema diff 事务 commitTS，latest cache 不证明任意未来 TS 精确 | `pkg/infoschema/cache.go:GetBySnapshotTS/getSchemaByTimestampNoLock` |
| MDL 不跟踪尚未访问目标表的旧事务，restricted SQL 也另有规则 | `pkg/sessionctx/variable/session.go:RemoveLockDDLJobs` |
| 原 delete-range 按 GC safePoint 限制 | `pkg/store/gcworker/gc_worker.go:deleteRanges` |
| row checksum 有 handle 相关版本 | `pkg/util/rowcodec/encoder.go:RawChecksum` |

## 独立模型

- `models/partition_split_plan.py`：纯细分 admission、64 位边界、PID/相邻约束；不是生产 SQL evaluator。
- `models/partition_row_delta.py --reconcile`：当前脏键修复；startup stale Put/部分 Delete 否定 full-value replay。默认/--replay-all 保留历史对照。
- `models/partition_split_concurrency.py`：两个写者的快照准备、提交、冲突、回滚；--lifecycle 只在旧读者已排空前提下检查退休。
- `models/partition_split_safety.py`：MVCC 布局绑定、缓存、错误历史写、RR own writes、回退历史读取、unknown 晚写/tombstone GC；假设底层原子 MVCC 与认证成功语义。

模型不是当前 mutation hook、allocator/cache、真实 RPC/GC/DDL 状态组合的实现证明。正常与破坏变体的准确命令见最终设计；不能把退出 1 的预期反例当测试失败，也不能把有限状态数量当全面证明。

## 本机 SQL 证据

`models/partition_split_sql_probe.py` 仅连接自己创建的本地 socket/store，自建进程 finally 停止。binary 是旧 dirty 10d7421b80（2026-08-07），不是当前源码。关系 fixture 模拟 KV 命名空间，实际 REORGANIZE 仍是原 txn 模式。

| 证据 | 报告（本机 `.codex-tmp/` 前缀） |
| --- | --- |
| 父/marker/fence 的 9007、RR 空槽位 1205、跨父移动、用户回滚、MDL 持续阻塞 2.5s | `ps-9xjo991z/result.json`，早期完整报告 `ps-sxmhpbt5/result.json` |
| EXCHANGE 后两父域重复 row IDs 1/2，纯拆分保留四行 handle | 同上，ADMIN CHECK TABLE 通过 |
| TO_DAYS(DATETIME)、NULL、两个时区、强制 global index 读取 | 同上；只证明已有 txn-mode SQL |
| old RR late-first-access 静默 0/4 行 | `ps-t88cplw9/result.json`，早期 `ps-8f2ngd1f/result.json` |
| 既有 snapshot schema/data 绑定恢复普通及 global index 4 行 | `ps-398mypcm/result.json`；fixture safe point 沿 testkit 模拟，不是真实 GC |

最后一项证明历史绑定原语可用，不是新 RR hook 已修好，也不是建议在用户事务里 SET tidb_snapshot。

## TiKV 源码证据

只读检查本机 `/Users/joechenrh/code/pingcap/tikv` clean commit `11b128e16d58d838c5949d7f86153105eda3dadb`。`src/import/ingest.rs` 经 `raft_writer::wait_write` 等待 WriteEvent.Finished；`components/raftstore/src/store/fsm/apply.rs` 先 ingest pending SST/write_to_db 再回调。它支持该 classic 路径的正常 post-apply 成功合同，不支持把超时/cancel 视为无副作用，也不自动认证任意版本。

最终方案因此明确：所有 mutating RPC 有 Intent/回执封存；unknown 使未发布代永久隔离，fresh 命名空间重建，未确认物理清理另列 CleanupPending。无需虚构存储 range fence。

## 覆盖面边界

隐式 row ID、shard、时间表达式 RANGE 与 global index 的组合需要独立验收。拓扑适配不能替代部署与复制能力验证；本文实验使用本机合成 fixture，不包含客户调查标识或原始日志。
