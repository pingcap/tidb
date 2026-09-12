# 静态分区 BatchPointGet 修复

## 根因与 Go 证据

上一独立修复 `d260cb5cd8` 已推送，解决动态 PointGet 和缓存分区路由。本次处理剩余 `a_residual_conjunct_keeps_the_static_per_partition_batch_point_get`。

Go master 固定为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。在真实 nightly PD/TiKV 和独立 unistore 上执行相同 SQL，结果一致；证据 `/tmp/point-static-batch-go.out`、`/tmp/point-static-batch-go-unistore.out`。

```sql
SET @@tidb_partition_prune_mode = 'static';
SET @@tidb_default_string_match_selectivity = 0.8;
CREATE TABLE t (a VARCHAR(255), b INT PRIMARY KEY NONCLUSTERED, KEY(a))
PARTITION BY KEY(b) PARTITIONS 3;
INSERT INTO t VALUES ('Ab',1),('abc',2),('BC',3),('AC',4),('BA',5),('cda',6);
ANALYZE TABLE t;
EXPLAIN SELECT * FROM t WHERE b IN(1,2) AND a LIKE '%a%';
```

Go 计划为 PartitionUnion 2.60，P1 为 Selection 1.60 -> BatchPointGet 2.00，P2 为 Selection 1.00 -> BatchPointGet 1.00，结果仅 `abc, 2`。

Rust 红色证据 `/tmp/static-point-cost-probe.log`：P1 点读估算错误地使用过滤后的 1.60；P2 全表扫描成本 27.94、点读上 Selection 成本 49.90，最终错误选择全表。根因不是需要调低点读成本，而是漏掉了 Go 在成本比较之前的候选选择阶段。

`pkg/planner/core/stats.go:663` 的 `derivePathStatsAndTryHeuristics` 优先选择单次读取的唯一点路径；对需要回表的唯一点路径，按点数和 table filters 数量选择。覆盖范围路径只有严格包含其 access columns，且范围数小于唯一点路径的两倍时才能替代它。随后才进入 skyline 和成本比较。

`pkg/planner/core/find_best_task.go:3108` 的 `convertToBatchPointGet` 使用 `min(CountAfterAccess, len(Ranges))` 作为读取节点估算，并让上层 Selection 使用过滤后的估算。

## 实现与测试约束

`rust/crates/tidb-planner/src/find_best_task/candidate.rs` 增加上述原生候选规则与覆盖索引反例。`dispatch.rs` 在无排序要求的普通 root 枚举中收集完整候选事实，在 skyline 之前应用规则；候选事实缺失、排序或 runtime IndexJoin 不从不完整候选集合推导选择。候选物理树直接移动，不为启发式再克隆整棵树。

唯一索引 PointGet/BatchPointGet 的统计恢复为过滤前访问行数。测试去掉旧的 Projection 跳行和残留节点 ID，依据为本次实际 Go 计划；行数、分区、操作符及 SQL 结果断言均保留。临时成本日志已移除。

这是一类失败的修复，不是整个 Go planner package 已完成移植的声明。

## 验证记录

工作目录 `/tmp/tidb-hparser-current`，所有 Cargo 命令前缀均为 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_residual_conjunct_keeps_the_static_per_partition_batch_point_get
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_partition
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task::candidate::
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib
make lint
git diff --check
```

原回归通过；分区相关测试 **98 passed / 0 failed / 0 ignored**；候选规则 4 个测试全部通过；最终 session 集成 **310/310** 通过（`/tmp/static-point-final-integration.log`）；lint 退出 0（`/tmp/static-point-final-lint.log`）。没有新增 ignore 或改变失败断言。

完整单测暴露了一个实现中的回归：common-handle 的启发式事实漏了 Go `pkg/planner/util/path.go:429` 的完整键宽度条件，使 `mysql.user WHERE User='mu1'` 的部分主键范围被误判为完整点。修正后账户测试恢复，最终并行单测为 **1647 passed / 68 failed / 209 ignored**（`/tmp/session-after-point-heuristics-final.log`）；其中 sharded rowid、PD 全局变量 hook 两项单独运行均通过。

同一编译产物串行运行全部单测：`RUST_MIN_STACK=33554432 rust/target/debug/deps/tidb_session-dd518745a2685e5f --test-threads=1`，结果 **1652 passed / 63 failed / 209 ignored**（`/tmp/session-after-point-heuristics-serial.log`）。五个分区失败相对旧完整基线已消失，包含此前三项 Go 期望校正及本轮两个实现修复；其余计数变化不作为已修复数量。剩余失败仍须独立处理。

首次真实 access-path 复验通过（`/tmp/static-point-access.log`）；common-handle 宽度修正后的最终复验也退出 0，末行为 `the access-path differential passed`。使用固定 Go master 与 `ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly`，日志 `/tmp/static-point-final-access.log`、节点证据 `/tmp/static-point-final-access-evidence/rust-node.log:8`。真实 ready 的 schema_version=68、stats_loaded=4。所有本次测试节点、playground 和数据均已清理。

## 远端合入复验

正常 rebase 合入远端 `a5527590c4` 的 IndexJoin 外侧 ExpectedCnt 修复，无冲突，并保留已有 shared-statistics WIP。验证代码基线 `1248cafe80`：串行 session 单测仍为 **1652/63/209**，失败集合与 rebase 前串行运行完全一致（`/tmp/static-point-rebased-session.log`）；310 个集成测试和 lint 均通过（`/tmp/static-point-rebased-integration.log`、`/tmp/static-point-rebased-lint.log`）。真实 access-path 证据来自 rebase 前，不冒充远端 IndexJoin 合入后的再次真实回放。

新增远端回归也单独通过：同一 Cargo 前缀运行 `cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib an_uncorrelated_join_subquery_with_a_filter_keeps_its_table_statistics`，1 passed（`/tmp/static-point-rebased-indexjoin.log`）。验证完成后只更新本文档，不再修改生产代码。
