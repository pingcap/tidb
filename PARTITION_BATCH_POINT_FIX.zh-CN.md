# 分区整数主键 BatchPointGet 路由修复

原始基线 `846ba44e10`；Go master oracle 固定为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。本修复依赖独立提交 `0cc26bc6f3` 的空分区读取修复。

## 复现及 Go 契约

原测试 `a_batch_point_get_names_the_partitions_its_handles_reach` 在 `/tmp/partition-batch-current-red.log` 退出 101：Rust 返回独立的 partition:p1,P2 和 table:t，Go 返回单个 Batch_Point_Get 的 table:t, partition:p1,P2。根因是 `driver/access.rs` 对所有分区表直接退出 fast planner。

Go `pkg/planner/core/point_get_plan.go::newBatchPointGetPlan` 要求分区表达式是 Column；整数主键列表按原始 IN 长度估算。`physical_batch_point_get.go::PrunePartitionsAndValues` 去重并将每个键映射到物理分区，AccessObject 按定义顺序输出去重的分区名。真实输出 `/tmp/partition-point-go.out` 确认 IN(1,2,1) 的 estRows=3，handle:[1 2]。

显式 PARTITION 被 Go tryWhereIn2BatchPointGet 拒绝，进入普通规划。`find_best_task.go:2224` 在 dynamic 模式拒绝多点分区 batch 转换；Rust 原先缺少这一条件。实机 `/tmp/partition-batch-explicit-go.out`、`/tmp/partition-batch-oracle-final.out` 确认默认 dynamic 空集计划为 TableReader partition:dual，static 为 TableDual。原测试更正为分别覆盖两种模式，保留 SQL 空结果断言。

## 实现

恢复 HASH 裸列分区的整数主键 fast batch，PhysicalBatchPointGet 保存与 ranges 对齐的 partition_ids；深克隆保留它们。执行器在键去重和排序后重排路由，HandleSourceExec 将路由交给既有 stored_records_batched，按实际分区 ID 分组读取。长度不一致报错。EXPLAIN 从同一组 ID 按分区定义顺序生成名字，保留大小写。

普通 dynamic 分区多点查询恢复 Go 的转换限制。空集合 reader 的读取漏洞由独立提交 0cc26bc6f3 修复。本次不声称已实现 common-handle、unique-index 或其他分区方法的完整 fast path；它们仍保持普通规划路径并作为后续类别处理。

## Ready 验证

目录 `/tmp/tidb-hparser-current`，cargo 前缀 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_batch_point_get_names_the_partitions_its_handles_reach
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib partition_batch_reads_only_the_partitions_its_keys_reach
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_partition
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib physical::tests
make lint
git diff --check
```

原测试红测及中途后续断言失败分别保存在 `/tmp/partition-batch-current-red.log`、`/tmp/partition-batch-green.log`、`/tmp/partition-batch-dynamic-green.log`；最终原测试在完整分区相关套件中通过。最终套件 `/tmp/partition-batch-final-suite.log` 为 **89 passed / 7 failed**，此前 `/tmp/partition-batch-suite.log` 为 85 / 11。存储计数回归通过（`/tmp/partition-batch-storage.log`）：三分区表查询 IN(1,2,1) 返回两个正确值，恰好两个批量读，没有访问 P0。集成 310 passed（`/tmp/partition-batch-final-integration.log`），物理计划测试 46 passed（`/tmp/partition-batch-planner.log`），make lint 退出 0（`/tmp/partition-batch-final-lint.log`）。

Go 对照节点已 SIGTERM 并回收，退出 0。未改写整体全量 session 基线计数。已有 shared-statistics WIP 保留且未包含在提交中。

## 仍失败的分区相关用例

- a_narrow_unsigned_row_handle_is_ranged_over_without_a_split
- a_residual_conjunct_keeps_the_static_per_partition_batch_point_get
- grouped_having_intersection_reads_one_clustered_partition_point
- range_columns_pruning_reads_the_matching_tuple_partition
- static_prune_mode_fans_a_partitioned_scan_out_per_partition
- updates_and_deletes_restricted_to_partitions_do_not_escape_the_named_set
- tests_partition_projection::a_pruned_index_reader_decodes_the_column_the_scope_names

这些失败均出现在之前的全量 session 日志 `/tmp/current-session-full.log`，本轮未忽略或改期望。整体所有 Rust failed cases、RealTiKV、Go/Bazel 门禁目标保持未完成。
