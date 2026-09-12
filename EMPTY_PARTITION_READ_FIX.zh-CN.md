# 空分区裁剪结果必须读取零行

基线 `846ba44e10`，Go master source of truth 为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。

## 根因与修复

`driver/physical_builder.rs::build_table_scan` 只在裁剪后的分区名非空时限制读取，导致 `all_partitions=false, partitions=[]` 被当成不限制分区。Go `pkg/executor/builder.go:4103` 在完成 partitionPruning 后检查 len(partitions)==0，直接返回 TableDualExec。Rust 修复为只根据 all_partitions 判断是否限制，空 ID 列表明确读零分区。

原始 `range_pruning_reads_only_the_partitions_that_can_match`、`scalar_list_pruning_reads_only_matching_owners`、`list_columns_pruning_reads_matching_tuple_owners` 均已有红测 `/tmp/current-session-full.log`，本轮 `/tmp/partition-batch-suite.log` 再次失败。scalar LIST 的 a=2 实际读 6 行，要求读 0 行；不修改任何这些用例的断言。修复后上述三项均通过，见 `/tmp/partition-batch-final-suite.log`。

真实 Go 同一显式分区 SQL `SELECT * FROM t PARTITION(P0) WHERE a IN(1,2)` 返回空集，dynamic EXPLAIN 是 TableReader 的 partition:dual；static 才是 TableDual。输出 `/tmp/partition-batch-explicit-go.out`、`/tmp/partition-batch-oracle-final.out`。Go 对照节点已正常关闭。

## 验证范围

Ready 验证在当前工作树进行，该树同时包含独立的整数分区 BatchPointGet 修复；不是干净的单提交隔离测试。下面 cargo 命令统一前缀 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`，目录 `/tmp/tidb-hparser-current`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_partition
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
git diff --check
```

分区相关套件 89 passed / 7 failed（此前 85 passed / 11 failed）；session 集成 310 passed / 0 failed，`/tmp/partition-batch-final-integration.log`；make lint 退出 0，`/tmp/partition-batch-final-lint.log`。原失败的 3 个空裁剪用例通过；其他 7 项明确保留失败，不把该套件记为全绿。本提交只包含空集合读取修复及本文，batch 路由另行提交。完整 Rust、Go、RealTiKV、Bazel 质量目标仍未完成。
