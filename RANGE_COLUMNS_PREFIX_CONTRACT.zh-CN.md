# RANGE COLUMNS 前缀裁剪契约

Rust 基线 c6fb09769c，Go master 固定 fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85。

原 `range_columns_pruning_reads_the_matching_tuple_partition` 要求 a=1 扫描全部 6 行。红测 `/tmp/range-columns-contract-red.log` 退出 101，Rust 实际仅扫描 2 行。

Go `pkg/planner/core/rule/rule_partition_processor.go:1327` 的 multiColumnRangeColumnsPruner 使用 DetachCondAndBuildRangeForPartition，并对低高边界进行二分查找。它支持 RANGE COLUMNS(a,b) 的前缀条件 a=1。真实 oracle `/tmp/range-columns-contract-go.out` 明确记录 reader partition:p0、TableFullScan actRows=2、proc_keys=2，查询结果 c=11、19。

只将此条件的读取数量从 6 修正为 2，其他复合条件、返回数量断言不变；补充完整结果 11、19 的有序断言。未修改生产裁剪逻辑。

Ready 验证在 `/tmp/tidb-hparser-current`，cargo 环境 RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432：

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib range_columns_pruning_reads_the_matching_tuple_partition
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
git diff --check
```

定向 1 passed（`/tmp/range-columns-contract-green.log`），集成 310 passed（`/tmp/partition-contracts-integration.log`），lint 退出 0（`/tmp/partition-contracts-lint.log`）。整体所有 Rust failed cases 目标未完成，不据此改写最新完整 session 1641/72/209 基线。
