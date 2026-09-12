# dynamic reader 的分区访问对象契约

Go master oracle 固定 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。Rust 基线 ea9ec6ec45。

原 `static_prune_mode_fans_a_partitioned_scan_out_per_partition` 在 dynamic 控制组只期望 table:t2，遗漏 reader 的 partition:p1,p2。独立红测 `/tmp/static-pruning-contract-red.log` 退出 101，实际访问对象为 partition:p1,p2 和 table:t2。

Go `pkg/planner/core/operator/physicalop/physical_table_reader.go:168` 的 AccessObject 在 dynamic 模式调用 GetDynamicAccessPartition，并返回 DynamicPartitionAccessObjects；static 模式不在 reader 上显示它。实机 `/tmp/static-pruning-contract-go.out` 确認默认 dynamic 为 TableReader partition:p1,p2，scan 为 table:t2；static 则 PartitionUnion 下各 scan 分别显示 table:t2, partition:p1 和 table:t2, partition:p2；显式 PARTITION(p2) 只留一个 scan。

本次只修正 dynamic 控制组和对应注释。所有 static 严格访问对象断言不变，不修改生产规划逻辑。

Ready 验证目录 `/tmp/tidb-hparser-current`；cargo 前缀 RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432：

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib static_prune_mode_fans_a_partitioned_scan_out_per_partition
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
git diff --check
```

定向回归 1 passed（`/tmp/static-pruning-contract-green.log`），310 项集成已在相同生产代码下通过（`/tmp/partition-contracts-integration.log`），本次变更仅影响 lib test；lint 退出 0（`/tmp/static-pruning-contract-lint.log`）。Go 对照服务已正常退出并回收。

本轮两个原失败用例均已独立通过，没有重跑完整 session 并改写其基线 1641 passed / 72 failed / 209 ignored。其余分区失败及完整 Rust/RealTiKV/Go/Bazel 门禁仍未完成。unsigned 范围的 Go 新证据 `/tmp/narrow-unsigned-original-go.out` 显示原 SQL 是 TableFullScan，后续应先对照范围构建和结果语义，不能直接按旧测试要求修改为 range:(0,+inf]。
