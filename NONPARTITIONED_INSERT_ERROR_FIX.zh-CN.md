# 非分区表 INSERT PARTITION 返回 Go 错误 1747

Go source of truth 为固定 master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`；Rust 修复基线 `930a89b588`。

## 错误契约和根因

Go `pkg/planner/core/planbuilder.go:4183` 在 INSERT 指定 PARTITION、目标表却没有分区时返回 ErrPartitionClauseOnNonpartitioned。真实 Go UPDATE、INSERT 均返回 `ERROR 1747 (HY000): PARTITION () clause on non partitioned table`，见 `/tmp/partition-nonpartition-update-go.out` 和 `/tmp/partition-nonpartition-insert-go.out`。

原 Rust 测试将非分区表错误也期望为 1735。UPDATE 实现已经正确返回 1747，测试应修正；INSERT 实现则确实错误地返回 UnknownPartition/1735。按 Go 契约修正测试后，UPDATE 通过而 INSERT 在 `/tmp/partition-nonpartition-contract-final.log` 稳定失败，left=1735、right=1747，退出 101。这是本修复的生产缺陷红测。

修复 `driver/dml.rs::run_insert_with_physical` 的非 Kv 或缺少 partition spec 分支，返回现有 PartitionClauseOnNonpartitioned 类型；有分区表的未知分区名仍使用 UnknownPartition。测试检查 1747、HY000 和完整错误消息，同时保留分区表未知分区 1735、写入范围及最终行内容检查。没有放宽 SQL 结果断言。

## Ready 验证

工作目录 `/tmp/tidb-hparser-current`，cargo 环境 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib updates_and_deletes_restricted_to_partitions_do_not_escape_the_named_set
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib
make lint
git diff --check
```

定向回归 1 passed（`/tmp/partition-nonpartition-insert-green.log`），集成 310 passed（`/tmp/partition-nonpartition-integration.log`），lint 退出 0（`/tmp/partition-nonpartition-final-lint.log`）。Go 对照服务已正常关闭，原进程已回收。

完整 session 测试 `/tmp/session-after-partition-fixes-full.log`：**1641 passed / 72 failed / 209 ignored**，退出 101。先前完整基线 `/tmp/current-session-full.log` 为 1633 / 80 / 209。当前分区相关仍失败的五项是 unsigned row handle 范围上界、static residual batch、grouped HAVING clustered point、RANGE COLUMNS 读取计数、static pruning 展示契约。其他类别的失败也保留在完整日志中。

本次只完成非分区表 INSERT 错误类别，完整所有 Rust failed cases、RealTiKV、Go/Bazel 验收目标仍未完成；未变更 ignored 数量或绕过失败。原 shared-statistics WIP 单独保留。
