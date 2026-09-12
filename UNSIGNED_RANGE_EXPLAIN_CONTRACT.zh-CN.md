# unsigned 范围的 Go EXPLAIN 契约

基线 0eede926db，固定 Go master fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85。

原用例 a_narrow_unsigned_row_handle_is_ranged_over_without_a_split 强求 EXPLAIN 包含 range:(0,+inf]，红测 `/tmp/unsigned-range-current-red.log` 退出 101。实际 SQL 的结果与 Go 相同，只有展示断言失败。

Go `pkg/planner/core/operator/physicalop/physical_table_scan.go::IsFullScan` 调用 `pkg/util/ranger/types.go:170` 的 Range.IsFullRange。unsigned 分支仅检查低高边界值，不检查 exclusion flags，因此 (0,MaxUint64] 被命名为 TableFullScan，范围文本不展示；这不改变执行层排除低边界 0 的语义。

真实 `/tmp/unsigned-range-go-complete.out` 保留原始三行 0、1、4294967295；id>0 返回 1、4294967295，EXPLAIN 为 TableFullScan，EXPLAIN ANALYZE 的 scan actRows 和 proc_keys 都为 2。COUNT(id<4294967295) 为 2。

本次只修正过时的计划展示断言和注释，补充实际扫描行数必须为 2；原排序结果和 COUNT 断言不变。未修改生产代码。

Ready 验证目录 `/tmp/tidb-hparser-current`，cargo 前缀 RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432：

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_narrow_unsigned_row_handle_is_ranged_over_without_a_split
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
git diff --check
```

定向回归 1 passed（`/tmp/unsigned-range-contract-green.log`），集成 310 passed（`/tmp/unsigned-range-contract-integration.log`），lint 退出 0（`/tmp/unsigned-range-contract-lint.log`）。整体所有 Rust failed cases 和其余门禁仍未完成，不根据单项绿色推算完整 session 通过数量。

后续 grouped HAVING 分区 PointGet 缺口已取得原 SQL 的 Go 证据 `/tmp/grouped-partition-point-go.out`：Point_Get，partition:p2，结果 j9FsMawX5uBro%$p。该项需要修复实现，不能改变测试为 TableRangeScan。
