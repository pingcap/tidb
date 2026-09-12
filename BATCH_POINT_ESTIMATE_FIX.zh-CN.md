# 普通表路径 BatchPointGet 的估算上限

基线 `f1953ac550`。Go master oracle 固定为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。

## 两个发现

原 explain_select 在 USE INDEX() 后拒绝出现 clustered Batch_Point_Get，独立运行失败，日志 `/tmp/use-index-empty-red.log`。固定 Go 实例实际仍生成该计划，因此这条旧预期不能作为修复 Rust 的依据。

同一计划存在真正 Rust 差异：两点查询 Rust estRows=1.00，Go=2.00。Rust find_best_task/dispatch 的表路径同时构建 PointGet 和 BatchPointGet，却都使用 min(CountAfterAccess,1)。Go `pkg/planner/core/find_best_task.go:3106` 的 convertToBatchPointGet 使用 min(CountAfterAccess,len(Ranges))。

本次修正上限为 ranges.len()；单点依旧上限 1。未改扫描或点读的实际返回行。测试改为检查 Go 实际保留的 clustered batch 及其 2.00，并增加重复点去重后的 2.00、单点 1.00 断言。

## Go 对照

独立 unistore 参数：`--path=/tmp/empty-index-oracle-data --host=127.0.0.1 -P 14871 --status=14881`，日志 `/tmp/empty-index-oracle.log`。表 common_point(a BIGINT,b VARCHAR(8),v BIGINT,PRIMARY KEY(a,b))，行 (10,x,1)、(20,y,2)。

- `/tmp/empty-index-go.out`：USE INDEX() 及 (b,a) 两点 IN 仍为 Batch_Point_Get，estRows=2.00。
- `/tmp/batch-count-go.out`：重复三元列表产生两个 distinct 点，batch estRows=2.00；单点 Point_Get=1.00。

Go 实例已正常退出。

## Ready 验证

统一环境：`RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain::explain_select -- --exact
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib point
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
```

原用例及新增断言通过（`/tmp/batch-count-final.log`），lint 退出 0（`/tmp/batch-count-lint.log`）。planner point 过滤下 26 passed（`/tmp/batch-count-planner.log`），session integration 310 passed（`/tmp/batch-count-integration.log`）。

未重跑全量 session、workspace、RealTiKV、Go/Bazel gates。该上限影响 batch plan 的估算与成本，未宣称所有统计及访问路径差异已解决；整体目标保持未完成。
