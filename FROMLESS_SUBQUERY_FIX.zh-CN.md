# 无 FROM 子查询的空 schema 修复

## 根因

基线 `61a737a77b`。`uncorrelated_set_operation_subqueries_fold_like_top_level_queries` 在 `SELECT 2 IN (SELECT 1 EXCEPT SELECT 2)` 返回 `Unsupported("plan produces no schema")`，日志 `/tmp/set-subquery-red.log`。新增 `fromless_subquery_apply_has_an_empty_outer_schema` 修复前同样失败，日志 `/tmp/fromless-red.log`。

Go master oracle 固定为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。`pkg/planner/core/operator/logicalop/logical_schema_producer.go:80` 的 Schema getter 在没有子节点时初始化 `expression.NewSchema()`。Rust `build_table_dual` 未初始化 schema；子查询 Apply 重写需要外层 schema，因此错误地拒绝合法的无 FROM 查询。

修复仅在 TableDual 构建时设置空 Schema，保留其他缺失 schema 的错误检查。新增回归精确断言 IN 返回 0、ALL 返回 1，未修改原测试预期。

## Ready 验证

统一环境：`RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib fromless_subquery_apply
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_subquery
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib table_dual
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
```

- 新回归：1 passed，`/tmp/fromless-green.log`。
- 子查询组：13 passed / 1 failed，原 set-operation 测试通过。`/tmp/fromless-subquery-suite.log`。
- 剩余 `a_scalar_subquery_beside_an_aggregate_over_an_empty_outer_is_null`：实际 `[[0,0]]`、期望 `[[0,1]]`，在修复前 `/tmp/analyze-sampling-full.log` 已失败，本次不宣称解决。
- TableDual planner：6 passed，`/tmp/fromless-planner.log`。
- session integration：310 passed，`/tmp/fromless-integration.log`。
- lint：退出 0，`/tmp/fromless-lint.log`。
- 真实 access-path：退出 0，完整命令及 ready 证据见 `READINESS_CURRENT_STATUS.zh-CN.md`。

本改动使无 FROM 的逻辑叶节点持有合法空 schema，未改变行数、存储或协议。全量 session、workspace、Go/Bazel 及另外三个 RealTiKV runner 未在本轮完整重跑；不声明所有 failed cases 或完整 Go package 转写完成。共享统计加载的既有未提交工作保持独立。
