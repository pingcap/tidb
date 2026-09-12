# 派生表重复列名错误身份

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的 `pkg/planner/core/logical_plan_builder.go:567` 按 ColName.O 检查重复列，并返回 ErrDupFieldName。Rust 原来使用 internal 错误，导致1060变成1105。本次新增 PlanErrorKind::DuplicateColumnName，经现有 DriverError 映射保留1060，保持 Go 的原始名称比较规则。

新增回归 `derived_duplicate_column_preserves_mysql_identity` 在修复前于重复小写名称场景返回1105，预期1060（`/tmp/derived-duplicate-red.log`）。修复后小写及大写重复名称均为1060，`SELECT 1 FROM (SELECT 1 AS a, 2 AS A) q` 可构建（`/tmp/derived-duplicate-final.log`）。外层 SELECT * 存在单独的列引用歧义，未作为本项完成证据。

Ready 验证，Cargo 前缀 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`：

- `cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib derived_duplicate_column_preserves_mysql_identity`：1 passed。
- `cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all`：310 passed，`/tmp/derived-duplicate-integration.log`。
- `make lint`：退出0，`/tmp/derived-duplicate-lint.log`。
- `git diff --check`：通过。

未重跑 Go binary、RealTiKV、Bazel 或完整 session lib。不声明派生表综合 case 已完全修复；缺少别名检查和共享统计加载 WIP 仍未完成。本修复仅改变错误身份，无正常执行性能变化。
