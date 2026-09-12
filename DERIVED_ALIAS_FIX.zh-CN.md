# 派生表别名预处理修复

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的 `pkg/planner/core/preprocess.go:423` 在 TableSource 上拒绝普通模式下无别名的 SELECT/SetOpr 派生表，返回1248；ORACLE模式跳过该检查。Rust普通SELECT和EXPLAIN入口原来遗漏检查。

本次扩展现有完整AST visitor，将statement context的完整SQL mode传入；检查覆盖派生表、嵌套标量子查询和UNION派生表。新增 `derived_alias_validation_visits_nested_queries_and_respects_oracle_mode` 在修复前因错误返回Rows失败（`/tmp/derived-alias-new-red.log`），修复后四类拒绝及ORACLE例外均通过。

Ready 验证，Cargo前缀 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`：

- `cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib derived_alias_validation_visits`：1 passed，`/tmp/derived-alias-new-green.log`。
- `cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_view_body_may_contain_a_derived_table`：原综合case 1 passed，`/tmp/derived-alias-original-green.log`。
- `cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all`：310 passed，`/tmp/derived-alias-integration.log`。
- `make lint`：退出0，`/tmp/derived-alias-lint.log`；`git diff --check`通过。

未重跑Go binary、RealTiKV、Bazel或全量session lib。共享统计加载WIP仍独立保留，整体目标未完成。新增检查复用已有AST遍历，正常查询不增加一次树遍历。
