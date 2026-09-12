# LATERAL 列别名数量错误修复

Go source of truth: `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，`pkg/planner/core/logical_plan_builder.go:541` 在列别名数量不匹配时返回 `dbterror.ErrViewWrongList`。Rust 同一分支使用 internal 错误，导致 1353 变为 1105；改用现有 `PlanError::view_wrong_list()` 保留错误身份。

新增回归断言在修复前返回1105，预期1353，日志 `/tmp/derived-list-regression-red.log`。修复后原 `lateral_alias_column_list_renames_positionally` 整体通过，正常查询及数量过多、过少断言均保留。

Ready 验证（Cargo 环境 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`）：

- `cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib lateral_alias_column_list_renames_positionally`：1 passed，`/tmp/derived-list-green.log`。
- `cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all`：310 passed，`/tmp/derived-list-integration.log`。
- `make lint`：退出0，`/tmp/derived-list-lint.log`。
- `git diff --check`：通过。

本轮未重跑完整 session lib、Go binary、RealTiKV、Bazel；不宣称全部失败已经修复。共享统计加载测试的采样初始化改动与诊断报告仍为独立 WIP，不属于本提交。行为变化只涉及列别名数量不匹配的错误身份，不改变正常查询或执行性能。
