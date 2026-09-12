# View 依赖失效的错误身份

基线 `625f3b49cf`。两个原失败为 `a_view_over_a_dropped_table_is_invalid` 和 `a_view_column_type_follows_the_base_column`，分别覆盖删基表及删基列。前者修复前独立运行退出 101，日志 `/tmp/view-invalid-red.log`；完整历史证据 `/tmp/exists-final-full.log` 包含两者。

## Go source of truth

固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的 `pkg/planner/core/logical_plan_builder.go:5648` 在 view body 构建失败时，将普通错误转换为 ErrViewInvalid，并保留递归、专用 ErrNoSuchTable、内部、分组、EXPLAIN 权限和 NotSupportedYet 等例外。infoschema 的缺表错误不是该专用 ErrNoSuchTable。

真实 binary 以独立 unistore 运行，端口 14871、status 14881、路径 `/tmp/view-invalid-oracle-data`，日志 `/tmp/view-invalid-oracle.log`。创建 vi.base(x,y) 和 vi.vb 后，删 y 再查询 view 返回 1356/HY000（`/tmp/view-invalid-go.out`）；随后删 base 查询 view 同样为 1356/HY000（`/tmp/view-invalid-drop-go.out`）。实例已正常退出。

## 修复

Rust 原 view builder 直接透传 body 的 UnknownTable/UnknownColumn，已有 view_invalid helper 也仅生成内部字符串。现在使用 `PlanErrorKind::ViewInvalid` 保留限定 view 名，并映射至 `SchemaErrorKind::ViewInvalid`，由已有 MySQL mapper 输出 1356。body 边界保留目前已建模的 Go 例外（Internal 包含现有递归错误、分组错误、NotSupportedYet）。未引入尚无 producer 的权限或 rename 错误类型。

新增回归检查删列、删表的 1356 和 view 名，并检查直接查询缺表仍是 1146。原断言未放宽。

## Ready 验证

Cargo 环境为 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_view_over_a_dropped_table_is_invalid
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_view_
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib invalid_view_dependencies
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_views
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib view
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
```

`a_view_` 12 passed，新增回归 1 passed。view 组 22 passed / 1 failed，剩余 LATERAL x.a 解析是原有独立失败。日志分别为 `/tmp/view-invalid-green.log`、`/tmp/view-invalid-identity.log`、`/tmp/view-invalid-suite.log`。lint 退出 0（`/tmp/view-invalid-lint.log`）。planner view 过滤测试退出 0（`/tmp/view-invalid-planner.log`），session integration 310 passed（`/tmp/view-invalid-integration.log`）。测试启动期间 macOS dyld 延迟已通过原进程句柄等待完成，未重启或跳过。

本轮未重跑全量 session、workspace、RealTiKV、Go/Bazel gates；整体目标仍未完成。共享统计加载的未提交 WIP 保持独立。
