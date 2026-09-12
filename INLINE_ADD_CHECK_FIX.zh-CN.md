# ADD COLUMN inline CHECK 与 Go 一致

基线 `678d22748a`，Go oracle 固定为 master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。

## 根因与修复

`an_inline_add_column_check_is_discarded_even_while_enabled` 修复前失败：SHOW CREATE 意外包含 CONSTRAINT，日志 `/tmp/inline-check-red.log`。Go `pkg/ddl/add_column.go:384` 的 CreateNewColumn 调用 `buildColumnAndConstraint`，只保留 column，丢弃 constraints。

Rust 底层 build_added_column 已遵循该规则，但 catalog ALTER 路径在 add_column_action 后额外安装 inline CHECK，并在违反时回滚新增列。本次删除该额外注册和回滚分支，CREATE TABLE CHECK、显式 ADD CHECK 和 grouped table-level CHECK 的代码不变。

新增已有数据上的负默认值回填断言：ADD c DEFAULT -2 CHECK(c>0) 成功，行变为 (1,-1,-2)，没有持久化 CHECK。

## Go 实测与测试修正

独立 unistore 使用 `--path=/tmp/inline-oracle-data --host=127.0.0.1 -P 14871 --status=14881`。Go 日志 `/tmp/inline-oracle.log` 与 `/tmp/inline-oracle-second.log`，实例均已正常退出。

- `/tmp/inline-go.out`：ADD b CHECK(b>0) 后 SHOW CREATE 无 CHECK，插入 -1 成功。
- `/tmp/inline-default-go.out`：新增 c DEFAULT -2 CHECK(c>0) 成功，旧行回填 -2。
- `/tmp/inline-exact-go.out`：完整复验原 integration fixture，两行默认值为 -1；随后显式 ADD CONSTRAINT explicit_check CHECK(c>=0) 返回 3819。

集成测试 add_column_default_check_validation_source 原先错误预期 inline CHECK 返回 3819。生产修复后它成为唯一集成失败（309 passed / 1 failed，`/tmp/inline-check-integration.log`）。依据上述 Go 实测改为精确验证回填行、显式 CHECK 3819，以及失败的显式约束不拦截后续插入，未使用 skip 或放宽比较。

## Ready 验证

Cargo 统一环境：`RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib an_inline_add_column_check_is_discarded_even_while_enabled
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_check_constraints
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
```

CHECK 组 7 passed（`/tmp/inline-check-green.log`）。最终集成 310 passed（`/tmp/inline-check-final-integration.log`），lint 退出 0（`/tmp/inline-check-final-lint.log`）。

未重跑全量 session、workspace、RealTiKV、Go/Bazel gates，整体目标仍未完成。共享统计加载 WIP 保持独立。该修复对齐 Go 特定 ADD COLUMN 行为，不表示 TiDB 普遍忽略 CHECK。
