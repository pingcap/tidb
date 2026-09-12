# MODIFY NOT NULL 的错误码与 TIMESTAMP 边界

基线 `76aec831e6`，Go master oracle 为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。

原 `a_null_becomes_the_current_timestamp_when_the_column_turns_not_null` 在 int→datetime NOT NULL 的 NULL 拒绝上失败：Rust 1138，Go 1265。独立红色回归日志 `/tmp/modify-null-red.log`。

## Go 依据

`pkg/ddl/modify_column.go:2295` 的 MODIFY admission 固定调用 `checkForNullValue(..., true, ...)`；`pkg/ddl/column.go:1088` 跳过非 TIMESTAMP 转入 TIMESTAMP 的 NULL 检查，其余情况通过 LIMIT 1 检查，有 NULL 时返回 WarnDataTruncated（1265），行号为查询得到的 1。`column.go:773` 的 reorg worker 对受允许的 TIMESTAMP 转换替换当前时间。

真实 Go binary 以 unistore 启动，路径 `/tmp/modify-null-oracle-data`、SQL 14871、status 14881。三个实际输出均为 1265/01000，文本 `Data truncated for column 'b' at row 1`：

- int→datetime NOT NULL：`/tmp/modify-null-go.out`。
- int→int NOT NULL：`/tmp/modify-null-same-go.out`。
- timestamp nullable→timestamp NOT NULL：`/tmp/modify-null-timestamp-go.out`。

实例日志 `/tmp/modify-null-oracle.log`，已 SIGTERM 正常退出。

## 修改

MODIFY driver 将 KvTable 的 InvalidUseOfNull 映射为现有 DataTruncatedAtRow，保留小写新列名和 row=1。没有改变其他调用者的 InvalidUseOfNull 类型。NULL 时间替换仅允许旧类型不是 TIMESTAMP，避免同类型 nullable TIMESTAMP 被错误填入当前时间。

新增回归覆盖同类型 INT/TIMESTAMP、限定错误文本和失败后两行仍为 NULL。原 int→timestamp 当前时间回归仍保留。

## Ready 验证

Cargo 环境为 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_null_becomes_the_current_timestamp_when_the_column_turns_not_null
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_modify_column_null
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib modify
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
```

NULL 回归 2 passed（`/tmp/modify-null-green.log`），session integration 310 passed（`/tmp/modify-null-integration.log`），lint 退出 0（`/tmp/modify-null-lint.log`）。扩大 MODIFY 组首次为 26 passed / 1 failed：tests_core::ddl::modify_column 仍预期历史 1138（`/tmp/modify-null-suite.log`）。依据固定 Go 的 MODIFY admission 及同类型实测，将其改成 DataTruncatedAtRow(c,1) 精确断言。最终组 27 passed（`/tmp/modify-null-final-suite.log`）。

本轮未重跑全量 session、workspace、RealTiKV、Go/Bazel gates；整体目标保持未完成。共享统计 WIP 独立保留。
