# HashJoin 的 CARTESIAN 展示前缀

基线 `6a564c693a`。本轮修复前完整 session 为 1633 passed / 80 failed / 209 ignored，日志 `/tmp/current-session-full.log`，其中 explain_hash_join_operator_info_matches_go 仍失败。

## Go 依据

固定 master oracle `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的 `pkg/planner/core/operator/physicalop/physical_hash_join.go:231` 在没有 EqualConditions 时，依据 NAEqualConditions 输出 CARTESIAN 或 Null-aware 前缀。即使含有非等值条件，仍是 CARTESIAN。

真实 Go unistore 使用路径 `/tmp/hash-explain-oracle-data`、SQL 14871、status 14881。`/tmp/hash-explain-go.out` 记录：`hj1 JOIN hj2 ON hj1.a>hj2.a` 为 `CARTESIAN inner join, other cond:gt(test.hj1.a, test.hj2.a)`；无 ON 为 `CARTESIAN inner join`。日志 `/tmp/hash-explain-oracle.log`，实例已正常退出。

## 修复范围

Rust physical_operator_info 的 HashJoin 分支之前直接复用 join_info，丢失 HashJoin 独有前缀。现在无普通等值条件及键时按 Go 加前缀，保留其他 join 类型的文本。Rust 部分物理计划仅保存已解析 join keys，因此同时检查 keys 与 EqualConditions，避免把有效等值连接错标为 CARTESIAN。

新增物理 HashJoin 展示单测，原 session 回归覆盖等值、多键、非等值、笛卡尔和左右外连接。执行、成本和存储路径未修改。

## Ready 验证

Cargo 环境：`RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib explain_hash_join_operator_info_matches_go
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib explain::
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
```

lint 退出 0（`/tmp/hash-explain-lint.log`）；目标回归 1 passed（`/tmp/hash-explain-green.log`），展示层 7 passed（`/tmp/hash-explain-executor.log`）；集成 310 passed（`/tmp/hash-explain-integration.log`）。

本轮编译曾等待 macOS dyld 加载 proc-macro 动态库的签名检查，采样 `/tmp/hash-explain-compile.sample` 显示 dlopen/mapSegments/fcntl，codesign 验证最终返回 valid。原进程自行恢复，无重启、跳过或关闭系统检查。

完整 session 是修复前基线，不冒充修复后全量通过。未运行 workspace、RealTiKV、Go/Bazel 全 gates，整体目标继续保持未完成。
