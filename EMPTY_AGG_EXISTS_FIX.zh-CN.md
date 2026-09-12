# 空聚合行上的 EXISTS 与 semi join 输出列

## Go 依据与根因

基线 `f310d26c1f`。固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`：

- `pkg/planner/core/rule_decorrelate.go:252` 将无关联 Apply 转为 Join。
- `pkg/executor/join/hash_join_v1.go:1389` 对空输入聚合产生的默认行执行 Apply deselect；普通 Join 没有这条规则。
- Rust 原先为了避开 semi join 输出列错位，保留所有 marker Apply，导致 EXISTS 也错误 deselect。

原回归 `a_scalar_subquery_beside_an_aggregate_over_an_empty_outer_is_null` 修复前得到 `[[0,0]]`，期望 `[[0,1]]`，日志 `/tmp/empty-apply-red.log`。允许 EXISTS decorrelation 后原失败通过；新增 `exists_marker_on_an_empty_aggregate_uses_join_semantics` 又复现裁剪列错位：选择 b 得到 a 的 1/2，期望 10/20，日志 `/tmp/exists-marker-green.log`（文件名为尝试阶段名称，实际退出 101）。

修复为无关联 boolean marker Apply 使用 Join，并以 unique ID 映射保留的外层列，marker 追加到输出末尾。IN 的 NULL marker 与 AntiLeftOuterSemi 尚未被 JoinExec 完整支持，保留其已有 Apply 路径；没有修改 NULL 断言，也不声明全量 Go decorrelation 等价。

## 真实 Go SQL 对照

Go binary 以独立 unistore 启动：

```bash
/tmp/tidb-go-master-oracle/bin/tidb-server --store=unistore \
  --path=/tmp/exists-oracle-data --host=127.0.0.1 -P 14871 --status=14881
```

数据为 ex_outer(a,b)=(1,10),(2,20)，ex_inner(a)=(1)。输出 `/tmp/exists-go.out`：空外层聚合加 `EXISTS(... WHERE ex_outer.a IS NULL)` 为 0/1；选择 b 及关联 EXISTS 为 10/1、20/0；删除内表所有行后第一条为 0/0。日志 `/tmp/exists-oracle.log`。实例已 SIGTERM 并退出 0。

## Ready 验证

Cargo 环境均为 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_scalar_subquery_beside_an_aggregate_over_an_empty_outer_is_null
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib exists_marker_on_an_empty
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_subquery
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib decorrelat
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib
make lint
```

子查询组 15 passed（`/tmp/exists-final-subquery.log`）；planner 5 passed（`/tmp/exists-planner.log`）；session integration 310 passed（`/tmp/exists-integration.log`）；最终 lint 退出 0（`/tmp/exists-final-lint.log`）。完整 session 为 1625 passed / 85 failed / 209 ignored，退出 101（`/tmp/exists-final-full.log`），不能声明全量通过。

本轮没有重跑 RealTiKV、Go/Bazel 或 workspace 全门禁。整体所有 failed cases 目标继续保持未完成，共享统计加载的既有未提交工作独立保留。

对比此前 `/tmp/analyze-sampling-full.log` 的失败名单，本次未新增失败名称；消失的名称也包含中间已独立推送的 view、alias、无 FROM 修复，以及可能受全局状态影响的 SEM/embedding 测试，不能把数量差全部归因于本提交。
