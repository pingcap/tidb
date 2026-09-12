# DML 无关联标量子查询修复

## 问题与 Go 依据

基线为 `b9e9095621`，Go source of truth 为 master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。

原失败 `tests_sysbench_access::narrowing_survives_aliases_ordering_limits_and_subqueries` 在执行 `UPDATE sbtest1 SET pad = 'W' WHERE id = (SELECT MAX(id) FROM sbtest1)` 时返回 `Unsupported("uncorrelated subquery evaluation requires the executor hook")`，尚未修改行。2026-09-12 已重新运行原测试确认红测，日志 `/tmp/dml-subquery-red.log`。

Go `pkg/planner/core/logical_plan_builder.go::buildUpdate` 与 `buildDelete` 都通过普通表达式重写构造 WHERE；`expression_rewriter.go::handleScalarSubquery` 对无关联子查询构造 MaxOneRow，优化子计划，再调用 `EvalSubqueryFirstRow`。`pkg/executor/select.go:598` 安装这个回调；第 606 行通过 `SetSkipPlanCache("query has uncorrelated sub-queries is un-cacheable")` 防止计算结果跨 EXECUTE 复用。

Rust 的 SELECT 路径已经安装 `subquery_evaluator`，而 `physical_dml_source_plan_with_allocators` 创建的 PlanBuilder 漏接同一个回调。此次修复在 DML source builder 中复用既有 evaluator，并在共享 evaluator 的计划缓存构建路径设置 Go 对应的不可缓存原因。没有另写子查询求值器、没有特殊处理 MAX、没有放宽原断言。

## Go 实测

固定 Go binary 以 `--store=unistore --path=/tmp/dml-subquery-go-oracle --host=127.0.0.1 -P 14971 --status=14981` 启动。完整 SQL 与原始输出：

- `/tmp/dml-subquery-oracle.sql`
- `/tmp/dml-subquery-go.out`、`/tmp/dml-subquery-go.err`
- `/tmp/dml-subquery-prepared-oracle.sql`
- `/tmp/dml-subquery-prepared-go.out`、`/tmp/dml-subquery-prepared-go.err`

对初始行 `(1,10),(2,20),(3,30)`，`UPDATE ... SET v=(SELECT MAX(v)+1 ...)` 将所有 v 更新为 31；不会随逐行写入变成 31、32、33。DELETE 的 MAX 子查询删除 id=3；空标量子查询赋 NULL。多行标量子查询在 UPDATE 与 DELETE 均返回 `1242 / 21000 / Subquery returns more than 1 row`，剩余数据不变。

开启 `tidb_enable_plan_cache_for_subquery` 后，prepared UPDATE 在第二次执行前插入 id=3，第二次必须更新 id=3；prepared DELETE 连续执行必须依次删除 id=3 和 id=2。Go 的 `@@last_plan_from_cache` 均为 0。新增 Rust 回归同时断言数据与缓存状态。

Go oracle 已正常停止，没有遗留本轮服务进程。

## 验证记录

所有 cargo 命令在 `/tmp/tidb-hparser-current` 使用前缀 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

原失败红测：

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib \
  tests_sysbench_access::narrowing_survives_aliases_ordering_limits_and_subqueries -- --exact
```

新增普通 DML 回归在修复前也因同一 hook 缺失失败，日志 `/tmp/dml-subquery-extra-red.log`。安装 hook 后，原失败和新增回归均通过；整组 sysbench 为 7 passed / 7 failed，失败项仍是既有 EXPLAIN/访问路径断言，日志 `/tmp/dml-subquery-green.log`。此时尚未包含随后加入的 prepared 回归。

最终检查：

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib \
  prepared_dml_scalar_subquery_rechecks_the_snapshot_on_every_execute
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib -- --test-threads=1
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
git diff --check
```

最终 prepared 回归为 1 passed / 0 failed，日志 `/tmp/dml-subquery-prepared-green.log`。串行 session 为 **1655 passed / 62 failed / 209 ignored**，日志 `/tmp/dml-subquery-session.log`；对比基线 `/tmp/static-point-rebased-session.log` 的 1652 passed / 63 failed / 209 ignored，失败名单仅移除了本次原始 DML 子查询 case，没有新增失败，另外两项通过来自新增回归。SQL 集成为 **310 passed / 0 failed**，日志 `/tmp/dml-subquery-integration.log`。

`make lint` 已退出 0，日志 `/tmp/dml-subquery-lint.log`；`git diff --check` 通过。所有测试进程均已结束。格式化仅保留本次修改文件的变化，原有 `tests_analyze.rs` 与 `BLOCKER_RESOLUTION_REPORT.zh-CN.md` WIP 未纳入本次提交。

## 后续失败的证据边界

readiness 已解除，不是本次 SQL 失败的原因。UnionScan 的五项失败仍未修复，但已有 Go 实测证明旧测试说明中的“脏表全局按索引排序”不成立：Go 保留无序 snapshot 子流的顺序，只与 staged 子流逐行比较合并。不能通过对整个 Rust 结果集排序来消除这些失败。该类别需要独立实现 snapshot/staged 两个流的边界，并据真实 Go 输出修正测试；原始证据保留于 `/tmp/union-scan-go.out`。整体 BLOCKER_RESOLUTION 目标仍未完成。
