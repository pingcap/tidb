# 整数比较异常常量与空计划修复

## Go 对照与根因

修复基线为 `d58f3a35a3`。Go source of truth 固定为 master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，使用 `/tmp/tidb-go-master-oracle/bin/tidb-server` 的 unistore 实例在 14971 端口取证；实例现已停止。

原 session 失败为 `tests_sysbench_access::a_point_write_keys_the_row_a_scan_would_have_filtered_to`。`EXPLAIN DELETE FROM sbtest1 WHERE id=150.5` 在 Rust 返回 TableFullScan，而旧测试要求 TableRangeScan。实际 Go 输出是 `Delete_4 -> TableDual_7 rows:0`，因此两者都不正确。`id=150.0` 的 Go 输出仍为 Point_Get_1 handle:150。原始输出位于 `/tmp/fraction-point-go.out`。

Go `pkg/expression/builtin_compare.go::RefineComparedConstant` 返回转换后的常量及 `isExceptional`；`compareFunctionClass.refineArgs` 结合 NOT NULL 标志判断能否将整个比较改写为零和一的比较。在整数列不可能与小数相等、或常量超出整数域时，非空列的比较可折叠；可空列必须保留 NULL 传播。Rust 原实现将异常信号丢成 `None`，留下带类型转换的列比较，ranger 无法利用整数域中的不可能条件。

此次保留该异常标记，按 Go 的比较方向构造零/一参数对。可空列、携带 ParamMarker 或 DeferredExpr 的可变常量不被替换为固定真值。既有普通整数精化路径保持原样。

首个服务层回放生成了 TableDual_6。进一步比较 Go `logical_plan_builder.go::buildSelection`，发现 Go 在重写 WHERE 之前创建 Selection，即使随后折叠为 TableDual；Rust 在所有重写结束后才分配。修复将 Selection 的基对象创建移到重写之前，使空计划和子查询计划使用同一套 Go 分配顺序，没有直接调编号或去掉编号断言。

## 原始证据

- `/tmp/fraction-point-go.out`：Go 的 DELETE/UPDATE 计划。
- `/tmp/fraction-refine-go.out`：非空 a、可空 b 的等式、NULL 等式和超限字符串比较结果，以及对应 SELECT 计划。
- `/tmp/fraction-refine-red.log`：新增表达式回归修复前失败，`a='150.5'` 仍为列比较，无法折叠。
- `/tmp/fraction-point-session.log`：中间版本已经由 TableFullScan 改为 TableDual，但仍有 6 对 7 的编号差异。

Go 的 `a=150.5`、`a<=>150.5` 均为 0；当 b 为 NULL 时，`b=150.5` 为 NULL，`b<=>150.5` 为 0。`EXPLAIN SELECT ... WHERE a=150.5` 是 TableDual；可空 b 的计划仍保留 Selection。表达式回归覆盖小数等式、NULL 等式、正负溢出、左右镜像，以及可空列不可折叠。

旧 `TestCompareFunctionWithRefine` Rust 结构快照也明确写着本功能未实现。本次依据 Go 原始 `pkg/expression/builtin_compare_test.go::TestCompareFunctionWithRefine` 更新已实现参数对的断言；上下文无关 rewriter 中负数字面量仍可能以 unaryminus 子树出现，其已有未完成标记继续保留，不把整个表达式包宣称为完整移植。

## 验证

工作目录 `/tmp/tidb-hparser-current`；cargo 前缀为 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-expr --lib -- --test-threads=1
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib -- --test-threads=1
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
git diff --check
```

表达式串行全量结果为 **1174 passed / 1 failed / 99 ignored**，日志 `/tmp/fraction-refine-expr-final.log`。所有比较回归通过，唯一失败是 `json_schema_valid_resolves_file_and_http_references`：本地 schema HTTP server 五秒内未收到请求。并行和串行运行均观察到该失败，未跳过或放宽其断言，后续需独立诊断。

最终 session 为 **1656 passed / 61 failed / 209 ignored**，日志 `/tmp/fraction-refine-session-final.log`。与上一轮 `/tmp/dml-subquery-session.log` 比较，失败名单仅移除了 `a_point_write_keys_the_row_a_scan_would_have_filtered_to`，没有新增失败。该 case 完整断言 Go 的 TableDual_7 和删除后的 13 行均通过；小数可精确表示、字符串整数、NULL、无符号最大主键等同一 case 的分支也通过。

SQL 集成为 **310 passed / 0 failed**，日志 `/tmp/fraction-refine-integration-final.log`。`cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib plan_builder:: -- --test-threads=1` 为 **134 passed / 0 failed**，日志 `/tmp/fraction-refine-planner.log`。`make lint` 退出 0，日志 `/tmp/fraction-refine-final-lint.log`；diff 检查通过。所有本轮测试进程均已结束。原有 `tests_analyze.rs` 和 `BLOCKER_RESOLUTION_REPORT.zh-CN.md` 的 WIP 保留，不纳入此次提交。整体 Rust failed cases 与质量门禁目标尚未完成。
