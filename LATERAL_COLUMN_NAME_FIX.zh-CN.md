# 串联 LATERAL 的关联列命名

基线 `a624d08fe7`，Go master source of truth 固定为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。

## 根因

`lateral_derived_table_join_shapes` 修复前返回 UnknownColumnInClause("x.a")，日志 `/tmp/lateral-chain-red.log`。`SELECT t.a` 在 LATERAL 内重写为 CorrelatedColumn，Rust 的 projection_field_name 只对普通 Column 使用列名规则，因此得到表达式文本 `t.a`。后一个 LATERAL 无法以 `x.a` 引用它。

Go `pkg/planner/core/logical_plan_builder.go:1594` 明确为 CorrelatedColumn 从 AST 取列名（a），显式 alias 优先。Rust 两个投影构建入口现在传入关联列身份，在命名处采用相同规则。输出表达式、列 ID 和求值未改。

新增 `lateral_correlated_projection_names_are_columns_not_expression_text` 检查链式引用、输出名及显式 alias。命名修复后原用例推进至 LEFT JOIN 旧断言，日志 `/tmp/lateral-chain-green.log`，该阶段 6 passed / 1 failed。

## 旧断言的 Go 实测修正

Go master `buildLateralJoin`（同文件约 1005 行）支持 LEFT JOIN，仍拒绝 RIGHT JOIN。原测试声称 LEFT JOIN 必须返回 3809，已不符合固定 oracle。

Go binary 独立 unistore：`--path=/tmp/lateral-oracle-data --host=127.0.0.1 -P 14871 --status=14881`。日志 `/tmp/lateral-oracle.log`，链式查询输出 `/tmp/lateral-go.out`，与测试相同 fixture 的 LEFT JOIN 输出 `/tmp/lateral-left-fixture-go.out`：

- t.a 序列为 1、1、2、3、3、3。
- 增加 v>300 后为 (1,NULL)、(2,NULL)、(3,301)、(3,302)。

据此将过时拒绝断言改为精确结果断言，并新增 NULL 补行检查；RIGHT JOIN 的错误断言保持。实例已收到 SIGTERM 并正常退出。

继续执行原用例后，RIGHT JOIN 的拒绝仍是内部字符串，无法满足既有 DriverError::InvalidLateralJoin 断言（`/tmp/lateral-views-final.log`）。增加 PlanErrorKind::InvalidLateralJoin，并将 RIGHT/NATURAL/USING 的拒绝映射到已有 DriverError 和 3809。NULL 行检查使用已有 row_text helper；query_text 将 SQL NULL 显示为 `<nil>`，两者仅显示约定不同。

## 验证

统一 Cargo 环境：`RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib lateral_derived_table_join_shapes
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib lateral_
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_views
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib projection
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib
make lint
```

Ready profile：最终生产代码下 view 组 24 passed（`/tmp/lateral-complete-views.log`），session integration 310 passed（`/tmp/lateral-complete-integration.log`），lint 退出 0（`/tmp/lateral-complete-lint.log`）。projection planner 56 passed（`/tmp/lateral-planner.log`，在追加 3809 错误类型前完成，验证命名改动）。追加 wire code 断言后的 view 组仍为 24 passed（`/tmp/lateral-ready-views.log`）；完整 session 为 1629 passed / 83 failed / 209 ignored，退出 101（`/tmp/lateral-ready-full.log`）。未运行 RealTiKV、Go/Bazel、workspace 全量 gates，整体目标保持未完成。
