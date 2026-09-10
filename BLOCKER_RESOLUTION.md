# Rust Integration Test Blocker Resolution

本文档说明如何解决 `LOCAL_TEST_REPORT.md` 中剩余失败项的 blocker。核心约束是：Rust 行为必须以 Go 代码和 Go 测试为 source of truth；不能通过修改 golden、放宽断言或忽略失败来取得假绿。

## 1. 建立可复现的 Go/Rust 对照环境

当前报告只有 suite 摘要，缺少每个 case 的 SQL、Go 输出、Rust 输出、执行计划、wire 行数和错误码。首先固定并记录：

- Go TiDB binary 的 commit、版本和构建参数
- Rust binary 的 commit、版本和构建参数
- PD/TiKV/TiDB 的 TiUP 版本与配置
- 测试数据初始化 SQL
- 每个失败 case 的 SQL
- Go/Rust stdout 和 stderr
- coprocessor DAG 或执行计划
- 返回行、错误码、错误文本和 wire 行数

每个脚本应把结果写入独立 artifact 目录：

```bash
ARTIFACT_DIR=/tmp/tidb-rust-diff/$(date +%Y%m%d-%H%M%S)
mkdir -p "$ARTIFACT_DIR"

go_query >"$ARTIFACT_DIR/go.out" 2>"$ARTIFACT_DIR/go.err"
rust_query >"$ARTIFACT_DIR/rust.out" 2>"$ARTIFACT_DIR/rust.err"
rust_wire >"$ARTIFACT_DIR/rust-wire.out" 2>"$ARTIFACT_DIR/rust-wire.err"
```

建议每个 case 同时输出机器可读记录：

```json
{
  "name": "cot_zero_projection",
  "sql": "SELECT COT(0)",
  "go_code": 1690,
  "rust_code": 1105,
  "go_message": "...",
  "rust_message": "...",
  "dag": "...",
  "rows_equal": false
}
```

没有这些记录时，不能可靠判断是 Rust bug、测试 harness bug 还是环境问题。

## 2. 重新录制 Go integrationtest 差异

报告列出的 Go suite 包括：

- `ddl/storage_class`
- `executor/explain`
- `expression/builtin`
- `planner/core/plan`
- `session/nontransactional`

使用仓库脚本帮助中确认的参数重新运行这些 suite，并保存完整 stdout/stderr。根据 Go binary 的结果区分：

1. Go golden 与当前 Go binary 不一致：修 Go fixture/golden。
2. Rust 与 Go binary 不一致：修 Rust 实现。
3. 两边语义一致但格式不同：修输出格式化逻辑。

不能只修改 Rust golden 来隐藏差异。

## 3. 保留 TiKV coprocessor 错误码

当前 Rust 解码路径把 TiKV `CoprocessorResponse.other_error` 转成普通字符串，之后只能映射为通用 1105；Go 的 COT(0) 需要返回 1690。先确认错误码是否实际存在于 `other_error`、batch/select response error、gRPC status details 或 kvproto 扩展字段。

如果协议提供错误码，Rust 错误模型应保留它，并使用 Go/TiKV 真实错误样本添加回归测试。不能把所有包含 `out of range` 的字符串都映射到 1690。若协议完全不携带 code，应优先扩展协议或让 Rust evaluator 在本地生成与 Go 相同的错误，而不是依赖模糊文本猜测。

## 4. 为 Bazel 建立本地 parser 映射

`go.mod` 中的 replacement `github.com/pingcap/tidb/pkg/parser => ./pkg/parser` 是 TiDB 上游长期配置，不能删除。Gazelle 的 `go_repository` 不能表达 file replacement，因此应在 Bazel 中显式声明本地 repository 或本地 `go_library`，并阻止 Gazelle 为该模块生成远程 `go_repository`。

完成后运行：

```bash
make bazel_prepare
bazel query //pkg/parser/...
bazel test //pkg/parser/...
```

不要手工大范围编辑 Gazelle 生成的 `DEPS.bzl`。

## 5. 拆分 Rust 超大源文件

`rust/scripts/check-source-size.sh` 当前报告以下超限文件：

- `crates/tidb-stmtsummary/src/statement_summary.rs`
- `crates/tidb-placement/src/bundle.rs`
- `crates/tidb-session/src/tests_partition.rs`
- `crates/tidb-session/src/show.rs`
- `crates/tidb-model/src/job_args.rs`
- `crates/tidb-model/src/table.rs`

检查脚本要求按职责拆成 sibling modules，不接受只加入白名单。建议按 data model、aggregation、persistence、formatting、encode/decode、PD delivery、DDL/pruning/routing、columns/indexes/partition metadata 等职责拆分，并在每次拆分后运行：

```bash
cargo test -p <affected-crate>
cargo check --workspace
bash rust/scripts/check-source-size.sh
```

## 6. 分类处理 scan-pushdown 失败

每个失败先归类：Go/Rust 行不同是 Rust 语义 bug；行相同但 DAG 不同是 pushdown 策略差异；DAG 相同但 wire count 断言错误是 harness bug；错误码不同是错误协议或映射 bug；projection gate 不同是列类型能力差异。

每个类别独立提交。POW、字符串比较和 DECIMAL projection 不能直接改期望；必须先用 Go 源码或 Go integrationtest 明确证明目标 pushdown 行为。

## 7. 提交与验收

每个修复提交应包含明确根因、回归测试、修复前失败命令、修复后通过命令，并只涉及一个失败类别。每个提交独立 cherry-pick 到 `origin/hparser-integration` 并推送。最终验收必须重新运行 `LOCAL_TEST_REPORT.md` 中列出的完整命令集合，而不是只运行新增的窄范围回归测试。
