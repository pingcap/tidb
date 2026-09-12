# 动态分区 PointGet 修复与 readiness 复核

## 结论与范围

readiness 竞争已由 `1f89c30b65`、`9839a744e0` 修复并推送。四个 runner 等待真实 `cluster_session_node_ready`，不会在 TCP 开放后只检查一次并误杀仍在初始化的节点。本轮四入口的延迟 ready、提前退出、持续无 ready 回归均通过。

本次继续修复实际 SQL 失败：动态分区单键访问未转换为 PointGet，参数化执行还可能因保留旧分区而漏行。未修改原 grouped HAVING 的计划或结果断言。此提交不声明全部 Rust failed cases、Go package 移植或历史 chunk panic 已完成。

## Go source of truth

固定 master SHA：`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，源码 `/tmp/tidb-go-master-oracle`，实际二进制 `bin/tidb-server`。

- `pkg/planner/core/find_best_task.go:2220`：允许动态分区单点转换，拒绝普通动态多点 BatchPointGet；隐式 rowid 仍要求显式单分区。
- `pkg/planner/core/operator/physicalop/physical_batch_point_get.go:350`：`PointGetPlan.PrunePartitions` 每次缓存绑定后根据当前键重新定位分区；显式 PARTITION 限制与派生分区独立保存。
- 同一函数对非二进制 common handle 从 access conditions 重建原始 SQL 值，不能用排序键路由。

实际 Go 输出：`/tmp/grouped-partition-point-go.out`、`/tmp/dynamic-partition-point-compact-go.out`、`/tmp/point-partition-extra-go.out`。Go 返回 `Point_Get partition:p2` 和原始字符串行；整数及大小写不敏感字符串的第二次跨分区执行均确认 `last_plan_from_cache=1`。显式 P0 排除 P1，NULL 返回空集。

## 实现

`PhysicalPointGet` 保留逻辑表 ID、显式分区名和当前物理分区。规划器允许单个动态主键点转换；执行阶段重新计算路由，不能把初次裁剪出的物理 ID 固定进缓存。common handle 使用现有 partition ranger 重建原值。无匹配分区构造零行执行器；SELECT、DML、EXPLAIN 和进程计划发布使用相同准备逻辑。

生产修改位于 `rust/crates/tidb-planner/src/find_best_task/dispatch.rs`、`physical/mod.rs`、`rust/crates/tidb-executor/src/driver/physical_builder.rs`、`driver/dml.rs` 和 `src/explain.rs`。其余构造器同步新增元数据，回归位于 `tidb-session/src/tests_partition.rs`。

## 验证

在 `/tmp/tidb-hparser-current` 执行，Cargo 命令统一带：

```bash
export RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib grouped_having_intersection_reads_one_clustered_partition_point
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib dynamic_partition_
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_prepared_plan_cache
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib physical::tests::
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests::
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
git diff --check
```

修复前：`/tmp/grouped-point-current-red.log` 返回 TableRangeScan；`/tmp/dynamic-partition-point-compact-red.log` 期望 `[11]` 却得到空集。修复后原用例通过，`/tmp/dynamic-point-final-regressions.log` 两个新增回归通过，覆盖跨分区缓存、缺失键、无匹配分区、显式限制、非二进制排序规则、NULL 和 SELECT/UPDATE/DELETE EXPLAIN。

Ready 检查完成：42 个缓存测试、46 个物理计划测试、12 个执行器构建测试通过。最后 DML 展示补充后的 310 个 session 集成测试也全部通过（`/tmp/dynamic-point-final-integration.log`），lint 退出 0（`/tmp/dynamic-point-final-lint.log`）。

最终分区套件为 97 passed / 1 failed / 0 ignored（`/tmp/dynamic-point-final-partition.log`），剩余 `a_residual_conjunct_keeps_the_static_per_partition_batch_point_get` 已用 Go 再确认，属于独立静态 BatchPointGet 估算/选路问题，未跳过、未放宽断言。

## 真实依赖与启动

Go master 不能直接搭配脚本默认 PD v8.5.6：本轮 Go 节点日志明确报 `unknown method QueryRegion for service pdpb.PD`，证据 `/tmp/dynamic-point-go-pd-incompatibility.log`。该运行在 Go 端口开放前失败，Rust 尚未启动。使用已验证的 nightly 依赖运行：

```bash
RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432 \
  ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
  ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
  ACCESS_PATH_KEEP_LOGS=/tmp/dynamic-point-access-nightly-evidence \
  bash rust/scripts/run-realtikv-access-path.sh > /tmp/dynamic-point-access-nightly.log 2>&1
```

这些脚本来自本分支已有提交，并非 Go master 官方测试入口：access-path 首次引入为 `0e229647f5`，analyze 为 `610c123952`。可用 `git log --reverse -- rust/scripts/run-realtikv-access-path.sh` 查完整演进。它们执行真实 Go/Rust 对照，但覆盖范围不能代替仓库全部质量门禁。

本轮真实回放退出 0，末行 `the access-path differential passed`。`/tmp/dynamic-point-access-nightly-evidence/rust-node.log:8` 是真实 ready，地址 `127.0.0.1:47600`、schema_version=73、stats_loaded=4。测试节点、playground 和数据已清理；用于核验 RPC 不兼容的独立诊断 playground 也已清理。未重跑另外三个 runner 的完整套件或全量 Go/Bazel 门禁。
