# Rust 集成测试 Blocker Resolution

## 2026-09-11 autocommit 预取遵守实际事务模式

准备 UPDATE 独立失败，错误为 `only a pessimistic transaction locks
statement keys`（`/tmp/prepared-lock-red.log`）。点写在绑定阶段产生
prelock_keys，但 begin_autocommit_write 返回的事务可能是乐观模式。
DeferredSnapshot 之前仅检查 key 是否为空，无条件请求悲观锁。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/planner/core/point_get_plan.go:1208` 和 1215 仅在
TxnCtx.IsPessimistic 时设置 point/batch-point UPDATE 的 Lock。
Rust 现在同样检查实际预取事务的 is_pessimistic()；乐观模式从原事务
读取 snapshot，悲观模式保留锁返回值及 for_update_ts 路径。
未改变事务后端模式、提交时间戳、锁接口错误检查或原断言。

新增 `optimistic_autocommit_point_write_uses_its_snapshot_without_prelocking`
覆盖同一准备语句对存在与不存在 key 的更新，并验证提交值。
旧实现失败（`/tmp/optimistic-prelock-unit-red.log`），修复后新用例、
原准备 UPDATE/DELETE 用例及真实悲观点更新锁用例均通过
（`/tmp/server-prelock-green.log`）。Ready gate `make lint` 通过
（`/tmp/optimistic-prelock-lint.log`）。
全量 `RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib` 正常结束，
**386 passed / 45 failed**，41.30 秒；这是本轮观测，不能将所有减少项
都归因于本修复，跨测试全局状态相关失败仍需独立复现和治理。

## 2026-09-11 reader 内 TopN 的标量排序键

独立复现 `cluster_views_are_registered_from_go_table_info`：包含
SUM 视图和 MAX 标量子查询的 EXPLAIN 返回 `Get unexpected expression`
（`/tmp/cluster-view-red.log`）。探针证明报错节点是 reader 内的
coprocessor TopN，排序表达式是聚合消除后保留的 cast_decimal(v)。
root 的投影注入未遗漏：Go 的对应 pass 本来不进入 TiKV reader。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/store/mockstore/unistore/cophandler/closure_exec.go:1037` 对
orderByExprs 逐项 Eval 并保存独立的排序 key；普通 Go Sort/TopN 则要求列键。
Rust 的本地 reader 此前直接将 coprocessor 表达式交给普通 TopN。
现在仅在 reader 执行适配中以私有投影物化排序键，并从输出裁去额外列。
原物理计划、EXPLAIN、普通 TopN 限制及断言均保持不变；诊断日志已移除。

新增 `max_over_a_derived_sum_materializes_coprocessor_topn_keys`：
以主键聚合的派生 SUM 再取 MAX，包含非主键顺序的最大值及 NULL。
关闭修复后独立失败（`/tmp/cop-topn-unit-red.log`），开启修复后
executor 全量 **1297 passed / 0 failed**（`/tmp/cop-topn-executor-green.log`）。
原 server 视图用例通过（`/tmp/cluster-view-green.log`），`make lint`
通过（`/tmp/cop-topn-lint.log`）。命令使用
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-executor --lib`，按 Ready 范围验证。
其他 server 和外部集成失败仍不据此宣称完成。
本轮 server 全量正常结束，**374 passed / 56 failed**，耗时 40.93 秒
（`/tmp/server-after-cop-topn.log`），无 readiness 或 DDL owner 永久等待。

## 2026-09-11 HashJoin 保留规划表达式的比较规则

server 原始用例独立失败：`a_case_insensitive_cluster_column_orders_groups_and_dedups_by_its_collation`
自连接 COUNT 实际 4、预期 6（`/tmp/server-collation-red.log`）。
两侧列保留 utf8mb4_general_ci，但 executor 的 physical_builder
丢弃 EqualConditions，按列重建 Tiny 类型 eq，比较规则成为 Binary。
因此 B/b 两行未产生应有的交叉匹配。诊断探针已全部移除。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
`pkg/executor/builder.go:1950` 明确从 EqualConditions[i].CharsetAndCollation()
设置左右 HashJoin key 的比较规则。Rust HashJoin 现在 resolve 原有
等值表达式，保留规划阶段的规则及表达式元数据；没有修改预期结果。

新增 `hash_join_preserves_the_planned_comparison_collation` 回归，用
B/b 两行同时核对默认 CI 自连接 4 行和显式 utf8mb4_bin 自连接 2 行。
旧实现前者返回 2，red 日志 `/tmp/hash-collation-unit-red.log`；
修复后新旧两个 server 用例均通过（`/tmp/hash-collation-server-green.log`）。
`make lint` 通过（`/tmp/hash-collation-lint.log`）。
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-executor --lib` 全量 1296 passed / 0 failed
（`/tmp/hash-collation-executor-green.log`），使用 Ready 验证范围。
本次修复范围为物理 HashJoin，其他 join 类型和整体剩余门禁不据此声明完成。

## 2026-09-11 HAVING 子查询区分结果集合与排序契约

executor 剩余 subqueries 失败独立复现于 `/tmp/subquery-order-red.log`：
无 ORDER BY 的 GROUP BY/HAVING 返回 3,2，旧断言要求 2,3。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`
的 `pkg/executor/test/aggregate/aggregate_test.go:487` 对无排序聚合
先 Sort() 再比较。Rust 测试现在保留原 SQL，按多重集合精确比较结果，
并额外执行 ORDER BY a 的同一查询，直接断言有序结果。
没有修改执行器、过滤条件、预期行值或行数。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-executor --lib` 全部 1296 用例通过
（`/tmp/subquery-order-green.log`）；`make lint` 退出 0
（`/tmp/subquery-order-lint.log`）。server 的其余失败与外部集成门禁
仍需逐项处理，不能将 executor 全绿等同于整体目标完成。

## 2026-09-11 point-get 显式输出保留隐藏列

独立复现 loaded_hidden_columns_preserve_native_layout_and_index_values：
UPDATE t SET v=30 WHERE id=1 返回 point-get output column is outside the row，
`/tmp/hidden-point-red.log`。HandleSourceExec 的输出 Stored(offset) 指向
完整表列，但 next 先通过 visible_of 去掉隐藏列，导致表达式索引维护需要
的隐藏列偏移落在截断行之外。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
pkg/executor/point_get.go 按 e.Schema() DecodeRowValToChunk，并依照 schema
填充虚拟列。Rust 显式物理输出映射现使用完整 decoded row；默认可见输出
继续使用原前缀。没有改变隐藏列可见性、表存储布局或索引断言。

新增 mapped_point_read_retains_hidden_columns_required_by_the_plan 回归，
按隐藏列在前、普通列在后的映射检查输出；旧实现同样越界失败
（`/tmp/hidden-point-unit-red.log`），修复后通过。原始 server 用例
包括 UPDATE 后 FORCE INDEX(vi) 读取也全部通过，0.03 秒
（`/tmp/hidden-point-server-green.log`）。

验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-executor --lib` 为 **1295 passed /
1 failed**（`/tmp/hidden-point-executor-green.log`）；剩余 subqueries 在
2183 行实际顺序 3,2、预期 2,3，尚未修复，不能视为全绿。
`cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib
loaded_hidden_columns_preserve_native_layout_and_index_values` 通过；
两条 cargo 命令均使用上述工具链与栈设置。`make lint` 退出 0
（`/tmp/hidden-point-lint.log`），`git diff --check` 通过。

## 2026-09-11 嵌入式 DDL owner 按存储隔离

check_constraint_runs_through_the_owner_job_queue 单独执行 1.25 秒通过
（`/tmp/check-owner-red.log`，文件名不代表失败），并行全量却反复等不到
历史记录。`/tmp/server-parallel-diagnosis.log` 显示已关闭存储的 scheduler
持续扫描失败；该诊断运行取证后终止，不能计为完成。

根因：RealClusterDdl 的无 etcd 分支调用 MockManager 时传入 store_id=None，
所有独立嵌入式库都使用同一个 mock_store_id/DDL_OWNER_KEY。一个库的
owner 会阻止其他库取得 ownership，因而无法处理后者的持久化 job。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/ddl/ddl.go` 传入 opt.Store，`pkg/owner/mock.go::NewMockManager`
以 store.UUID() 为选举命名空间，只有 nil store 才使用 mock_store_id。

Rust 现使用嵌入式存储的稳定 read authority ID 作为本地 store 身份，
opener 克隆保持同一 ID。新增回归创建两个独立 authority 和一个同库竞争者，
确认独立库都可成为 owner、同库仍互斥。旧实现 2.02 秒失败
（`/tmp/ddl-owner-isolation-red.log`），修复后通过
（`/tmp/ddl-owner-isolation-green.log`）。未修改队列、job 完成断言或超时。

Ready 验证：`make lint` 退出 0（`/tmp/ddl-owner-isolation-lint.log`），
`git diff --check` 通过。完整命令
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-server --lib -- --nocapture` 正常结束，42.83 秒，
**371 passed / 58 failed**（`/tmp/server-owner-isolation-full.log`）；
原 check-constraint 用例通过，没有跳过任何测试。全量仍为失败，58 项
保持待修复，不能把解除调度等待当作整体目标完成。

## 2026-09-11 MODIFY/RENAME COLUMN 统计事件同步

独立复现 `modify_column_ddl_recreates_missing_default_statistics_like_go`：
MODIFY 后 histogram 返回 `[]`，期望 `[["0", "3", "0", "0"]]`，
1.25 秒失败（`/tmp/modify-column-stats-red.log`）。固定 Go master
`pkg/statistics/handle/ddl/subscriber.go` 的 ActionModifyColumn 在
非 analyzed 事件中调用 insertStats4Col；统计初始化发生在事件消费阶段。
此 fixture 与前述 ADD COLUMN 相同，没有启动统计 owner。

在 CREATE、MODIFY、RENAME 后使用已验证的真实 notifier 消费 helper，
保留删除原 histogram 后再检查重建及无 bucket 的全部断言。未修改生产
统计实现、期望值或 golden。完整用例 1 passed，4.26 秒，日志
`/tmp/modify-column-stats-green.log`。

验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-server --lib
modify_column_ddl_recreates_missing_default_statistics_like_go -- --nocapture`；
`make lint` 退出 0（`/tmp/modify-column-stats-lint.log`）；
`git diff --check` 通过。此项独立关闭，其他失败和 scheduler 等待仍未完成。

## 2026-09-11 ADD COLUMN 统计测试的事件同步

独立失败 `add_column_ddl_initializes_statistics_like_go` 的根因是测试没有
消费持久化 DDL 事件。临时探针证明 ALTER 后 job 3/4 已在
mysql.tidb_ddl_notifier 中，等待 1.5 秒后 processed_by_flag 仍均为 0；
统计行依然为空（`/tmp/stats-ddl-delivery-probe.log`）。fixture 仅创建
Unistore stack，没有 production boot 的 campaign_stats_owner 步骤。

固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/statistics/handle/ddl/ddl_test.go::TestDDLHistogram` 在 ALTER 后显式
`statstestutil.HandleNextDDLEventWithTxn(h)`，再断言统计结果。Rust 回归
现在对应地运行实际 notifier，确认持久化队列清空后停止 worker，再检查
原有 histogram/bucket/version 断言。5 秒内队列未清空仍失败，不增加
固定成功延迟，不改统计值，不直接伪造事件或写入统计行。

所有 ADD COLUMN 分支均通过：nullable/default/NOT NULL/virtual、多列
sub-job，以及 IF NOT EXISTS 跳过列不重建已删除 histogram 的约束。
原始红色证据 `/tmp/server-stats-single-red.log`；最终 1 passed，8.29 秒，
`/tmp/stats-ddl-drain-final.log`。临时探针全部删除。

Ready 验证命令：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib add_column_ddl_initializes_statistics_like_go
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-server --lib ddl_notifier
# 3 passed; /tmp/stats-ddl-notifier-regression.log
make lint
# exit 0; /tmp/stats-ddl-drain-lint.log
git diff --check
# exit 0
```

仅关闭此失败类别；不能把其他统计失败或 DDL scheduler 等待归为相同原因，
完整 server 和原始集成清单仍未全部通过。

## 2026-09-11 catalog-load 单表分发修复

原始脚本先失败于过时断言：它要求拒绝 VARCHAR(64)，但节点已支持并加载。
Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/util/rowcodec/decoder.go` 明确将 TypeVarchar 解码为字符串。
脚本现在精确验证两张表的 ID、列形状、无拒绝表，以及 Go/Rust 实际返回行；
VARCHAR fixture 覆盖普通值、空串、前后空格和 64 字符上限。
新增 Go binary 和集群版本参数，以固定 master 与兼容 nightly 实测。

更新元数据断言后，原始单表 SELECT 显露真正生产错误：
`configured ORDER BY/LIMIT planning failed: RelationBinding(ExactlyTwoBaseRelationsRequired)`，
日志 `/tmp/quality-catalog-load-master.log`。多表 adapter 根据 catalog 中
配置了两张表便将全部文本查询送入双表 Join planner。
Go `logical_plan_builder.go::buildJoin` 对 `Right == nil` 明确只规划 Left。
Rust 现从 AST 的 FROM 关系选择单表路径，通过既有 catalog resolver 和
ReadOnlyScanPlan 降低计划，再复用匹配 table_id 的 reader、取消与结果处理。
无事务状态的锁定读取仍拒绝；Join/TopN 路由保持原有验证。

新增无网络回归 `single_table_queries_are_admitted_with_two_configured_tables`：
旧实现失败（`/tmp/catalog-single-route-red.log`），修复后精确验证两个 table_id、
限定名/别名、未知表与锁定读取；原 Join/TopN 回归也通过
（`/tmp/catalog-single-route-green.log`）。最终增加锁定断言后的测试在完整
server 运行中亦为通过。

```bash
RUSTFLAGS='' RUSTUP_TOOLCHAIN=1.97 \
CATALOG_LOAD_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
CATALOG_LOAD_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
bash rust/scripts/run-realtikv-catalog-load.sh
# exit 0; /tmp/quality-catalog-load-master-green.log
make lint
# exit 0; /tmp/catalog-single-route-lint.log
git diff --check
# exit 0
```

完整 `cargo test -p tidb-server --lib` **未通过**：在
`/tmp/catalog-single-route-server.log` 记录 61 项失败，check-constraint
测试持续等待 DDL 历史记录，超过两分钟后取栈并终止本次进程（退出 101），
没有跳过或改写测试。栈 `/tmp/catalog-server-hang.sample` 定位到
`cluster_session_node/ddl.rs::wait_persisted_job` 与 scheduler 等待。
独立执行 `add_column_ddl_initializes_statistics_like_go` 也稳定失败：
实际统计行 `[]`，Go 预期 `[["0", "3", "0"]]`，日志
`/tmp/server-stats-single-red.log`，耗时 1.24 秒。这些是下一组明确失败入口，
并非 readiness 或外部取证缺口。整体质量目标仍未完成。

## 2026-09-11 PD-route 入口与 Bazel 复验

PD-route 旧脚本调用不存在的 `--test realtikv_pd_route`，原始真实运行
失败见 `/tmp/quality-pd-route-red.log`。改为聚合 `all` 与完整名称
`realtikv_pd_route::pd_only_input_discovers_route_and_reaches_tikv`，新增
恰好 1 passed、0 failed 检查，防止 `--exact` 失配导致零测试假通过。
使用 `RUSTFLAGS='' RUSTUP_TOOLCHAIN=1.97 bash
rust/scripts/run-realtikv-pd-route.sh` 完整运行退出 0，日志
`/tmp/quality-pd-route-green.log`。`bash -n`、`git diff --check`、
`make lint` 通过（`/tmp/quality-pd-route-lint.log`）。未改动 RPC 实现或预期。

Bazel parser 的历史 replacement 阻塞已不再复现。`make bazel_prepare`
第一次在 tazel filepath.Walk 回调中空指针退出，当时 Cargo 正在构建；
临时文件变化只是待证假设。Cargo 完成后相同命令退出 0，最终没有任何
生成文件差异，日志 `/tmp/quality-bazel-prepare-retry.log`。
`bazel query //pkg/parser/...` 退出 0；`bazel test //pkg/parser/...`
实际执行 12 个 target，全部通过，parser 分片 test.log 亦为 PASS。
日志分别为 `/tmp/quality-bazel-parser-query.log`、
`/tmp/quality-bazel-parser-test.log`。不修改 go.mod 的本地 parser replacement。

catalog-load 使用固定 Go master 的新证据是：两张表加载成功后，普通
单表 SELECT 返回 `RelationBinding(ExactlyTwoBaseRelationsRequired)`。
此失败在更新过时 VARCHAR 拒绝断言后显露，尚需修复；不能计为通过。

## 2026-09-10 adaptive-forwarding 测试入口修复

原命令实测退出 1（`/tmp/quality-adaptive-forwarding.log`）：
`no test target named realtikv_replica_read`。当前 Cargo manifest 使用
`autotests=false` 和共享 aggregate-tests.rs，普通集成源已归入 `all`。
脚本改用 `--test all` 与完整模块测试名
`realtikv_replica_read::adaptive_forwarding_reuses_proxy_then_recovers_direct`。
新增执行数量断言，必须恰好 1 passed、0 failed，保留全部行为 marker。
此次仅修复测试接线，没有修改 Rust RPC 行为或 Go 对照预期。

```bash
RUSTFLAGS='' RUSTUP_TOOLCHAIN=1.97 \
bash rust/scripts/run-realtikv-adaptive-forwarding.sh
# exit 0; /tmp/quality-adaptive-forwarding-green.log
bash -n rust/scripts/run-realtikv-adaptive-forwarding.sh
make lint
# exit 0; /tmp/quality-adaptive-lint.log
```

真实三节点 TiKV 证明 forwarded_header=tikv-forwarded-host、首次与复用
响应可用、busy_sequence=500,800,150、恢复直连且 preference_cleared=true。
清理检查亦通过。原始失败已由同一端到端脚本复验关闭。

## 2026-09-10 最新 readiness 实测与格式门禁

在 `2337e56a36` 修复上重跑真实集群，完整 access-path 对照退出 0，
末行 `the access-path differential passed`。Rust 日志实际输出
`cluster_session_node_ready`，地址 127.0.0.1:47600，schema_version 68，
stats_loaded 4；不能再将 readiness 描述为尚未解除的 blocker。

```bash
RUSTUP_TOOLCHAIN=1.97 \
ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
ACCESS_PATH_KEEP_LOGS=/tmp/access-pruned-probe-evidence \
bash rust/scripts/run-realtikv-access-path.sh > /tmp/access-pruned-probe.log 2>&1
bash rust/scripts/test-access-path-readiness.sh
# delayed ready / exited / stuck 全部符合预期
```

继续复验历史质量门禁，`cargo fmt --all -- --check` 检出 78 个格式差异块，
涉及 40 个 Rust 文件（`/tmp/pruned-probe-fmt-check.log`）。仅用 cargo fmt
自动格式化；同一检查随后退出 0（`/tmp/pruned-probe-fmt-green.log`），
作为独立格式类别提交，不改变 SQL 断言或 golden。

`RUSTUP_TOOLCHAIN=1.97 cargo check --manifest-path rust/Cargo.toml --offline
--locked -j12 --workspace` 从仓库根目录执行并退出 0，日志
`/tmp/pruned-probe-workspace-check.log`。从 rust 子目录直接使用稳定版
会读取 `.cargo/config.toml` 的 nightly 专用 `-Zthreads=8` 而失败；这是
工具链与入口不匹配，采用既有根目录入口即可，不修改 nightly 配置。
格式修正后 `make lint` 退出 0（`/tmp/quality-format-lint.log`），
`git diff --check` 通过。整体目标仍保留外部脚本、Go suites、Bazel 等
完整验收项；此处不宣称全部集成测试完成。

历史 parser 命令亦重跑：`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml --offline --locked -j12 -p tidb-parser --lib --test all`
退出 0，integration target 为 100 passed、0 failed、1 ignored，日志
`/tmp/quality-parser-tests.log`；没有新增 ignore。

## 2026-09-10 IndexJoin 保留裁剪后的可用索引前缀

condition eleven 剩余 customer 路径差异并非成本偏低：候选日志证明只有
Table 路径到达成本比较。`path_matches_index_join_runtime` 遇到被裁剪的
尾列直接返回 false，错误淘汰已匹配连接键的 idx_customer。
固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/planner/util/column.go::indexInfo2ColsImpl` 遇到缺失列仅截断
prefixCols，保留前面的可用键；`stats.go::fillIndexPath` 将该前缀传入路径。
Rust 现按相同边界终止匹配，保留此前结果；不跨越前缀缺口。

新增 `index_join_keeps_a_usable_prefix_when_trailing_columns_are_pruned`，
旧实现断言失败（`/tmp/pruned-probe-red.log`），修复后通过，同时验证缺失
首列不能用于探测后面的连接键。原始 condition eleven 完整计划断言通过，
包含有序 idx_customer；没有修改 SQL、Go golden 或既有计划断言。

Ready 验证：

```bash
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib
# 928 passed, 0 failed; /tmp/pruned-probe-planner.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1295 passed, 0 failed; /tmp/pruned-probe-full-executor.log
make lint
# exit 0; /tmp/pruned-probe-lint.log
git diff --check
# exit 0
```

临时成本探针已删除。此结果关闭 condition eleven 的剩余路径失败，不能
代替 BLOCKER_RESOLUTION.md 要求的所有外部集成、workspace 和 Bazel 门禁。

## 2026-09-10 aggregate repair 输出 ID 修复

Go master `logical_aggregation.go::PruneColumns` 在补充 COUNT/FIRST_ROW 时调用
AllocPlanColumnID；Rust local helper 使用 i64::MIN 占位，整树调用者未完成分配，
物理 EXPLAIN 泄漏 ScalarQueryCol#-9223372036854775808。本次在整树裁剪点使用
既有 RuleContext.column_allocator 分配，保持 local helper 接口。新增完整
condition eleven 计划断言禁止 repair 输出出现 ScalarQueryCol，占位旧实现
失败于该断言（`/tmp/aggregate-repair-id-red.log`），修复后通过。

按同 JSON 统计 Go 证据同步更正历史测试：顶层 IndexHashJoin、下层 IndexJoin，
键顺序 district/warehouse，orders 使用 TableRangeScan；保持 customer 的
idx_customer 和全部扫描行数断言。当前测试因此停在 customer 索引路径断言，
而不再被错误 MergeJoin 预期遮挡。未修改 Go golden。

Ready 验证：`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml
-p tidb-planner --lib` 927 passed（`/tmp/aggregate-repair-id-planner.log`）；
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-executor --lib` 1294 passed / 1 failed
（`/tmp/aggregate-repair-id-executor.log`）；`make lint` 退出 0
（`/tmp/aggregate-repair-id-lint.log`）；`git diff --check` 通过。
condition eleven 尚未完成，下一项为 customer index/table 候选成本差异。

## 2026-09-10 propagated predicate 访问路径估算修复

condition eleven 的 new_order 扫描新断言要求 Go 实测 9000 行，旧实现为
11250，红色证据 `/tmp/eleven-static-range-red.log`。临时记录揭示原始访问
估算只有 90 行、DataSource 9000 行、table_path_count_after_access=None，
随后触发 9000/0.8 调整。AST WHERE 只含 customer 条件，优化器传播给
new_order/orders 的条件没有经过真实 histogram 访问估算。

统计加载 bridge 现在使用 source.pushed_down_conds 和现有 native ranger
重算索引访问范围，调用现有 access_cost::index_row_count，保留 RowEstimate
边界，并把 common-handle primary 估算同步到 table path。范围使用语句求值器、
范围配额与 fallback handler。列裁剪后保留连续可解析的索引前缀，缺少尾列不
意味着缺少仓库等值范围。第一次要求完整索引列的实验未修复（native-path-estimates.log），
采用前缀后 9000 行断言通过（native-path-prefix.log）。临时诊断代码已删除。

Ready 验证：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-executor --lib`：1294 passed / 1 failed，
日志 `/tmp/native-path-executor-final.log`；condition eleven 越过新的 9000 行
及先前 10 行断言，仍失败于旧 MergeJoin 断言。`make lint` 退出 0，日志
`/tmp/native-path-lint.log`；`git diff --check` 通过。此提交修复访问估算缺口，
不代表 condition eleven 的所有物理计划差异已经解决，整体目标继续 active。

## 2026-09-10 table probe 范围选择率与扫描反算

本轮将 `lt/le/gt/ge` 单列条件经现有 ranger 构建 ColumnRange，调用
`get_row_count_by_column_ranges`（包括空 histogram 的 pseudo 回退），采用列
自身 collation。列间比较继续走原默认分支，不将范围条件硬编码成 0.8。
回归 `range_with_metadata_only_statistics_uses_range_estimator` 精确校验
`(rows/3-rows/1000)/rows`，符合 Go 对非 NULL 下界的 pseudo 估算；关闭新增
范围分支后失败，日志 `/tmp/metadata-range-red.log`。

table probe 保留被运行时 join key 替换的静态条件为 residual，按 Go
`constructDS2TableScanTask` 将输出行数除 residual selectivity 后应用 access
floor 与唯一键上限；扫描和 Selection 共享同一选择率。pseudo source 使用
原 pseudo range 路径，analyzed source 使用含范围处理的现有 helper。
orders 10.00 行断言修复前失败（`/tmp/table-probe-rows-red.log`），修复后通过。
此前退化的 subqueries 两层 IndexHashJoin 也恢复通过，与同 JSON 的 Go 对照一致。

验证命令与证据：`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-planner --lib`，`/tmp/probe-range-planner-final.log`；
`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path
rust/Cargo.toml -p tidb-executor --lib`，`/tmp/probe-range-executor-final.log`；
`make lint`，`/tmp/probe-range-lint.log`。实际结果为 planner 927 passed、
executor 1294 passed / 1 failed，lint 退出 0，使用 Ready 验证范围；
condition eleven 其余旧 MergeJoin/路径/行数断言仍待 Go 校正
与实现修复，不以新断言通过声明整个 case 或整体目标完成。

## 2026-09-10 subqueries 回归的 Go 证据

已导出 wait_orders/wait_lineitem 的原始 Rust fixture 统计到
`/tmp/wait-probe-oracle/`，Go master 加载同一 JSON 并执行 setup.sql 中的查询。
`go.out` 确认两层 IndexHashJoin（semi、anti semi），旧 subqueries 类型断言正确。
未修改生产实现时原测试通过，日志 `rust-export.log`。临时统计导出代码已移除。

Go l2 probe Selection 400.08、TableRangeScan 1203.85，过滤为
`lt(l_orderkey,100000)`；l3 Selection 320.06、TableRangeScan 400.08，过滤为
列间 `gt(l_receiptdate,l_commitdate)`。因此两个过滤选择率分别约 0.3323 和 0.8。
上一轮实验的 analyzed_filter_selectivity 对未识别的 lt 也返回 0.8，不能直接
用于 table probe 的范围反算。JSON 只有 NDV 而无 histogram buckets；现有
cardinality/row_count_estimator.rs::get_row_count_by_column_ranges 在 histogram
total_row_count 为零时回退 pseudo_row_count，提供了正确的后续实现入口。
应通过 ranger 构建 ColumnRange 并复用该估算器，而不是硬编码 1/3 或把所有
条件送入 analyzed helper。该证据解释了实验引入的计划排序回归，整体目标未完成。

## 2026-09-10 table probe 行数实验与回归约束

新增 condition eleven orders 扫描 10.00 行断言，当前实现失败（1.00），日志
`/tmp/table-probe-rows-red.log`。实验将被 runtime keys 替换的静态 access 条件
加入 residual 估算，先按选择率反算扫描，再应用 access floor 与唯一键上限。
orders 10 行断言通过，但 executor 全量出现新增
`driver::tests::subqueries::subqueries` 失败（subqueries.rs:2500，要求两个
decorrelated joins，实际一个）；全量 1293 passed / 2 failed，日志
`/tmp/table-probe-rows-experiment.log`。这尚不证明新的 plan 错误，也不证明旧
断言正确，需要用 Go master 对照该具体 SQL，并区分 pseudo 与 analyzed
选择率调用。为避免集成未经验证的行为，本轮实验生产改动已撤回；保留 orders
扫描行数红色回归。下一步须按 Go chosenRemained 构建及 Selectivity 路径完整
核实，而不是仅匹配列 ID 或在全路径调用 analyzed 估算器。整体目标未完成。

## 2026-09-10 table probe 统计版本保留

Go `constructDS2TableScanTask` 为 probe scan 设置
`StatsVersion: ds.StatsInfo().StatsVersion`。Rust 无 access floor 时新建的
StatsInfo 默认版本为零，导致 analyzed orders probe 错误显示 `stats:pseudo`。
现保留 source 统计版本，仍不附加 Go 此处刻意省略的 NDV。

在 condition eleven 的原始 analyzed 计划上新增断言：orders access 不得显示
`stats:pseudo`。旧实现运行退出 101，直接失败于此断言，日志
`/tmp/probe-version-red.log`。修复后该断言通过，随后仍失败于原有两层 MergeJoin
断言，日志 `/tmp/probe-version-green.log`；这个文件名不表示整个测试通过。
执行命令为 `RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test
--manifest-path rust/Cargo.toml -p tidb-executor --lib
tpcc_condition_eleven_pushes_filters_through_nested_derived_joins`。
`make lint` 退出 0（`/tmp/probe-version-lint.log`），`git diff --check` 通过。
这是可独立验证的版本丢失修复，condition eleven 的扫描行数、路径和 synthetic
COUNT 问题仍未解决，未声明完整 case 通过。

## 2026-09-10 condition eleven 精确 Go master 对照

在 `882cc27174` 上复现剩余 executor 测试，并使用现有 load_stats 导出 API
导出 customer/orders/new_order 的实际 fixture 统计。Go oracle 仍固定
`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，以 unistore 启动并加载相同 JSON。
证据位于 `/tmp/tpcc-eleven-oracle/`：三份表名 JSON、setup.sql、setup.out、
query.sql、go-plan.out、rust.log。导出及可读计划临时日志代码已经移除。

重要更正：Go master 本例没有两层 MergeJoin；实际为顶层 IndexHashJoin，
new_order build 与 orders probe 的下层 IndexJoin，与 Rust join 类型一致。
旧断言不能作为 Go 行为依据。但不能仅修改类型让测试通过，因为以下真实差异
已由相同统计证实：

| 节点 | Go master | Rust |
| --- | --- | --- |
| new_order TableRangeScan | 9000 行 | 11250 行 |
| orders probe TableRangeScan | 10 行 | 1 行，错误显示 pseudo |
| orders probe Selection | 1 行 | 1 行 |
| customer probe | idx_customer IndexReader，扫描 10 行 | TableReader，扫描 1 行 |
| customer synthetic COUNT | Column#41 | ScalarQueryCol#-9223372036854775808 |

Go `constructDS2TableScanTask` 在过滤前计算 rowCount/selectivity，随后应用
accessRowsFloor 和唯一键上限，并保留统计版本；Rust 当前 runtime table path
直接把过滤后 rowCount 作为 scan 统计。该入口是下一修复点。new_order 静态
扫描另经过 CountAfterAccess < dsStats 时的 0.8 调整，需要独立检查加载统计
后的 native common-handle range 估算。未经证明，不把这些差异视为格式噪音。
本轮为 WIP 取证，没有新增生产修复，也未修改现有失败断言；整体目标 active。

## 2026-09-10 condition nine 修复并通过

已定位成本约十倍的原因：Rust 把 accessRowsFloor 直接作为 index filter 后的
runtime 行数，再除 index 选择率生成扫描行数。Go master
`exhaust_physical_plans.go::constructDS2IndexScanTask` 将下限用于
CountAfterAccess，并保持 CountAfterIndex/CountAfterAccess 比率。
本次沿现有 index-filter 选择率路径将下限换算到过滤后行数，并保留原始 runtime
行数作为另一项下限。无 index filter 时选择率为 1，唯一键仍最多一行。

原始候选逐节点证据 `/tmp/nine-inner-tree.log`：每次 probe 扫描
299990.000083 行、过滤后 29999.5 行；修复后 `/tmp/nine-floor-stage-green.log`
为扫描 29999.5、过滤后 3000。outer 8 行对应 EXPLAIN 扫描 239996、过滤后
24000，与此前相同 JSON 统计导入 Go master 的实测结果一致，计划自然选择
district outer / history inner IndexHashJoin。

根据固定 master 的 `load-analyzed.out`，将历史测试的 IndexJoin 类型改为
IndexHashJoin；根据 Go `base_physical_agg.go` 为 partial state 分配新 UniqueID
的逻辑，将错误的 `Column#0 -> Column#0` 改为精确验证 partial 输出等于 final
输入、final 输出与 partial 输出不同。其余树结构、24000/239996 行数和 access
keys 断言保留。不是删掉失败断言或重录 Rust golden。

更新后的同一回归，暂时恢复错误的 floor 阶段后退出 101，日志
`/tmp/nine-floor-regression-red.log`；恢复修复后退出 0，日志
`/tmp/nine-floor-stage-final.log`。所有临时树日志已移除。

Ready 验证：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1294 passed / 1 failed，剩 condition eleven；/tmp/nine-floor-executor.log
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib
# 926 passed / 0 failed；/tmp/nine-floor-planner.log
make lint
# exit 0；/tmp/nine-floor-lint.log
git diff --check
# exit 0
```

本次修复现有 secondary-index residual-filter 路径的下限阶段；table-filter
补偿、独立 index selectivity 求值及其他 IndexJoin 路径的完整 Go 覆盖仍需审计，
不将此单例通过视为整个 Go planner 包完成。condition eleven 与其余完整集成
gates 仍待执行和修复，整体目标继续保持 active。

## 2026-09-10 IndexJoin fractional outer 平均 probe 修复

Go master `exhaust_physical_plans.go::enumerateIndexJoinByOuterIdx` 对正数
buildRows 直接计算 `EqualCondOutCnt / buildRows`，零或缺失统计时为零。
Rust dispatcher 错误地先将 outer 行数钳到至少 1，导致 outer 0.8、join 0.8
时平均 probe 为 0.8 而非 1。现移除此下限并保留 Go 的零值分支。
新增回归直接检查真实 `exhaust_physical_plans` 生成的 IndexJoin runtime
property；同时覆盖 outer 0.8 和 0，不以孤立公式测试代替接线验证。

修复前 `index_join_probe_average_preserves_fractional_outer_rows` 失败
（left 0.8 / right 1.0），日志 `/tmp/fractional-outer-red.log`。
修复后 `RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml
-p tidb-planner --lib` 全量 **926 passed**，日志 `/tmp/fractional-outer-planner.log`；
`make lint` 退出 0，日志 `/tmp/fractional-outer-lint.log`，采用 Ready profile。
TPCC 定向组仍 **5 passed / 2 failed**，日志 `/tmp/fractional-outer-tpcc.log`。

已记录 condition nine 的候选成本 `/tmp/nine-candidate-costs.log`：analyzed
district outer 的 IndexHashJoin 18564874.10，history outer 的 IndexJoin
6981768.48，双方输出估算均 0.8000267。此前相同 JSON 统计的 Go master
district outer IndexHashJoin 成本 1869948.23。因此后续应检查 inner 子计划
成本及 probe 缩放；不能为通过测试直接固定 join 方向。临时成本日志代码已移除。
两个 TPCC 的完整修复及其余 BLOCKER_RESOLUTION.md gates 仍未完成。

## 2026-09-10 physical 深链 ResolveIndices 栈溢出修复

原有 `physical::tests::deep_chain_walks_and_tears_down_without_recursion`
构造 40000 层 Selection，在 `resolve_indices()` 递归访问普通子节点时栈溢出，
32 MiB 栈仍失败，单独复现日志 `/tmp/resolve-depth-red.log`。
`schema()` 的继承路径也使用递归。本次将两处改为显式迭代，保留节点 schema
优先、Sequence 使用末子节点以及 Go 的子节点先于父节点绑定顺序。

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`operator/physicalop/base_physical_plan.go::ResolveIndices` 遇首个错误返回；
Rust 在首个错误后停止绑定并重新组装全部普通子节点，新增回归验证树及后续
兄弟表达式未丢失或被继续绑定。`core/resolve_indices.go::resolveIndices4PhysicalSelection`
只在 Conditions 循环中查询子节点 schema，因此 Rust 空 Selection 同样直接返回，
避免空条件深链反复查找 schema 的二次开销。reader/CTE 等特殊字段的既有绑定
顺序未修改，本次不声称所有特殊嵌套字段均已去递归。

验证使用 Ready profile：

```bash
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib
# 925 passed / 0 failed，默认栈；/tmp/resolve-depth-planner-suite.log
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib physical::tests::deep_chain_walks_and_tears_down_without_recursion -- --exact
# 最终增加深层 schema 断言后 1 passed，0.13 秒；/tmp/resolve-depth-final.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1293 passed / 2 failed；/tmp/resolve-depth-executor-suite.log
make lint
# exit 0；/tmp/resolve-depth-lint.log
git diff --check
# exit 0
```

executor 两项失败仍为 condition nine / eleven 的物理计划差异，未修改其计划
期望。前一轮 Projection 修复另已通过 381 项逻辑规划器测试，独立提交为
`12ea885d12`。原始 readiness blocker 不再存在；整体目标仍未完成。

## 2026-09-10 Projection 组合 NDV 传播修复

固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，使用 Rust
`statistics_table_from_planner_statistics` 和 `gen_json_table_from_stats` 导出
condition nine 的原始 fixture，随后以 Go `LOAD STATS` 导入同一份 JSON。
证据目录 `/tmp/tpcc-master-oracle.0t3pI6/`：`district-rust-stats.json`、
`history-rust-stats.json`、`load-analyzed.sql`、`load-analyzed.out`。
Go 估算 join 0.80 行，district 8 行，history probe 24000 行；实际选择
IndexHashJoin。历史测试中的 IndexJoin 类型及 `Column#0` 不是该 master 的输出。

Rust 根因之一已定位到 `logical/projection.rs::derive_stats`：新建 StatsInfo
丢弃子节点 GroupNDVs，缓存路径也没有按 Go 刷新组合统计。Go
`logical_projection.go::getGroupNDVs` 仅映射直接列引用，丢弃无法完整映射的组合，
并按输出 UniqueID 排序。修复严格沿用此规则，包括缓存刷新和重复投影最后映射语义。
修复前 district 单列 NDV 都为 0.8，join 以分母 1 得到 6.400213；修复后
保留主键组合 NDV 8，join 得到 0.8000267，显示为 0.80，与 Go 一致。

回归 `projection_preserves_and_refreshes_renamed_group_ndvs` 修复前失败：
实际 `[]`，期望 `[GroupNdv { columns: [11, 12], ndv: 8.0 }]`；修复后通过。
日志 `/tmp/projection-group-red.log`、`/tmp/projection-group-green.log`。
定向命令 `RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml
-p tidb-planner --lib projection_preserves_and_refreshes_renamed_group_ndvs`。
`make lint` 退出 0，日志 `/tmp/projection-group-lint.log`。

TPCC 定向组仍为 5 passed / 2 failed，命令使用 `RUST_MIN_STACK=33554432`
及 `RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml
-p tidb-executor --lib tpcc_condition_`。condition nine 仍存在物理路径选择差异，
condition eleven 仍缺少预期 MergeJoin，未放宽断言。
额外 planner 全量分别在默认栈和 32 MiB 栈触发
`physical::tests::deep_chain_walks_and_tears_down_without_recursion` 栈溢出；
日志 `/tmp/projection-group-planner-suite.log` 和
`/tmp/projection-group-planner-suite-stack.log`，不计为通过。
原始 readiness 竞态已由 `1f89c30b65` 修复，真实 access-path 已有通过证据；
当前失败不得再次归为 readiness 外部阻塞。整体目标保持未完成。

## 2026-09-10 condition nine analyzed 统计链路取证

当前提交 a075207030，聚合列绑定断言仍为本地 WIP。开启 Go master 正确的
Fix44855 默认值后，计划选择 history 为 Build、district 为 Probe，而非测试要求的
district 为 Build。日志 `/tmp/tpcc-nine-floor-probe.log`：history 全表 300000，
Selection 30000.50，HashAgg 1.00，过滤后 0.80；顶层 IndexJoin 6.40。

进一步 probe `/tmp/tpcc-nine-ndv-probe.log` 显示分组列 ID 正确为 17/18，输入行
30000.50000833347，两列 NDV 都为 1.000033334166685，group NDV 为空。
这来自 fixture 的原 NDV 10 按默认 skew=1 的选择率约 0.1 缩放，并非列 ID 丢失。
Go master `cardinality/ndv.go::estimateSkewedNDV` 和 Rust 对应公式均为
originalNDV * selectedRows / originalRows。此前称“一行分组异常”只是待验证假设，
不能直接认定该数字为实现 bug。

下步需要将同一 fixture 的 histogram/NDV/realtime 数据导入 Go master 并录制 analyzed
计划，核对历史断言的 0.80 join 行和 24000 probe 行。当前实际生产修复没有新增，
没有放宽这些断言；临时 DEBUG 探针已移除。完成了候选方向与统计来源的取证，
并不表示两个 TPCC failures 或整体 gates 已通过。

## 2026-09-10 Fix44855 的 probe 下限默认开启

固定 Go master `exhaust_physical_plans.go:868` 的 `indexJoinProbeAccessRowsFloor`
读取 `GetBoolWithDefault(..., true)`，而同文件约 1182 行的 NDV 上限读取 false。
Rust bridge 把下限也传为默认 false，混淆了两个不同默认值。现在下限默认 true，
显式 OFF 仍禁用；未改变上限算法。

原测试错误声称下限默认 OFF。保留其 OFF 行为断言，改为显式设置 OFF，再验证
默认计划与 ON 计划完全一致，且 ON 使用完整 join-key 二级索引。恢复旧默认 false
后新增默认一致性断言稳定失败（`/tmp/fix44855-default-red.log`，退出 101）；
恢复 true 后测试通过（`/tmp/fix44855-default-green.log`）。命令：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib index_join_probe_rows_use_only_the_access_paths_join_keys
make lint
```

该修复不代表 TPCC 已通过。condition nine 仍需核对 analyzed 计划：开启正确默认值后
inner 候选形状变化；condition eleven 仍无预期 MergeJoin。另有本地 WIP 将错误的
Column#0 常量断言改为 partial/final 实际绑定关系，依据 Go `BuildFinalModeAggregation`
分配新 UniqueID、保留 original final schema 的源码；该 WIP 尚未作为完成修复提交。

## 2026-09-10 按当前谓词验证 DataSource 统计缓存

修复 `InitStats`：把原 AST 谓词绑定到 DataSource 当前 schema，比对全部
pushed_down_conds。仅等价时复用原有 histogram/range 估算；新增或改变条件时交给
planner 当前表达式推导。AND/OR 允许结合顺序和排列变化，叶子保持现有类型/列身份
相等检查，条件一对一匹配。原 handle/index 路径估算保留。

新增 `derived_aggregate_null_filter_refreshes_source_statistics`，固定 master 在
全新数据库用完全相同 DDL/三行数据验证 Selection=8、scan=10、结果 d_id=1；
证据 `/tmp/tpcc-master-oracle.0t3pI6/derived-null-exact.out`。复用旧数据库的另一份
结果为 1/1.25，统计状态不同，明确不采用为此 fixture 的 oracle。
恢复旧无条件缓存写入后新增测试稳定失败 10 != 8
（`/tmp/predicate-cache-derived-red.log`）；修复后通过。

完整 executor `/tmp/predicate-cache-final-full.log`：1293 passed / 2 failed。
两个剩余失败是 TPCC condition eleven，以及 condition nine 已推进到 analyzed SUM
合成列编号断言（Column#27 -> Column#24 与旧 Column#0 -> Column#0）。
没有更改这些断言。global-count OR-of-BETWEEN 的 5.75 估算也通过，避免了直接清空
stats 实验的额外回归。`make lint` 退出 0（`/tmp/predicate-cache-lint.log`）。
命令：`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib`。
Ready 仅覆盖本项，仍不代表整体目标或统计 package 完整移植完成。

## 2026-09-10 验证并否决直接重新推导统计

可撤销实验：`InitStats::descend` 在已有 pushed_down_conds 时清除预填 DataSource stats，
使后续 planner 从当前条件推导。condition-nine 的 district 8.00 和 IndexHashJoin 断言
均通过，测试推进到 analyzed 阶段 SUM 的合成列编号断言：实际 Column#27 -> Column#24，
旧测试要求 Column#0 -> Column#0。日志 `/tmp/tpcc-native-stats-experiment.log`。
这证明遗漏优化后条件影响实际成本选择，但不证明直接清空统计是可用修复。

完整 executor 实验 `/tmp/native-stats-full-experiment.log` 为 1286 passed / 8 failed，
较此前新增 global-count/index-range、TPCH Q14、common-handle ordered-limit、join-filter、
TPCC 两点查、TPCH Q2 六项失败。命令如下：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib
```

实验代码已撤销，原有 AST histogram/range 估算能力保留，红色回归断言保留。
正式接入应让优化后表达式复用完整估算入口；不能用只支持部分算子的
`logical/rewrite.rs::analyzed_filter_selectivity` 替代完整统计路径。
`ColumnResolver::resolve_expression` 可返回完整绑定表达式，值得用于接入设计，
但仅把整棵表达式包装成 opaque Column 会绕过 AST 条件分类，也不能作为修复。

## 2026-09-10 TPCC NULL 过滤估算的精确红色入口

在当前 `9197fccf9b` 上确认工作树干净后继续诊断。`InitStats::descend` 的临时探针
证明：第二次统计初始化时 district 的 `pushed_down_conds` 已包含 `d_w_id=1` 和
`NOT ISNULL(CAST(d_ytd AS DECIMAL(34,2)))`，但传给 access_cost 的原始 AST 仍只有
`d_w_id=1`，得到 selectivity=0.001。日志 `/tmp/tpcc-source-stats.log`。
`PlannerStatisticsLoad::initialize` 复用 `InitStats`，随后 `recursive_derive_stats`
发现 DataSource 已有 stats 就直接返回，遗漏新增过滤。

在原 condition-nine 回归中新增针对 district NULL Selection 的 estRows 断言，保持
原计划、结果行和其他断言。固定 master 的 `/tmp/tpcc-master-oracle.0t3pI6/condition-nine.out`
证明期望为 8.00。以下命令已执行，退出 101，耗时 0.03 秒，实际 10.00 != 8.00：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup
```

日志 `/tmp/tpcc-nine-filter-red.log`。临时 `[DEBUG-tpcc-source]` 探针已移除。
新增测试断言仍为本地 WIP，尚无生产修复，不将此项当作 green 或完成提交。
下一步应让统计初始化消费当前优化后的条件，并保留已有 column/index histogram、
参数上下文和路径估算；直接清空 stats 或只修 pseudo 分支不能证明完整修复。

## 2026-09-10 TPCC condition nine 的 master 实测取证

以同一固定 master binary 启动独立 unistore，完整 DDL、数据和 SQL 位于
`/tmp/tpcc-master-oracle.0t3pI6/condition-nine.sql`；结果和版本位于同目录
`condition-nine.out`。Go 返回 COUNT=1，选择 IndexHashJoin；Rust 也返回 1，但选择
IndexJoin。Go 的 district Selection 从 10 行降为 8，Rust 保留 10 行；Go 此次计划
没有 cop partial HashAgg，Rust 有。不能只把 Rust 测试中 IndexHashJoin 改成 IndexJoin。

临时成本和计划探针日志 `/tmp/tpcc9-cost-probe.log`、`/tmp/tpcc9-plan-probe.log`：
Rust 候选 build_rows=10、probe_rows=0.8、build_size=56、probe_size=80。
EXPLAIN 的 inner 8 行是乘以 outer 次数后的显示，不能与单次 probe 成本输入直接比较。
下一步核对 `logical/rewrite.rs::pseudo_range_filter_selectivity`、source stats bridge、
runtime avg_inner_row_count 和聚合候选成本。当前仍属未完成诊断，未修改 TPCC golden 或断言，
临时 DEBUG 探针已移除。三个已完成修复提交为 `9f18bd4f96`、`0c5d738da8`、`cd4514ae09`，
已分别推送 origin/hparser-integration。

## 2026-09-10 异步统计加载队列的并行测试隔离

完整并行 executor 曾在 `an_unloaded_column_is_queued_for_async_load` 随机失败，串行通过。
两个 fixture 共用 table_id=11 / column_id=1，加载完成测试在清理阶段删除另一个测试刚入队
的项。固定 master `pkg/statistics/column.go::ColumnStatsIsInvalid` 同样以物理表 ID 和列 ID
构造 `TableItemID`；生产队列的键语义正确，测试身份不应碰撞。

新增固定交错回归 `cleaning_loaded_fixture_preserves_other_fixture_pending_load`，先入队
unloaded fixture，再执行 loaded fixture 的清理和加载状态检查。旧固定 ID 下稳定失败
（`/tmp/stats-queue-red.log`）；改用模块内 AtomicI64 唯一表 ID 后通过。原有全部断言保留，
没有用串行化或生产锁隐藏竞争。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib index_async_load_queue_tests
# 8 passed；/tmp/stats-queue-green.log
RUST_MIN_STACK=33554432 bash -c 'for iteration in {1..30}; do rust/target/debug/deps/tidb_executor-980779d52cc3a360 index_async_load_queue_tests --test-threads=8 || exit; done'
# 30 轮全部通过；/tmp/stats-queue-stress.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib
# 1292 passed / 2 TPCC failed；/tmp/executor-queue-full.log
make lint
# 退出 0；/tmp/stats-queue-lint.log
```

本项 Ready 验证完成。整个目标仍未完成，剩余 TPCC 和外部完整 gates 继续处理。

## 2026-09-10 远端读取保留隐藏 record handle

`TableScanExec::open` 的旧逻辑认为远端不能提供 `_tidb_rowid`，无条件回退本地 cursor，
导致两个 write-range 回归的 cop 请求数为 0。实际上 `RemoteRowCursor::next_keyed_row`
已经在 staged merge 中保留真实 record key。现在从 codec 解码 Int handle，在虚拟列
materialization 后插入输出；需要额外 handle 的扫描不走直接 chunk 交接，以保证输出 schema。
这遵循 Go 以 record handle 定位 UPDATE/DELETE 行、保留 ExtraHandle 列的契约。

新增 `remote_extra_handle_survives_pruning_and_staged_merge` 覆盖只选 `_tidb_rowid`、
用户列值与物理 handle 不同、staged UPDATE 和 DELETE。临时恢复旧回退后测试稳定失败
（cop_scans 0 != 1，`/tmp/remote-handle-red.log`）；恢复修复后完整 remote-scan 组
28 passed（`/tmp/reader-final-green.log`），包括两个原始 write-range 失败和虚拟列回归。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib remote_scan::tests
make lint
git diff --check
```

以上通过，lint 日志 `/tmp/reader-final-lint.log`。同步远端 catalog 改动后重跑了该组。
虚拟依赖独立提交 `9f18bd4f96`；本项独立提交。Ready 仅覆盖本项修复，整个目标仍有
TPCC 计划选择、并行统计队列测试隔离和其他完整验收待完成；不能宣称所有 Rust cases 通过。

## 2026-09-10 Reader 虚拟列依赖补齐

Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 的
`pkg/planner/core/operator/physicalop/task_base.go` 在构造 root reader 前调用
`ExpandVirtualColumn`；`physical_utils.go` 递归收集依赖、去重并保留尾部 synthetic
handle，reader 外层投影恢复 SQL 输出。Rust 原先缺少这个步骤，导致强制二级索引读取
`b AS (a+10)` 时 schema 只有 b,c，绑定 a 报 `Can't find column with UniqueID 1 in schema`。

修复在 CopTask 转 root 时补齐依赖，透传 Selection/Limit/Sort/TopN 的输出 schema，
保留 Aggregate/Projection 的输出契约，最后隐藏新增列。已解析的 protobuf 元数据必须
包含依赖元数据，否则明确报错，不猜测列默认值。新增测试覆盖嵌套虚拟列、重复输出、
有/无主键、强制/忽略索引、SUM，以及 synthetic handle 排序和重复调用。

红色证据 `/tmp/virtual-write-red.log`、`/tmp/virtual-reader-disabled-red.log`；
恢复 expansion 后新增 SQL 测试通过，planner task 测试 59 passed。
仅此修复使原 write-range 测试推进到 UPDATE cop 请求计数断言，不能称该原用例已完全通过；
后者由独立的远端 record handle 修复处理。`make lint` 退出 0
（`/tmp/virtual-handle-lint.log`），`git diff --check` 通过。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib virtual_dependency_expansion_preserves_reader_output
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib task::
```

本项按 Ready 范围验证；整个目标仍进行中。串行 executor 在两个 reader 修复叠加时
1291 passed / 2 TPCC failed，并行另有共享统计队列竞争；这些剩余失败没有跳过或放宽。

## 2026-09-10 Fix52592 接入普通物理点查转换

固定 Go master 的实际 SQL 对照保存在 `/tmp/go-null-oracle.8cduRk/fix52592.txt`：`SELECT b FROM t WHERE a>=5 AND a<=5 AND b>1` 默认包含 Point_Get；`SET tidb_opt_fix_control='52592:ON'` 后为 TableReader/Selection/TableRangeScan，范围仍是 `[5,5]`。Go `pkg/planner/core/find_best_task.go` 在计算 `canConvertPointGet` 时读取此 fix，同时控制 table 和 index 路径。临时 Go unistore 已收到 SIGTERM 并正常退出，日志和 SQL 输出保留。

Rust 原来只在 AST fast-plan 入口读取 52592，普通 DispatchContext 没有该状态，导致相同查询开关 ON 后仍发点查。`/tmp/fix52592-red.log` 为精确失败证据（退出 101）。现在 bridge 读取执行上下文的 fix control，并传递 point conversion permission；dispatch 对普通 table/index 的 PointGet 和 BatchPointGet 转换统一应用。默认值保持允许转换，开关解析继续使用现有 Go-compatible bool getter。

原远端回归验证真实 get/scan 操作及 residual、ORDER BY、LIMIT，修复后通过。新增 `fix52592_disables_unique_index_point_conversion` 覆盖 unique index 单点和批量、缺失值、residual，并连续切换 OFF/ON/OFF 检查行结果及读操作，结果通过。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
a_cluster_point_get_is_one_key_lookup_and_no_coprocessor_request
# 1 passed；/tmp/fix52592-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
fix52592_disables_unique_index_point_conversion
# 1 passed；/tmp/fix52592-unique-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -- --test-threads=1
# 1287 passed / 4 failed；/tmp/executor-fix52592.log
make lint
# 退出 0；/tmp/fix52592-lint.log
```

当前稳定失败为 TPCC condition nine、condition eleven、`write_range_reader_preserves_record_identity_and_staged_rows`、`write_range_reader_reconstructs_virtual_columns`。并行统计队列干扰和 BLOCKER_RESOLUTION.md 其余全量验收仍未完成；不能将上述串行结果称为整个 Rust 测试体系通过。

## 2026-09-10 NULL-bound 测试按实际 Go master 更正

旧 `an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request` 要求 `a BETWEEN NULL AND NULL` 读取并返回 100 行再过滤。该预期与实际 Go master 不符，不应通过增加 Rust 无效扫描满足它。

使用固定 master binary `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 启动独立 unistore（127.0.0.1:47891，路径 `/tmp/go-null-oracle.8cduRk`），执行真实 MySQL 协议 SQL：创建 clustered BIGINT 主键表、插入两行、EXPLAIN 和执行 NULL-bound 查询。`/tmp/go-null-oracle.8cduRk/results.txt` 保存版本和输出：`TableDual_6 0.00 root rows:0`，SELECT 返回空；正常 `a>97` 对照仍为 TableRangeScan。完整启动日志同目录。Go `logical_datasource.go::Conds2TableDual` 所在优化链路不能用旧注释中单独 `IsConstNull` 的行为替代。

测试现在断言 NULL-bound 查询结果为空、`StorageOps::default()`（无 get/scan/cop 请求）、wire rows=0；正常 BETWEEN 98 AND 100 的三行远端读取对照保留。这是用 Go 实测更正并加强请求断言，不是放宽结果断言、改 golden 或跳过 case；Rust 生产逻辑未改。

修复前失败见 `/tmp/executor-common-catalog.log`；`RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request` 现在 1 passed（`/tmp/null-bound-green.log`）。`make lint` 退出 0（`/tmp/null-bound-lint.log`）。整体目标仍未完成。

## 2026-09-10 无显式 PRIMARY 索引的 common-handle catalog 修复

稳定红色复现：`dirty_common_handle_reads_share_the_remote_staged_merge` 在 unsigned common-handle `a=18446744073709551615` 下返回空（`/tmp/common-remote-red.log`，退出 101）。表已保存 common-handle offsets，但 catalog 没有独立 PRIMARY KvIndex。Go 的 `TableInfo.Indices` 始终保留 clustered PRIMARY 元数据，是否维护独立索引记录是另一个问题；Rust `handle_range::clustered_primary_metadata` 已实现该重建，planner catalog 却漏用它。

`Catalog::planner_catalog` 现在复用此函数，将缺失的 clustered PRIMARY 元数据加入 SourceTable 索引视图；已有真实 PRIMARY 保留其 ID、列和前缀。未新增物理索引记录，也未修改 SQL 断言。原 dirty common-handle 失败与 `composite_cnf_writes_fetch_only_the_matching_keys` 均通过。前者扩展了单点、重复 IN、缺失键断言，并验证这些查询不打开普通/远端扫描；覆盖 signed、unsigned、大小写不敏感字符串三种键，保留原 staged UPDATE/DELETE、LIMIT、聚合和投影断言。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
dirty_common_handle_reads_share_the_remote_staged_merge
# 1 passed；/tmp/common-catalog-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -- --test-threads=1
# 1284 passed / 6 failed；/tmp/executor-common-catalog.log
make lint
# 退出 0；/tmp/common-catalog-lint.log
```

remote_scan 模块为 22 passed / 4 failed（`/tmp/common-metadata-tests.log`），另有两项 TPCC 聚合失败。并行队列隔离及其他完整验收项仍未完成，整体目标保持未完成。

## 2026-09-10 unsigned staged ORDER BY 元数据修复

`unsigned_staged_rows_merge_in_the_readers_value_order` 修复前返回 `u64::MAX, 0, 1`，期望 `0, 1, u64::MAX`。已用单测独立复现，`/tmp/unsigned-merge-red.log` 退出 101。

定点探针排除了 ORDER BY 未传入和合并器 unsigned 比较缺失：reader 实际收到 keep_order=true、unsigned=true，但只有一段扫描；planner 传入的是 `Int(i64::MIN)..Int(i64::MAX)`。证据 `/tmp/unsigned-ranges-probe.log`。根因在 `Catalog::planner_catalog`：`SourceTable.pk_is_handle` 来自 `KvTable.pk_handle_offset`，`SourceColumn.is_primary_key` 只取 PRI_KEY flag。通过 `register_kv` 安装的表已设置 handle offset，但未复制该 flag，导致 planner 找不到 primary handle 列并以 signed LongLong 生成范围。

Go master `fdfadb96b2c` 的 `pkg/planner/core/operator/logicalop/logical_datasource.go::getPKIsHandleColFromSchema` 要求 PKIsHandle 和 primary-column flag 一致；`pkg/distsql/request_builder.go::SplitRangesAcrossInt64Boundary` 随后按 unsigned 值顺序拆成两段。Rust catalog 转换现在将 integer/common handle offsets 同时用于 primary-column 标识，保留已有 PRI_KEY 标识，保证传给 planner 的元数据与实际 storage handle 一致。未修改 signed/unsigned 编码或合并器比较，也没有给结果额外排序来遮盖问题。

原回归覆盖 staged UPDATE/DELETE、升降序、LIMIT、无序结果集合以及两段远端请求数。新增投影裁掉主键的 `SELECT b FROM t WHERE a>=1 ORDER BY a` 断言，结果仍按 unsigned 主键正确排序。所有 `[DEBUG-unsigned-merge]` 探针已删除。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
unsigned_staged_rows_merge_in_the_readers_value_order
# 1 passed；/tmp/unsigned-merge-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -- --test-threads=1
# 1282 passed / 8 failed；/tmp/executor-unsigned-metadata.log
make lint
# 退出 0；/tmp/unsigned-metadata-lint.log
```

remote_scan 模块运行 20 passed / 6 failed（`/tmp/remote-metadata-green.log`）；另外两项稳定失败仍为 TPCC 聚合。并行统计队列干扰仍未修复，串行结果不能代替并行门禁。其他 BLOCKER_RESOLUTION.md 完整验收项继续保留，整体目标未完成。

## 2026-09-10 clustered PRIMARY 点查返回空修复

`primary_batch_reads_use_written_common_handle_encoding` 在修复前稳定失败：DECIMAL(8,2) 主键插入 `(5.00,10),(6.00,20)` 后，IN 查询返回空，期望为 10、20。日志 `/tmp/common-handle-red.log`，退出 101。

根因不是 readiness，也不是仅缺少 short-handle padding。物理计划允许 PointGet/BatchPointGet 保留 PRIMARY 的 index ID；`UniqueIndexPointSourceExec::open` 无条件走普通唯一索引的 `_i` 键查询，而 `KvIndex.clustered_primary` 写入路径不维护独立索引键。Go master `fdfadb96b2c` 的 `pkg/executor/builder.go::isCommonHandleRead` 明确返回 `tbl.IsCommonHandle && idx.Primary`；`point_get.go::Next` 和 `batch_point_get.go::initialize` 都对该情形跳过普通索引读取并构造记录 handle。

Rust 现在使用已有 `clustered_primary` 元数据识别该路径，调用与写入相同的 `KvTable::common_handle_from_values`，保留表级 collation 和 CommonHandle padding；普通 unique index 继续走原有 lookup。扩展 SQL 回归，在已有 DECIMAL、VARCHAR、BIGINT UNSIGNED 的重复 IN/不存在值用例后分别检查单点查询。原有回归由红转绿，新增单点断言也通过。临时 `[DEBUG-common-key]` 探针已删除。

验证命令（仓库根目录）：

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib \
primary_batch_reads_use_written_common_handle_encoding
# 1 passed；/tmp/common-handle-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -- --test-threads=1
# 1281 passed / 9 failed；/tmp/executor-common-primary-serial.log
make lint
# 退出 0；/tmp/common-primary-lint.log
```

并行全量为 1280 passed / 10 failed（`/tmp/executor-common-primary.log`）：PRIMARY case 已通过，另外出现 `access_cost::index_async_load_queue_tests::a_fully_loaded_column_is_not_queued`。串行不出现该额外失败；该模块多个测试共用全局异步队列的 table=11/column=1，测试隔离问题仍待独立修复，不能把串行通过等同于并行门禁通过。稳定的 9 项为下节 10 项去掉 PRIMARY case；其他完整门禁仍待验收。

## 2026-09-10 prepared filter 参数上下文修复

最终合并状态完整 access-path 再次退出 0：`/tmp/access-merged-readiness.log`；节点日志 `/tmp/access-merged-readiness-evidence/rust-node.log` 第 8 行包含 `cluster_session_node_ready`。沿用下节固定 Go master 和 nightly 命令，仅替换 `ACCESS_PATH_KEEP_LOGS` 目录。索引候选修复独立提交为 `891ed3c537`；prepared filter 修复单独提交。

根因：`tidb-expr/src/evaluator.rs::eval_vectorized_expression` 的非 deferred 常量分支调用 `constant.eval()`，没有传递执行上下文。SelectionExec 的向量过滤因此无法读取已经绑定的 prepared parameter，抛出 `unbound prepared parameter`。StmtContext 绑定和 clone 均保留参数；问题在过滤求值调用点。

Go master `fdfadb96b2c` 的 `pkg/expression/constant.go::Constant.VecEvalInt` 等方法调用 `genVecFromConstExpr`，通过当前 EvalContext 求值；`getLazyDatum` 从该上下文的 ParamValues 取参数。Rust 现与已有 projection batch 路径一致，对每个非空 batch 调用 `constant.eval_in(ctx)` 一次并广播结果；空 batch 不求值，deferred 表达式仍按行执行。

新增回归 `vector_filter_reads_current_parameter_once_per_nonempty_chunk`：复用过滤表达式，切换 NULL、0、1、-1 参数，验证 0、1、8 行的过滤 mask、NULL mask 和参数读取次数。新增回归和已有 `prepared_filter_and_projection_use_fresh_execution_state` 修复前均失败，日志 `/tmp/vector-filter-red.log`、`/tmp/prepared-filter-red.log`。

```bash
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-expr --lib evaluator::tests
# 11 passed，/tmp/vector-filter-green.log
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
# 1280 passed / 10 failed，/tmp/executor-after-vector-filter.log
```

已 fast-forward 同步远端 `cb4043ed3a`，保留其他作者的 statement context 和字符映射提交。合并后再次运行 executor 全量，仍为 **1280 passed / 10 failed**（`/tmp/executor-merged-readiness.log`）；`make lint` 退出 0（`/tmp/readiness-merged-lint.log`）。新增 skyline 回归和已有 prepared filter 回归均通过。

仍失败的 10 项如下，未跳过、未改断言，且不属于 readiness：

- `driver::tests::aggregates::tpcc_condition_eleven_pushes_filters_through_nested_derived_joins`
- `driver::tests::aggregates::tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup`
- `driver::tests::point_get::primary_batch_reads_use_written_common_handle_encoding`
- `remote_scan::tests::a_cluster_point_get_is_one_key_lookup_and_no_coprocessor_request`
- `remote_scan::tests::an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request`
- `remote_scan::tests::composite_cnf_writes_fetch_only_the_matching_keys`
- `remote_scan::tests::dirty_common_handle_reads_share_the_remote_staged_merge`
- `remote_scan::tests::unsigned_staged_rows_merge_in_the_readers_value_order`
- `remote_scan::tests::write_range_reader_preserves_record_identity_and_staged_rows`
- `remote_scan::tests::write_range_reader_reconstructs_virtual_columns`

从仓库根目录执行 Cargo 不会自动读取 `rust/.cargo/config.toml`，全量测试需要显式设置 `RUST_MIN_STACK=33554432`。未设置时出现的 grouped subquery 栈溢出不是本轮参数修复的测试结论。

## 2026-09-10 最新结论：readiness 已解除，access-path 对照通过

以下为最新状态，后文保留早期失败和 WIP 记录作为时间线，不能用早期描述覆盖本节结果。

- 启动竞争修复已推送：`1f89c30b65`。TCP listener 先于应用 ready 是正常启动窗口；原脚本在窗口内只 grep 一次就退出并杀掉节点。现有界轮询 ready，同时检测进程退出。确定性回归验证延迟 ready、提前退出、永久无 ready 三种情况。
- Go master 基准固定为 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，binary 为 `/tmp/tidb-go-master-oracle/bin/tidb-server`；配套 nightly PD/TiKV 版本和 hash 见下文。不能用 v8.5.6 的估算代替 master。
- 最新 skyline 代码完整运行 access-path，退出 0：`/tmp/access-final-skyline.log`。节点 `/tmp/access-final-skyline-evidence/rust-node.log` 第 8 行输出 `cluster_session_node_ready`，schema_version=68、stats_loaded=4。所有该脚本断言通过，没有修改 SQL golden 或放宽断言。
- 新增 ANALYZE 后复合索引支配回归，包含局部驱逐统计 payload 的情况；旧 cost-only 路径选 idx_rare，当前选择 Go master 的 idx_cover。analyzed 状态来自 existence metadata，完整 RowEstimate 保留估算上下界，skyline 使用 LIMIT 调整前的行数。
- 此修复的接入范围仅为有 analyzed 元数据且无残余 index filter 的普通索引候选。表路径、pseudo、残余 index filter 等未知指标候选继续保留供 cost 选择；不声称 Go planner package 或全部 skyline 行为完成。比较器提供的 fix-control relevance 回执尚未接入 statement tracing。

复现命令（仓库根目录 `/tmp/tidb-hparser-current`）：

```bash
RUSTUP_TOOLCHAIN=1.97 \
ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
ACCESS_PATH_KEEP_LOGS=/tmp/access-final-skyline-evidence \
bash rust/scripts/run-realtikv-access-path.sh
bash rust/scripts/test-access-path-readiness.sh
RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 \
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib skyline_tests
make lint
```

readiness 回归通过；skyline_tests 实际运行 3 项并全部通过（`/tmp/skyline-final-tests.log`）；`make lint` 退出 0（`/tmp/readiness-final-lint.log`）。全量 executor 基线为 1279 passed / 11 failed；禁用新增 skyline 后为 1278 passed / 12 failed，差异只有新增复合索引回归，证明这 11 项不是本次裁剪引入。日志分别为 `/tmp/skyline-executor-all-stack.log`、`/tmp/skyline-executor-disabled.log`。

整体质量目标仍未完成。readiness 不能再作为剩余 SQL/执行器失败的 blocker；后续应按实际失败继续修复。原 chunk panic 的完整 stack/reproduction 仍未建立，已有 guard 不能视为根因闭环。

## 2026-09-10 scan-pushdown

提交 `53fc7659a2` 在 Rust 1.97 下通过完整 Real TiKV scan-pushdown：

```text
RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-scan-pushdown.sh
EXIT:0
```

- `PI()` 与 Go 返回完全相同的 `id` 行集合。
- residual predicate 不再触发 `Chunk::column` 越界、1105 或断连接。
- COT predicate/projection 保持 Go errno `1690`。
- PD shutdown request-handle 警告仅影响 receipt 观察，不影响 SQL 结果比较。

## PD bootstrap 继续验证（2026-09-10）

新增的确定性 gRPC 回归先返回启动错误、下次返回合法 membership；无需 sleep 改写 fixture。
原始错误类型 `1` 是 UNKNOWN，`NOT_BOOTSTRAPPED` 是 `2`，之前将两者混同的分析不成立。
回归在仅重试 NOT_BOOTSTRAPPED 的实现下失败，错误精确为 `[PD:server:ErrServerNotStarted]server not started`。
现在同时重试该明确启动错误，保留其他错误及跨集群检查。退避上限参照 Go `pkg/store/store.go` / `pkg/util/misc.go`，启动重试依据 go.mod 固定 PD client 的 `servicediscovery.serviceDiscovery.initRetry`。

验证：`RUSTUP_TOOLCHAIN=nightly-2026-08-22 cargo test --manifest-path rust/Cargo.toml -p tidb-pd-client --test all`：45 passed。
日志：`/tmp/pd-bootstrap-red.log`、`/tmp/pd-bootstrap-green.log`。这是 WIP 范围验证，未宣称全部门禁通过。

lock-recovery 最近保留日志显示 `the cluster catalog has no table mysql.tidb`，不是测试成功。
进一步阅读脚本发现它使用 Go `beforeCommitSecondaries` failpoint 构造已提交 primary / 未提交 secondary，再由 Rust 集成测试验证锁恢复。
此前强行添加 Rust read-table wrapper 是 harness 方向错误；下一步恢复 Go fixture server，仍由 Rust 测试验证核心行为。不能把缺失系统表当成这条测试必须移植的前置条件。

## lock-recovery 端到端通过（2026-09-10）

修正脚本后使用 Go failpoint-enabled fixture：

```text
LOCK_RECOVERY_TIDB_SERVER=/Users/chenhuansheng/Documents/GitHub/db9-ai/hparser-integration-tidb/bin/tidb-server
RUSTUP_TOOLCHAIN=nightly-2026-08-22 bash rust/scripts/run-realtikv-lock-recovery.sh
lock-recovery lock recovery passed: campaign13_lock_recovery status=committed ... cop_attempts=2 publications=1
```

这确认 PD readiness 重试、Go failpoint 的 primary/secondary 构造、Rust lock resolver 和第二次 cop response 全部跨进程工作。此前 `mysql.tidb` 缺失来自错误地用 Rust server 承担 Go fixture；现已移除该 wrapper。测试过滤器也改为 aggregate target 的全限定 case 名，避免 0 tests 假失败。

## access-path 当前证据（2026-09-10）

完整脚本运行完成，但统计阶段仍有 2 个硬失败、7 个路径差异：

- `bucket=1 AND rare=7`：Go `idx_cover` 估算 1（伪统计阶段 0.10），Rust 伪统计 1.25。
- `SELECT bucket,rare ... bucket=1`：Go 覆盖索引估算 500，Rust 2000。
- ANALYZE 后 Rust 对 `rare=7`、`rare>0`、`u(a=1,b=2)` 等选择退化为全表或错误索引，说明问题不止成本常数，而是 Go `GetRowCountByIndexRanges` 的复合索引等值前缀、直方图 total-row-count 和列统计回退语义未完整接入。

权威运行：`RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-access-path.sh`。该结果未宣称通过；下一修复类别继续对照 `pkg/planner/cardinality/row_count_index.go` 与 Rust `row_count_estimator.rs`。

## 2026-09-10 access-path reload 修复验证

提交 `c058347ad5` 修复了 Go 语义中的统计装载条件：当缓存项 histogram 版本相同但处于 evicted（仅元数据）状态时，必须重新读取完整 histogram，而不能因版本比较直接复用。该修复已独立推送到 `origin/hparser-integration`。

重跑 `run-realtikv-access-path.sh` 后仍观察到相同的 2 个 hard failure 与 7 个路径 divergence，说明当前剩余问题位于统计 payload 本身或 planner 的索引估算调用链，而非该缓存复用条件。证据已保留在 `/tmp/access-path-rerun.log`，下一步继续检查 index histogram payload 与 range 编码的一致性。

追加提交 `5de8ec9007`：修正前一提交中的分支方向，evicted payload（`!is_full_load`）现在进入 `load_item(..., full_load=true)`，避免仅保留 metadata。已推送到 `origin/hparser-integration`。

## 2026-09-10 剩余 skyline 差异的最小回归（WIP）

本轮进一步修正 candidate 的 analyzed 判断：Go `isCandidatesPseudo` 使用 `ColAndIdxExistenceMap.HasAnalyzed`，与 NDV payload 是否在内存中无关。WIP 原先通过 `hist.index_ndvs().contains_key` 判断会在局部 eviction 时漏掉复合索引。现在 bridge 传递 `index_stats_existence` 中为 true 的索引 ID 到 `DataSource.analyzed_index_ids`，每次统计初始化重建，并在 clone 时保留；dispatch 使用这个集合。

扩展已有 ANALYZE 回归为两种状态：完整 payload；仅驱逐多列索引 payload、保留单列索引 payload 和全部 analyzed 元数据。曾尝试全部驱逐，但旧逻辑也通过，不能作为修复证据，已改成局部驱逐。临时恢复旧 NDV 判断后运行同一测试，`evicted=true` 明确失败并输出 idx_rare，日志 `/tmp/skyline-eviction-red.log`（退出 101）。恢复 existence map 判断后执行 `RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::tests::primary_keys`，**15 passed**，日志 `/tmp/skyline-existence-green.log`（退出 0）。`git diff --check` 通过。本轮验证为 WIP；最新 existence map 修改尚未完整集成重跑，未提交推送；表路径/pseudo/residual-filter 全面接入仍待完成。

本轮普通 dispatch 已接入第一组可完整构造的索引候选：有已分析索引统计、无残余 index filter 时，构造 access/index 列长度集合、covering、property、global/MV、eq/IN 和 min/max 估算，使用共享逆序循环保留 skyline，然后才对保留任务比较 cost。其他候选暂以未知 metrics 保留，不凭空补风险或 CountAfterIndex。该范围仍未达到 Go 全部 skyline 行为，尤其表路径、pseudo 分类及 residual index filter 的 Selectivity 尚待补齐，不以本次 case 通过代替完整目标。

验证发生实质变化：`analyzed_composite_index_dominates_single_equality_index` 从失败变为通过，实际选择 idx_cover；`driver::tests::primary_keys` 实际 **15 passed**；固定 master/nightly 的 `run-realtikv-access-path.sh` 输出 **the access-path differential passed**，完整日志 `/tmp/access-skyline-wip.log`，节点日志 `/tmp/access-skyline-wip-evidence/`。此集成构建之后又将 skyline 使用的访问行数改为 LIMIT 调整前的值（Go 在物理 scan 调整前做 skyline）；最新代码重新运行主键测试 15 passed、`make lint` 退出 0，但该最后修改未重新进行完整集成。集成命令与上节固定 master 命令相同，仅日志目录改为上述路径。所有这些实现仍为未提交 WIP，下一步补齐剩余候选信息后再独立提交推送。

后续核验模块树发现重要更正：`find_best_task/candidate.rs` 和 `index_join/candidate.rs` 中存在比较器及其引用，但 `find_best_task.rs` 原本没有声明 candidate 模块；不能据文件搜索就声称其在运行时已由 IndexJoin 使用。本轮已声明 `pub mod candidate`，将 Go `skylinePruning` 的逆序淘汰循环实现为 `insert_skyline_candidate`，保留互不支配候选，返回 pseudo winner / fix45132 使用信息，允许跳过 TiFlash。第一次测试过滤实际运行 0 项，未计入通过；接线后处理测试闭包生命周期错误并重跑，`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib skyline_tests` 实际 **3 passed**，涵盖双向插入顺序下的复合索引支配、互不支配/属性冲突保留和 TiFlash 保留，日志 `/tmp/skyline-frontier-tests.log`。此为可测试的裁剪原语，普通 dispatch 的候选构造与调用仍未接入，完整 SQL 红色回归尚未解决，改动仍未提交推送。

后续 WIP 已发现并补足一个前置数据缺口：`driver/planner_bridge.rs` 原先调用只返回 `.est` 的 `index_range_row_count`，丢弃估算器已有的 `min_est/max_est`。现保存完整 `RowEstimate` 到 `DataSource.index_path_row_estimates`，重新配置统计时清空旧数据，并在 DataSource clone 中保留。既有 `index_path_count_after_access` 继续接收相同 est，尚未改变路径选择。`RUSTUP_TOOLCHAIN=1.97 cargo check --manifest-path rust/Cargo.toml -p tidb-executor` 已退出 0（日志 `/tmp/skyline-bounds-check.log`）。这些更改尚未提交推送；仍需接入候选属性及 Go skyline 淘汰循环，再验证下方红色回归。不能把数据传递完成等同于 planner bug 已解决。

新增本地测试 `driver::tests::primary_keys::analyzed_composite_index_dominates_single_equality_index`：创建三个索引，插入 2000 行，调用真实 `analyze_kv_table` 并安装统计，最后 EXPLAIN `SELECT * FROM t WHERE bucket=1 AND rare=7`。命令：

```bash
RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib analyzed_composite_index_dominates_single_equality_index
```

已运行，退出 101；测试本身耗时 0.06 秒。实际为 `IndexLookUp -> IndexRangeScan(idx_rare) + Selection(eq(bucket,1)) -> TableRowIDScan`，估算 1 行。期望 Go master 的 `idx_cover`，与上一完整集成对照相同。原始输出 `/tmp/analyzed-skyline-red.log`。该测试目前有意保持红色作为后续修复入口，不代表修复完成。

静态调用链证据：共享 `find_best_task/candidate.rs::compare_candidates` 只有 `find_best_task/index_join/candidate.rs` 调用；普通 `dispatch.rs` 在 `for path in ds.enumerated_paths` 中直接比较 task cost，只对 IndexJoin 另做 skyline count 比较。`logical/rule_prune_indexes.rs` 的提前过滤是相关列评分/数量限制，不是 Go `compareCandidates` 的支配关系裁剪。因此下一步应补普通 DataSource 的 skyline candidate 构建和比较，并保留 Go 的 property、covering、pseudo、risk、eq/IN 与 fix45132 约束；不能只按索引列数强制选择复合索引。本轮未改变生产计划选择逻辑，使用 WIP 验证范围。

## 2026-09-10 固定 Go master 的完整 access-path 结果

已建立干净 worktree `/tmp/tidb-go-master-oracle`，固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。构建命令 `env -u LDFLAGS make server` 退出 0，`bin/tidb-server -V` 确认该 hash、Go 1.26.2、无 dirty 后缀；`git status --short` 为空。首次直接 `make server` 因本机 LDFLAGS 中的 ICU `-L...` 参数被传给 Go linker 而失败，仅清除该命令的环境变量即解决，未修改 Go 源码。

master Go 与 PD v8.5.6 组合不能 bootstrap：PD 返回 `Unimplemented: unknown method QueryRegion for service pdpb.PD`。该运行日志为 `/tmp/access-master-diff.log`。新增 `ACCESS_PATH_CLUSTER_VERSION` 参数，允许选择兼容的 PD/TiKV，默认值仍为 v8.5.6，输出中明确记录版本。随后使用本机已有 nightly：PD `d71c0396ac26eb96a969c28b0efdafef9dd5aac3`，TiKV `1167092fea81ff8cb16ac49779f5702f7e225e79`。

完整运行命令：

```bash
RUSTUP_TOOLCHAIN=1.97 \
ACCESS_PATH_CLUSTER_VERSION=v9.0.0-beta.2.pre-nightly \
ACCESS_PATH_TIDB_SERVER=/tmp/tidb-go-master-oracle/bin/tidb-server \
ACCESS_PATH_KEEP_LOGS=/tmp/access-master-nightly-evidence \
bash rust/scripts/run-realtikv-access-path.sh > /tmp/access-master-nightly-diff.log 2>&1
```

结果：**0 failure(s), 1 divergent choice(s)，退出 1**。Go/Rust 的 pseudo 复合索引均为 1.25；ANALYZE 后 covering 查询均为 500；大表查询返回行对照通过。唯一差异为 ANALYZE 后 `SELECT * FROM t WHERE bucket=1 AND rare=7`：Go 使用 `idx_cover(bucket,rare)`，Rust 使用 `idx_rare(rare)`，双方 estRows 都为 1。该差异仍保留为失败，尚需对照 master skyline pruning 和 cost 调用链修复。不能把旧运行的 2 failures / 7 divergences 继续描述为此次 master 基准的结果，也不能仅凭一次运行将所有历史统计加载差异都归因于版本。

节点日志位于 `/tmp/access-master-nightly-evidence/`；此次没有改动 Rust 生产实现或 SQL 断言。验证 profile 为 Ready 范围：`make lint`、`bash -n rust/scripts/run-realtikv-access-path.sh`、`bash rust/scripts/test-access-path-readiness.sh` 均退出 0。整体目标仍未完成，其余脚本和 Go integration suites 也不能据此视为通过。

## 2026-09-10 access-path 的 Go 基准版本缺口

脚本的 Go 节点固定为 TiUP **v8.5.6**，不是用户要求的 Go master。对照 `origin/master` 的 `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`，`pkg/planner/cardinality/selectivity.go` 在返回前明确执行 `ret = max(ret, 1.0/float64(coll.RealtimeCount))`。该下限来自 `11b8149926`（2026-05-28，#67841）。随后 `pkg/planner/core/stats.go::adjustCountAfterAccess` 将较低的路径估算调整为 `ds.StatsInfo().RowCount / cost.SelectionFactor`。

因此 `bucket=1 AND rare=7` 的原始 pseudo index 估算虽为 `0.10`，经 master 一行下限和 `0.8` selection factor 调整后为 `1.25`。Rust 现有结果与这段 master 源码一致，不能为追平 v8.5.6 的 `0.10` 删除下限。本轮保留生产估算逻辑，并增加 EXPLAIN characterization 测试 `pseudo_composite_index_applies_master_selectivity_floor`，核对实际 scan 节点（不是 reader 的 operator info 引用）及 idx_cover。

脚本新增 `ACCESS_PATH_TIDB_SERVER`：指定可执行的 Go binary 后使用 TiUP 的 `--db.binpath`，并打印该 binary 的 `-V`；未指定时明确打印旧版本基准提示。PD/TiKV 仍为原版本，SQL 断言与 golden 均未修改。当前可见本地 Go binary 为 `7a8404bd17-dirty`，不能称为固定 master 的验证。尚需构建指定 master revision 并用此入口重跑，剩余 ANALYZE 后统计估算错误也未解决。

验证：`RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib pseudo_composite_index_applies_master_selectivity_floor`；`bash -n rust/scripts/run-realtikv-access-path.sh`；`bash rust/scripts/test-access-path-readiness.sh`；`git diff --check`；`make lint`。这是基准版本取证和测试入口改进，不是全部 access-path failures 的完成声明。

## 2026-09-10 readiness 竞争修复与证据更正

此前对话将一次 `never reported ready` 输出反复描述为已复现的服务端死锁，证据不足，应撤回。旧脚本只等待 TCP 端口开放，然后立即执行一次 ready 日志 grep；grep 失败就触发 EXIT trap 杀掉节点。因此日志停在 `mysql_tls` 不足以证明节点持续阻塞。

Rust `sql_node.rs::ConcurrentSqlNode::bind` 在创建 memory runners 之前调用 `TcpListener::bind`，`cluster_session_node/boot.rs` 则在 bind 返回、安装 signal handler 后才输出 ready。端口可连接与 ready 日志之间存在正常时序窗口。Go source of truth `origin/master`（`fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`）的 `pkg/server/server.go::Run` 同样先 `initTiDBListener`，随后启动网络 listener，最后才设置 `s.health.Store(true)`；TCP 可连接不是应用 ready 的充分条件。

修改 `run-realtikv-access-path.sh`：端口开放后轮询原 ready 事件，最多等待 180 秒；进程退出立即失败，持续无 ready 仍超时失败并打印日志。未删除 ready 断言，未改动 Rust 服务启动或任何 SQL golden。

回归 `test-access-path-readiness.sh` 从生产脚本提取实际启动检查代码，模拟端口已开放但 ready 延迟一秒。修复前退出 1，打印 `the Rust node never reported ready` 和 `mysql_tls`；修复后通过。另验证提前退出和活进程永久无 ready 都被拒绝，超时测试通过推进 Bash SECONDS 避免等待三分钟。

验证命令与结果：

```bash
bash rust/scripts/test-access-path-readiness.sh
# PASS: delayed ready; exited node rejected; stuck node rejected
bash -n rust/scripts/run-realtikv-access-path.sh rust/scripts/test-access-path-readiness.sh
git diff --check
make lint
# 均退出 0，使用 Ready 验证范围
RUSTUP_TOOLCHAIN=1.97 ACCESS_PATH_KEEP_LOGS=/tmp/access-readiness-evidence \
  bash rust/scripts/run-realtikv-access-path.sh > /tmp/access-readiness-fixed.log 2>&1
```

真实运行已输出 `cluster_session_node_ready`，地址 `127.0.0.1:47600`，schema_version 60；完成所有 access-path SQL 对照，最后因原有 **2 failures / 7 divergent choices** 退出 1。节点日志保存在 `/tmp/access-readiness-evidence/rust-node.log`。启动 blocker 已解除，整体目标仍未完成；剩余失败为 strict-superset pseudo estRows 和 ANALYZE 后 covering index estRows，须继续对照 Go cardinality 实现修复，不能将本次运行记为全套通过。

## 2026-09-10 chunk panic 修复

定位到 StreamAgg DECIMAL SUM 快速路径使用原始列 offset；child chunk prune 后列数不足时会在 `chunk.rs:212` 越界。现已在两个快速路径入口验证 `index < chunk.num_cols()`，布局不匹配时回退通用表达式求值，避免 panic 并保持 Go 语义。提交：`rust: guard decimal stream aggregation column access`。`tidb-executor` 聚合相关测试编译完成；已有 prepared plan receipt 测试失败与本改动无关，需继续按 Go planner source of truth 处理。
