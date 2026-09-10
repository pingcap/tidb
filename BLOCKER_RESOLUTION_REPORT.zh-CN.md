# Rust 集成测试 Blocker Resolution

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
