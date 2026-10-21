# Align the Rust node's structure with Go TiDB, in dependency order

This ExecPlan is a living document. Keep Progress, Surprises and discoveries, Decision log and Outcomes current at every stopping point. It records the current state only; superseded text is removed, and git history keeps it.

## Purpose and outcome

The Rust node must answer every statement the way Go TiDB (the oracle, run from a scratch worktree of this branch) does. Fixing one wrong answer at a time produces emulations of structural differences, where Rust decides something at a different time, in a different place, or twice. So the structure is corrected first, in dependency order, accepting temporary regressions, and no emulation is added for anything a structural item covers (user directive, 2026-10-17).

After this plan, privileges are decided from the plan, a view body is built by one code path, every session runs one DDL implementation over one store (as Go's `testkit` runs every test), and the statement paths that exist twice (in-process session and cluster node) share one definition per statement kind. Observe it through the in-process replay (`cargo test -p difftest-result-tests --test integration_diff integrationtest_replay_matches_recorded_tidb_output`), the production replay (`cargo test -p tidb-server --lib production_replay_stays_within_budget -- --ignored`) and the Go oracle probes (`difftool.sh` in the session scratchpad).

## Context for a reader new to the repository

`rust/crates/tidb-session` holds `Session`, the statement dispatcher (`dispatch.rs`), privilege collection and the session-side seams (`ddl_service`, `analyze_service`, `stats_statement_service`, the store-statement services). `rust/crates/tidb-executor` holds the volcano executors, the in-memory `Catalog` (`driver/catalog.rs`), the DML drivers and the in-process DDL (`ddl/`). `rust/crates/tidb-exec` holds the cluster tier: `cluster_ddl.rs` and `cluster_ddl/` lower DDL statements, admit them as Go's executor does and step persisted jobs as Go's `ddl` worker does; `real_tikv_ddl.rs` submits and runs them against the store; `table_info_build.rs` builds `TableInfo` as Go's `ddl` package does. `rust/crates/tidb-planner` is the port of Go's planner. `rust/crates/tidb-server/src/cluster_session_node` is the production session: it wraps `tidb_session::Session` and routes DDL and writes to the cluster tier. A "visit info" is Go's list of (database, table, privilege) entries the plan builder appends while planning and `CheckPrivilege` verifies afterwards.

## The open structural mismatches

DDL exists twice. Every cluster schema change Go runs as a job is a persisted job in `mysql.tidb_ddl_job`, stepped by the owner through Go's schema states; the in-process executor DDL (`tidb-executor/src/ddl`, about 23,000 lines) still runs its own one-step implementation, and a bare `Session::new()` reaches only that one. Milestone 3 removes it.

Transactions exist twice (audited in Milestone T below). Production runs two transaction objects at once (the session's `Transaction` and the node's explicit store transaction, kept in step by call order); there is no isolation-provider seam; `handlePessimisticDML`, the retry loop, savepoints, implicit commit and the `autocommit=0` lazy opener are each implemented twice. Target: one TxnManager in `tidb-session` over a store-transaction trait, the in-process catalog one backend of it. Two beliefs this paragraph used to carry were wrong against Go and are removed: Go's `BEGIN` is eager, and `tidb_disable_txn_auto_retry=OFF` does not exist (see Milestone T, Audit).

Caches (done; what remains is listed here). `tidb_session::GlobalSysvars` is Go's `GlobalVarAccessor` over `tidb_domain::sysvar_cache::SysVarCache`: its GLOBAL tier is the cache rebuilt from `mysql.global_variables` (`rebuildSysVarCache`, every GLOBAL SetGlobal hook run with its value), `@@global.x` reads its global view, and a new session copies its session view (`loadCommonGlobalVariablesIfNeeded`), a `SkipInit` variable being read through at first use and kept. Both tiers share Go's cache lifecycle: a rebuild after every `SET GLOBAL` (`NotifyUpdateSysVarCache(true)`) and `LoadSysVarCacheLoop`'s 30-second tick (the in-process tier over its committed catalog, the cluster tier also on the `/tidb/sysvars` etcd notify). Still different from Go: in-process restricted SQL runs in the issuing session's transaction (the transactions item). `information_schema` providers read the statement's pinned schema; TABLES/PARTITIONS sizes are estimated over that schema from the node's `TableRowStatsCache`, and DDL_JOBS resolves names through it while reading the latest jobs. The cluster node pins a transaction's schema at BEGIN to the version it has loaded, where Go loads the schema at the transaction's start ts.

Execution and storage. `system_row_write.rs` is a third row writer. Cop selection is lowered through a second, bounded predicate IR that silently drops conjuncts it cannot express. `real_tikv_dml.rs` is a non-deployed DML path tests still exercise. EXPLAIN ANALYZE runtime counters, EXPLAIN FOR CONNECTION's runtime columns and commit details are missing (commit details wait on the native committer).

Session versus server duplicates. LOCK STATS partition resolution and ANALYZE target resolution live in `tidb-exec` as well as in the session; TRACE is written twice.

Bootstrap layout. Under the classic kernel, Go's bootstrap gives every `mysql.*` table an ordinary global id through a DDL job (user tables start at 117; the 58 bootstrap jobs are in DDL history). Rust's bootstrap follows the next-gen layout (system tables at reserved ids, user tables from 4, no bootstrap history). Ids leak into `tidb_table_id`, region keys and `ADMIN SHOW DDL JOBS`. The fix keeps next-gen's layout for a next-gen kernel and first removes the fixed-id dependencies (`advisory_lock_key`, the system database read by its fixed id).

Planner statistics set-up runs in the executor: `planner_bridge.rs` decides which statistics to load from the AST's WHERE (a predicate in a derived table loads none), recomputes index ranges and fills row estimates with the executor's own `GetRowCountByIndexRanges` (`access_cost.rs`), where Go does it in `initStats` / `fillIndexPath` / `deriveIndexPathStats`. Non-SELECT statements have no `PlanBuilder.Build`.

DDL details still open: the reorganize owner does not repeat `checkReorgPartitionDefs`, `checkColumnsPartitionType`, `checkPartitionFuncType` and `checkPartitionKeysConstraint`; TiFlash's PD rules for new partitions and `checkPartitionReplica`; global-index reads during a PARTITION BY's last states; the region split after a split policy (`preSplitAndScatterTable`); the masking-policy rows Go maintains on DROP / MODIFY COLUMN and RENAME TABLE (no writer); DDL-time analyze (`tidb_stats_update_during_ddl`); `ALTER RANGE ... PLACEMENT POLICY` (parsed, never executed).

## Plan of work

Milestone 2: a view body is one code path. Done for CREATE VIEW, the view's metadata reads and EXPLAIN: `tidb_planner::plan_builder::create_view` is Go's `buildDDL` arm (the body built with `canExpandAST`, `renamingViewName` and `isCreateView`, the AST writes `buildSelect` makes recorded per `QueryBlockOffset` and applied to the statement, `v.Cols` from the plan), `tidb_planner::preprocess` is `handleTableName`, and `tidb-executor/src/view.rs` keeps only the preprocessor's `checkCreateViewWithSelect`, the DDL checks and `ddl.BuildViewInfo`. DESCRIBE and `information_schema.columns` build the view through `BuildDataSourceFromView` (`tryFillViewColumnType`), and the EXPLAIN view privileges (`ErrViewNoExplain`, SHOW VIEW visits, `StmtCtx.ViewDepth`) are the planner's. Two syntactic walks of stored view bodies remain outside the planner, each owned by another structure: the session's per-statement virtual-table materialisation (`information_schema_tables_in_query`) and the MDL table-id collection (`Catalog::mdl_table_ids`, Go's `tryLockMDLAndUpdateSchemaIfNecessary` in `buildDataSource`).

Milestone 3: one DDL implementation, as decided. 3b: move the glue a store-backed session needs (boot, schema sync, the DDL worker) below `tidb-session` into a domain crate, as Go's `domain` sits below `session`. 3c: give `Session::new()` an in-process unistore through that crate. 3d: migrate statement kinds one per gate, with their tests, deleting the executor DDL for each kind once nothing calls it. 3e: delete the in-process catalog's row storage.

Milestone 4: execution details (DML reads through the coprocessor, EXPLAIN ANALYZE runtime info).

Milestone 5: planner statistics set-up. Move InitStats, index-path statistics and row estimates from `planner_bridge.rs` / `access_cost.rs` into the planner as Go's functions, starting with a prototype on one access path.

The bootstrap mismatch is fixed in its own batches as it is reached; the GC worker follows its own plan.

Milestone T: one transaction manager. The audit, the checked Go facts and the staged milestones are below; each stage lands as its own gated commit.

### Milestone T, Audit (2026-10-21)

Go facts checked in this tree (`pkg/`), each by reading the source and by a live statement on the Go oracle:

1. `BEGIN` is eager. `SimpleExec.executeBegin` (`pkg/executor/simple.go:631`) calls `TxnManager.EnterNewTxn(EnterNewTxnWithBeginStmt)` (`pkg/session/txnmanager.go:156`); `baseTxnContextProvider.OnInitialize` (`pkg/sessiontxn/isolation/base.go:81`) sets `activeNow := true` for that type and calls `ActivateTxn`, which waits the TSO future (`base.go:283-307`) and stores `TxnCtx.StartTS`. Live: `select @@tidb_current_ts` is `0` before `BEGIN` and a real TSO right after it, with no statement in between; `BEGIN; COMMIT` records `@@tidb_last_txn_info` with that `start_ts` and `commit_ts` 0 (the empty commit never reaches `tikvTxn.Commit`, but `doCommitWithRetry` and `setLastTxnInfoBeforeTxnEnd` do run). So a lazy `BEGIN` would be a regression, not an alignment, and `ClusterServerSession::open_explicit` taking the TSO at `BEGIN` is Go's shape. The only reuse is `canReuseTxnWhenExplicitBegin` (`base.go:661`): a `BEGIN` that is the first statement of an `autocommit=0` transaction keeps the transaction already activated. The doc comment of `tidb-server/src/session_transaction.rs` ("opened lazily, on the first statement after `BEGIN` ... `BEGIN; COMMIT;` spends no PD timestamp") states the opposite and is false.
2. The lazy opener is `EnterNewTxnBeforeStmt` (`base.go:115`: `activeNow = false`), entered by `session.PrepareTxnCtx` (`session.go:4908`) for any statement that finds no valid or pending transaction (autocommit, and `autocommit=0` outside a transaction). The transaction is then pending (`LazyTxn.pending()`, `txn.go:230`) and is activated by the first `GetStmtReadTS` / `GetStmtForUpdateTS` / `Txn(true)` / `ActivateTxn` caller: table readers, point gets, DML builders, `SAVEPOINT`, `NewTxnInStmt` statements (DDL, `GRANT`, `REVOKE`). Live, with `autocommit=0`: `select 1`, `set @a=1`, `show tables`, `show databases`, `explain select 1`, `select now()`, a `TableDual` read and an `information_schema` read all leave `@@tidb_current_ts` at `0`. `ActivePendingTxn` does not exist in this tree; `ActivateTxn` on the provider replaced it.
3. `tidb_disable_txn_auto_retry` can only be ON. Its `Validation` (`pkg/sessionctx/variable/sysvar.go:2595`) warns on `OFF` and returns `ON`. `doCommitWithRetry` (`session.go:806`) retries only a retryable error with `TxnCtx.CouldRetry`, `RetryLimit > 0`, a non-pessimistic, non-pipelined, non-BatchInsert transaction (`session.go:874`), and `CouldRetry` is the autocommit statement case. The refusal of `OFF` is therefore Go's behavior (`disable_txn_auto_retry_off_warns_and_stays_on` pins it), and the explicit-transaction replay loop `session.retry` (`session.go:1162`) is unreachable for a client transaction. What remains duplicated is the autocommit-statement retry (`may_retry_autocommit_statement` in the node, `StatementFinish::Retry` in the session).
4. `setLastTxnInfoBeforeTxnEnd` (`session.go:1056`) runs at the top of both `CommitTxn` (`session.go:982`) and `RollbackTxn` (`session.go:1022`) and records the start-only struct only when `TxnCtx.StartTS != 0`; `kv.CommitHook` overwrites it after a commit that really ran. Live on the oracle: `USE`, `SET @x`, `PREPARE`, `SET GLOBAL` and `select 1` leave the record alone; a DML matching no row, a table read, `BEGIN; ROLLBACK` and `BEGIN; COMMIT` leave start-only; DDL leaves start-only with a fresh TSO (`NewTxnInStmt`, `executor/ddl.go:151`).

Transaction objects in the tree, and the Go counterpart of each:

| Rust | What it is | Go counterpart |
| --- | --- | --- |
| `tidb-session/src/txn.rs` `Transaction` (`open`, `begin_implicit_transaction`, `open_transaction`, `commit`, `control_transaction_stmt`) | the in-process catalog tier's transaction: a working catalog copy over a mutex'd catalog, counter TSOs | `LazyTxn` (`pkg/session/txn.go`) over `tikvTxn`; only the backend differs |
| `tidb-server/src/cluster_session_node/mod.rs` `explicit: Option<Box<dyn OpenClusterTransaction>>` with `open_explicit`, `commit_explicit`, `discard_explicit`, `begin_if_autocommit_off` | the cluster node's store transaction, opened at `BEGIN` or at the first statement of an `autocommit=0` transaction | `LazyTxn` + `txnManager` provider (`txnmanager.go`, `isolation/*.go`) |
| `tidb-session` `begin_transaction_statement`, `finish_transaction_statement`, `acquire_pessimistic_locks`, `TransactionStatement`, `StatementFinish::Retry` | the in-process statement loop: lock step and rerun at a newer view | `ExecStmt.handlePessimisticDML` (`executor/adapter.go:1167`), `handlePessimisticLockError` (`:1399`), `runPessimisticSelectForUpdate` (`:1108`) |
| node `attempt_statement_inner`, `lock_pessimistic_statement_keys`, `PessimisticStep`, `StatementLockFailure`, `with_bound_statement` replay budget, `may_retry_autocommit_statement` | the same loop over the store transaction, plus the autocommit retry | the same three functions, and `doCommitWithRetry` (`session.go:806`) |
| node `record_set.rs` `begin_read_statement` / `end_read_statement` | a third statement begin/finish, for streamed reads | `ExecuteStmt` (`session.go:2419`) -> `runStmt` -> `finishStmt` |
| `txn.rs` `set_savepoint`, `rollback_to_savepoint`, `release_savepoint` (catalog images) and node `apply_savepoint` (`BufferCheckpoint`s) | savepoint stacks, one per tier | `executeSavepoint` / `executeReleaseSavepoint` and `TxnCtx.Savepoints` (`simple.go:662-686`) |
| node `control_transaction_parsed_inner` (implicit commit at `BEGIN`, refused `READ ONLY`, `reset_transaction_scope`) and the session's `control_transaction_stmt` | `BEGIN` / `COMMIT` / `ROLLBACK` handled in both, in step by call order | `executeBegin` / `executeCommit` / `executeRollback` (`simple.go`), `CommitBeforeEnterNewTxn` (`isolation/internal`) |
| `txn.rs` `commit` fake commit and `dispatch.rs` autocommit block (counter TSOs, `record_commit`, `last_txn_info`) | the catalog tier's autocommit bookkeeping, which also ran on the cluster | `finishStmt` -> `autoCommitAfterStmt` -> `CommitTxn` |
| `tidb-server/src/session_transaction.rs` `SessionTransaction` | a lazy-begin state machine with no production caller (`transaction_options` in the same file is the live part) | none; its premise is false (fact 1) |
| `tidb-exec` `multi_statement_transaction.rs` `MultiStatementTransaction`, `cluster_table_storage::SessionTransaction` | transactions of the non-deployed real-TiKV DML path, the DDL/ANALYZE/LOAD STATS helpers and one server test | `session.NewTxn` of an internal session (`ExecRestrictedSQL`) |

Observable divergences found by probing the Go oracle against this node (`@@tidb_current_ts`, `@@tidb_last_txn_info`, `information_schema.tidb_trx`):

- A statement that runs no transaction (`USE`, `SET @x`, `PREPARE`, a DML matching no row) wrote a two-phase-commit `@@tidb_last_txn_info` at catalog-counter timestamps, because the catalog tier's autocommit bookkeeping ran on the cluster node too (fact 4). Fixed by Stage T1.
- Under `autocommit=0`, every statement (`select 1`, `show tables`, `set @a=1`) opened the store transaction and spent a TSO, and a read after another session's commit saw the old data when the first statement had been a `select 1` (fact 2). Open: Stage T2.
- `information_schema.tidb_trx` reports `Running` for the session's own row after `BEGIN` where Go reports `Idle` (the statement reading the table is not yet counted when Go samples). Open, not yet traced.
- `select @@tidb_current_ts` returns a number where Go returns a string (`0` is left-aligned in the Go client output). Open, outside this milestone.
- DDL, `GRANT`/`REVOKE` and the other `NewTxnInStmt` statements take no session transaction on this node, so they leave `@@tidb_current_ts` and `@@tidb_last_txn_info` untouched where Go records a fresh start-only entry. Open.

Stages, in dependency order. Each is one gated commit; none is accepted unless the gate in "Validation and acceptance" holds.

- [x] T0: this audit and the two corrected beliefs.
- [x] T1: the transaction that ends records `@@tidb_last_txn_info` (Go `setLastTxnInfoBeforeTxnEnd` plus `CommitHook`), once, in the node, for autocommit reads and writes (`attempt_statement_inner`, `end_read_statement`) and explicit `COMMIT`/`ROLLBACK` (`commit_explicit`, `discard_explicit`); the session's catalog-counter record no longer runs when a server owns the statement transactions (`Session::statement_txns_are_external`).
- [ ] T2: the `autocommit=0` opener becomes Go's pending transaction: the node opens the store transaction at the first activating read, lock or write and not at statement entry.
- [ ] T3: delete the dead `SessionTransaction` and fix its false documentation.
- [ ] T4: one statement lifecycle in the node (`attempt_statement_inner` and `begin_read_statement` / `end_read_statement` share `finishStmt`).
- [ ] T5: one savepoint stack, in `tidb-session`, over a checkpoint trait the catalog tier and the store buffer implement.
- [ ] T6: a store-transaction trait in `tidb-session` (Go `kv.Transaction` as `LazyTxn` sees it); the catalog `Transaction` and the node's `OpenClusterTransaction` become its two backends, and `begin_transaction_statement` / `attempt_statement_inner` collapse into one `handlePessimisticDML`.

## Milestone 4 progress: EXPLAIN ANALYZE execution info

Go reads every `EXPLAIN ANALYZE` cell from the statement's `RuntimeStatsColl` (`getRuntimeInfoStr`: `RootRuntimeStats.String()` plus, for pushed operators, `CopRuntimeStats.String()`, with the row count of whichever exists). This tier had `RuntimeStatsColl`, `BasicRuntimeStats`, `RootRuntimeStats`, `CopRuntimeStats`, `RuntimeStatsWithCommit` and the percentile types ported byte for byte in `tidb-exec/src/runtime_stats.rs`, but `EXPLAIN ANALYZE` rendered from an ad-hoc map (`PhysicalRuntimeCounter` in `tidb-executor/src/driver/physical_builder.rs`) and nothing fed the collection's cop side.

Audit of 2026-10-21 (Go oracle on the shared cluster against this tier on the same cluster, same tables `ea_stats.t1`/`t2`, 12 rows, `ib`/`ic` secondary indexes; timings differ, so the audit compares keys, formats, which operators have which stats and counts). Differences found, in the order of the chain that Go builds them:

1. Every pushed (`cop[tikv]`) operator printed `N/A` execution info, and its `actRows` was whatever the reader's scan counter held: the scan under a Selection showed the Selection's rows (4 for 12 scanned), a `TableRowIDScan` and an `IndexLookUp`'s probe side `N/A`, an `IndexFullScan` under a pushed `StreamAgg` `0`. Go: `tikv_task:{time:.., loops:..}` per operator (or `{proc max:.., min:.., avg: .., p80:.., p95:.., iters:.., tasks:..}` with several tasks), the root pushed operator of each request additionally `scan_detail: {total_process_keys, total_process_keys_size, total_keys, get_snapshot_time, rocksdb: {delete_skipped_count, key_skipped_count, block: {...}}}` and `time_detail: {total_process_time, total_wait_time, total_kv_read_wall_time, tikv_grpc_process_time, tikv_grpc_wait_time, tikv_wall_time}`; `actRows` is the operator's own `num_produced_rows`.
2. Readers (`TableReader`, `IndexReader`, the index side and table side of `IndexLookUp`, each partial and the table side of `IndexMerge`, an index join's inner readers) lack the select result's stats, `cop_task: {num, max, [min, avg, p95,] [max_proc_keys, p95_proc_keys,] proc_keys, tot_proc, tot_wait, copr_cache_hit_ratio | copr_cache: disabled, build_task_duration, [limiter_wait,] max_distsql_concurrency, [max_extra_concurrency]}, fetch_resp_duration: .., rpc_info:{Cop:{num_rpc:.., total_time:..}}[, backoff{..}]` (`selectResultRuntimeStats`).
3. The pushed roots of an `IndexLookUp`/`IndexMerge` carry basic stats in Go (`time:.., open:.., close:.., loops:..`, or `total_time:.., total_open:.., total_close:.., loops:..` for the table side) in front of the select result's and the cop stats; here they have none.
4. `IndexLookUp` (`index_task: {total_time, fetch_handle, build, wait}, table_task: {total_time, num, concurrency}, next: {wait_index, wait_table_lookup_build, wait_table_lookup_resp}`), `IndexMerge` (`index_task:{fetch_handle, merge}, table_task:{num, concurrency, fetch_row, wait_time}`), `HashJoin` (`build_hash_table:{concurrency, time, fetch, max_partition, total_partition, max_build, total_build}, probe:{concurrency, time, fetch_and_wait, max_worker_time, total_worker_time, max_probe, total_probe}`), `IndexJoin` (`inner:{total, concurrency, task, construct, fetch, build}, probe:..`), `Projection` (`Concurrency:OFF|N`), and the `HashAgg` stats are absent; `MergeJoin`, `Sort`, `TopN`, `StreamAgg`, `Selection`, `Limit` and the point readers match in shape.
5. The `memory` and `disk` cells are `N/A` for every operator (Go: the operator's tracker `MaxConsumed`, `0 Bytes` for a `Sort`'s disk, `N/A` only where no tracker exists).
6. DML: `Insert_N` printed only `RU:`; Go prints `time:.., open:.., close:.., loops:.., prepare: .., insert:.., RU:..` (and the `Insert:{prepare, check_insert:{total_time, mem_insert_time, prefetch, rpc:{BatchGet:{num_rpc, total_time}}}}` blocks when the statement reads or checks keys, `commit_txn: {prewrite, get_commit_ts, commit, region_num, write_keys, write_byte, txn_retry}` when it commits inside the statement); `Update_N` and `Delete_N` printed only `RU:` where Go prints `time:.., open:.., close:.., loops:..`. Their `SelectLock`/`UnionScan` children match in shape.
6a. `RU:` for DML printed `0.00` for an INSERT (Go `3.56`/`1.70`).
7. The in-process unistore returns no execution summaries (`collect_execution_summaries` is honoured only by a real TiKV), so the same statement on `Session::new()` still shows `N/A` for every pushed operator.

Done: the pushed-operator chain (difference 1) in `tidb-executor`, `tidb-exec` and `tidb-session`:

- `PushdownStatementContext` carries Go's `copPlanIDs` source (`cop_plan_chain`, the reader's pushed plan nodes leaf first, each with the executor kind it lowers to) and `rootPlanID` (`cop_root_plan_id`); `build_reader`, the index-merge partials, an index lookup's two sides (`lookup_cop_plan_chain`, taken by `for_lookup_batch`) and an index join's inner readers set them from the physical tree. `cop_scan.rs::cop_plan_ids_for` gives each DAG executor the next pushed node of its own kind (this tier builds the DAG from a scan request, so a node whose conditions stayed local has no executor), where Go zips the two lists.
- `RuntimeStatsCollSink` (`tidb-exec/src/statement_cop_stats.rs`) is `selectResult.updateCopRuntimeStats` for TiKV: after the statement's exec-details consumer, the response's summaries go positionally to `RecordOneCopTask` and, for the last (root) executor, `RecordCopStats` with the response's scan and time details; a summary list that does not line up records nothing, an empty one records the root's evidence only. `statement_context_ignoring` creates one `RuntimeStatsColl` per statement and wraps the sink around it; `EXPLAIN ANALYZE` creates one when `tidb_enable_collect_execution_info` is OFF, as `executeAnalyzeExec` does.
- `ExecutorRuntimeStatsSource::cop_runtime_info` (Go `GetCopStats(explainID)`) is what `explain.rs` renders for a pushed operator: `actRows` from `CopRuntimeStats.GetActRows`, execution info from `CopRuntimeStats.String`.

Live check: the pushed-operator cells of the `ea_stats` set match Go's keys and formats for a point/table/index reader, Selection, TopN, StreamAgg, IndexLookUp's two sides, IndexMerge's partials and probe (two tasks: the `proc max` form) and the inner readers of an index join. A pushed operator this tier does not push (a TopN over a string key stays a local executor) has no executor in the DAG, so it reports nothing where Go reports its summary. Open: differences 2 to 7.

## Validation and acceptance

Every batch ends with the gate (the main-crate nextest run, the in-process replay with its plan-text budget, the extra-crate nextest run and the production replay with its divergence budget), followed by the locked `tidb-server` build. A batch is accepted when no replay reports a divergence outside its stale list, the `OutOfDomain` budget has not grown, and the new behavior is pinned by a test that fails before the change. A drop in the compared-statement count means statements moved into a skipped class and must be explained.

## Idempotence and recovery

Each batch lands as its own commit through the pre-commit hook, so any one can be reverted alone. A structural change that makes a replay worse is reverted only if the worsening cannot be explained as emulation code the change makes redundant.

## Progress

- [x] Milestone 0: the replay compares error codes by default, keeps an exact stale-recording list and asserts an `OutOfDomain` budget.
- [x] Milestone 1: privileges come from the plan builder's visit info, for reads and writes.
- [x] Milestone 3a: the session owns the orchestration of DDL, accounts, `SET GLOBAL`, ANALYZE and the statistics statements behind seams.
- [x] The production replay is in the gate.
- [x] The 2026-10-18 review findings are worked off, except the transactions, caches, execution, duplicates and bootstrap items listed above.
- [x] Every cluster schema change Go runs as a job is Go's persisted job (columns, indexes, tables, schemas, views, sequences, placement, resource groups, LOCK TABLES, every partition form, multi-schema changes), and the one-transaction DDL path is deleted (Batches 3 to 10, last `123c321787`).
- [x] Milestone 2: CREATE VIEW, DESCRIBE / `information_schema.columns` of a view and the EXPLAIN view privileges run through the planner's Go-shaped paths; the executor canonicaliser (`view.rs` 1246 to 367 lines) and the catalog-copy `renaming_view` fallback are deleted. The virtual-table and MDL walks of view bodies are open (see the plan of work).
- [ ] Milestone 3b to 3e.
- [ ] Milestone 4.
- [ ] Milestone 5.
- [x] Caches: the sysvar cache is the GLOBAL tier on both tiers, and `information_schema` reads the statement's pinned schema.
- [ ] Transactions, execution, duplicates, bootstrap layout, and the DDL details above.
- [x] The client stack exists once. TiDB's transactions, point reads and scans, coprocessor, ANALYZE, MPP and peer RPCs, timestamps and GC run on the node's `ngaut/client-rust` store (the Rust client-go, vendored at `third_party/tikv-client-rs`), which also backs the embedded unistore; `tidb-txnkv`'s duplicate region routing, transport, lock resolution and `client_bridge` are deleted (`client-rust-unistore-execplan.md` Step 6, 2026-10-20). TRACE spans, the statement's `ExecDetails`/RU details and the client's log lines come from client-rust itself.
- [x] The GC worker runs on the TiKV node (`gcworker-execplan.md`); its store-backed tests run on the embedded store through client-rust (`client-rust-unistore-execplan.md`).

## Surprises and discoveries

A harness change once hid 56 statements in `OutOfDomain` behind a definer host-pattern bug; only the compared-count drop showed it, which is why the budget assertion exists.

The visit-info pass plans a statement a second time, and planning has effects: it raises warnings (truncated afterwards), constant folding evaluates builtins (suspended for the pass), and an uncorrelated subquery runs at plan time (answered with a row of NULLs in the pass). Double planning is the temporary price of Milestone 1; the end state plans once and checks afterwards.

A view body's stored text is the planner's AST after `buildSelect`, and this planner never mutates its AST, so the CREATE VIEW build records Go's writes keyed by `SelectStmt.QueryBlockOffset` (now an AST field, written by `QBHintHandler` as Go does). The key survives the clones the builder makes (a recursive CTE's terms, a HAVING scratch copy); keying by node address did not. The correlated-aggregate resolver's `#corragg#` markers are put back before recording, since Go keeps the aggregate in the AST.

Two Go details only the live oracle showed: `ColumnName.Restore` drops the schema of a column whose table is a CTE in scope (the old canonicaliser built CTE columns without one; the planner gives them the current database), and `StmtCtx.ViewDepth` counts every view a statement expands rather than the nesting depth, so under EXPLAIN the second view of a join needs SELECT too.

`tryFillViewColumnType` runs in Go's system session, whose SQL mode carries no ONLY_FULL_GROUP_BY; the Rust build uses the statement's context, so a view whose body is a no-GROUP-BY aggregate over a plain column (`select a from t having sum(a) > 0`) is still left out of `information_schema.columns` and refused by DESCRIBE where Go lists it.

## Decision log

Decided 2026-10-17 by the owner ("follow Go version implementation"): one DDL implementation, as Go has. Go's `ddl` executor builds a job, its worker applies it, and every session, a unit test's included, runs on a store. The Rust target is the cluster tier, reached by every session through one seam, over a store a bare `Session` also owns.

Decided 2026-10-18 (owner: "you make the call"): the production engine is the measure. The production replay is in the gate, and Milestone 3 here and Milestone 4 of `one-engine-corpus-execplan.md` are one program: one session over one store. The DDL job model came first (Batches 3 to 10); the GC worker follows it.

Structure first, symptoms later (user direction, 2026-10-17). Harness visibility comes before every refactor; the stale-recording list is exact rather than a tolerance.

## Outcomes and retrospective

Milestone 2 batch: 8990 tests across the touched crates green (one load flake, `unistore_cop::analyze_preserves_generated_execution_error`, green alone), 5 plan-text divergences in the in-process replay, production replay 8 diverged of 153 topics with none out of domain; SHOW CREATE VIEW, `information_schema.views` and view reads identical to the Go oracle across about 200 probe statements, except pre-existing parser and planner gaps.

Gate g275 (`123c321787`): 10379 + 2833 tests green, 5 plan-text divergences in the in-process replay, production replay 8 diverged of 153 topics. Milestones 2, 3b-3e, 4 and 5 are open.
