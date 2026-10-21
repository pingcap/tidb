# Align the Rust node's structure with Go TiDB, in dependency order

This ExecPlan is a living document. Keep Progress, Surprises and discoveries, Decision log and Outcomes current at every stopping point. It records the current state only; superseded text is removed, and git history keeps it.

## Purpose and outcome

The Rust node must answer every statement the way Go TiDB (the oracle, run from a scratch worktree of this branch) does. Fixing one wrong answer at a time produces emulations of structural differences, where Rust decides something at a different time, in a different place, or twice. So the structure is corrected first, in dependency order, accepting temporary regressions, and no emulation is added for anything a structural item covers (user directive, 2026-10-17).

After this plan, privileges are decided from the plan, a view body is built by one code path, every session runs one DDL implementation over one store (as Go's `testkit` runs every test), and the statement paths that exist twice (in-process session and cluster node) share one definition per statement kind. Observe it through the in-process replay (`cargo test -p difftest-result-tests --test integration_diff integrationtest_replay_matches_recorded_tidb_output`), the production replay (`cargo test -p tidb-server --lib production_replay_stays_within_budget -- --ignored`) and the Go oracle probes (`difftool.sh` in the session scratchpad).

## Context for a reader new to the repository

`rust/crates/tidb-session` holds `Session`, the statement dispatcher (`dispatch.rs`), privilege collection and the session-side seams (`ddl_service`, `analyze_service`, `stats_statement_service`, the store-statement services). `rust/crates/tidb-executor` holds the volcano executors, the in-memory `Catalog` (`driver/catalog.rs`), the DML drivers and the in-process DDL (`ddl/`). `rust/crates/tidb-exec` holds the cluster tier: `cluster_ddl.rs` and `cluster_ddl/` lower DDL statements, admit them as Go's executor does and step persisted jobs as Go's `ddl` worker does; `real_tikv_ddl.rs` submits and runs them against the store; `table_info_build.rs` builds `TableInfo` as Go's `ddl` package does. `rust/crates/tidb-planner` is the port of Go's planner. `rust/crates/tidb-server/src/cluster_session_node` is the production session: it wraps `tidb_session::Session` and routes DDL and writes to the cluster tier. A "visit info" is Go's list of (database, table, privilege) entries the plan builder appends while planning and `CheckPrivilege` verifies afterwards.

## The open structural mismatches

DDL exists twice. Every cluster schema change Go runs as a job is a persisted job in `mysql.tidb_ddl_job`, stepped by the owner through Go's schema states; the in-process executor DDL (`tidb-executor/src/ddl`, about 23,000 lines) still runs its own one-step implementation, and a bare `Session::new()` reaches only that one. Milestone 3 removes it.

Transactions exist twice. Production runs two transaction objects at once (the session's `Transaction` and the node's explicit store transaction, kept in step by call order); there is no isolation-provider seam; `handlePessimisticDML`, the retry loop, savepoints, implicit commit and the `autocommit=0` lazy opener are each implemented twice; `tidb_disable_txn_auto_retry=OFF` retries are refused on the cluster. Target: one TxnManager in `tidb-session` over a store-transaction trait, the in-process catalog one backend of it.

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

The transactions and bootstrap mismatches are fixed in their own batches as they are reached; the GC worker follows its own plan.

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
