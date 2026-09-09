// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Documentary ports for `pkg/planner/core/tests/prepare/` — items 1144–1169
//! of `pkg/planner.part20` (all 1278 `Test*`/`Benchmark*` declarations under
//! `pkg/planner/` on `origin/master`, sorted by file then line, chunked by 60).
//!
//! `prepare/main_test.go:25 TestMain` (item 1144) is bootstrap-only
//! (`testsetup.SetupForCommonTest` + the goleak ignore list) and is recorded
//! as skipped-reason in the batch receipt — no behavior to assert — following
//! crate precedent for bootstrap-only families.
//!
//! Every remaining test in `prepare_test.go` drives prepared statements
//! through `testkit` over a mock store: `prepare`/`execute` round-trips,
//! `Session.PrepareStmt`/`ExecutePreparedStmt` binary protocol, the
//! `metrics.PlanCacheCounter`/`PreparedStmtGauge` prometheus surfaces,
//! `@@last_plan_from_cache`, `SessionVars.FoundInPlanCache`,
//! `StmtCtx.IndexNames`/`ReadFromTableCache`, `admin flush plan_cache`,
//! and `explain for connection`. The crate has no session, executor, plan
//! cache, or metrics surface (the boundary recorded by receipts b083/b084/
//! b092/b095/b096 for neighbouring planner parts), so all 24 tests are
//! documentary `#[ignore]` ports. Each doc comment re-derives the pinned
//! contract from the master source; nothing is approximated into passing.

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:49
/// TestPointGetPreparedPlan4PlanCache`.
///
/// Contract: on `ps_text.t (a int, b int, c int, primary key k_a(a),
/// unique key k_b(b))` seeded 1/2/3, prepare `select * from t where a = ?`,
/// then force `PlanCacheStmt.StmtCacheable = false` (:67) before the first
/// `ExecutePreparedStmt` with param 0 and a second execute with a nil param
/// (:69-75). With the statement marked uncacheable every execution must
/// generate the plan afresh, so the nil param cannot be served from the plan
/// built for `a = 0` (the regression this pins).
///
/// go-parity-gap: PrepareStmt/ExecutePreparedStmt over a live session plus
/// the PlanCacheStmt.StmtCacheable flag are unported.
#[test]
#[ignore = "go-parity-gap: prepared execution + PlanCacheStmt.StmtCacheable need the session stack"]
fn point_get_prepared_plan4plan_cache_uncacheable_flag_forces_replan() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:79
/// TestRandomFlushPlanCache`.
///
/// Contract: two sessions each prepare the same five statements (the t1×t2
/// join plus plain and point selects on t1/t2). After a warm-up phase where
/// every statement must report `@@last_plan_from_cache = 1` (:114-121), ten
/// rounds of random `admin flush instance plan_cache` / `admin flush session
/// plan_cache` (picked by `rand.Intn(10)`: 0 → instance scope issued on
/// either session, 1 → tk's session, 2 → tk2's session, :124-143) must make
/// the flushed scope(s) miss (`last_plan_from_cache = 0`, :147-150; an
/// instance flush resets BOTH sessions while a session flush leaves the
/// other session's cache warm), then hit again after the refilling execute.
/// The closing `admin flush global plan_cache` must fail with exactly
/// "Do not support the 'admin flush global scope.'" (:158-159).
///
/// go-parity-gap: admin flush plan_cache scopes and per-session plan caches
/// are unported.
#[test]
#[ignore = "go-parity-gap: admin flush plan_cache + per-session caches need session/executor machinery"]
fn random_flush_plan_cache_scope_semantics() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:162
/// TestPrepareCache`.
///
/// Contract: on `t(a int primary key, b int, c int, index idx1(b, a),
/// index idx2(b))` seeded with (1,1,1)..(6,1,2): `execute stmt1 using @a,@b`
/// (use index idx1, `a=? and b=?`) returns `1 1 1`; `stmt2` (idx2, `b=?`)
/// returns `1 1` and `6 1`; `stmt3` (`a=?`) returns `1 1 1`; `stmt4`
/// (`a > ?` with @a=3) returns `4 4 4`,`5 5 5`,`6 1 2`; `stmt5`
/// (`select c from t order by c`) returns `1 2 2 3 4 5`; `stmt6`
/// (`distinct a order by a`) returns 1..6 — each executed twice so the
/// second run exercises the cache (:173-199). The privilege arm: user
/// `u_tp@localhost` with SELECT on `test.tp` executes prepared
/// `ps_stp_r` (`select * from tp where c1 > ?`) three times hitting the
/// cache; after root `revoke all on test.tp` the next execute must ERROR;
/// after re-grant it hits again (:196-231).
///
/// go-parity-gap: prepare/execute protocol, use-index plans, and privilege
/// re-validation on cached plans are all session/executor surfaces.
#[test]
#[ignore = "go-parity-gap: prepared execution with index hints and grant/revoke cache invalidation unported"]
fn prepare_cache_hits_across_index_and_privilege_changes() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:318
/// TestPrepareCacheChangingParamType`.
///
/// Contract: for the five typed tables t_tinyint, t_unsigned, t_float,
/// t_decimal(10,2), t_year (each with ten random valid rows plus null
/// variants, :335-341), repeatedly prepare the seven parameter shapes
/// `a=?`, `b=?`, `a in (?,?,?)`, `b in (?,?,?)`, `a>?`, `b>?`, `a>? and b>?`
/// (:347-354) and execute them with random parameter values drawn from the
/// null/valid/out-of-range/invalid/str generators (`randValue` :238-316).
/// Every prepared-execution result must equal the corresponding literal SQL
/// (`compareResult` :349-357 sorts both sides; errors must match errors).
///
/// go-parity-gap: randomized execution over typed columns with
/// prepared/literal result comparison needs the executor.
#[test]
#[ignore = "go-parity-gap: randomized prepared-vs-literal result equality needs the executor stack"]
fn prepare_cache_changing_param_type_matches_literal_sql() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:376
/// TestPrepareCacheDeferredFunction`.
///
/// Contract: `sel1` = `select id, c1 from t1 where c1 < now(3)` on
/// `t1(id int PRIMARY KEY, c1 TIMESTAMP(3) ...)` is built twice through
/// `core.NewPlanBuilder().Init` + `builder.Build` + `core.GetPlanFromPlanCache`
/// (:400-413). Each cached plan string must match the IndexReader regexp
/// `IndexReader\(Index\(t1.idx1\)\[\[-inf,<timestamp-millis>\)\)\]` (:387),
/// the `metrics.PlanCacheCounter{label="prepare"}` counter must read 0 then 1
/// (:415-417), and `planStr[0] < planStr[1]` lexically (:420) — now(3) is a
/// deferred function, so the cached plan is REBUILT per execution with a
/// fresh timestamp bound that only grows.
///
/// go-parity-gap: PlanBuilder.Build + GetPlanFromPlanCache + core.ToString +
/// metrics counters are session-level machinery.
#[test]
#[ignore = "go-parity-gap: plan-cache rebuild of deferred now(3) needs builder+cache+metrics surfaces"]
fn prepare_cache_deferred_now_rebuilds_index_range_per_execution() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:423
/// TestPrepareCacheNow`.
///
/// Contract: prepared `select now(), current_timestamp(), utc_timestamp(),
/// unix_timestamp(), sleep(0.1), now(), ...` — on the SECOND (cached)
/// execution, columns 5..8 must equal columns 0..3 respectively (:433-436):
/// now-family functions are statement-time constants, stable across the
/// sleep(0.1) inside one execution. Second arm with plan cache disabled:
/// `select sleep(a), now(6), sysdate(6), sysdate(6)=now(6) from t` changes
/// its result set when `tidb_sysdate_is_now` flips from 0 to 1 (:441-447) —
/// sysdate is NOT a statement constant unless the variable says so.
///
/// go-parity-gap: statement-time constant freezing and sysdate semantics
/// live in the executor/expression runtime.
#[test]
#[ignore = "go-parity-gap: now()/sysdate() statement-constant semantics need the executor runtime"]
fn prepare_cache_now_statement_constant_vs_sysdate() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:449
/// TestPrepareOverMaxPreparedStmtCount`.
///
/// Contract: `metrics.PreparedStmtGauge` goes +1 on `prepare stmt1` and back
/// on `deallocate prepare stmt1` (:455-461); `@@max_prepared_stmt_count`
/// reads -1 by default and honors `set @@global.max_prepared_stmt_count = 2`
/// (:465-467); closing the session releases its prepared statement
/// (gauge -1, :470-473); preparing statements until the gauge reaches the
/// limit must make the next prepare fail with
/// `errno.ErrMaxPreparedStmtCountReached` (:477-484).
///
/// go-parity-gap: PreparedStmtGauge metrics and the global limit enforced in
/// the session layer are unported.
#[test]
#[ignore = "go-parity-gap: prepared-stmt gauge + global max_prepared_stmt_count live in the session layer"]
fn prepare_over_max_prepared_stmt_count_gauge_and_limit() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:502
/// TestPrepareWithSnapshot`.
///
/// Contract: after recording a transaction's `@@tidb_current_ts` (:516-518),
/// updating the row to v=3, preparing `s1`/`s2` and setting
/// `@@tidb_snapshot = <ts>` (:523), executing the prepared plans must
/// read the SNAPSHOT: `1 2`, not the newer v=3 (:524-525) — the snapshot
/// flag set between executions must be honored by prepared plans.
///
/// go-parity-gap: tidb_snapshot reads need transaction/storage plumbing.
#[test]
#[ignore = "go-parity-gap: prepared execution under tidb_snapshot needs txn/storage plumbing"]
fn prepare_with_snapshot_reads_recorded_ts() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:528
/// TestPrepareCacheForPartition`.
///
/// Contract: for each `@@tidb_partition_prune_mode` in {static, dynamic},
/// `planCacheUsed` is "0"/"1" (:537-539). Over hash-partitioned
/// t_index_read (PK (id,k), `partition by hash(id+k) partitions 10`) and
/// t_table_read (PK id), prepared point selects return abc/xyz per param,
/// first execute `last_plan_from_cache=0` (:549-558). Over range-
/// partitioned t_range_index/t_range_table (p0<4, p1<14, p2<20), analyzed,
/// each new param value (1/5/13/17) hits the cache ONLY in dynamic mode
/// (:583-596, :607-623). Over list (`id*2-id`) and list-columns (id)
/// partitioned t_list_index, params 1/5/9/12/100 behave the same, with
/// @id=100 returning EMPTY rows while still hitting the cache in dynamic
/// mode (:646-651, :673-678).
///
/// go-parity-gap: partition pruning per execution mode inside the prepared
/// plan cache needs the full pruning + cache stack.
#[test]
#[ignore = "go-parity-gap: static/dynamic partition pruning interplay with the plan cache is unported"]
fn prepare_cache_for_partition_static_vs_dynamic_prune() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:680
/// TestIssue33031`.
///
/// Contract: `Issue33031(COL1 int, COL2 bigint, UNIQUE KEY UK_COL1(COL1))
/// PARTITION BY RANGE (COL1) (P0 VALUES LESS THAN (0))` with row (-5,7).
/// Static mode: `execute stmt using @d,@a,@b,@c` on
/// `select *,? from Issue33031 where col2 < ? and col1 in (?, ?)` never sets
/// `FoundInPlanCache` (:692-699). Dynamic mode (after analyze): the
/// non-matching param set misses, the matching set `@a=112,@b=-2,@c=-5`
/// returns `-5 7 33` AND hits the cache (:700-712); `explain for connection`
/// shows `IndexLookUp` (:715-717). After
/// `alter table Issue33031 remove partitioning` (:718), execution returns the
/// warning "Warning 1105 skip prepared plan-cache: Batch/PointGet plans may
/// be over-optimized" twice, `FoundInPlanCache` stays false, and explain
/// shows `Batch_Point_Get` (:719-727).
///
/// go-parity-gap: FoundInPlanCache + skip-cache warnings + explain-for-
/// connection are session/executor surfaces.
#[test]
#[ignore = "go-parity-gap: partition point/batch-get cache-skipping warnings need the session stack"]
fn issue_33031_partition_pointget_skips_cache_with_overoptimize_warning() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:742
/// TestPlanCacheUnionScan`.
///
/// Contract (counter-driven, `metrics.PlanCacheCounter{label="prepare"}` with
/// the resettable test hook :747-751): stmt1 `select * from t1 where a > ?`:
/// before `begin` the plan is cached outside txn context; inside a txn the
/// first two executes CANNOT reuse it (counter stays 0, :764-766); after
/// `insert into t1 values(1)` the cached plan is invalid, not chosen, and
/// removed (counter 1, :770-778); an insert into the UNRELATED t2 keeps t1's
/// cached plan chosen (counter 2, :779-787); after `rollback` the
/// union-scan-bearing cached plan IS reused for correctness (counter 3,
/// :788-794). stmt2 (left join on true) repeats the matrix with the
/// `<nil>`→`1 1` join results (:796-838); stmt3 (`select 1 from t3 where
/// a = null`) repeats it keeping the result empty through insert/rollback
/// (:840-870).
///
/// go-parity-gap: union-scan plan invalidation inside transactions plus
/// prometheus counters need the executor.
#[test]
#[ignore = "go-parity-gap: txn-scoped union-scan invalidation with cache counters is unported"]
fn plan_cache_union_scan_invalidation_and_reuse_matrix() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:872
/// TestPlanCacheSwitchDB`.
///
/// Contract: preparing `select * from t` on a session with NO default
/// database errors `[planner:1046]No database selected`, while the explicit
/// `select * from test.t` prepares fine (:884-890) on an LRU plan cache
/// built via `core.NewLRUPlanCache(100, 0.1, math.MaxUint64, ...)`. After
/// `use plan_cache` (where a NEW t(a int) holds (1)), the OLD prepared
/// statement still reads test.t (-1) and hits the cache on the second
/// execute (:897-903); re-preparing the same SQL now binds plan_cache.t
/// (returns 1) (:905-912); re-preparing `select * from test.t` binds test.t
/// again (:913-918) — the plan cache key includes the resolved DB.
///
/// go-parity-gap: session default-DB resolution inside the cache key is a
/// session surface.
#[test]
#[ignore = "go-parity-gap: prepared-stmt DB binding in the plan cache key needs the session stack"]
fn plan_cache_switch_db_keeps_resolved_database() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:919
/// TestInvisibleIndexPrepare`.
///
/// Contract: on `t(a int, unique idx_a(a))`, two executes of prepared
/// `select a from t order by a` record `StmtCtx.IndexNames == ["t:idx_a"]`
/// (:931-934); after `alter table t alter index idx_a invisible` two more
/// executes leave `IndexNames` EMPTY (:936-939); after re-visible it is
/// `["t:idx_a"]` again (:941-945) — the cached plan must respect the index's
/// visibility flag at (re)build time.
///
/// go-parity-gap: StmtCtx.IndexNames and index-visibility replanning need
/// the executor.
#[test]
#[ignore = "go-parity-gap: invisible-index replanning and StmtCtx.IndexNames are executor surfaces"]
fn invisible_index_prepare_updates_stmtctx_index_names() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:946
/// TestPlanCacheSnapshot`.
///
/// Contract: `t(id int)` with rows 1..4, prepared `select * from t where
/// id=?` warms the cache (miss, hit; :963-969). A recorded start-ts
/// (:972-975) becomes `@@tidb_snapshot`; after inserting another id=1 row,
/// executing the cached plan under the snapshot must STILL return only `1`
/// (the old snapshot) and hit the cache (:977-982).
///
/// go-parity-gap: prepared reads under tidb_snapshot need txn plumbing.
#[test]
#[ignore = "go-parity-gap: plan-cache reads under tidb_snapshot need txn/storage plumbing"]
fn plan_cache_snapshot_reuses_plan_against_old_ts() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:985
/// TestPartitionTable`.
///
/// Contract: dynamic prune mode over six t1/t2 scheme pairs — hash(int, 20
/// parts), range(int 0..100M in 5 parts), range columns(varchar), range
/// columns(datetime), range columns(date), list(int 0..19 in 4 parts) —
/// each seeded with 2048 identical random rows and analyzed (:1042-1095).
/// Prepared `select * from %v where a > ?` runs on t1 and t2: the first
/// execute misses (`@@last_plan_from_cache = 0` on BOTH, :1107-1110), then
/// for 100 random @a values the t1/t2 executions must return IDENTICAL
/// sorted rows (:1118, :1127-1128) and hit the cache (:1125, :1128); a run
/// may instead emit warnings, which `show warnings` must contain verbatim
/// as "skip plan-cache: plan rebuild failed, " (:1120-1123). A run may emit
/// at most 4 such warnings total (`require.Less(t, numWarns, 5)`, :1130).
/// Seeded by `time.Now().UnixNano()` (:995).
///
/// go-parity-gap: randomized cross-partition prepared consistency + cache-hit
/// ratio needs the executor.
#[test]
#[ignore = "go-parity-gap: randomized partition plan-cache consistency needs the executor stack"]
fn partition_table_plan_cache_randomized_dynamic_prune() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1148
/// TestPartitionWithVariedDataSources`.
///
/// Contract: dynamic mode; six tables — trangePK/thashPK/tnormalPK (int
/// primary key) and trangeIdx/thashIdx/tnormalIdx (int unique key) over the
/// same 1000 random distinct rows, analyzed (:1158-1195). For PK tables,
/// prepared tablescan (`a > ? and a < ?`), pointget (`a = ?`), batchget
/// (`a in (?,?,?)`) must return identical sorted results across the three
/// tables and hit the plan cache from loop 1 on (:1213-1259); same for the
/// unique-key tables' indexscan/indexlookup/pointget-on-idx/batchget-on-idx
/// (:1262-1314); note the pointget-miss tolerance guard at :1296 compares
/// `tbl == 'tnormalPK'` inside the trangeIdx/thashIdx/tnormalIdx loop, so it
/// can never fire as written (Go-side quirk, kept verbatim here). At most
/// `3 + 10*loops*3*4/100` cache misses are allowed overall (:1321-1323); one
/// loop intentionally exercises duplicate IN-list values (:1207-1212,
/// :1282-1284).
///
/// go-parity-gap: cross-table prepared result equality + tolerance-bounded
/// cache-hit accounting need the executor.
#[test]
#[ignore = "go-parity-gap: randomized prepared workload over partitioned/normal tables needs the executor"]
fn partition_varied_data_sources_plan_cache_hit_tolerance() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1326
/// TestCachedTable`.
///
/// Contract: `t(a int, b int, index i_b(b))` with (1,1),(2,2) is
/// `alter table t cache`d (:1336); within 50 plain reads the table cache
/// must load (`StmtCtx.ReadFromTableCache` true, :1346-1357). Four prepared
/// statements — tableScan (`a>=?`), indexScan (`select b use index(i_b)
/// where b>?`), indexLookup (`b>? and b<?`), pointGet (`b=?`) — are warmed
/// (:1359-1362), then re-executed: tableScan/indexScan/pointGet read FROM
/// the table cache AND hit the plan cache (:1364-1371, :1379-1381);
/// indexLookup with @a=1,@b=3 (resolving to b=2) reads the cache but must
/// NOT hit the PLAN cache (:1374-1376) —
/// cached-table reads forbid the plan cache for range accesses.
///
/// go-parity-gap: cached-table (mem-cache) reads plus plan-cache interaction
/// are unported.
#[test]
#[ignore = "go-parity-gap: cached-table + plan-cache interaction needs the executor"]
fn cached_table_prepared_plans_read_table_cache() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1385
/// TestPlanCacheWithRCWhenInfoSchemaChange` (classic kernel only: skipped
/// under next-gen at :1386-1388).
///
/// Contract: two pessimistic READ-COMMITTED sessions prepare the same
/// `select /*+use_index(t1, ic)*/ * from t1 where 1` (text and binary
/// protocol, :1392-1400) and both miss then read empty. A third session
/// drops index ic and inserts (1, 0) (:1405-1407); the next executes in BOTH
/// protocols must see `1 0` and MISS the plan cache
/// (`@@last_plan_from_cache = 0`, :1409-1417) — the RC isolation forces
/// latest-info-schema reads, invalidating the cached plan.
///
/// go-parity-gap: RC isolation + infoschema-version invalidation of cached
/// plans need the session stack.
#[test]
#[ignore = "go-parity-gap: RC-isolation cache invalidation on schema change is a session surface"]
fn plan_cache_rc_infoschema_change_misses_cache() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1429
/// TestConsistencyBetweenPrepareExecuteAndNormalSql` (classic kernel only,
/// skipped under next-gen at :1430-1432).
///
/// Contract: `t1(id int primary key, c int)` with (1,1),(2,2); the same
/// `select * from t1` is run as text-protocol `execute s`, binary-protocol
/// `ExecutePreparedStmt`, and plain SQL — all three return `1 1`,`2 2`
/// (:1450-1458). After tk2 `alter table t1 drop column c` and insert (3),
/// all three must return `1 1`,`2 2`,`3 <nil>` (:1460-1472) — MDL disabled,
/// RC isolation keeps the executing txn on its snapshot infoschema. After
/// COMMIT and a new pessimistic txn, plain SQL sees the latest schema
/// (`1`,`2`,`3`, :1475-1489).
///
/// go-parity-gap: three-protocol result equality across DDL needs the
/// executor.
#[test]
#[ignore = "go-parity-gap: text/binary/plain-SQL consistency across DDL is an executor surface"]
fn prepare_execute_normal_sql_consistency_across_ddl() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1508
/// TestCacheHitInRc` (classic kernel only, skipped under next-gen at
/// :1509-1511).
///
/// Contract: in a pessimistic READ-COMMITTED txn over
/// `t1(id int primary key, c int)`, prepared `select * from t1` — first
/// `execute s` misses (`last_plan_from_cache = 0`), then a binary-protocol
/// `ExecutePreparedStmt` HITS, and the next text `execute s` hits again
/// (`verifyCache` :1483-1506, driven at :1526-1537).
///
/// go-parity-gap: hit/miss tracking across protocols needs the session
/// stack.
#[test]
#[ignore = "go-parity-gap: RC cache hit/miss tracking across protocols is a session surface"]
fn cache_hit_in_rc_first_miss_then_hits() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1540
/// TestCacheHitInForUpdateRead`.
///
/// Contract: identical verifyCache sequence to TestCacheHitInRc but for
/// prepared `select * from t1 where id = 1 for update` inside a pessimistic
/// txn (:1540-1563): first execute misses, then binary + text executions hit
/// the plan cache — FOR UPDATE reads do not disable the cache by themselves.
///
/// go-parity-gap: for-update reads + cache hit tracking need the executor.
#[test]
#[ignore = "go-parity-gap: for-update reads + cache hit tracking need the executor"]
fn cache_hit_in_for_update_read() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1564
/// TestPointGetForUpdateAutoCommitCache`.
///
/// Contract: autocommit (no explicit txn) prepared point-get
/// `select * from t1 where id = 1 for update`: first execute misses, second
/// HITS (:1581-1589). After tk2 `alter table t1 drop column c` and
/// `update t1 set id = 10 where id = 1` (:1591-1592), the next execute
/// returns EMPTY and MISSES (schema changed, row gone), and the following
/// execute hits again (:1594-1602).
///
/// go-parity-gap: autocommit point-get caching + DML-driven invalidation
/// need the executor.
#[test]
#[ignore = "go-parity-gap: autocommit for-update point-get caching needs the executor"]
fn point_get_for_update_autocommit_cache_after_ddl_and_update() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1605
/// TestPrepareCacheForDynamicPartitionPruning`.
///
/// Contract: `t(a int, b bigint, UNIQUE KEY(a)) PARTITION BY RANGE (a)
/// (P0 VALUES LESS THAN (0))` with (-5,7), analyzed; prepared
/// `select * from t where a = ? and b < ?`. Static mode: the non-matching
/// param (@a=1) explains as `TableDual_7` and never hits the cache; the
/// matching param (@a=-5) explains as `Selection_8` over `Point_Get_7` with
/// warning "Warning 1105 skip prepared plan-cache: query accesses partitioned
/// tables is un-cacheable if tidb_partition_pruning_mode = 'static'"
/// (:1636-1650). Dynamic mode: both arms explain as `Selection_6` and the
/// matching arm HITS the cache with no warnings (:1638-1642). The TableDual
/// arm's `FoundInPlanCache` equals (mode == dynamic) (:1653-1656).
///
/// go-parity-gap: explain-for-connection goldens + prune-mode cache gates
/// need the session stack.
#[test]
#[ignore = "go-parity-gap: partition-prune-mode cache gating with explain goldens is unported"]
fn dynamic_partition_pruning_pointget_cache_matrix() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1660
/// TestHashPartitionAndPlanCache`.
///
/// Contract: `t(b varchar(255), a int primary key nonclustered, key(b))
/// PARTITION BY HASH (a) partitions 5` with 10 rows, analyzed. Prepared
/// `select * from t where a = ?`: @a=1 explains (explain for connection) as
/// `Point_Get_1` and misses; @a=2 returns `2 2` and HITS, still explaining
/// as `Point_Get_1` (:1673-1696). Re-created with `a int, unique key (a)`
/// (non-PK unique), the same two-param sequence repeats the miss-then-hit
/// Point_Get pattern (:1698-1710).
///
/// go-parity-gap: hash-partition point-get caching with explain-for-
/// connection needs the session stack.
#[test]
#[ignore = "go-parity-gap: hash-partition point-get cache hits need the executor"]
fn hash_partition_pk_pointget_plan_cache() {}

/// GO PORT of `pkg/planner/core/tests/prepare/prepare_test.go:1711
/// TestBatchPointGetPlanCacheMixedInList`.
///
/// Contract: `t (k1 int, k2 int, v int, unique key uk(k1, k2))` with
/// (1,2,100),(3,2,200),(1,4,300); prepared
/// `select v from t where (k1, k2) in ((1, ?), (?, 2))` mixes a literal and
/// a parameter marker WITHIN one IN-list row (the comment at :1716-1719
/// records the historical nil index-column-type deref panic on the second,
/// cache-hit execution). @a=2,@b=3 returns 100/200; @a=4,@b=3 returns
/// 200/300 WITH `FoundInPlanCache` true (:1721-1727) — the batch-point-get
/// range rebuild must fill every index column's type from the resolved
/// schema, not from the parameter.
///
/// go-parity-gap: batch-point-get rebuild on cache hit needs the executor.
#[test]
#[ignore = "go-parity-gap: batch-point-get range rebuild on cache hit needs the executor"]
fn batch_pointget_plan_cache_mixed_literal_param_in_list() {}
