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

//! Port ledger for `pkg/planner/core/casetest/instanceplancache/
//! others_test.go` (`pkg/planner.part5`, items 266–283 of all `Test*`/
//! `Benchmark*` declarations under `pkg/planner/` on `origin/master`,
//! sorted by file path then line).
//!
//! Family contract: the domain-scoped instance plan cache is keyed well
//! enough (SQL digest + user/host + charset/collation + binding + schema
//! state, planner/core/plan_cache_utils.go GetPlanCacheKey family) that a
//! plan populated by ONE session is reused — or correctly refused — by
//! EVERY other session, with meta/runtime info exposed through
//! `PlanCacheValue` (plan_cache_utils.go:538-572) and
//! `information_schema.tidb_plan_cache`.
//!
//! All eighteen items are honest gap ports: this crate has neither the
//! mock store/session stack nor any instance-plan-cache carrier; the only
//! Rust traces are sysvar DEFAULTS recorded in
//! tidb-session/src/sysvar/catalog/optimizer.rs (enable OFF,
//! max_size "104857600", reserved_percentage 0.1) without the Go
//! SetGlobal validators (sessionctx/variable/sysvar.go:1706-1740).

/// GO PORT of `others_test.go:32 TestInstancePlanCacheMinSize`.
///
/// Re-derived contract: `set global tidb_instance_plan_cache_max_size`
/// rejects 0, 1, 101KiB, 10001KiB (<10MiB) and 99MiB but accepts 100MiB,
/// 101MiB and 2000000KiB — Go validates in the sysvar SetGlobal hook
/// (sysvar.go:1727-1739): parseByteSize failure or negative value errors
/// "invalid ... value", anything below MinTiDBInstancePlanCacheMemSize =
/// 100MiB (vardef/tidb_vars.go:1770) errors "should be at least 100MiB".
#[test]
#[ignore = "go-parity-gap: byte-size sysvar validator lives in unported sessionctx SetGlobal hooks"]
fn instance_plan_cache_max_size_rejects_below_100mib_floor() {}

/// GO PORT of `others_test.go:45 TestInstancePlanCacheVars`.
///
/// Re-derived contract: defaults are enable=0, max_size=104857600,
/// reserved_percentage=0.1 (vardef/tidb_vars.go:1768-1770; Rust mirrors
/// the values only in optimizer.rs); negative or garbage max_size errors;
/// 1234560000 round-trips exactly through @@; reserved_percentage -1 and
/// 1.1100 truncate to the bound [0,1] emitting `Warning 1292 Truncated
/// incorrect tidb_instance_plan_cache_reserved_percentage value: '<raw>'`
/// while 0.1 leaves warnings empty.
#[test]
#[ignore = "go-parity-gap: needs SET GLOBAL pipeline with type-clamp warnings, unported"]
fn instance_plan_cache_sysvar_defaults_and_validation_matrix() {}

/// GO PORT of `others_test.go:74 TestInstancePlanCacheBinding`.
///
/// Re-derived contract: a prepared select warms the cache (miss→hit);
/// `create global binding using <other-index-plan>` makes the NEXT execute
/// miss (binding becomes part of the cache key/binding field,
/// plan_cache_utils.go:546) and the one after hits again; the same cycle
/// repeats for fuzzy cross-db bindings under
/// tidb_opt_enable_fuzzy_binding=1 (`*.t2`); statements carrying
/// `/*+ ignore_plan_cache() */` never report a hit, whether the hint comes
/// from the statement itself or from the created binding.
#[test]
#[ignore = "go-parity-gap: needs bindings + PREPARE/EXECUTE + instance plan cache"]
fn instance_plan_cache_binding_cycles_invalidate_then_rewarm() {}

/// GO PORT of `others_test.go:131 TestInstancePlanCacheReason`.
///
/// Re-derived contract: the first execute of an uncacheable statement
/// surfaces `Warning 1105 skip prepared plan-cache: <reason>` exactly once:
/// uncorrelated scalar sub-query reason injected at executor/select.go:605;
/// LIMIT count above MaxCacheableLimitCount reason produced during key
/// building (planner/core/plan_cache_utils.go:472, reached via the
/// ParamMarkerExpr LIMIT check :459-476); string-param-to-INT coercion
/// reason emitted from builtin_compare (pkg/expression/builtin_compare.go:1743
/// and :1752) when comparing int column with `'123'`.
#[test]
#[ignore = "go-parity-gap: SetSkipPlanCache plumbing + SHOW WARNINGS pipeline unported"]
fn instance_plan_cache_skip_reasons_surface_as_warning_1105() {}

/// GO PORT of `others_test.go:158 TestInstancePlanCacheStaleRead`.
///
/// Re-derived contract: preparing `as of timestamp ?` is rejected (bound
/// stale-read parameters unsupported); literal-timestamp as-of prepared
/// selects ARE cacheable — each distinct timestamp string compiles fresh
/// (miss) then hits on repeat — and every read reflects rows committed
/// before that timestamp (empty before insert1; row 1 between inserts;
/// rows 1..3 after insert2), so TTL/key includes the AS OF value.
#[test]
#[ignore = "go-parity-gap: needs stale-read executor + as-of cache keying + instance plan cache"]
fn instance_plan_cache_stale_read_as_of_timestamp_keyed_and_pit_accurate() {}

/// GO PORT of `others_test.go:197 TestInstancePlanCacheInTxn`.
///
/// Re-derived contract: warmed autocommit plan (hit); entering BEGIN makes
/// the first in-txn execute MISS (txn-state flips the optimizer env /
/// IsAutocommit bit) then hit; inserting dirty data mid-txn makes the very
/// next execute of the affected-table plan MISS once (dirty-data guard),
/// the repeat hits; ROLLBACK restores immediate hits; a new BEGIN now hits
/// right away (env re-learned).
#[test]
#[ignore = "go-parity-gap: needs txn lifecycle + dirty-table tracking + instance plan cache"]
fn instance_plan_cache_txn_boundaries_and_dirty_data_guard() {}

/// GO PORT of `others_test.go:230 TestInstancePlanCacheSchemaChange`.
///
/// Re-derived contract: a warmed `select * from t` plan MISSES on the
/// first execute after each DDL (ALTER TABLE add/drop column bumps schema
/// version, flushing the shared cache) and HITS again on the second —
/// pinned once per DDL direction.
#[test]
#[ignore = "go-parity-gap: needs schema-version invalidation stream + instance plan cache"]
fn instance_plan_cache_schema_change_flushes_cached_plans_once_per_ddl() {}

/// GO PORT of `others_test.go:255 TestInstancePlanCachePrivilegeChanges`.
///
/// Re-derived contract: u1 (granted SELECT on test.t) warms a cached plan;
/// REVOKE makes the cached plan REFUSE to run — the next execute raises the
/// privilege error instead of replaying; re-granting SELECT lets the same
/// cached plan hit again, proving runtime privilege re-validation against
/// current grants rather than trusting the stored plan.
#[test]
#[ignore = "go-parity-gap: needs auth/grant tables + privilege re-check on cache hit"]
fn instance_plan_cache_privilege_revocation_blocks_then_regrants_restore() {}

/// GO PORT of `others_test.go:281 TestInstancePlanCacheDifferentCollation`.
///
/// Re-derived contract: u1 (default collation_connection) prepares+executes
/// and gets hits; u2 flipping @@collation_connection=utf8mb4_0900_ai_ci
/// MISSES on the equivalent statement (collation part of
/// OptimizerEnvHash/key, plan_cache_utils.go:546-ish env hash inputs
/// connCollation :442) ; u3 with default collation again HITS u1's entry
/// on its first execute — collation partitions the shared pool without
/// poisoning it.
#[test]
#[ignore = "go-parity-gap: needs multi-session isolation + collation-scoped cache keys"]
fn instance_plan_cache_collation_change_partitions_shared_entries() {}

/// GO PORT of `others_test.go:307 TestInstancePlanCacheDifferentCharset`.
///
/// Re-derived contract: identical sharing matrix over
/// @@character_set_connection=latin1 — mismatching connection charset
/// forces one recompile, matching charsets share instantly
/// (connCharset hashed into the key, plan_cache_utils.go:441).
#[test]
#[ignore = "go-parity-gap: needs multi-session isolation + charset-scoped cache keys"]
fn instance_plan_cache_charset_change_partitions_shared_entries() {}

/// GO PORT of `others_test.go:333 TestInstancePlanCacheDifferentUsers`.
///
/// Re-derived contract: with identical SQL, u1@"%" warms an entry that a
/// duplicate u1@"%" session hits IMMEDIATELY on first execute; u1@localhost
/// and u2@"%" both need their own compile (parse user/host pair is part of
/// the cache identity) before hitting.
#[test]
#[ignore = "go-parity-gap: needs authenticated multi-session setup + user-keyed cache entries"]
fn instance_plan_cache_identity_pair_gates_cross_session_sharing() {}

/// GO PORT of `others_test.go:380 TestInstancePlanCachePartitioning`.
///
/// Re-derived contract: hash-partitioned t(a) in dynamic prune mode: point
/// gets re-bindable across partition ids (@a=1 then @a=4) HIT the shared
/// plan; switching @@tidb_partition_prune_mode='static' forces one miss
/// and then reports
/// `Warning 1105 skip prepared plan-cache: Static partition pruning mode`
/// on every further execute (SetSkipPlanCache at
/// pkg/planner/core/rule/rule_partition_processor.go:104), i.e. static-mode
/// plans never enter or reuse the cache.
#[test]
#[ignore = "go-parity-gap: needs partition prune modes + skip-warning sink + instance plan cache"]
fn instance_plan_cache_dynamic_prune_hits_but_static_never_caches() {}

/// GO PORT of `others_test.go:404 TestInstancePlanCachePlan`.
///
/// Re-derived contract: eleven cachable shapes — Point_Get on PK and on
/// unique key, Batch_Point_Get over `a in (?,?)`/`c in (?,?)`, Union on
/// two plain UNION ALL forms, IndexJoin via tidb_inlj hint, IndexMerge via
/// use_index_merge(b,c), plus Update/Delete/InsertIgnore — each leaves
/// `show warnings` EMPTY after the first execute (nothing legitimately
/// skipped), the first execute's live plan (`explain for connection`)
/// contains the expected operator substring, and the second execute sets
/// `@@last_plan_from_cache=1`; together this pins the "everyday plans ARE
/// cacheable without warnings" whitelist behavior.
#[test]
#[ignore = "go-parity-gap: needs EXPLAIN FOR CONNECTION + executor population + instance plan cache"]
fn instance_plan_cache_everyday_plans_warn_free_and_hit_second_execute() {}

/// GO PORT of `others_test.go:507 TestInstancePlanCacheMetaInfo`.
///
/// Re-derived contract: after caching a bounded-range select, a bound-hint
/// select, an INSERT with one const col (`insert into t values (?, 1)`) and
/// a DELETE, GetDomain(...).GetInstancePlanCache().All() returns exactly 4
/// entries whose sorted-by-SQLDigest metadata matches field-for-field the
/// PlanCacheValue layout (plan_cache_utils.go:538-556): non-empty
/// SQLDigest/OptimizerEnvHash, verbatim SQLText, StmtType ∈ {Select, Insert,
/// Delete}, ParseUser root, empty Binding except the hinted select whose
/// restored form is
/// `SELECT /*+ use_index(`t` `a`)*/ `a` FROM `test`.`t` WHERE `a` = 1 AND `b` > 1`,
/// and ParseValues rendered as bare `1` versus tuple `(1, 2)`.
#[test]
#[ignore = "go-parity-gap: PlanCacheValue carrier + domain instance cache absent in crate"]
fn instance_plan_cache_meta_info_fields_match_value_layout() {}

/// GO PORT of `others_test.go:571 TestInstancePlanCacheRuntimeInfo`.
///
/// Re-derived contract: RuntimeInfo() (plan_cache_utils.go:584-594) reports
/// the executed counter equal to the number of EXECUTEs (4 for st1 warm-up×4,
/// 2 for st2) with non-zero summed latency, and one more execute bumps st1
/// to 5 — counters accumulate on the SHARED entry across sessions.
#[test]
#[ignore = "go-parity-gap: runtime counters live in unported PlanCacheValue"]
fn instance_plan_cache_runtime_info_execution_counters_accumulate() {}

/// GO PORT of `others_test.go:614 TestInstancePlanCacheView`.
///
/// Re-derived contract: `information_schema.tidb_plan_cache` exposes one
/// row per shared entry ordered by sql_text with sql_text verbatim,
/// stmt_type Select, parse_user root, parse_values `1` / `(1, 2)` and
/// executions counting up (2→3) in lockstep with executes.
#[test]
#[ignore = "go-parity-gap: infoschema tidb_plan_cache memory-table view unported"]
fn instance_plan_cache_view_reports_rows_ordered_by_sql_text() {}

/// GO PORT of `others_test.go:641 TestInstancePlanCacheIssue58395`.
///
/// Re-derived contract (issue #58395 regression): `c in (?, ?, '2033-11-23')`
/// over a DATETIME primary key mixes bound params with a literal constant;
/// executing with valid datetimes succeeds WITHOUT error, and executing the
/// SAME cached statement with non-datetime strings ('a','b') afterwards
/// must fail gracefully (conversion error) rather than panic or poison the
/// shared cache for later executions.
#[test]
#[ignore = "go-parity-gap: needs prepare/execute protocol + mixed-literal IN-list handling"]
fn instance_plan_cache_issue_58395_mixed_literal_datetime_in_list_survives_bad_params() {}

/// GO PORT of `others_test.go:655 TestInstancePlanCacheWithDualTable`.
///
/// Re-derived contract: `prepare stmt from 'select 1 from dual'` hits the
/// per-session prepared plan cache twice with instance plan cache DISABLED
/// (`@@last_plan_from_cache=1`), and after enabling the instance plan cache
/// globally the same statement keeps reporting hits — dual/dummy-table
/// queries are eligible for BOTH cache layers and instance-cache wiring
/// does not disturb the session-layer hit attribution.
#[test]
#[ignore = "go-parity-gap: needs dual-table planning + layered cache-hit accounting"]
fn instance_plan_cache_dual_table_hits_under_both_cache_layers() {}
