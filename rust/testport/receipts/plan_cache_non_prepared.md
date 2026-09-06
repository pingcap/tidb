# `pkg/planner/core` non-prepared plan cache parity receipt

Pinned source: the repository Go tree tip (2026-09-06). Owner:
`rust/crates/tidb-session/src/non_prepared_plan_cache.rs` (826 lines), the
container at `access.rs` (`PreparedSelectPlan.cached_plans`), and the session
funnel in `tidb-session/src/dispatch.rs`.

## Verified face: SELECT admission and parameterization

The port mirrors, function for function, the SELECT half of Go
`plan_cacheable_checker.go` and `getPlanFromNonPreparedPlanCache`:

- `parameterize_select` = `NonPreparedPlanCacheableWithCtx`'s SELECT arm plus
  `isSelectStmtNonPrepCacheableFastCheck` (`fast_check`): clause-level
  refusals before the visitor, at-most-two tables, the fixed node admission
  list (`nonPreparedPlanCacheableChecker.Enter`), the filter-depth-scoped
  column-type rule, the IN-list summed-length refusal ordered ahead of the
  general literal cap, `GROUPING`/uncacheable-function list, aggregate and
  window refusals, and the per-statement literal cap with Go's
  `FIX_44823`-overridable `PlanCacheMaxParamNum` (200).
- `extract_table_names`/`table_names_cacheable` reproduce Go's table-name
  extraction against the temp-and-system filters.
- The retained marker-bearing statement, its restored-text cache key
  (`current_db | <restored parameterized statement>`), and repeated execution
  through `bind_non_prepared_select` (parameter-type compatibility, schema and
  statistics version invalidation, recursive range rebuild, partial-rebuild
  discard) match `getPlanFromNonPreparedPlanCache`'s contract through the same
  `PreparedSelectPlan` object SQL and binary PREPARE use.
- `@@last_plan_from_cache` reflects non-prepared hits (`prev_found_in_plan_cache`),
  capacity tracks `tidb_non_prepared_plan_cache_size`, and the runtime gate is
  `tidb_enable_non_prepared_plan_cache`.

Regression coverage lives in `tidb-session/src/tests_non_prepared_plan_cache.rs`
(admission, parameterization, hits, invalidation, refusal reasons).

## Former divergence: DML admission — IMPLEMENTED (2026-09-06)

Go `NonPreparedPlanCacheableWithCtx` admits UPDATE / INSERT (values and
insert-select) / DELETE statements under
`tidb_enable_non_prepared_plan_cache_for_dml` (default ON). The port now
mirrors that: `parameterize_dml` runs Go's fast checks (table hints,
multi-table UPDATE/DELETE, insert-select source kind, the
rows-times-columns parameter cap), lowers the DML target table like a
SELECT reference, and walks SET values, VALUES rows, ON DUPLICATE values,
and the WHERE predicate through the shared checker and replacer — leaving
ORDER BY and LIMIT literals verbatim per Go's replacer skip list
(pkg/planner/core/plan_cache_param.go:57-77). The session holds
`NonPreparedDmlCache` (key → `PreparedDmlPlan`), the funnel binds through
`bind_cached_for_statement`/`bind_for_statement`, and execution shares the
prepared path's `execute_cached_prepared_dml` — privilege checks, metadata
locks, statement context, and the DML executor stay common. Regressions:
`dml_statements_cache_and_rebind_like_go` (UPDATE/INSERT/DELETE hit
sequences, the gate-off refusal, hinted and multi-table refusals); the
previously-marked flip-test now asserts the hit.

## Recorded divergence: the cache container

Go `plan_cache_lru.go` (`LRUPlanCache`, 280 lines) is a session-wide,
cross-statement LRU with bucket maps, a memory guard and quota, an on-evict
callback, and `SetCapacity`/`Close`. The port keeps the retained entries on
each `PreparedSelectPlan` (per prepared handle) and sizes the NON-PREPARED
cache with a simple bounded map (`resize`/`get`/`put`). Client-visible plan
answers are identical; the deltas are cross-statement sharing within a session
and the memory-guard accounting, both covered by the same queued DML/container
batch under `rust/docs/go-physical-plan-parity-execplan.md`.

### Container audit detail: plan_cache_lru.go (280 lines)

Function-level mapping of Go's `LRUPlanCache` to the port's owners:

| Go function | Port owner | Note |
| --- | --- | --- |
| `NewLRUPlanCache` (capacity guard: <1 → 100) | `NonPreparedPlanCache::resize` / `PreparedSelectPlan.cached_plans` | per-handle Vec stands in for the session-wide LRU |
| `Get` + `pickFromBucket` (paramTypes compatibility) | `prepared_parameter_types_compatible` lookup in `bind_inner`/`bind_cached_for_statement` | one key may hold several plans keyed by parameter-type signature |
| `Put` (replace-compatible / push-new, evict-oldest at capacity) | the cached-plans `retain` + `push` + miss rebuild | capacity unbounded per handle (Go bounds per session) |
| `Delete` / `DeleteAll` / `Size` / `SetCapacity` / `Close` | `invalidate_on_fresh_stats` schema-version filtering; no explicit capacity setter | SetCapacity <1 errors in Go |
| `memoryControl` (quota × guard via `memory.InstanceMemUsed`) | not ported | the port has no session-plan memory quota loop |
| `MemoryUsage` / `updateInstanceMetric` / `updateInstancePlanNum` (grafana instance gauges) | not ported | prometheus instance-metric face absent |
| `onEvict` (test-only) | n/a | |

The unported slice is exactly the session-wide sharing, memory-quota loop,
and instance metrics; statement answers are unaffected. It stays queued with
the DML/container follow-up under `rust/docs/go-physical-plan-parity-execplan.md`.

## Validation

    cargo +nightly-2026-08-22 test --offline --locked -p tidb-session --lib non_prepared_plan_cache
    # 24 passed; 0 failed (2026-09-06). Three fixes this round closed the last
    # five tip failures:
    # - go_refuses_tables_in_every_system_schema_owned_by_filter panicked
    #   because rule_collect_plan_stats.rs passed DataSource.db_name (the
    #   session's spelling, e.g. "DM_HEARTBEAT") into
    #   filter::is_system_schema, whose Go contract receives the lowered
    #   CIStr.L form (plan_cacheable_checker.go:516 uses node.Schema.L); the
    #   two call sites now lower it like Go.
    # - a_schema_change_invalidates_the_entries_built_before_it panicked at
    #   rowcodec.rs:651 indexing handle_column_ids[0] on an empty id list (a
    #   rowid table projects no handle column); the Int guard now mirrors Go
    #   tryDecodeHandle (IsPKHandle / ExtraHandleID, no indexing).
    # - The last three (go_admits_custom_restore_func_call_shapes,
    #   a_set_var_hint_breaks_the_cache,
    #   go_refuses_a_user_variable_and_only_the_listed_uncacheable_functions)
    #   all failed inside CachedSelectPlan::bind's rebuild for a marker
    #   inside a scalar-function argument (`a = ABS(?)`). Two causes, both
    #   fixed: buildFromBinOp's Rust-only ConstLevel::STRICT pre-gate refused
    #   ConstOnlyInExecution operands before the unconditional eval Go
    #   performs (points.rs; Go pkg/util/ranger/points.go:326 evals with no
    #   const-level check), and CachedPlanRebuildContext never carried a
    #   deferred evaluator, so any deferred constant failed the rebuild
    #   closed. The bind call sites now evaluate deferred expressions via
    #   eval_expression_once over NoColumns: deterministic functions of
    #   installed markers rebind like Go's rebuild ranger, while
    #   session-bound functions (the statement clock) fail closed and force
    #   a replan — exactly the previous fail-closed behavior.
    # Companion fix: rowcodec decode_handle_column empty-id-list crash (the
    # schema-change test above). The executor lib failure set is a subset of
    # the pristine tip's sibling-in-flight cluster, with the point-get and
    # prepared-rebind families this batch repaired removed (verified by
    # stash-diff against the tip: 15 tip-only failures fixed, 0 new).

No Go file changed; the Bazel gate is not required.
