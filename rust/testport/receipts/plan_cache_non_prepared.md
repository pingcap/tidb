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

## Recorded divergence: DML admission (Go default-ON)

Go `NonPreparedPlanCacheableWithCtx` also admits UPDATE / INSERT (values and
insert-select) / DELETE statements, gated by
`tidb_enable_non_prepared_plan_cache_for_dml`, whose default
(`DefTiDBEnableNonPreparedPlanCacheForDML`, `pkg/sessionctx/vardef/tidb_vars.go:1742`)
is **true**. The port refuses every non-SELECT statement (Go's own
`"not a SELECT/UPDATE/INSERT/DELETE statement"` reason surfaces only for the
non-Query shapes). Observable delta on a default-config session: a repeated
identical UPDATE answers `@@last_plan_from_cache` = 1 in Go and 0 here, and Go
saves the re-plan. Results are identical; the gap is plan-reuse only.

This is marked where Go's behavior will land:
`tests_non_prepared_plan_cache.rs` ("DML is refused here ... When it is, this
test FLIPS"). Porting it needs the DML physical-plan reuse funnel
(`run_insert/update/delete_meta_stmt_with_physical` all exist), Go's DML fast
checks (table-hints, multiple-table UPDATE/DELETE, insert-select, the
`nRows*nCols > maxNumParam` values cap), and the DML branch of the retained
statement key. Queued as the next plan-cache fix batch.

## Recorded divergence: the cache container

Go `plan_cache_lru.go` (`LRUPlanCache`, 280 lines) is a session-wide,
cross-statement LRU with bucket maps, a memory guard and quota, an on-evict
callback, and `SetCapacity`/`Close`. The port keeps the retained entries on
each `PreparedSelectPlan` (per prepared handle) and sizes the NON-PREPARED
cache with a simple bounded map (`resize`/`get`/`put`). Client-visible plan
answers are identical; the deltas are cross-statement sharing within a session
and the memory-guard accounting, both covered by the same queued DML/container
batch under `rust/docs/go-physical-plan-parity-execplan.md`.

## Validation

    cargo +nightly-2026-08-22 test --offline --locked -p tidb-session --lib non_prepared_plan_cache
    # 21 passed; 3 failed at the tip (2026-09-06). Two of the five original
    # failures are fixed in this batch:
    # - go_refuses_tables_in_every_system_schema_owned_by_filter panicked
    #   because rule_collect_plan_stats.rs passed DataSource.db_name (the
    #   session's spelling, e.g. "DM_HEARTBEAT") into
    #   filter::is_system_schema, whose Go contract receives the lowered
    #   CIStr.L form (plan_cacheable_checker.go:516 uses node.Schema.L); the
    #   two call sites now lower it like Go.
    # - a_schema_change_invalidates_the_entries_built_before_it panicked at
    #   rowcodec.rs:651 indexing handle_column_ids[0] on an empty id list (a
    #   rowid table projects no handle column); the Int guard now mirrors Go
    #   tryDecodeHandle (IsPKHandle / ExtraHandleID, no indexing) and the
    #   test passes.
    # The remaining three (go_admits_custom_restore_func_call_shapes,
    # a_set_var_hint_breaks_the_cache,
    # go_refuses_a_user_variable_and_only_the_listed_uncacheable_functions)
    # share one located root cause: a marker inside a scalar-function
    # argument (`a = ABS(?)`). Probed at the executor seam: raw-statement
    # build_prepared_select_plan succeeds and parameterization produces the
    # correct key (test|SELECT `a` FROM `t` WHERE `a`=ABS(?)), but
    # PreparedSelectPlan.bind returns None on the fresh path — the failure
    # sits inside CachedSelectPlan::bind's
    # rebuild_plan_for_cache_in_place for the ABS-bearing selection (the
    # in-place marker install reaches function arguments through the AST
    # visitor). Next probe: whether PointBuilder::build evaluates a
    # ScalarFunction-of-installed-marker to a range point at all (vs leaving
    # the condition to remained_conds, which then trips range_is_safe's
    # full-range comparison) — the fix is teaching that value extraction to
    # fold the installed constant the way Go's rebuild ranger does.

No Go file changed; the Bazel gate is not required.
