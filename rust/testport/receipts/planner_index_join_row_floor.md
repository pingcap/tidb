# `pkg/planner/core` — IndexJoin probe-row floor parity receipt

Comparison source: Go `origin/master` commit
`19a41f0d4a348a5ab5213d4fa4e19c43eeec7a0f` (`planner: estimate index join
probe-side row count from the join keys the access path can use`). The
changed source artifacts were read in full; the complete `pkg/planner/core`
and nested casetest inventory is maintained by `receipts/b082.md` and its
adjacent planner receipts.

## Go behavior

The source adds `indexJoinProbeAccessRowsFloor` to the two IndexJoin inner
scan constructors. When an access path can build ranges from only a leading
subset of equality join keys, the post-join estimate is too small for the
rows physically scanned by each probe. Go therefore computes
`TableStats.RowCount / NDV(used equality prefix)` and raises `CountAfterAccess`
to that floor. A trailing range, complete equality-key coverage, pseudo or
missing statistics, and the explicitly disabled `Fix44855` control do not
apply the correction. The Go regression uses a clustered `(k1,id)` path that
can use only `k1` versus a secondary `(k1,k2)` path that uses both keys; the
former must be priced near 1000 rows per probe and the latter near one.

## Rust implementation

`tidb-planner::find_best_task::dispatch` now owns the bounded equivalent. The
dispatcher:

* derives the leading access prefix from runtime join keys plus full-length
  local equality predicates;
* uses the existing Go-shaped `estimate_cols_ndv_with_matched_len` against
  non-pseudo `StatsInfo` and computes the row-count floor;
* declines the floor for range residuals, complete key coverage, integer
  handles, invalid NDVs, or a disabled `DispatchContext` Fix44855 setting;
* applies the floor to both table/common-handle and secondary-index probe
  scan profiles before IndexJoin costing, while preserving the existing
  `avg_inner_row_count` fallback when no floor is proven.

The focused regression
`find_best_task::dispatch::index_join_probe_floor_uses_only_the_accessed_equality_prefix`
asserts the 2000/1000 = 2 floor and the complete-key and Fix44855-off
fallbacks. The source-derived SQL plan-tree test remains `#[ignore]`: this
crate still has no dependency-closed mock-store/analyze/cascades harness for
the full two-path plan choice.

### 2026-09-09 correction: the default is OFF, and the inner scan's stats

Three Go behaviors the first batch did not carry:

1. **`Fix44855` defaults to disabled.** Go reads it with
   `fixcontrol.GetBoolWithDefault(map, Fix44855, false)`
   (`exhaust_physical_plans.go:1124`); the session's
   `tidb_opt_fix_control` is the only way to turn it on, and the sysvar's
   default text is empty (`sysvar.go:3497`). `DispatchContext::new` therefore
   defaults `index_join_probe_row_count_fix` to `false`, and
   `physical_plan_for_logical` (`planner_bridge.rs`) now resolves the
   statement's parsed fix-control map with the same `false` fallback. The
   earlier default of `true` priced every prefix probe with the floor even
   though the default session does not.
2. **The inner Selection carries the runtime count.**
   `constructDS2TableScanTask` builds the pushed-down Selection with
   `selStats := ts.StatsInfo().Scale(selectivity)`, i.e. the per-outer-row
   average after the table filters, not the DataSource's full post-filter
   estimate. The table-scan arm now reuses the runtime `stats` for the
   Selection when `index_join_prop` is set (the secondary-index arm already
   did this whenever table filters remained).
3. **A complete unique equality probe is capped at one row.**
   `indexJoinPathGetRangeInfoAndMaxOneRow` (`index_join_path.go:588`) marks a
   unique path whose every key column is an equality access condition as
   `maxOneRow`, and `constructDS2TableScanTask` caps `rowCount` at `1.0`.
   `index_join_path_is_max_one_row` reproduces the admission (runtime join
   keys plus equality-fixed columns covering the whole chosen key), so the
   inner probe is priced at one row and plain `IndexJoin` -- whose hash table
   is built over `probeRowsOne * buildRows` -- can beat `IndexHashJoin`, which
   is what Go chooses for TPCC condition 06.

The executor regression
`driver::tests::joins::index_join_probe_rows_use_only_the_access_paths_join_keys`
now asserts BOTH fix states: the default session keeps the broad clustered
`TableRangeScan` and never mentions `idx_k1_k2`, while an explicit
`44855:ON` statement selects `idx_k1_k2`.

## Validation

Ready validation for the Rust owner:

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-planner
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner --lib index_join_probe_floor_uses_only_the_accessed_equality_prefix \
  -- --nocapture
git diff --check
```

The Rust commands pass. The planner build emits pre-existing warnings in
`tidb-model`, `tidb-chunk`, `tidb-txnkv`, and other planner modules; no new
warning is introduced by this batch. The full Ready profile and Go source
package suite remain tracked in the continuing repository audit. The source
focused Go command was attempted but is currently blocked before test
execution by an unrelated checkout mismatch: `pkg/session/session.go`
references `metrics.GlobalMemArbitratorSubTasks.CancelWaitAversePlan` and
`CancelStandardModePlan`, which are absent from the checked-out metrics type.
The local Bazel executable is also unavailable for `make bazel_prepare`.

## Boundary and risk

The correction is intentionally conservative: it changes only the scan
cardinality used to price a proven partial equality prefix. It does not invent
IndexJoin range construction, statistics loading, SQL plan rendering, or
session-variable plumbing that the Rust crate does not yet own. A caller that
resolves Go's session fix-control map can pass its value through
`DispatchContext::with_index_join_probe_row_count_fix`; the default is Go's
enabled behavior. The remaining full SQL regression is therefore an explicit
integration boundary rather than a silently approximated plan.
