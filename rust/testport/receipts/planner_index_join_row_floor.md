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
