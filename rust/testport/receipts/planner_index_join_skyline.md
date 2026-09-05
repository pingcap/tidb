# `pkg/planner/core` — IndexJoin skyline row-count parity receipt

Comparison source: Go `origin/master` commit
`fc7788ff517c3407dc7e000be989ab23e6648211` (`planner: fix countAfterAccess
for IndexJoin in Skyline Pruning Comparison`). The changed production and test
artifacts were read in full; the complete `pkg/planner/core` and nested
casetest inventory remains maintained by `receipts/b082.md`,
`receipts/planner_index_join_row_floor.md`, and the adjacent planner receipts.

## Go behavior

Go's IndexJoin inner-path search reuses skyline pruning. The ordinary
`AccessPath.CountAfterAccess` cannot see runtime join values, so
`indexJoinPathCountAfterAccess4Compare` divides it by the NDV of a stable,
single, full-length runtime join key before the empirical `Fix45132` ratio
comparison. Pseudo/missing statistics, prefix index columns, multiple runtime
keys, or an invalid NDV make that comparison ineligible. The `Fix45132` ratio
defaults to 1000 and is disabled by a non-positive setting. With both adjusted
counts above 100, a path more than the threshold times larger loses; otherwise
normal skyline/cost rules continue.

## Rust implementation

`tidb-planner::find_best_task::dispatch` now owns the bounded selector:

* `DispatchContext` carries the resolved `index_join_skyline_threshold` with
  Go's 1000 default and a setter for session fix-control plumbing.
* Each admitted secondary-index IndexJoin path derives a compare-only count
  from the raw access estimate and one full-length runtime-key NDV. The helper
  fails closed for prefix keys, multiple runtime keys, pseudo statistics,
  missing/invalid NDVs, non-finite counts, and absent runtime keys.
* The DataSource path loop applies the strict Go ratio rule before ordinary
  task costing, retaining the compare-only count separately so the chosen
  physical IndexJoin's execution cardinality is unchanged.

The implementation intentionally leaves Go's richer risk/predicate metrics to
the existing Rust task coster; the strong ratio rule is used only when both
candidate counts are proven and no LIMIT property is present. Table, TiFlash,
IndexMerge, and unproven paths delegate to ordinary cost comparison.

## Focused regressions

* `find_best_task::dispatch::index_join_skyline_count_uses_one_stable_runtime_key`
  pins the 200000/100 NDV division and the prefix/multi-key/pseudo-statistics
  refusal cases.
* `find_best_task::dispatch::index_join_skyline_ratio_respects_fix_control_and_row_floor`
  pins both winner directions, Go's strict `> 100` floor, LIMIT bypass, and
  the non-positive disable setting.

The source-derived Go `TestIssue70757IndexJoinInnerIndexSelection` remains an
explicit ignored integration boundary because this workspace does not yet own
the full analyzed mock-store/Cascades skyline harness.

## Ready validation

```text
cargo test --offline --locked -p tidb-planner \
  'index_join_skyline_' -- --nocapture
git diff --check
cargo fmt --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex make lint
```

All commands pass; the Rust build emits only pre-existing warnings in
unrelated crates and modules.
