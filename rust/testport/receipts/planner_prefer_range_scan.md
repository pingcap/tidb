# `skylinePruning`'s prefer-range override keeps an `=`/`IN` index path

Go source: `origin/master` at
`f6c6d1adba7edae5786a4fffbacb31b1ec9c42c6` (2026-09-08).

## Go behavior (the oracle)

`skylinePruning` (`pkg/planner/core/find_best_task.go:1778`) ends with the
prefer-range pass (`:1877`):

```go
preferRange := ds.SCtx().GetSessionVars().GetAllowPreferRangeScan()
if preferRange {
    preferRange = preferMerge || idxMissingStats || ds.TableStats.HistColl.Pseudo || ds.TableStats.RowCount < 1
}
if preferRange && len(candidates) > 1 {
    ...
    indexFilters := c.eqOrInCount > 0 || len(c.path.TableFilters) < len(c.path.IndexFilters)
    if preferMerge || ((c.path.IsSingleScan || indexFilters) && (prop.IsSortItemEmpty() || c.matchPropResult.Matched())) {
        if !c.isFullRange { preferredPaths = append(...); hasRangeScanPath = true }
    }
    ...
}
```

The preferred set REPLACES the candidate list, so a full table scan is not
offered to any property — including the cop properties a parent probes for
push-down. That is what makes `tidb_opt_prefer_range_scan` (default ON)
choose an IndexLookUp under pseudo statistics even when the table scan prices
lower.

## The Rust gap

The Rust candidate loop (`find_best_task/dispatch.rs`) priced every path and
kept the cheapest, with no prefer-range pass. For
`SELECT a FROM t WHERE b IN (1..50)` on a non-covering index, the root
property's IndexLookUp candidate existed, but the parent projection's
`CopSingleRead` probe saw a cheap full-scan candidate and chose it, so the
executor read the table by range scan instead of the batch handle lookup.

## Changes

- `DispatchContext` gained `prefer_range_scan` (Go's default ON; the executor
  does not expose the session override yet) with a builder method.
- `index_path_is_preferred_range` computes Go's `eqOrInCount > 0 &&
  !isFullRange` for one index path before the loop, so the preference applies
  to EVERY property the DataSource is probed for.
- The table path is skipped when a preferred range index path exists, and the
  post-loop pass replaces a chosen full-scan task with the preferred
  range-scan task (Go's `preferredPaths` replacement).

## Regression

`access_path::tests::the_double_read_issues_one_batch_get_per_index_batch`:
before, `gets == 0` (table range scan); after, the plan is
`Projection -> IndexLookUpReader -> IndexScan/TableScan` and `gets == 1`, one
batch get for the 50 index rows.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-executor --lib access_path::tests::the_double_read_issues_one_batch_get_per_index_batch
# ok

cargo test -p tidb-planner --lib -- --test-threads=1
# 999 passed / 0 failed

cargo test -p tidb-executor --lib -- --test-threads=1
# 1230 passed / 22 failed; the test above removed from the baseline, no additions

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --check <changed files>
git diff --check -- rust
```
