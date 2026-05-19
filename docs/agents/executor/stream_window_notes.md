# Stream Window Executor Notes

## 2026-04-29: Executor slice for ordered-input window work

Context:
- Issue: https://github.com/pingcap/tidb/issues/67989
- Demo PR: https://github.com/pingcap/tidb/pull/67696
- This slice only extracts executor-side building blocks. Planner admission,
  physical plan enumeration, and plan goldens should be reviewed separately.

Executor boundary:
- `StreamWindowExec` is a thin wrapper over `PipelinedWindowExec`.
- The executor does not prove ordering by itself. Its caller must only choose it
  when the child already satisfies the required `PARTITION BY` / `ORDER BY`
  property.
- `PipelinedWindowExec.OpenSelf()` exists for builder paths that already opened
  children while constructing nested executors, such as index-join inner reader
  construction. It resets only this executor's state.

Partition pruning boundary:
- `PartitionTopNWindowExec` is a local executor helper for
  `row_number() <= K` style stream-window pruning.
- It consumes ordered child rows, emits at most `K` rows per partition, and
  materializes the `row_number` result column for emitted rows.
- It is not a general `rank()` / `dense_rank()` pruning primitive. Tie-aware
  rank functions need a different boundary because they cannot be reduced to a
  hard row count per partition.

Testing notes:
- Cross-chunk coverage is required. A test that keeps each partition inside one
  chunk can miss bugs where row numbers reset at chunk boundaries.
- Direct executor tests are useful for this slice because planner enumeration is
  intentionally left out of the PR.
