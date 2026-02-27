# TODO: Investigate stats_meta delta double-counting after truncate

## Observed behavior

When running `truncate-stats` (truncates `mysql.stats_meta`) followed by a full
ANALYZE (sample OFF), then a single-partition re-analyze (sample OFF), every
partition's `row_count` and `modify_count` doubles. Example with 10 partitions
of 130K rows each:

- After truncate + full analyze: count=130000, modify=0 (correct)
- After single-partition re-analyze: count=260000, modify=130000 (all partitions!)

This does NOT happen when the full analyze is done without truncating first.

## Likely cause

`DumpStatsDeltaToKV` flushes accumulated DML deltas (from session trackers or
in-memory cache) to `stats_meta` using incremental updates
(`count = count + delta`). When stats_meta is truncated, the baseline is lost
but the accumulated deltas are not cleared. The fresh rows written by ANALYZE
then get the stale deltas re-applied on top.

Possible contributing factor: the stats cache may retain old counts across the
truncate, causing the delta computation to use a stale baseline.

## Impact

- Only affects the old partition-merge path (sample OFF).
- Sample-based path computes global row_count from collector data, so it's immune.
- Only triggered by manual truncation of internal stats tables, not normal operation.

## Priority

Low — pre-existing issue, not related to sample-based global stats changes.
