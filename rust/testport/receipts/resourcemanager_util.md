# `pkg/resourcemanager/util` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `mock_gpool.go`
- `shard_pool_map.go`
- `shard_pool_map_test.go`
- `util.go`

## Rust ownership and integration

- `tidb-resourcemanager::util` owns the goroutine-pool contract, pool container,
  five component values, mutable 200 ms minimum interval, and mutable maximum
  overclock count of one.
- `ShardPoolMap` has the source eight shards, selects a shard from the first key
  byte, rejects duplicates outside `intest`, overwrites them inside tests, and
  retains each shard's read lock while visiting its entries. Empty keys retain
  the source panic boundary.
- `MockGPool` preserves the source test-support behavior: tuning, capacity,
  name, original concurrency, a last-tune time ten seconds in the past, and
  `implement me` panics for unsupported operations.
- The sole sharded-map source test retains its identity and exact add/iterate/
  delete counts. No supplemental behavior or test API was added.

## WIP validation

Commands and results are shared with
`receipts/util_cpu.md`: the combined resource-manager suite passed both library
source tests with `failpoints,intest`, and the affected crates checked cleanly
apart from existing workspace warnings.

Not run: workspace-wide tests or the Ready-profile `make lint` gate.
