# `pkg/resourcemanager/poolmanager` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `task_manager.go`
- `task_manager_iterator.go`
- `task_manager_scheduler.go`

## Rust ownership and integration

- `tidb-resourcemanager::poolmanager` owns task metadata, its running-worker
  counter and task/exit channels, the eight-shard registry, and the source
  overclock/downclock selection.
- Registration replaces an existing task ID in its `id % 8` shard; deletion is
  silent when absent. The manager retains its initial concurrency unchanged.
- Iteration visits shards in source order while holding each read lock. It
  preserves Go's first-running-task seed, newer-task boost preference,
  under-initial-concurrency early boost, older-task pause preference,
  over-initial-concurrency early pause, and nonblocking exit notification.
- A shared channel wrapper provides Go's explicit close behavior over
  crossbeam queues: all clones refer to the same queue, close disconnects its
  sole shared sender while retaining buffered values, workers share receivers,
  and pause uses a nonblocking send.
- The pinned package has no tests, fixtures, or support artifacts, so no
  supplemental test surface was added. The concrete `spool` consumer remains
  a separate package unit.

## WIP validation

Commands and results are shared with
`receipts/resourcemanager_pool.md`: the owner checked successfully and both
existing dependent resource-manager source tests passed.

Not run: the concrete spool package tests, workspace-wide tests, or the
Ready-profile `make lint` gate.
