# `pkg/statistics/handle/cache/internal/lfu` parity audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

This is an audit receipt, not a package-completion claim. The package depends
on `github.com/dgraph-io/ristretto`; repository policy requires that external
package to be consumed as a complete implementation or transcreated as its own
complete pinned package. No complete Rust Ristretto owner exists in this
workspace or the local dependency cache.

## Atomic inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 37 | `c8e67ed8c85bb67450941c7730836852d05efb01` |
| `key_set.go` | 74 | `5e3e6f1deef23f84af1b52b225b96b83139f22f3` |
| `key_set_shard.go` | 69 | `396842a6ef2465784ac0a4c37cf6495720a584eb` |
| `lfu_cache.go` | 286 | `20ee62bf973c888d228550025f40c4b1520dcb97` |
| `lfu_cache_test.go` | 316 | `e77571fb936d033ab73072d213cc9637ac016413` |

There are no generated, platform-specific, fixture, or benchmark artifacts.
The BUILD test target enables the race detector, is flaky, has ten shards, and
contains all ten tests in `lfu_cache_test.go`.

## Removed false surfaces

The former `tidb-stats` modules `stats_key_set`, `stats_key_set_shards`, and
`memory_cost` were not the Go package:

- key sets stored caller-provided costs instead of shared statistics tables,
  so `Remove` could not derive `TotalTrackingMemUsage` from the value;
- the shard wrapper publicly exposed its internal shard count and extra
  `Default`/`is_empty` behavior, and changed negative-key behavior from Go's
  invalid negative array index to Euclidean routing;
- memory adjustment accepted a caller-provided optional memory total and
  exposed private Go constants/functions as public policy;
- their eight tests were absent from the pinned Go package and did not execute
  asynchronous admission, primary-before-secondary lookup, rejection,
  eviction, table-copy/drop behavior, close suppression, metrics, or
  concurrency.

Those modules, tests, and exports were removed. The stale function-batch
`b044.md` receipt was also removed; its LFU entries referred to ignored test
functions that no longer existed, and package completion cannot be claimed by
function batches.

## Remaining package behavior

A complete owner must preserve all three production files together, including:

- 256 table-valued, independently locked fallback shards;
- Ristretto's TinyLFU counters, buffered asynchronous admission, resident-key
  update behavior, sampled rejection/eviction, callbacks, metrics, `Wait`,
  dynamic `MaxCost`, clear, and close behavior;
- primary-cache-first reads and their documented stale-read window;
- tracking-memory accounting across put, reject, evict, exit, table
  `CopyAs(AllDataWritable)`, `DropEvicted`, fake negative-key eviction
  triggers, and closed callback suppression;
- the exact 20%-of-host-memory and test-mode capacity paths;
- all ten source tests, including the race-enabled concurrency fixtures and
  eventual asynchronous memory-control checks.

The existing synchronous insertion-order cache in `tidb-session` explicitly
documents that it is not Ristretto-equivalent and was not reused here.
