# `pkg/statistics/handle/autoanalyze/refresher` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 53 | `fa1451885086b9849d399bee65be0d9887725f82` |
| `main_test.go` | 34 | `5dda135affd0a7a422495d1fcde9101def90ac0d` |
| `refresher.go` | 278 | `d36b515fc727325bd415dd8024e404c426d65169` |
| `refresher_test.go` | 608 | `f46abbfc21d626c76e3391e07a36cb00f46dd085` |
| `worker.go` | 152 | `3bb89627bdd00b7aaccbea7776e1dc2d9f622b6b` |
| `worker_test.go` | 180 | `40c4c87e28c21961d6c38cce9685c8e31f06b30d` |

All 1,305 lines were read. The package has 12 top-level assertion tests plus
six `TestWorker` subtests, one shared leak-checking `TestMain`, and no
benchmark.

## Go behavior

`Refresher` owns the live priority queue and worker. It registers the real DDL
notifier, reads current session parameters, initializes or rebuilds the queue,
does so even outside the execution window, updates concurrency, skips already
running or invalid jobs, and submits highest-priority jobs. The worker
synchronizes running identities and concurrency, executes concrete jobs with
the statistics handle/process tracker, recovers panics, waits, and stops.

Tests validate enable/disable notifier progress, outside-window initialization,
prune-mode rebuilds, ratio zero, nil/pseudo/tiny filtering, ordered and
concurrent real ANALYZE effects, deleted-table retry behavior, recent-failure
backoff, worker admission/update/snapshots, and panic cleanup.

## Rust comparison and decision

Rust exposed two public scalar leaves: `should_rebuild_queue` copied the
condition comparing ratio/prune mode, while `worker_capacity_available` and
`worker_concurrency_changed` copied two mutex-guarded branch conditions. Six
source-absent tests exercised only those values. Repository-wide tracing found
no production caller.

Go exposes none of these helpers as independent behavior. The leaves omitted
the queue, sessions, DDL handler, worker, concrete jobs, concurrency state,
background execution, panic cleanup, time window, stats effects, and every Go
test. Both modules, their root exports, and all six synthetic tests were
removed. The package remains unclaimed until its `exec`, `priorityqueue`,
ordinary handle/types/session, and notifier dependencies are complete and all
six artifacts can land atomically.

## WIP validation

- `cargo check --locked -p tidb-stats` passed.
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
  passed: 277 run, 277 passed, 105 skipped.
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs` passed.
- `git diff --check` passed.

No Go or Bazel source changed, so `make bazel_prepare` was not required. This
is a WIP package audit, not a repository-wide Ready parity claim.
