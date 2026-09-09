# `pkg/statistics/handle/handletest/initstats` package receipt

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob | Rust owner |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 26 | `2bd4f9f3147fac32219df911cb9fe1945b03b72c` | `tidb-exec` aggregate integration-test target and `tidb-server` unit target |
| `init_stats_test.go` | 437 | `14a77df6c90f33f1d21a544cd9b2cf360908bd28` | `real_tikv_stats`, `cluster_stats_load`, server startup, and source-backed regressions |
| `main_test.go` | 34 | `13d11c7eaef3f97af1539b0d17db370e17c98b95` | Rust test-process cleanup and thread joins |

All 497 lines were read. The package has eight tests, no benchmarks, fixtures,
generated inputs, or platform variants. Go's BUILD target is test-only, has
`shard_count = 8`, and is tagged flaky; Rust's aggregate test build preserves
one compiled integration binary while targeted commands select these cases.

## Behavior mapping

- `load_initial_stats_snapshot` is the shared `InitStatsLite` / `InitStats`
  boundary. An empty physical-ID list loads every current target; a nonempty
  list loads only those IDs and safely permits repeated requests.
- Targets come only from the current catalog and expand logical partitioned
  tables through `StatsTarget::for_table`. Persisted rows for a dropped table
  therefore cannot enter the returned cache, while every current physical
  partition ID does.
- Lite initialization loads meta and histogram existence/state only. Buckets,
  TopN, and CMSketch remain absent and every initialized item is `allEvicted`.
- Non-lite initialization now matches Go's asymmetric shape: index buckets,
  TopN, and CMSketch are fully loaded, while columns remain `allEvicted` for
  predicate-driven sync/async loading. Rust's former full-column startup was
  removed.
- Both Go memory-limit variants have the same observable cache shape; memory
  pressure changes staged admission, not the final metadata-only column state.
  Rust uses the shared canonical cache image and verifies that shape once,
  without a second behaviorally identical test-only branch.
- Startup reads the live `lite_init_stats` and `skip_init_stats` performance
  fields. Skip suppresses only the immediate initialization pass; the leased
  periodic updater remains live and performs a lite load on its first tick.
- The all-table regression verifies completion through the greatest current
  physical ID, covering Go's last-table/page-boundary assertion.

## Original test mapping

| Go test | Executable Rust evidence |
| --- | --- |
| `TestLiteInitStatsWithTableIDs` | `initial_stats_matches_go_table_scope_and_payload_shapes` targeted, repeated, all-current, dropped, and partition-ID cases |
| `TestNonLiteInitStatsWithTableIDs` | same regression's `InitialStatsLoad::IndexFull` assertions |
| `TestConcurrentlyInitStatsWithMemoryLimit` | same regression's canonical non-lite column/index load-state assertions |
| `TestConcurrentlyInitStatsWithoutMemoryLimit` | same observable assertion; no Rust-only duplicate policy path |
| `TestDropTableBeforeConcurrentlyInitStats` | stale persisted stats row omitted from the current target inventory |
| `TestDropTableBeforeNonLiteInitStats` | same omission under `IndexFull` |
| `TestSkipStatsInitWithSkipInitStats` | `skip_init_stats_skips_only_bootstrap_and_preserves_the_periodic_loader` |
| `TestNonLiteInitStatsAndCheckTheLastTableStats` | highest current physical ID is present and fully processed |

`main_test.go` installs Go leak detection because the Go tests create domains
and workers. Rust owns worker threads with join-on-shutdown guards; both
targeted tests exit with all owned threads joined, so no separate test-main
scaffold is required.

## Validation

WIP package gate:

- `cargo test -p tidb-exec --test all initial_stats_matches_go_table_scope_and_payload_shapes -- --nocapture`
- `cargo test -p tidb-server skip_init_stats_skips_only_bootstrap_and_preserves_the_periodic_loader -- --nocapture`
- `cargo check -p tidb-server`

The batch also receives the repository Ready formatting, lint, and diff gates
before commit.
