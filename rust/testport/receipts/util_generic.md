# `pkg/util/generic` — complete package transcreation

Pinned TiDB source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly five artifacts, all read in full:
`bounded_min_heap.go`, `bounded_min_heap_test.go`, `sync_map.go`,
`sync_map_test.go`, and `BUILD.bazel`. There is no package doc, README,
fixture, benchmark, harness, generated/platform variant, or ownership file.
The local Go package is byte-identical to the pin.

Production behavior comprises a bounded best-N heap with a signed comparator,
nil-comparator and negative-capacity panics, zero-capacity behavior, wrapping
comparator negation for best-to-worst snapshots, and a generic RWMutex-backed
map with store, load, delete, and key snapshots. The source has exactly eight
tests: seven heap tests and one synchronized-map test.

## Rust ownership and audit result

`rust/crates/tidb-util/src/generic/` owns the package. The heap now accepts an
architecture-width signed capacity and an optional signed comparator, checking
nil before negative capacity exactly like Go. It retains comparator magnitude
and uses wrapping negation when sorting instead of narrowing comparisons to
`Ordering`. The Rust-only `is_empty` method was removed.

`SyncMap` retains native `Option<V>` results for Go's `(V, bool)` pairs and
recovers poisoned locks because Go mutexes do not add a poison failure mode.
The stats TopN builder now uses the corrected heap contract directly.

Two supplemental owner tests, the two-test external contract, its semantic
manifest, and a stale standalone audit plan were removed. The owner now has
exactly the eight Go tests, including the formerly missing constructor safety
test.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/generic` — passed.
- `go test ./pkg/util/generic -count=1` — passed, 8 tests.
- `cargo test --offline --locked -p tidb-util --lib generic:: --no-fail-fast` — passed, exactly 8 tests.
- `cargo test --offline --locked -p tidb-util --no-run` — passed.
- `cargo test --offline --locked -p tidb-stats --test all builder_source::` — passed, 27 consumer tests.
- `cargo fmt -p tidb-util -p tidb-stats -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: both exact package suites pass, including nil/negative safety,
  and all TopN builder consumer tests pass with the signed comparator contract.
- Compatibility: removes Rust-only APIs/tests/docs and changes the heap
  constructor to represent Go's nullable comparator and signed capacity. The
  sole production caller is migrated in the same commit.
- Performance: heap geometry and operations are unchanged; preserving the
  signed comparator avoids an information-losing conversion. Lock poison
  recovery only affects panic paths.
