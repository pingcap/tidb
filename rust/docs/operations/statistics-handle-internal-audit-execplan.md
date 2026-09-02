# `pkg/statistics/handle/internal` parity audit ExecPlan

## Objective

Keep the complete statistics test helper aligned with Go's actual-table
equality contract for counts, histogram text, CMSketch, TopN, and existence
maps, without restoring an opaque snapshot carrier.

## Progress

- [x] Read both Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 68 lines across BUILD metadata
  and the complete `AssertTableEqual` helper.
- [x] Confirm there are no package docs, tests, fixtures, benchmarks, fuzz
  targets, generated inputs, or build/platform variants.
- [x] Re-read the complete `tidb-stats-handle-internal` owner and its
  statistics-table dependencies. The native helper already compares actual
  table graphs with Go's textual histogram and nil/empty TopN behavior; the
  former snapshot carrier and source-absent tests remain deleted.
- [x] Refresh the package receipt and top-level ExecPlan to Ready status.

## Validation gate

This is a Ready support-package authority refresh. No Go, Bazel, or module file
changed, so `make bazel_prepare` is not required.

- [x] Current and detached exact-master Go package probes pass (`[no test
  files]`).
- [x] The Rust owner tests/checks, downstream statistics suite, formatting,
  pinned lint, and scoped diff gates pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Continue with the next unrefreshed statistics-handle support boundary. Future
changes must keep equality tied to actual `tidb_stats::Table` values and must
not reintroduce caller-authored encoded snapshots.
