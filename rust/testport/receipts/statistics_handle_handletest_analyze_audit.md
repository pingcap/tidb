# `pkg/statistics/handle/handletest/analyze` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 28 | `0c79358956b339889ad3ca88f2ff38ff1d39a9fe` |
| `analyze_test.go` | 313 | `5d9579e22293d1454625a1bad022f14b76c395d7` |
| `main_test.go` | 34 | `0cadfcccdafbb2246473a603a26f6b98ab15230d` |

All 375 lines were read. The package has six tests and no benchmark.

## Go behavior

This external test package exercises virtual columns, persisted global-stats
options, dynamic partition pruning, partition FM-sketch writes, and manual and
automatic ANALYZE metrics through the real testkit/domain/statistics-handle
stack. Its Bazel target is flaky, race-enabled, and six-way sharded.

## Rust comparison and decision

The mixed Rust batch carrier represented only three tests from a later
origin/master snapshot, each as an ignored empty function. It omitted the
other three pinned tests and implemented none of the integration behavior.
Those entries and their stale batch receipt were removed. This package remains
unclaimed until its complete session/domain/ANALYZE surface can land atomically.
