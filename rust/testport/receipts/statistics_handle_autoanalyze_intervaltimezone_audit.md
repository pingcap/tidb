# `pkg/statistics/handle/autoanalyze/priorityqueue/intervaltimezone` audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 22 | `5051d7da615b034fc75efb62d6e213d68266ef1c` |
| `interval_timezone_test.go` | 81 | `95b063d3df5e17f521bbcb947f40a4fa1e111216` |
| `main_test.go` | 34 | `fbeff3a7847c428e6fddee7c77431ec4509580aa` |

All 137 lines were read. The package has one assertion test, one shared
leak-checking `TestMain`, and no benchmark.

## Behavior and Rust decision

The sole test intentionally contaminates both TiDB's system timezone and
Go's `time.Local` before bootstrap, creates a mock store/domain, sets a
different global session timezone, inserts/starts/fails an analyze job through
the real statistics handle, then queries the duration through the handle's
session pool. A small positive result proves pooled sessions reset to the
global timezone.

Rust has no corresponding package or test. The old `b043` empty ignored marker
was removed with the false parent runtime; it executed none of this behavior.
This package remains unclaimed until the ordinary statistics handle, analyze
job persistence, session pool timezone reset, and parent interval query exist
together. A timestamp arithmetic helper would not be equivalent.

No Rust source changed solely for this audit. This is a WIP inventory, not a
package parity or repository-wide Ready claim.
