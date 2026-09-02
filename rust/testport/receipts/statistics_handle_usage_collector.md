# `pkg/statistics/handle/usage/collector` — complete package transcreation

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (Go `master` at
the audit boundary).

## Complete inventory

The package has exactly three artifacts, all read in full and byte-compared
against the pin:

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 18 | `d6cf0630093b1233fc6613db6562d4479a74a46c` | `6533df4d823166c3efce69ef7c78e0899042c7c2f34d3f8b2930d0182e2aa8d1` |
| `collector.go` | 172 | `8fa98c284ac8c0109db5a7fe9399c0040a480a89` | `600904639107e21d2698fd0883a02d8c2f49acad0b4dfbb6b980bf55840797a1` |
| `collector_test.go` | 99 | `197f4df86298f3d6f944492ccf34bc248d309e03` | `28677856df44ab774c6364fd82760ab0f0c569e1b6bcb51bb1413d695d7f81b5` |

The production/test surface is 289 lines. Every function, test, and BUILD
attribute was read; the current checkout is byte-identical to this pin.

There is no `doc.go`, fixture, benchmark, generated source/input, or
build/platform variant.

## Rust ownership and behavior

`rust/crates/tidb-stats-handle-usage-collector` is the package owner:

- normal and high-priority queues each retain the source capacity of ten;
- normal sends are nonblocking until five minutes without an accepted update,
  after which they use the synchronous high-priority path;
- synchronous sends wait for high-priority capacity;
- every `start_worker` starts a source-shaped merge worker, with high-priority
  selection ahead of normal data;
- close is once-only, wakes and joins existing workers, and workers drain
  accepted updates before returning;
- all three source tests run in the package owner.
- the owner also retains a focused regression proving that a spawned Go
  session can enqueue synchronously after `Close`, because the source leaves
  `sessionCollector.closeCh` nil.

The former `tidb-stats::usage_collector` module and its aggregate test copy
were removed. The source-absent public timeout/capacity constants and capacity
assertion were also removed. `usage/indexusage` now consumes the distinct
package directly.

## Validation

Profile: Ready. This is a complete package authority refresh inside the
continuing repository audit, not a repository-wide readiness claim.

- Complete pinned-package inventory/diff gate: passed.
- Current and detached exact-master Go package tests: passed.
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -p
  tidb-stats-handle-usage-collector`: passed, 4 tests.
- The owner source and all `usage/indexusage` consumers were re-read; no
  source-vs-owner behavior gap or Rust-only production path was found.
- Ready Rust formatting, pinned repository lint, and `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk and unverified boundaries

- Correctness: the native mutex/condition-variable queues preserve accepted,
  rejected, synchronous, priority, drain, and close outcomes; scheduling among
  concurrently runnable workers remains intentionally nondeterministic.
- Compatibility: the public owner moved to the Go package boundary;
  `usage/indexusage` is the only production consumer and is rewired directly.
- Performance: each source channel maps to a preallocated bounded deque; the
  single normal call path remains nonblocking when full.
- Broader repository and integration suites remain outside this package-scoped
  gate and are tracked by the continuing parity ExecPlan.
