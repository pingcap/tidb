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

## Follow-up: discardable collector construction returns (2026-09-06)

The complete three-artifact, 289-line package was rechecked at current
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; it is
byte-identical to the earlier pin. The production collector, all three Go
tests, and Bazel metadata remain the whole package, with no docs, fixtures,
generated inputs/outputs, benchmarks, fuzz targets, or platform variants. The
Rust owner inventory is `Cargo.toml`, `src/lib.rs`, and
`tests/collector_source.rs`.

Go allows callers to discard `NewGlobalCollector` and `SpawnSession` results.
Rust added `#[must_use]` to both direct counterparts, producing diagnostics
that do not exist at the source boundary. The annotations were removed; queue
capacity, timeout escalation, worker priority, close/drain, and send behavior
are unchanged. The new source regression invokes both calls under
`#[deny(unused_must_use)]`. It failed before the production edit with exactly
two diagnostics and passes afterward.

Ready validation for this follow-up:

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-usage-collector --test collector_source constructor_and_spawn_result_may_be_ignored_like_go --offline --locked -- --exact --nocapture --test-threads=1
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-usage-collector --offline --locked -- --test-threads=1
PASS; 5 integration tests passed, 0 failed; doc tests had 0 tests.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-stats-handle-usage-collector --all-targets --offline --locked
PASS.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source/tests and parity documentation changed. No Go, Bazel, Cargo
metadata, or module dependency changed, so `make bazel_prepare` is not
required.
