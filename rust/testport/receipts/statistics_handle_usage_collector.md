# `pkg/statistics/handle/usage/collector` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full and byte-compared
against the pin:

- `BUILD.bazel` — one public generic library and one short, flaky, three-shard
  test target;
- `collector.go` — the generic global/session collector implementation;
- `collector_test.go` — three normal, parallel-normal, and
  parallel-synchronous send tests.

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

The former `tidb-stats::usage_collector` module and its aggregate test copy
were removed. The source-absent public timeout/capacity constants and capacity
assertion were also removed. `usage/indexusage` now consumes the distinct
package directly.

## Validation

Profile: WIP. This completes one atomic package in the continuing parity audit,
not a repository-wide readiness claim.

- Complete pinned-package inventory/diff gate: passed.
- Pinned Go package test: passed.
- `cargo test -p tidb-stats-handle-usage-collector`: passed, 3 tests.
- `cargo check -p tidb-stats`: passed with the extracted dependency.
- Full `tidb-stats` translated integration suite: passed after removal of the
  supplemental test.
- Scoped Rust formatting and `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk and unverified boundaries

- Correctness: the native mutex/condition-variable queues preserve accepted,
  rejected, synchronous, priority, drain, and close outcomes; scheduling among
  concurrently runnable workers remains intentionally nondeterministic.
- Compatibility: the public owner moved to the Go package boundary;
  `usage/indexusage` is the only production consumer and is rewired directly.
- Performance: each source channel maps to a preallocated bounded deque; the
  single normal call path remains nonblocking when full.
- Repository-wide lint and integration suites remain deferred to the Ready
  profile after the full parity goal is complete.
