# `pkg/statistics/handle/autoanalyze/priorityqueue/intervaltimezone` — complete test-package transcreation

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 22 | `5051d7da615b034fc75efb62d6e213d68266ef1c` | `eac7690b8c60d18e81d981584a41a0dfafbfc6f5843e105708999572b9ff6786` |
| `interval_timezone_test.go` | 81 | `95b063d3df5e17f521bbcb947f40a4fa1e111216` | `5f1f32179cde89ece89f49204154712abf5dff070cb6765297f63a683a8b9dbb` |
| `main_test.go` | 34 | `fbeff3a7847c428e6fddee7c77431ec4509580aa` | `f0b976877b7240ec19d32807de4281338bb354c4cc4a2ae8e6781a419fbf7aea` |

All 137 lines were read. This external test package has no production source,
`doc.go`, support helper, fixture, generated input or output, benchmark, fuzz
target, example, build-tag variant, or platform variant. It contains one
assertion test, one shared leak-checking `TestMain`, and one Bazel test target.

## Behavior and Rust mapping

`TestLastFailedAnalysisDurationUseCorrectTimezone` intentionally contaminates
both TiDB's system timezone and Go's `time.Local` with `America/New_York`
before bootstrap, creates a mock store/domain, publishes `Europe/Berlin` as
the global session timezone, inserts/starts/fails an analyze job through the
statistics handle, then queries the duration through the same
statistics-session pool. A duration in `(0, 1 minute)` proves the persisted
start instant and reused session timezone agree.

Rust executes that complete observable contract through
`cluster_session_node::tests::unistore_cop::failed_analysis_duration_resets_the_pooled_session_timezone`:

- `ClusterSessionFactory` owns the advanced statistics-session pool used by
  `ClusterPriorityQueueSource`;
- each `call_with_sctx` checkout runs the transcreated
  `UpdateSCtxVarsForStats`, so the reused `America/New_York` session replaces
  its stale `time_zone` with the live `Europe/Berlin` global value;
- analyze-job insert/start/finish use the production TiKV persistence plans;
  Rust writes their UTC instants directly, which is the stored result of Go's
  `CONVERT_TZ(utc_string, '+00:00', @@TIME_ZONE)` statement path;
- the duration is read through the production restricted SQL/coprocessor path,
  including the request timezone during TIMESTAMP response encoding.

Rust does not mutate process-global timezone state in the shared server test
binary. Instead, the test deterministically checks out the same pooled session
under `America/New_York`, publishes `Europe/Berlin`, and proves both the reused
session value and `TIMESTAMPDIFF` result. The explicit two-second stored
interval replaces Go's nondeterministic sleep while preserving the original
strict bounds. Source-absent assertions on the pool's idle size were removed;
they constrained Rust's internal structure rather than the Go test behavior.

`main_test.go::TestMain` contributes common test setup plus a Go-specific
goroutine leak checker and five allowlisted Go goroutines. Rust's test harness
has no goroutine or package-level `TestMain` hook, so it has no executable Rust
counterpart. `BUILD.bazel` maps to the existing `tidb-server` aggregate test
target; its short/flaky scheduling metadata has no Rust semantic behavior.

No new behavioral bug was found in the 2026-08-30 re-audit, so no artificial
fail-before case was created. The only code change removes two source-absent
pool-size assertions and corrects the pinned test name in the source comment.

## Validation

- PASS: `cargo test --locked -p tidb-server cluster_session_node::tests::unistore_cop::failed_analysis_duration_resets_the_pooled_session_timezone -- --exact --nocapture`
  (the sandboxed attempt could not read `sysctl hw.memsize`; the identical
  host-access rerun passed 1/1)
- PASS: `cargo check --locked -p tidb-server` (pre-existing warnings remain)
- PASS: `rustfmt --edition 2021 --check crates/tidb-server/src/cluster_session_node/tests/unistore_cop.rs`
- PASS: `git diff --check`
- BLOCKED by another agent's owned
  `crates/tidb-executor/src/driver/catalog/sync_load.rs` indentation diff:
  `cargo fmt --all -- --check`

No Go or Bazel file changed, so `make bazel_prepare` is not required. The
primary integration batch owns the Ready-profile `make lint` rerun.
