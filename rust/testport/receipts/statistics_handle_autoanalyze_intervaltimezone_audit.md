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

## Behavior and Rust mapping

The sole test intentionally contaminates both TiDB's system timezone and
Go's `time.Local` before bootstrap, creates a mock store/domain, sets a
different global session timezone, inserts/starts/fails an analyze job through
the real statistics handle, then queries the duration through the handle's
session pool. A small positive result proves pooled sessions reset to the
global timezone.

Rust now exercises that complete behavior through
`cluster_session_node::tests::unistore_cop::failed_analysis_duration_resets_the_pooled_session_timezone`:

- `ClusterSessionFactory` owns one capacity-200 advanced system-session pool,
  matching Go Domain ownership and `MaxSessionPoolSize`.
- every checkout runs the already-transcreated `UpdateSCtxVarsForStats`, so a
  reused session replaces its stale `time_zone` with the live global value;
- analyze-job insert/start/finish rows use the production TiKV persistence
  plans, and the duration is read through the ordinary restricted SQL path;
- the Unistore coprocessor response encoder now passes the request timezone to
  Go-shaped `EncodeValue`. This is required because scan decode has already
  converted a `TIMESTAMP` from UTC to the session location; flattening without
  that location made the root decoder apply the offset twice.

Rust does not mutate process-global timezone state in this shared 380-test
binary. Instead, the test deterministically contaminates the same pooled
session with `America/New_York`, publishes `Europe/Berlin`, and proves the
reused session, pushed scan, and `TIMESTAMPDIFF` produce a small positive
duration. That preserves the Go test's observable contract without adding a
Rust-only production branch.

`main_test.go` contributes only Go's test leak checker; Rust's test harness has
no package-level leak-check hook to transcreate. `BUILD.bazel` is represented
by the existing Cargo targets and no generated input is omitted. The package
is complete at the pinned commit.

## Validation

- `cargo test -p tidb-server failed_analysis_duration_resets_the_pooled_session_timezone -- --nocapture`
- `cargo test -p tidb-session time_zone_uses_go_parser_and_error`
- `cargo check -p tidb-server`
- `cargo fmt --all -- --check`
- `git diff --check`
