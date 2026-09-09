# `pkg/statistics/handle/autoanalyze/exec` package audit

Historical reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.
Current Go source rechecked at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 46 | `54e30b2c58b8e14798fd55374f97ddb9cce81bca` |
| `exec.go` | 157 | `8d7675dd3849f00f486aad176605f43a7b85ef37` |
| `exec_test.go` | 184 | `f3fc031207c82abd270f377efdf00d7f87d11031` |

All 387 lines were read. The package has three tests and no benchmark:
`TestExecAutoAnalyzes`, `TestExecAutoAnalyzeRewritesLegacyStatsVersionToV2`,
and `TestKillInWindows`.

## Go behavior

`AutoAnalyze` and `RunAnalyzeStmt` execute through the current session with
stats-version-2, analyze-snapshot, partition-prune, and process-tracking
options. They allocate and always release an auto-analyze process ID, recover
and log panics, record success/failure metrics, emit the escaped legacy-version
rewrite warning, and surface query interruption. The package also reads the
three global parameters and contains private ratio/time parsing helpers.

The tests validate the integrated execution and cache result, exact escaped
warning and version rewrite for table/partition/global statistics, and killing
an analyze when it leaves the configured window. They do not test the private
parsers in isolation.

## Rust implementation and integration

`tidb-stats-handle-autoanalyze-exec` now owns the package as a whole. Its
`auto_analyze` and `run_analyze_stmt` functions apply the v2,
analyze-snapshot, partition-prune, current-session, and process-tracking
options to the shared restricted executor. Process-ID release and process
untracking use drop guards, including the recovered-panic path. The package
also owns Go's success/failure metrics and logging, escaped legacy-version
warning, global parameter reads, ratio parser, and fixed-offset analysis-window
parser.

The production priority-queue source calls this shared path directly. It no
longer pre-renders identifiers or invokes a cache-only/server-only ANALYZE
shortcut. A checked-out system session supplies its registered connection ID;
the pool owns that ID for the session lifetime, while the per-statement guard
still invokes the generator's release operation. The global auto-analyze
process list is connected to the live process registry, and the domain's
post-statistics-GC window check interrupts registered analyzes outside the
configured interval, in Go's worker order.

The original three Go tests map to Rust coverage as follows:

| Go test | Rust evidence |
| --- | --- |
| `TestExecAutoAnalyzes` | package option/execution tests plus `auto_analyze_exec_uses_live_tracking_and_current_session_like_go` |
| `TestExecAutoAnalyzeRewritesLegacyStatsVersionToV2` | `source_legacy_rewrite_still_executes_as_version_two` |
| `TestKillInWindows` | `auto_analyze_window_check_kills_only_outside_the_window_like_go` |

There are no omitted build variants, generated inputs, fixtures, support
files, benchmarks, or fuzz tests at the pinned package boundary. The package
is complete; scheduling and job selection remain owned by the parent and
priority-queue packages.

## WIP validation

- `cargo check --offline -p tidb-server -p tidb-stats-handle-autoanalyze-exec`
  passed.
- `cargo test --offline -p tidb-stats-handle-autoanalyze-exec --lib -- --nocapture`
  passed: 5 passed.
- `cargo test --offline -p tidb-server --lib auto_analyze_exec_uses_live_tracking_and_current_session_like_go -- --nocapture`
  passed.
- `cargo test --offline -p tidb-server --lib auto_analyze_window_check_kills_only_outside_the_window_like_go -- --nocapture`
  passed.
- `cargo fmt --all -- --check` and `git diff --check` passed.

The broader existing priority-queue integration test currently fails before
execution, while observing asynchronous removal of a dropped-table queue job
(`current_jobs` is 2 instead of 1). The changed exec path has not run at that
assertion. This is recorded rather than masking the separate queue/lifecycle
failure in this package receipt.

No Go or Bazel source changed, so `make bazel_prepare` was not required. This
is a WIP package audit, not a repository-wide Ready parity claim.

## Follow-up: discardable auto-analyze returns (2026-09-06)

The complete three-artifact, 387-line Go package was re-read at current
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; it remains
byte-identical to the historical reference. The package contains the 157-line
production implementation, its 184-line three-test integration file, and a
46-line BUILD target. There is no `doc.go`, fixture, generated input/output,
benchmark, fuzz target, example, or platform/build-tag variant. Every Go
function and test was inventoried, and all direct Rust call sites in the
priority-queue, refresher, and server owners were checked.

The complete Rust owner is the single 617-line `src/lib.rs` plus its manifest.
Its five pre-existing native tests cover the Go execution, legacy-version
rewrite, interruption/window, panic recovery, and parameter-parser behavior.

Go permits callers to discard `AutoAnalyze` and `ParseAutoAnalyzeRatio`; Rust's
direct `auto_analyze` and `parse_auto_analyze_ratio` counterparts instead
emitted two `unused_must_use` diagnostics. The annotations were removed
without changing analyze options, process tracking/release, metrics, warning
logging, panic recovery, or ratio parsing. A focused executable regression
invokes both APIs under `#[deny(unused_must_use)]`; it failed before the
implementation edit with exactly two diagnostics and passes afterward.

Ready validation for this follow-up (Rust scope, per the request to skip Go
code execution):

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-autoanalyze-exec --lib source_return_values_may_be_ignored_like_go --offline --locked -- --nocapture
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-autoanalyze-exec --offline --locked -- --test-threads=1
PASS; 6 unit tests passed, 0 failed; doc tests had 0 tests.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-stats-handle-autoanalyze-exec --all-targets --offline --locked
PASS; pre-existing dependency warnings remain outside this crate.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source/tests and parity documentation changed. No Go, Bazel, Cargo
metadata, or module dependency changed, so `make bazel_prepare` is not
required. The Go tests and live server integration were not rerun, per the
Rust-only scope; the complete Rust owner suite and all-target check cover the
changed caller contract.
