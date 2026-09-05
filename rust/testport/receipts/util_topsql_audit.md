# `pkg/util/topsql` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`42db2099af50704e424b792626f10a87f4247413` (2026-09-02). The only commit
touching this package after the Rust extraction point is
`17b780783925eea71af5e2bdd1a0b1c171efc650` (`topsql: reduce reporter loss,
fix panic accounting, and enforce statement stats cap`).

This receipt records the complete source inventory and the behavior that can
be implemented in the current Rust owners. It does not claim that all of Go's
TopSQL package is transcreated: the gRPC reporter/data-sink, profiler, and
top-level wiring remain explicit integration boundaries.

## Complete Go package inventory

The package has exactly 47 tracked artifacts: 19 production Go files, 20 Go
test/harness files, and 8 Bazel build files, totaling 14,542 Go/Bazel lines.
Every production file, test, benchmark, generated Top-RU case carrier, mock,
fixture/support artifact, and build target was read and enumerated before
editing. There is no `doc.go`, `testdata` fixture tree, generated production
source, or platform-specific Go variant.

| Artifact | Lines |
| --- | ---: |
| `BUILD.bazel` | 51 |
| `topsql.go` | 214 |
| `main_test.go` | 33 |
| `topsql_test.go` | 458 |
| `collector/BUILD.bazel` | 33 |
| `collector/cpu.go` | 345 |
| `collector/main_test.go` | 196 |
| `collector/mock/BUILD.bazel` | 17 |
| `collector/mock/mock.go` | 228 |
| `reporter/BUILD.bazel` | 78 |
| `reporter/datamodel.go` | 815 |
| `reporter/datamodel_test.go` | 663 |
| `reporter/datasink.go` | 152 |
| `reporter/datasink_test.go` | 326 |
| `reporter/main_test.go` | 33 |
| `reporter/metrics/BUILD.bazel` | 22 |
| `reporter/metrics/metrics.go` | 82 |
| `reporter/metrics/metrics_test.go` | 29 |
| `reporter/mock/BUILD.bazel` | 17 |
| `reporter/mock/pubsub.go` | 67 |
| `reporter/mock/server.go` | 283 |
| `reporter/pubsub.go` | 407 |
| `reporter/pubsub_test.go` | 739 |
| `reporter/report_ticker.go` | 55 |
| `reporter/reporter.go` | 452 |
| `reporter/reporter_test.go` | 1,538 |
| `reporter/ru_datamodel.go` | 699 |
| `reporter/ru_datamodel_test.go` | 766 |
| `reporter/ru_window_aggregator.go` | 261 |
| `reporter/ru_window_aggregator_test.go` | 981 |
| `reporter/single_target.go` | 451 |
| `reporter/single_target_test.go` | 273 |
| `reporter/topru_case_runner_test.go` | 283 |
| `reporter/topru_generated_cases_test.go` | 136 |
| `state/BUILD.bazel` | 24 |
| `state/state.go` | 173 |
| `state/state_test.go` | 87 |
| `stmtstats/BUILD.bazel` | 52 |
| `stmtstats/aggregator.go` | 295 |
| `stmtstats/aggregator_bench_test.go` | 156 |
| `stmtstats/aggregator_test.go` | 667 |
| `stmtstats/kv_exec_count.go` | 76 |
| `stmtstats/kv_exec_count_test.go` | 45 |
| `stmtstats/main_test.go` | 33 |
| `stmtstats/rustats.go` | 85 |
| `stmtstats/stmtstats.go` | 454 |
| `stmtstats/stmtstats_test.go` | 1,212 |

The inventory contains 291 production declarations, 145 test declarations
(including 9 benchmarks), and all 145 named source tests were checked against
their Rust owner or an explicit boundary. The generated Top-RU cases are
source-shaped test data, not generated production code.

## Rust ownership and parity decisions

Rust ownership is split between `tidb-util::topsql_state`,
`topsql_stmtstats/{aggregator,kv_exec_count,ru_details,rustats,ruv2_metrics,stmtstats}`
and `topsql_reporter/{datamodel,ru_datamodel,ru_window_aggregator}`. The
reporter module documents that it is a data-model layer only. Go's
`collector/cpu.go`, top-level `topsql.go`, `reporter/datasink.go`,
`reporter/pubsub.go`, `reporter/reporter.go`, `reporter/single_target.go`,
their mocks, and the gRPC/profiler consumers have no dependency-closed Rust
owner; no cache-only substitute or fabricated transport API was added.

Commit `17b7807839` makes four relevant changes:

* `stmtstats/aggregator.go` changes registration to a CAS reservation with
  `current >= maxStmtStatsSize`, preventing both the exact-boundary max+1 bug
  and concurrent cap overshoot. Rust now uses the same CAS loop before
  publishing a session, with a focused exact-boundary and 64-worker regression.
* `reporter/datamodel.go` moves normalized SQL/plan admission into
  generation-local reservations and retries registration if `take` swaps the
  generation. Rust already takes the map mutex in `take`; moving the capacity
  check under that same mutex gives the same no-lost-registration guarantee
  without adding a second generation abstraction. Focused SQL and plan
  registration races assert that `MaxCollect = 1` never admits more than one
  entry.
* `reporter/reporter.go` adds a bounded report-data channel and drops closed
  report windows under backpressure; `reporter/metrics/metrics.go` adds the
  corresponding counter. Rust has no reporter worker or channel/data-sink
  owner, so this remains an integration boundary.
* `reporter/single_target.go` wraps each concurrent gRPC send in recovery and
  accounts panics as failed reports. Rust has no `SingleTargetDataSink` or
  gRPC agent owner, so this transport behavior remains an integration boundary.

The Go `reporter/metrics` leaf now also exposes and initializes
`IgnoreReportDataByBackpressureCounter` with the
`ignore_report_data_by_backpressure` label. Its package regression verifies
that the bound counter is usable and increments monotonically. The parent
reporter worker that must increment this handle remains an explicit boundary.

No Rust-only TopSQL behavior was found that could be removed without deleting
the only executable owner of a Go contract. The Rust mutex-backed maps and
process counters are representation choices, not extra wire or SQL behavior.

## Rust-only diagnostic alignment (`2026-09-06`)

The complete `pkg/util/topsql/state` inventory above was rechecked before
editing. Its Rust owner, `rust/crates/tidb-util/src/topsql_state.rs`, carried
four `#[must_use]` annotations on the direct Go-shaped queries
`top_sql_enabled`, `top_profiling_enabled`, `top_ru_enabled`, and
`get_top_ru_item_interval`. Go permits callers to inspect these package-level
flags and interval values without using the result, so the annotations were
removed without changing state transitions or synchronization.

The focused `#[deny(unused_must_use)]` regression
`topsql_state::tests::source_api_returns_may_be_ignored_like_go` discards all
four results. On a detached pre-fix worktree at `043654a5908`, it failed with
exactly four `unused_must_use` diagnostics; after the fix it passes. The
existing five state tests continue to pass.

Validation for this bounded Rust-only batch:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib topsql_state::tests::source_api_returns_may_be_ignored_like_go --offline --locked -- --exact` — passed after the fix; the detached pre-fix owner failed with the four expected diagnostics.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib topsql_state::tests --offline --locked -- --test-threads=1` — passed; all five state tests.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-util --all-targets --offline --locked` — passed.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `make lint` — passed under the Ready profile.
- `git diff --check` — passed.

No Go, Bazel, module, or Cargo manifest file changed, so `make
bazel_prepare` was not required.

## Validation (Ready profile)

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/topsql/reporter/metrics -run '^TestIgnoreReportDataByBackpressureCounter$' -count=1` — passed after the implementation; before it the test failed to compile because the counter was undefined.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/topsql/reporter/metrics -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/util/topsql/reporter -run 'Test_normalized(SQL|Plan)Map_(register|take|toProto)$' -count=1` — passed; failpoints enabled and disabled with refcount 0.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/util/topsql/stmtstats -run 'Test(AggregatorRegisterCollect|DrainPushRUCapsAtMax|AggregatorRunOrderKeepsFinishedRU|AggregatorDetectsRUVersionHandover)$' -count=1` — passed; failpoints enabled and disabled with refcount 0.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-util --lib topsql -- --test-threads=1` — 108 passed, including all TopSQL Rust owner tests and the three focused cap regressions.
- `rustup run nightly-2026-08-22 rustfmt --edition 2021 --check crates/tidb-util/src/topsql_stmtstats/aggregator.rs crates/tidb-util/src/topsql_reporter/datamodel.rs` — passed after formatting.
- `git diff --check` — passed.
- `make bazel_prepare` — blocked because the local checkout has no `bazel` executable; required after adding the package test target.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed as the Ready gate after the package and receipt edits.

Go and Bazel files changed for the metrics leaf, so `make bazel_prepare` was
required; it is blocked locally because the `bazel` executable is unavailable.

## Risks and unverified surfaces

- Correctness risk is concentrated in the registration cap and map/take
  locking order; the CAS and two concurrent map tests exercise the boundary.
- Compatibility risk remains at the profiler, gRPC, pubsub, report-worker,
  and top-level `SetupTopProfiling` integration boundary. Those Go files were
  inventoried but cannot be implemented in `tidb-util` without their absent
  Rust protocol and server owners.
- Performance impact is one CAS loop only on session registration and one
  mutex acquisition already required for map insertion; steady-state map
  reads and report conversion are unchanged.
- The full Go-master reporter integration suite and Bazel sharded targets were
  not run locally; only the failpoint-enabled source tests listed above were
  run. The full Rust `tidb-util` workspace was not rebuilt beyond the scoped
  TopSQL lib target and its existing owners.
