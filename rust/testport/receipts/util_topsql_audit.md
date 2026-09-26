# `pkg/util/topsql` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`56970b286a362b1f0b150c7665453c8f5ff997a9` (2026-09-23). Since the earlier
audit at `42db2099af50704e424b792626f10a87f4247413`, three commits touched
this package: `17b780783925eea71af5e2bdd1a0b1c171efc650` (report backpressure,
panic accounting, and stats cap), `8bccb81a1c0a81d33ebca76545e465535e374870`
(remove deprecated RU-v2 plumbing), and
`51263506a5005ca119f3a191a21c3ea91a419b16` (report finalized RU-v2
consumption).

This receipt records the complete source inventory and the behavior that can
be implemented in the current Rust owners. It does not claim that all of Go's
TopSQL package is transcreated: the gRPC reporter/data-sink, profiler, and
top-level wiring remain explicit integration boundaries.

## Complete Go package inventory

The package has exactly 47 tracked artifacts: 19 production Go files, 20 Go
test/harness files, and 8 Bazel build files, totaling 14,133 Go/Bazel lines.
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
| `reporter/datamodel.go` | 761 |
| `reporter/datamodel_test.go` | 543 |
| `reporter/datasink.go` | 152 |
| `reporter/datasink_test.go` | 326 |
| `reporter/main_test.go` | 33 |
| `reporter/metrics/BUILD.bazel` | 24 |
| `reporter/metrics/metrics.go` | 82 |
| `reporter/metrics/metrics_test.go` | 29 |
| `reporter/mock/BUILD.bazel` | 17 |
| `reporter/mock/pubsub.go` | 67 |
| `reporter/mock/server.go` | 283 |
| `reporter/pubsub.go` | 407 |
| `reporter/pubsub_test.go` | 739 |
| `reporter/report_ticker.go` | 55 |
| `reporter/reporter.go` | 445 |
| `reporter/reporter_test.go` | 1,473 |
| `reporter/ru_datamodel.go` | 699 |
| `reporter/ru_datamodel_test.go` | 766 |
| `reporter/ru_window_aggregator.go` | 243 |
| `reporter/ru_window_aggregator_test.go` | 949 |
| `reporter/single_target.go` | 432 |
| `reporter/single_target_test.go` | 224 |
| `reporter/topru_case_runner_test.go` | 283 |
| `reporter/topru_generated_cases_test.go` | 136 |
| `state/BUILD.bazel` | 24 |
| `state/state.go` | 173 |
| `state/state_test.go` | 87 |
| `stmtstats/BUILD.bazel` | 52 |
| `stmtstats/aggregator.go` | 290 |
| `stmtstats/aggregator_bench_test.go` | 156 |
| `stmtstats/aggregator_test.go` | 625 |
| `stmtstats/kv_exec_count.go` | 76 |
| `stmtstats/kv_exec_count_test.go` | 45 |
| `stmtstats/main_test.go` | 33 |
| `stmtstats/rustats.go` | 85 |
| `stmtstats/stmtstats.go` | 454 |
| `stmtstats/stmtstats_test.go` | 1,212 |

The current tree contains 114 named tests (plus four `TestMain` harnesses) and
9 benchmarks. Each was checked against its Rust owner or an explicit boundary.
The generated Top-RU cases are source-shaped test data, not generated
production code.

## Rust ownership and parity decisions

Rust ownership is split between `tidb-util::topsql_state`,
`topsql_stmtstats/{aggregator,kv_exec_count,rustats,stmtstats}`, the separate
`tidb-util::ruv2_metrics` execution implementation, and
`topsql_reporter/{datamodel,metrics,ru_datamodel,ru_window_aggregator}`. Go's
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

### Complete `reporter/metrics` leaf

`pkg/util/topsql/reporter/metrics` has three tracked artifacts: its 82-line
`metrics.go`, 29-line `metrics_test.go`, and 24-line `BUILD.bazel`; it has no
generated input. Its `TestIgnoreReportDataByBackpressureCounter` verifies
that the report-backpressure handle is available and increments by one.
Rust owns the leaf in `tidb-util::topsql_reporter::metrics`, with all 11
ignored-counter handles, 10 report-duration observers, and four report-data
observers bound to the same three Prometheus families and label combinations.
The server registers those families from this owner, so there is no second
TopSQL metric definition in `tidb-server`. The SQL/plan admission caps, RU
aggregation cap, and late compacted RU drops increment their corresponding
handles. Rust leaf tests check every label series, histogram family definition,
and the backpressure counter increment contract against Go. The parent
reporter worker that must increment that handle on channel drops and observe
report timings remains unavailable; the `reporter` package is still
incomplete.

Validation for this leaf:

- `go test ./pkg/util/topsql/reporter/metrics -count=1` — passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib topsql_reporter:: -- --test-threads=1` — 69 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib topsql -- --test-threads=1` — 115 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib topsql_stmtstats::aggregator::tests::drain_push_ru_caps_at_max -- --exact` — passed.
- `cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --all-targets` and the equivalent `-p tidb-server --all-targets` — passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-server --lib server_metrics::init_tests::init_registers_every_family -- --exact` — passed.
- `make -o tools/bin/revive lint` — passed using the already-installed pinned linter.
- Scoped `rustfmt --check` and `git diff --check` — passed.

No Go or Bazel files changed in this leaf update, so `make bazel_prepare` was
not required. The workspace-wide `cargo fmt --all -- --check` still reports
pre-existing formatting differences outside this change.

No Rust-only TopSQL behavior was found that could be removed without deleting
the only executable owner of a Go contract. The Rust mutex-backed maps and
Prometheus-backed telemetry preserve the Go-visible behavior without adding
wire or SQL behavior.

## Current-master RU-v2 finish accounting (`2026-09-23`)

Current Go no longer stores `RUV2Metrics` or `RUV2Weights` in
`ExecBeginInfo`/`ExecutionContext`. `currentRUTotal` returns zero for RU-v2, so
in-flight Top-RU samples are v1-only. At statement finish, the executor
finalizes RU-v2 counters, computes `TotalRUV2`, and passes that scalar through
`ExecFinishInfo`; `addRUOnFinishLocked` uses it for RU-v2 and keeps the existing
`RUDetails` RRU+WRU calculation for v1.

Rust now follows that collector contract: `ExecBeginInfo` and
`ExecutionContext` carry no RU-v2 metrics/weights, `ExecFinishInfo` carries
`total_ru_v2`, and RU-v2 sampling returns zero until finish. The former Rust
tests for live RU-v2 metrics and drain-only counters were removed and replaced
with coverage for zero in-flight RU-v2 and finalized finish totals for both RU
versions. The TiDB Rust execution adapter that computes and supplies the
finalized value is still absent, as are the reporter, profiler, and top-level
integration owners; this remains audit evidence, not a complete Go-package
transcreation claim.

Validation for this current-master delta:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib topsql_stmtstats:: -- --test-threads=1` — passed; all 41 statement-stats owner tests.
- `rustfmt --edition 2021 crates/tidb-util/src/topsql_stmtstats/mod.rs crates/tidb-util/src/topsql_stmtstats/rustats.rs crates/tidb-util/src/topsql_stmtstats/stmtstats.rs` — passed.
- `git diff --check` — passed.

No Go, Bazel, or module files changed, so `make bazel_prepare` was not
required.

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

## Rust-only diagnostic alignment for `stmtstats` (`2026-09-06`)

The complete `pkg/util/topsql/stmtstats` production, test, benchmark, and
Bazel inventory above was rechecked before editing. Fourteen Rust
`#[must_use]` annotations on direct Go-shaped APIs were removed:
`DefaultRUVersion`, `NormalizeRUVersion`, `Aggregator.new`/
`currentRUVersion`/`closed`, `CreateKvExecCounter`, `RPCInterceptor`,
`newSQLPlanDigest`, `NewKvStatementStatsItem`, `NewStatementStatsItem`,
`CreateStatementStats`, `StatementStats.Take`, `StatementStats.Finished`, and
`StatementStats.MergeRUInto`. Rust-only conveniences such as map constructors,
`BinaryDigest::as_bytes`, collector membership probes, and the interceptor's
test-facing target accessors remain annotated because they have no direct Go
return API.

The focused `#[deny(unused_must_use)]` regressions named
`source_api_returns_may_be_ignored_like_go` discard every affected Go-shaped
return. On a detached pre-fix worktree at `e48748cd816`, the filtered compile
failed with exactly fourteen diagnostics; after the fix the four focused tests
pass. The complete `topsql_stmtstats` owner suite passes all 42 tests.

Validation for this bounded Rust-only batch:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib contract_tests::source_api_returns_may_be_ignored_like_go --offline --locked` — passed after the fix; the detached pre-fix owner failed with the expected fourteen diagnostics across three contract-test modules.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib topsql_stmtstats::stmtstats::tests::source_api_returns_may_be_ignored_like_go --offline --locked -- --exact` — passed after the fix.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib topsql_stmtstats:: --offline --locked -- --test-threads=1` — passed; all 42 owner tests.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-util --all-targets --offline --locked` — passed.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `make lint` — passed under the Ready profile.
- `git diff --check` — passed.

No Go, Bazel, module, or Cargo manifest file changed, so `make
bazel_prepare` was not required.

## Rust-only diagnostic alignment for `reporter` (`2026-09-06`)

The complete `pkg/util/topsql/reporter` inventory above was rechecked before
editing. Forty-four Rust `#[must_use]` annotations on source-shaped
datamodel, RU datamodel, and RU window-aggregator APIs were removed, covering
the Go proto conversions, Top-N helpers, record/map constructors and queries,
RU collecting constructors/compaction, interval alignment, and report
construction. The two `FloatCounter` annotations remain because that counter
is a local Rust test/metrics boundary rather than a Go reporter return API.

The focused `#[deny(unused_must_use)]` regression
`topsql_reporter::contract_tests::source_api_returns_may_be_ignored_like_go`
discards every affected return. On a detached pre-fix worktree at
`293c267f891`, it failed with exactly 44 diagnostics; after the fix it passes.
The complete reporter data-model owner suite passes all 67 tests.

Validation for this bounded Rust-only batch:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib topsql_reporter::contract_tests::source_api_returns_may_be_ignored_like_go --offline --locked -- --exact` — passed after the fix; the detached pre-fix owner failed with the expected 44 diagnostics.
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib topsql_reporter:: --offline --locked -- --test-threads=1` — passed; all 67 reporter owner tests.
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
