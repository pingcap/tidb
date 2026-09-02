# `pkg/util/stmtsummary` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`94eb995357f34b7bab4889a82f0405797046447d` (2026-09-02).

This receipt records the current Go-master delta implemented by the native
`tidb-stmtsummary` owner. It does not claim that the whole Go package is
transcreated: the v2 reader/logger/table-test artifacts and the executor,
infoschema, and planner integration remain an explicit dependency boundary.

## Complete Go package inventory

The package has exactly 22 tracked artifacts and 11,214 Go lines. Every
production file, test, benchmark, nested table-test harness, and Bazel target
was read and enumerated before editing. There is no `doc.go`, fixture tree,
generated source, or platform-specific Go variant.

| Artifact group | Files | Lines | Inventory |
| --- | ---: | ---: | --- |
| v1 production | `evicted.go`, `reader.go`, `statement_summary.go` | 2,665 | eviction rollups, column factories, summary map/statistics, RU and network summaries |
| v1 tests/harness | `evicted_test.go`, `main_test.go`, `statement_summary_test.go` | 2,842 | eviction, row/column, concurrency, metrics, history, and test bootstrap |
| v1 build | `BUILD.bazel` | 66 | library/test targets and dependencies |
| v2 production | `v2/column.go`, `v2/logger.go`, `v2/reader.go`, `v2/record.go`, `v2/stmtsummary.go` | 3,366 | persistent records, columns, rotation, logging, and history reader |
| v2 tests/harness | `v2/column_test.go`, `v2/main_test.go`, `v2/reader_test.go`, `v2/record_test.go`, `v2/stmtsummary_benchmark_test.go`, `v2/stmtsummary_test.go` | 1,398 | column/record/reader/rotation tests and three benchmarks |
| v2 table harness | `v2/tests/main_test.go`, `v2/tests/table_test.go` | 778 | information-schema table integration tests |
| v2 build | `v2/BUILD.bazel`, `v2/tests/BUILD.bazel` | 99 | production, test, and table-test targets |

The Go source contains 240 non-test declarations, 76 test/benchmark
declarations (including three benchmarks). Rust ownership is split across `src/statement_summary.rs`,
`src/evicted.rs`, `src/reader.rs`, and the v2 `column.rs`, `record.rs`, and
`stmtsummary.rs` modules. Rust has no v2 `reader.rs`, `logger.rs`, or
`v2/tests` equivalent yet.

## Go-master delta and parity decisions

Three Go-master commits changed this owner since the Rust extraction point:

* `8bab3c26d7` prevents an evicted “other” row from leaking into a later
  current interval. The Rust reader already carries the same begin-time guard.
* `655769534b` snapshots evicted-count fields under the evicted mutex and uses
  `Peek` plus record locking during internal-query cleanup. Rust's evicted
  aggregate is always accessed through its owning mutex, and the v2 window and
  record are separate mutexes with the same lock order; no additional adapter
  path was needed.
* `381ac705f9` adds `IAExecCountStr = "IA_REMOTE_EXEC_COUNT"` to both
  reader/column surfaces and tracks executions with at least one IA remote-read
  segment. IA counts are incremented only when
  `GetIARemoteReadSegmentStats(...).Count > 0`; segment count/bytes/wait-time
  statistics continue to sum every execution. The value is included in v1
  eviction rollups, v2 record merges, and v2 persisted JSON as
  `ia_remote_exec_count`.

The same Go-master source also replaces the old v1 plan-error nil path with
`PlanDiscardedEncoded` and initializes a newly-created summary's
`isInternal` from its first statement before applying logical AND on later
  statements. Rust now mirrors those two behaviors as well as IA tracking in
  v1 `StmtSummaryStats` and v2 `StmtRecord`, registers the new column factory,
  preserves Go column order, and emits the JSON field. Focused source-derived
  regressions cover one IA execution plus one ordinary execution, current and
history rows, chunk round-trips, eviction aggregation, v2 merges and JSON,
plan-encoding failure fallback, and internal-only cleanup.

The 2026-09-02 Go package batch restores all of the above root-package
behavior in `pkg/util/stmtsummary` from the current Go master source,
including the eviction lock snapshot, stale-interval filter, IA
execution-count column, internal-query LRU cleanup, plan-error fallback, and
associated BUILD shard metadata. No Rust owner source was changed in this
batch; the v2 and executor/infoschema/planner boundaries remain explicit
below.

## Validation (Ready profile)

- Failpoint-enabled Go root targeted run:
  `./tools/check/failpoint-go-test.sh pkg/util/stmtsummary -run 'Test(ToDatumIAColumns|ToDatumIAColumnsChunkRoundTrip|AddStatementPlanEncodeError|ToEvictedCountDatumConcurrent|CurrentRowsExcludePreviousIntervalEvictedOther|DisablingInternalQueryPreservesLRUOrder)$' -count=1`
  — passed.
- Failpoint-enabled Go root full package run:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/util/stmtsummary -count=1 -vet=off`
  — passed (0.553s).
- Failpoint-enabled Go v2 targeted run:
  `./tools/check/failpoint-go-test.sh pkg/util/stmtsummary/v2 -run 'Test(IAAvgColumns|IAAvgColumnsChunkRoundTrip|HistoryReader|StmtRecord|StmtWindow)' -count=1`
  — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-stmtsummary --lib -- --test-threads=1` — 46 passed, including the two new v1 regressions.
- `cargo +nightly-2026-08-22 check --offline --locked -p tidb-stmtsummary --all-targets` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/executor -run '^TestAdminShowSlowIARemoteReadStats$' -count=1` — passed against Go master.
- `rustup run nightly-2026-08-22 rustfmt --edition 2021 --check` over all five edited Rust owner files — passed.
- `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- The required `make bazel_prepare` was attempted for the BUILD shard and new
  top-level tests, but is blocked locally because no `bazel` executable is
  installed (`make: bazel: No such file or directory`).

The broad `pkg/util/stmtsummary/v2/tests` table suite was started with the
failpoint runner but did not complete within the local run window; no table
fixture was changed in this batch.

OpenSSL-dependent Rust commands use the bundled Poppler root as `OPENSSL_DIR`
and its `lib` directory in `DYLD_LIBRARY_PATH`.

## Risks and unverified surfaces

- Correctness risk is concentrated in the IA count predicate and the stable
  persisted JSON field name/type, plus the first-statement internal flag and
  plan-error fallback; all four are covered by source-derived tests.
- Compatibility risk remains at the integration boundary: Rust does not yet
  implement Go's v2 history reader/logger/table tests or the executor
  `SHOW SLOW` and infoschema/planner changes from the same Go commit.
- Performance impact is one conditional increment per summary update and one
  additional persisted integer; no new allocation or hot-path scan is added.
- The Go executor regression passes with the required `-tags=intest`; the
  broader executor, infoschema, and planner integration remains outside this
  crate's current validation surface.
