# `pkg/util/stmtsummary` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`febee17ec716d86b1e355e5400ef9e4f4f190bad` (2026-09-02).

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

The same package boundary includes the 2026-09-02 `pkg/util/stmtsummary/v2`
batch. Its complete direct inventory (five production files, six test/
benchmark files, and one BUILD target) was re-read before editing. Go-master
behavior now records IA execution counts in v2 records and JSON, exposes the
column factory and history/memory readers, snapshots the evicted window begin
time, and serializes internal-query cleanup under the record mutex. The
nested `v2/tests` table package was audited but unchanged; Rust's missing v2
reader/logger/table ownership and SQL integration remain explicit boundaries.

## Latest Go-master follow-up (`78cac443a4f46c13bfe27eb247b5c80657952547`)

The fetched Go `origin/master` is now `78cac443a4f46c13bfe27eb247b5c80657952547`.
This package-scoped batch applies that commit's 15-file delta (569 insertions,
135 deletions) as one unit across the already inventoried 22 artifacts. v1
history collection now returns the newest retained intervals in chronological
order, average KV/PD/backoff/write-response columns divide by execution count,
table-name serialization skips empty table entries, and history reset keeps
the newest element. The v2 record uses the same table-name filtering and
normalized SQL formatting. The v2 history reader now enumerates paths before
opening files, preserves a rotating current file by inode, bounds open file
descriptors through worker-side close, and handles metadata lookup failures;
the v2 table harness covers open-ended time ranges. Focused regressions were
added for each of these source behaviors, including the pre-fix evicted-history
failure reproduced locally.

The v2 table test could not compile in this shared workspace because an
unrelated in-progress `pkg/statistics/handle/util` edit references the absent
`vardef.TiDBAnalyzeStoreBatchSize` symbol. That edit is outside this package
and was preserved. The Rust owner and executor/infoschema/planner integration
remain explicit boundaries; no speculative Rust changes were made.

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
- Pre-fix v2 regression run (before restoring the Go fields/column): the
  failpoint runner failed to compile with the expected missing
  `IAExecCountStr` and `StmtRecord.IAExecCount` symbols.
- Post-fix failpoint-enabled v2 focused run:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/util/stmtsummary/v2 -run '^(TestIAAvgColumns|TestIAAvgColumnsChunkRoundTrip|TestMemReader|TestHistoryReader|TestStmtRecord|TestStmtWindow|TestEvictedConcurrentWithRotate)$' -count=1 -vet=off`
  — passed (0.856s).
- Post-fix failpoint-enabled v2 full package run:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/util/stmtsummary/v2 -count=1 -vet=off`
  — passed (0.806s).
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

Latest `78cac443a4` follow-up evidence:

- Pre-fix failpoint-aware run:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/util/stmtsummary -run '^TestEvictedHistoryCollectionKeepsLatestIntervals$' -count=1 -vet=off`
  — failed as expected (`actual: 1`, `expected: 2`).
- Root focused run for latest regressions with the same failpoint wrapper and
  environment, `-run '^(TestEvictedHistoryCollectionKeepsLatestIntervals|TestExecutionAverageColumnsUseExecCount|TestTableNamesSkipEmptyTables)$'`
  — passed.
- Root full package run with the same wrapper and environment — passed
  (0.479s).
- v2 focused run with the same wrapper and environment,
  `-run '^(TestStmtFiles|TestHistoryReader|TestStmtRecordTableNamesSkipEmptyTables|TestStmtRecordFormatsDigestText)$'`
  — passed.
- v2 full package run with the same wrapper and environment — passed
  (0.811s).
- Focused `pkg/util/stmtsummary/v2/tests`
  `TestStmtSummaryHistoryOpenEndedTimeRange` — blocked at compile time by the
  unrelated in-progress statistics edit's undefined
  `vardef.TiDBAnalyzeStoreBatchSize` reference.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed; no Rust source changed in this follow-up.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make bazel_prepare`
  — required for the test/BUILD changes and blocked because the local `bazel`
  executable is unavailable.

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
