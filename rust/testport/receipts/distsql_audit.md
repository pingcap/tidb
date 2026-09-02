# `pkg/distsql` — Go-master batching contract audit receipt

Status: complete Go inventory; implemented the dependency-closed request
batching slice from Go commit `b1daa76b65` (request flags, setters, context
projection, coprocessor wire fields, and unhinted opt-in task batching).
The later Go-master runtime-stat changes from `bc04813887` and `db35d47066`
remain an explicit boundary: the Rust response owner does not yet own Go's
`ExecDetails`, TiKV scan/read-pool evidence, RU accounting, or percentile
collector, so those behaviors were not guessed or partially duplicated.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has
15 tracked Go artifacts and 5,455 lines. No package `doc.go`, fixture or
testdata directory, generated Go source, platform variant, benchmark fixture,
or nested Go package exists beyond the listed `bench_test.go`.

## Complete Go inventory

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 108 | library/test target, deps, and shard metadata |
| `OWNERS` | 10 | package ownership |
| `bench_test.go` | 74 | benchmark helper |
| `context/BUILD.bazel` | 47 | context subpackage target |
| `context/context.go` | 132 | DistSQL request context |
| `context/context_test.go` | 139 | context unit tests |
| `context_test.go` | 47 | package context helpers |
| `distsql.go` | 297 | Select/Analyze/Checksum result constructors |
| `distsql_test.go` | 733 | select, analyze, checksum, and mock response tests |
| `main_test.go` | 34 | package setup/teardown |
| `request_builder.go` | 949 | request defaults, ranges, and batching options |
| `request_builder_test.go` | 975 | request/range/concurrency tests |
| `select_result.go` | 1,343 | response decoding and runtime statistics |
| `select_result_test.go` | 567 | result decoding/runtime-stat tests |

All 15 files were read in full before editing. The inventory contains 126
production function/method declarations and 63 test/helper declarations.

## Implemented Rust slice

- `tidb-txnkv::Request` now carries
  `allow_batch_task_data_merge` and `execute_batch_tasks_serially`, matching
  Go `kv.Request` fields 18/19's request-level meaning.
- `DistSqlContext`, `ReadRequestMetadata`, and `KvRequestMetadata` preserve
  both flags through `SetFromSessionVars`-equivalent projection.
- `RequestBuilder` exposes source-shaped setters for store batch size and both
  flags.
- `coprocessor.proto` and `CoprocessorRequestEnvelope` encode/decode the exact
  bool fields at wire tags 18 and 19.
- Region task construction permits unhinted batching only with the explicit
  merge opt-in, keeps normal hint-based batching unchanged, and retains
  `row_count_hint = -1` for merged unhinted parents/children.

No Rust-only request flag or batching behavior was removed. Existing direct
unary transport still clears the per-request batch size for ordinary retries,
which is the source `BuildCopIterator` ownership rule; the new opt-in flags
remain immutable metadata and are only serialized when a task is sent.

## Follow-up Go package batch: cop-request limiter handoff

The current Go-master dependency snapshot (`a74cc596996d`, pulled 2026-09-02)
uses `kv.CoprRequestLimiter` rather than the removed client-go `RateLimit` field.
`RequestBuilder.SetCoprRequestLimiter` now stores that typed limiter, the
query-scoped limiter is copied from `DistSQLContext` in `Select`, and the
store-batching option setters are restored. `TestRequestBuilderCoprRequestLimiter`
covers the limiter pointer and all three request-option projections; the
pre-fix build failed with `Request.CoprRequestRateLimit undefined` when the
dependent copr test package was compiled.

Ready evidence for this bounded batch:

- failpoint-wrapped `pkg/distsql` focused test
  `TestRequestBuilderCoprRequestLimiter` passes;
- `git diff --check` passes;
- `make lint` and `make bazel_prepare` remain completion gates for the package
  commit (the latter is expected to be blocked locally because Bazel is not
  installed).

The executor-side merge-sort caller still passes the legacy rate-limit type;
that is a separate `pkg/executor` package boundary and is intentionally not
claimed by this receipt.

## Follow-up Go package batch: limiter wait runtime statistics

The current Go-master snapshot (`1c1a334d2b`, pulled 2026-09-02) exposes
coprocessor request-limiter wait totals and maxima through `selectResult`.
`close` now preserves the response close error while harvesting
`copr.HasLimiterWaitStats`, `selectResultRuntimeStats` carries the aggregate
through clone/merge, and its textual runtime-stat rendering reports the
`limiter_wait` field. `TestSelectResultCloseRecordsLimiterWaitStats` covers
collection, formatting, clone, and merge semantics. The Rust
`tidb-distsql` runtime contract now carries the same saturating total/max
aggregate and source-derived regression coverage; concrete Rust transport
response plumbing remains an explicit boundary because no dependency-closed
coprocessor response owner exists yet.

Ready evidence for this package-level batch:

- failpoint-wrapped Go focused test
  `TestSelectResultCloseRecordsLimiterWaitStats` passes;
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-distsql --test all limiter_wait -- --test-threads=1` — one focused Rust test passed;
- `git diff --check` passes; the pinned `make lint` Ready gate is run before
  commit; `make bazel_prepare` remains required for the Go test/import changes
  but is blocked by the unavailable local Bazel executable.

The complete 15-artifact root inventory above remains the atomic Go package
boundary. Nested executor consumers and live TiKV transport are separate
claims, and no Rust-only limiter behavior was introduced beyond the bounded
runtime aggregate contract.

## Validation and boundaries

- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-distsql --test all -- --test-threads=1` — 252 passed, 2 ignored.
- Focused regressions: `batch_request_options_round_trip_from_context_and_builder`, `coprocessor_request_uses_source_field_numbers_and_preserves_payload`, and `unhinted_store_batching_requires_explicit_merge_opt_in` all pass within that run.
- Go `pkg/distsql` runtime-stat behavior was inventoried but not executed here;
  the Rust crate has no concrete Go `ExecDetails`/RU collector owner yet.
- The Go `pkg/store/copr` batching worker and live TiKV integration remain
  outside this dependency-closed Rust slice; wire flags are now preserved for
  the eventual transport owner.

This receipt is a bounded parity slice, not a claim that the entire distsql
package has been transcreated. Continue the package loop with the remaining
runtime-stat and transport boundaries recorded above.

## Current Go-master consumer batch (`78cac443a4f46c13bfe27eb247b5c80657952547`)

The current fetched Go `origin/master` is
`78cac443a4f46c13bfe27eb247b5c80657952547`. The complete package inventory
was re-read before editing and corrected to the actual 14 tracked artifacts:
11 root files (including `BUILD.bazel`, `OWNERS`, benchmark and test harnesses)
and the three `pkg/distsql/context` artifacts. The inventory totals 5,077
lines, 14 production/test/build artifacts, 126 production declarations, and
40 test/benchmark declarations; there is no `doc.go`, fixture/testdata tree,
generated source, platform variant, or additional nested package.

This one package-scoped batch applies the nine-file, 738-insertion/133-
deletion Go-master delta. `selectResult` now preserves response-close errors
while collecting unconsumed runtime stats, propagates read-pool details,
validates and records response-summary coverage, and supports raw Analyze
execution-stat collection with open-ended close handling. The request and
context surfaces retain the query-scoped cop limiter. Focused regressions
`TestSelectAppliesQueryCopStoreLimiter` and
`TestCloseCollectsUnconsumedStatsAfterResponseClose` pass, as does the full
failpoint-aware root package suite. The nested context test deletion is a
source-parity cleanup with no production behavior change.

The batch is intentionally Go-only: `pkg/util/execdetails` supplies the
runtime evidence API and `pkg/store/copr` remains the separate transport
owner. Rust's `tidb-distsql` crate has the bounded limiter aggregate but no
dependency-closed coprocessor response owner, so no speculative Rust wiring
was added. The remaining TiKV/client-go integration is an explicit boundary.

Latest validation evidence:

- Pre-fix failpoint-aware command:
  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/distsql -run '^TestCloseCollectsUnconsumedStatsAfterResponseClose$' -count=1 -vet=off`
  — failed as expected because the restored `RecordCopStats` API had not yet
  been connected (missing read-pool argument in existing callers).
- Post-fix focused failpoint-aware run for both new tests — passed (`0.793s`).
- Post-fix full failpoint-aware root package run — passed (`0.745s`), with
  expected warnings for intentionally malformed summary fixtures.
- `git diff --check` — passed before staging.
- `make lint` is required for the final Ready gate; `make bazel_prepare` is
  required by the BUILD/test changes and remains blocked locally because no
  Bazel executable is installed. No Rust source changed, so pinned Rust
  formatting is not applicable to this Go consumer batch.
