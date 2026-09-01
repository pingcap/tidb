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
