# `pkg/workloadlearning` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains exactly six tracked artifacts and 1,154 lines. Every
production file, source test, and Bazel target was read line by line before
comparing Rust owners. There is no `doc.go`, fixture/testdata directory,
generated source, benchmark, fuzz target, build-tagged source, or nested
package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 44 | `243584e0c3c7ef6398105bf867566d4590b08286` | `0c0e06b73491043fb11cd7fafeb33e0f990edf6828029b3fb38c6f52e51148a7` | workload-learning library and four-shard test target |
| `cache.go` | 154 | `026233397fa8785e3c8dd90d5e27e84f33ac9cff` | `10c77f41a00918036f757a267fe7753ff8b4f8d00bc4c4d69a2708c740f0a100` | table read-cost cache worker and defensive reads |
| `cache_test.go` | 79 | `3a119ca0911a92d9a2ffe1212599c2893fbad5a7` | `389a6c9da30d13723f816d9e9decf6bf27a7289d68c5194737eb4e65ac85e1ee` | cache refresh and empty-cache integration tests |
| `handle.go` | 728 | `40a8c261ac7d321733611924a04b1566cc77f24a` | `fd01794d006d17971c2a058403eb568aed2423df59b71af9eb04aa8230291c60` | statement-stat analysis, plan extraction, accumulation, and persistence |
| `handle_test.go` | 110 | `9913e43539035ea5a8d5bf8786fdb9d36bbc7662` | `8e21357bf184dddcf7f2175b7759d53fad4607a11b0a545a06760b3618c7e8f8` | persistence and table-group accumulation tests |
| `metrics.go` | 39 | `b3c2d572c6ccb9448d7483d7600bb862ff0ffc92` | `902d7d5bf86513e1135036ced250591b48321eeb4c748c5f178aaf66e39690d8` | table read-cost metric carrier |

### Production symbols

`metrics.go` defines `TableReadCostMetrics` with database/table names,
scan-time, memory, frequency, and normalized cost fields.

`cache.go` defines `TableReadCostCache` and `WLCacheWorker`, then implements
`NewWLCacheWorker`, `UpdateTableReadCostCache` (version query, JSON metric
loading, session recycle/destroy, and atomic replacement), the private
`updateTableReadCostCacheWithMetrics`, and `GetTableReadCostMetrics` (locked
lookup with a defensive metric copy).

`handle.go` defines the batch, category, type, snapshot, and SQL-template
constants plus `Handle` and `NewWorkloadLearningHandle`. Its complete method
and helper surface is `HandleTableReadCost`,
`analyzeBasedOnStatementStats`, `findClosestSnapshotIDByTime`,
`Handle.SaveTableReadCostMetrics`, `extractScanAndMemoryFromBinaryPlan`,
`AccumulateMetricsGroupByTableID`, `extractMetricsFromOperatorTree`,
`checkTiFlashOperator`, `extractTableNameFromAccessObject`,
`extractTableNameFromChildrenTableScan`, `extractTableNameFromIndexScan`,
`extractPartialMetricsFromChildrenIndexMerge`,
`extractOperatorTypeFromName`, `extractScanTimeFromExecutionInfo`, and
`extractScanTimeFromString`. The file also defines the AST visitor
`DBNameExtractor`, including its `Enter` and `Leave` methods. The read covered
session-pool ownership, restricted SQL, snapshot-window selection, plan
decompression/protobuf validation, TableReader/IndexReader/PointGet/
BatchPointGet/IndexMerge extraction, TiFlash filtering, recursive child
search, frequency multiplication, table-ID grouping, batched inserts, and
transaction commit/error paths.

### Tests, test by test

`cache_test.go` contains:

* `TestUpdateTableCostCache`: creates a table, writes one JSON metric version,
  refreshes the worker, and verifies every cached metric field;
* `TestGetTableReadCacheMetricsWithNoData`: verifies an absent table ID returns
  nil.

`handle_test.go` contains:

* `TestSaveReadTableCostMetrics`: persists one table metric through a mock
  domain and verifies the workload-values row;
* `TestAccumulateMetricsGroupByTableID`: resolves two tables, multiplies
  current metrics by frequency, merges one existing table, and verifies sums
  and untouched normalized costs.

The Bazel target lists both test files, four shards, and the testkit dependency;
the tests are ordinary Go tests but require TiDB's `intest` tag for the
mock-domain setup.

## Rust ownership and decision

Rust has no dependency-closed owner for this package's workload-based
optimizer. `tidb-workloadrepo` is a different package (`pkg/util/workloadrepo`)
that snapshots and samples repository tables; its
`tidb_workload_repository_*` variables and internal source marker do not
implement table read-cost analysis. The only related Rust fragments are the
workload-learning internal transaction label and default sysvar values. No
Rust crate provides Go's plan decompression/ExplainData traversal, operator
runtime-stat extraction, InfoSchema table-ID accumulation, versioned
`mysql.tidb_workload_values` writes, or cache worker. Porting a partial helper
or attaching analysis to the repository worker would create a new Rust-only
execution path and would not satisfy the package-atomic boundary.

This package is recorded as an explicit boundary with no speculative source
change and no new regression test. A future owner must port the complete
analysis, persistence, cache, and testkit behavior together and preserve the
existing workload-repository separation.

## Validation and risk

Profile: **WIP** for this docs-only audit; the rolling repository loop remains
in progress. No Go or Bazel source changed, so `make bazel_prepare` is not
required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/workloadlearning -count=1
# expected guard failure: mock-domain tests require --tags=intest

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest ./pkg/workloadlearning -count=1
# passed
```

- Correctness: no optimizer, plan, or workload persistence behavior changed;
  the full Go implementation remains the authority.
- Compatibility: a future Rust owner must retain SQL templates, seven-day and
  two-snapshot timing windows, runtime-stat parsing, TiFlash exclusion,
  transaction versioning, batch boundaries, and cache version semantics.
- Performance: unchanged.
- Not verified locally: real TiKV statement-summary history, compressed plan
  corpus coverage beyond source tests, Bazel analysis, and workspace-wide
  Ready validation.
