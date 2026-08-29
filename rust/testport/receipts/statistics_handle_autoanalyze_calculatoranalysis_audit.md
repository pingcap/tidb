# `pkg/statistics/handle/autoanalyze/priorityqueue/calculatoranalysis` audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 21 | `65b03ca973df7644e3a69e5135646dc5f5e9e6f0` |
| `README.md` | 59 | `1091d9f4e87b2b79bfcbfbaefaebef664d17f5b7` |
| `calculator_analysis_test.go` | 295 | `870e13bee326e275a16536837e566d849f9570f3` |
| `main_test.go` | 32 | `c49058648ec8f5cf49149bc92949637e0bc244f7` |
| `testdata/calculated_priorities.golden.csv` | 691 | `9d2099ad4e51cb4e411f13c2de55b366b36a45d8` |

All 1,098 lines were read. The package has one assertion test, one shared
leak-checking `TestMain`, and no benchmark.

## Behavior and Rust decision

The test generates 690 realistic table-size/change/elapsed-time jobs, invokes
the parent package's real `PriorityCalculator` through the full `AnalysisJob`
interface, stable-sorts the results, and byte-compares all rows with the CSV
fixture. Its update flag, README, fixture, build target, and harness are part of
the package unit.

Rust has no remaining calculator-analysis generator, fixture, or package test.
Repository-wide tracing found no corresponding artifact after the false parent
priority-queue runtime was removed. Reintroducing the formula or a reduced
fixture would not satisfy this package and would recreate a partial parent
claim. The package remains unclaimed until the real priority queue can land
with this complete golden test.

No Rust source changed solely for this audit. This is a WIP inventory, not a
package parity or repository-wide Ready claim.
