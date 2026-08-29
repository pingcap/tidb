# `pkg/statistics/handle/handletest/initstats` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 26 | `2bd4f9f3147fac32219df911cb9fe1945b03b72c` |
| `init_stats_test.go` | 437 | `14a77df6c90f33f1d21a544cd9b2cf360908bd28` |
| `main_test.go` | 34 | `13d11c7eaef3f97af1539b0d17db370e17c98b95` |

All 497 lines were read. The package has eight tests and no benchmark.

## Go behavior

The tests bootstrap stores and sessions and exercise lite and full InitStats,
table-ID scoping, partition and dropped-table filtering, concurrent loading
with and without memory pressure, skip-init configuration, lazy full loads,
and deterministic completion through the highest physical table ID.

## Rust comparison and decision

Rust represented all eight names only as ignored empty functions in a mixed
origin/master batch carrier. They neither invoked nor implemented InitStats.
The entries were removed; the package remains unclaimed until the complete
store-backed loader, cache, memory tracking, configuration, and session stack
can land with all artifacts and tests.
