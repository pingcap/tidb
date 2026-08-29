# `pkg/statistics/handle/handletest/statstest` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 28 | `87d3d57d5c38d97ac8e763964f1dd7c403782061` |
| `main_test.go` | 34 | `b67b9ea6815e74523833175bae8590006dfefc25` |
| `stats_test.go` | 872 | `f8c532b84dea4253643482cdc1e1eac2994fd69e` |

All 934 lines were read. The package has 17 tests and no benchmark.

## Go behavior

This external suite covers stats-cache versioning across ANALYZE and Update,
schema changes, memory tracking, storage round trips, lite/full/concurrent
InitStats, partitioned and predicate-column shapes, missing DDL histogram
metadata, version-2 and timestamp regressions, batch delta transactions,
TopN-only statistics, and memory exhaustion between TopN and bucket loading.

## Rust comparison and decision

Rust had a 196-line file containing all 17 names solely as ignored functions
whose bodies were `unreachable!`. It executed no cache, storage, loader,
session, failpoint, or ANALYZE behavior. The file and its stale origin/master
batch receipt were removed. The package remains unclaimed until all three Go
artifacts and their ordinary handle dependencies can be transcreated together.
