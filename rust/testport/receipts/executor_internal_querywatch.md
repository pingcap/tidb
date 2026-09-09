# `pkg/executor/internal/querywatch` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains four tracked artifacts and 542 lines. Every production
source, test harness, test case, and Bazel target was read line by line before
editing. There are no generated sources, platform-specific variants,
benchmarks, fuzz targets, or fixture files.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 50 | `ff904326ff51e7295b9432ac206c7a40a7d6040e` | `418c0680fb5b171c0090fcf4cb72c367d18452b4e08ccf2fb157b09d4e4dbba5` | internal query-watch library and two-shard flaky test target |
| `main_test.go` | 53 | `da8b63f17628ede2845f01ee802671616718c8f7` | `6a3e853fcbd738e11100c58107fd343cc07015fadc4ee3b50f22bca5d4786296` | failpoint setup, common test configuration, and goleak harness |
| `query_watch.go` | 215 | `23c56a9d95a7690750e2c1a99fb5b11f7839295b` | `99d8b9f7047ffe31cbb911cda8dfa959660e4b10be66c72cb49bcbcbc1dba8d7` | `ADMIN WATCH/DROP QUERY` executor construction, validation, and execution |
| `query_watch_test.go` | 221 | `c6d059a77d565d954cef1d80e3503c58b50ef5f7` | `3cb4d5508a8388383c4ea8d16f2271ea3dee3c0e783fe604c9cb6954677c2a15` | exact/similar/plan watch forms, quarantine, resource groups, and drop regressions |

`setWatchOption` parses resource-group expressions, actions, exact/similar/plan
watch types, and digest validation; plan watches obtain a digest through a
restricted-session `EXPLAIN`. `fromQueryWatchOptionList` creates a runaway
quarantine record with source, lifetime, and exceed-cause fields.
`validateWatchRecord` supplies the default resource group, resolves the global
resource-group controller, applies action defaults, and requires a watch type.
`AddExecutor.Next` obtains a system session, validates and installs the watch,
and returns its generated ID. `ExecDropQueryWatch` removes watches by resource
group, user variable, or numeric ID. The tests exercise all these paths,
including the `USE`/similar-watch issue regression and the `FastRunawayGC`
failpoint.

## Rust ownership and explicit boundary

Rust has no dependency-closed execution owner for this package. The Rust
parser/AST crates contain the `WATCH QUERY` syntax and parser tests, and
`tidb-metadef` contains the `mysql.tidb_runaway_watch` table definition, but
there is no Rust runaway manager/controller, query-watch executor, resource
group action resolver, restricted-session plan-digest path, or drop-command
execution owner. `tidb-exec` exposes generic query-time metrics only; it does
not install or remove watches.

Consequently no Rust-only query-watch behavior was found to remove and no
uncalled compatibility layer was invented. The runtime feature remains an
explicit SEED/boundary until a Rust runaway-management owner and its complete
Go package dependency closure exist. Parser and metadata parity alone do not
constitute package completion.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, Bazel metadata, or module files changed, so `make bazel_prepare` and
the Ready lint gate are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/executor/internal/querywatch -count=1
# passed; failpoints enabled before the run and disabled afterward
```

Not verified here: Rust query-watch execution (no owner exists), parser
integration beyond the existing parser tests, Bazel execution, TiFlash paths,
and full workspace tests. Existing unrelated privilege/session worktree
changes remain outside this receipt.

