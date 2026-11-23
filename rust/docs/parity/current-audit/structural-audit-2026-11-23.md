# Structural audit, 2026-11-23

A symbol-level re-audit, run after the 2026-11-22 rows were all closed. The
2026-11-22 pass read only Go files under 40% coverage. This pass scores every
Go function and type, maps every Go file to the crates that implement it, and
scans the Rust side for layers that have no Go owner.

## Method

1. Symbol coverage. The 451 `pkg/` packages linked into `cmd/tidb-server`
   (`go list -deps ./cmd/tidb-server`) hold 33,330 non-test, non-generated
   top-level functions, methods and struct or interface types. A symbol counts
   as ported when a Rust definition carries its name or snake case, or a Rust
   comment names it, or at least half of its string literals (12 characters
   or more) appear in the Rust sources. 6,810 symbols did not match. The 486
   of them with bodies of 30 lines or more were triaged one by one: by Go
   caller, by the feature's Rust home (grep on the concept, not the name),
   and by reading where the two disagreed. Shorter helpers are covered by
   their callers' ports and were not read individually.
2. Ownership map. Every Rust doc line `Go \`X\`` whose `X` is defined once in
   `pkg/` was attributed to its Go file. Go files whose claimed symbols land
   mostly in a crate of another layer (planner code in `tidb-executor`,
   executor or statistics code in `tidb-session`) were read.
3. Reverse scan. Rust modules of 400 lines or more whose header names no Go
   owner, and every Rust text that admits a missing port ("this server has
   none", "not ported", "this tier does not ..."), were read.
4. Duplicate scan. Re-run of the 2026-11-22 scan (a Go symbol defined once,
   claimed by two Rust bodies in different crates). Only thin wrappers and the
   pairs accepted on 2026-11-22 remain.
5. Live differential. A Go server built from this tree's `pkg/` and the Rust
   `tidb-server` on unistore, side by side through pymysql: skew distinct
   aggregation, `tidb_dml_type='bulk'` with each fallback condition, and every
   `performance_schema` virtual table.

## Read and not registered

- Ported under other names (most of the 486): the skew distinct-aggregate
  rewrite (`tidb-planner/src/rule/rule_aggregation_skew_rewrite.rs`), the
  cost model ver1 functions, the parser's recursive-descent functions, check
  constraints, `ShuffleExec`, merge join, the coprocessor cache, SAN checks
  (`tidb-privilege/src/tls_policy.rs`), cgroup limits (`tidb-cgroup`), the
  workload repository, the TiFlash replica manager, null-aware anti join keys.
- Test-only or build-flag code: `sanity_check.go`, `mock_bootstrap.go`,
  `adminCheckTableBeforeDrop` (`config.CheckTableBeforeDrop`), test utilities.
- Next-gen kernel only: `getTableSizeByID` / DXF resource calculation for
  ADD INDEX (`kerneltype.IsNextGen()`).
- Dead in Go: `checkPlacementPolicyBeforeCommit` (the transaction scope is
  always global).
- Historical statistics (`history_stats.go`, `DumpHistoricalStats`): Go
  master removed them; Rust follows master (S01).
- Lightning, `dumpformat`, `objstore` and `util/dbutil` helpers outside the
  server's statement paths, and metrics initialisation.

## Registered

The twelve rows T01-T12 in [structural-findings.md](structural-findings.md):
four missing behaviours (pipelined DML, `performance_schema` virtual tables,
`tidb_auth_token`, the consistency reporter) and eight places where Go code is
owned by the wrong layer.
