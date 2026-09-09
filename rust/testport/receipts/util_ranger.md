# `pkg/util/ranger` parity receipt

## Local range-fallback prerequisite refresh (2026-09-08)

Current Rust batch carries the shared fallback handler through all six index
range quota exits, including recursive and DNF construction. DNF accounting now
uses actual range memory usage. The MAX/MIN logical rule consumes hint-filtered
paths and the session quota; prepared execution preserves cache-admission state
and planning warnings across statement boundaries. Rejected candidates execute
once without cache insertion. Focused red/green evidence is recorded below.

Validation now includes 64 ranger tests, five MAX/MIN tests, two executor context
tests, six SQL regression tests and two logical integration tests, all passing,
plus successful make lint. Expanded session tests have 11 failures with identical names and panic
payloads on HEAD; current WIP adds seven passing tests. This is a bounded integration batch, not a claim of whole
planner/statistics/optimizer parity. Final review and publication remain pending.

Authority for the current audit is f5cf8f6337612c6ae51fb6e384e4bb3469dde680.
Compared all 13 tracked artifacts with the recorded follow-up authority
a0cdff369bd4c7060a840e3943049a79470e8af4. Only four files differ, and all four
were fully re-read: bench_test.go (268 lines, blob
8156f263d1627a6ca662c266bce9078c0b0c2ccc), detacher.go (1616 lines,
f69fa894d8e6816caf8e2bfe90db444dbad7f245), points.go (1054 lines,
5ad1caf7c5c87260bb22e83a7a1f3daa8aa9b541), ranger.go (988 lines,
5f3ced201ee7bf154016143adf37951c59e8ee33). The nested context package was
also fully read again; the remaining unchanged artifacts retain the prior
reading evidence. No new tracked fixture/generated/platform artifacts exist.

The numeric fallback behavior alone does not complete parity: recursive quota
events must reach shared warning/cache handlers even if candidate ranges are
discarded. Public wrappers currently discard Rust internal cache-skip state.
The current batch wires MAX/MIN; other callers and partition wrappers remain
explicit follow-up audit surfaces. Documentation-only changes remain local.

Point conversion observations: IN skips impossible YEAR/ENUM values and sorts
before prefix cutting; NOT IN builds its complement before prefix cutting and
sort-key conversion. LIKE uses separate trimmed and untrimmed sort keys for
PAD SPACE lower and upper bounds. These source contracts remain unchanged by
the in-place conversion refactor and must remain intact during fallback work.

- **Source baseline:** Go `master` (`0bc44483e3e41a8ea917d4382dc202369468d200`)
- **Rust baseline:** `hparser-integration` (`5a005978dda57fbb3373a303660ea0a5f7990b38`)
- **Audit boundary:** `pkg/util/ranger`, including its nested `context` package
- **Audit mode:** complete package inventory before the Rust change; source and test behavior were read, then the missing SQL-rendering helpers were implemented.

## Go inventory

The package contains 13 files (8,324 logical lines): production source, unit/benchmark tests, nested context source/tests, and Bazel build inputs. There are no additional fixture, generated, or platform-variant files under this package.

| Go artifact | lines | SHA-256 |
| --- | ---: | --- |
| `pkg/util/ranger/BUILD.bazel` | 78 | `8a863248df8da897a2e8fd0cef382bc1f230fb17df9a137ebfa428f1a290f03b` |
| `pkg/util/ranger/bench_test.go` | 268 | `73fa5f419a0741b6d0737cabf557490dba5988d45684821492f508002ed4e154` |
| `pkg/util/ranger/checker.go` | 241 | `f598237d881893b3eee596e96df96e16afab7e83c3c2ae482892e06cbd9cd190` |
| `pkg/util/ranger/context/BUILD.bazel` | 30 | `aa35b8fecf5c158ee0c96e84da01f47604189fd23c9c0e940c2731c6efda92ad` |
| `pkg/util/ranger/context/context.go` | 49 | `520b1da413d8d9e41d0bb779dce46d1b583d5546bf14d28edcc3a364adc106c4` |
| `pkg/util/ranger/context/context_test.go` | 62 | `895af280ccd2c92e94508fdf6499b7406e5eae999a6438e9e924e47365d03bfe` |
| `pkg/util/ranger/detacher.go` | 1,616 | `9d593ac6690d99288efcecf5e25de6582f8dc090504c515c363bbe9ccdc0817f` |
| `pkg/util/ranger/main_test.go` | 48 | `2c6c5fde2cceb2108c39c2d93d7bd95fa6b8bcf19e5a3f3e95e9a0fc18b02af2` |
| `pkg/util/ranger/points.go` | 1,054 | `d4cae86260648e4cc117ae3e021bdfa000a7283f6a760607a5850544617d9b0e` |
| `pkg/util/ranger/ranger.go` | 988 | `7d9203f1e676fd5cb0bd753fff760360661d5370202b29ed590c96d7229261e0` |
| `pkg/util/ranger/ranger_test.go` | 2,663 | `5be676a3ef5191a419c3a5288bcc20fd5db34637e259ac7ab7537c74e481c9ed` |
| `pkg/util/ranger/types.go` | 653 | `74daff5c473ce1aeb0923222c329d3a142ae0480aa99e658def4d764caa23e6b` |
| `pkg/util/ranger/types_test.go` | 574 | `f1de6478db23348b11f38d8289653bd8be4f2b874b31398643112c81cdcc441b` |

Production functions and test entry points were enumerated with `rg '^func'`. The root tests cover table/column/index ranges, unsigned overflow, YEAR, prefix indexes, shard indexes, DNF/CNF fallback, regression issues 40997/50051, binary collations, range algebra, and memory accounting. The nested context test covers `RangerContext.Detach`; the benchmark file also contains its benchmark smoke test. `main_test.go` owns package setup/teardown.

## Rust owner inventory

The Rust owner is `rust/crates/tidb-planner/src/ranger/` (7 files, 9,781 logical lines):

| Rust artifact | lines | SHA-256 |
| --- | ---: | --- |
| `checker.rs` | 583 | `60698549e649995d3087bc28a132a8d93b8b8baf47373db8cd29f792f2839ccc` |
| `detacher.rs` | 2,427 | `eae9f65f00251baa6d3275d9ead2993d9b38028622962c50d6920c3d07103b5c` |
| `go_cases.rs` | 2,553 | `68454fba0ef75c8e2289fbc2c6b1560bd936dc5b8bc1b50c6ddbd4fd3ac2c49a` |
| `mod.rs` | 265 | `7facc32fdb75cc0df82f7fb155765f2e93ed0febe388f47a7c8ddca51e1bef67` |
| `points.rs` | 1,925 | `0dbc34107e8c520a8af072b4f42d1d2f3246c05fcb7900d31d34bfe470afc264` |
| `ranger.rs` | 912 | `cac5ad847e4f5bc1893e778f612807e317fe7472fc09939cd1a6aa5d2cf1585f` |
| `types.rs` | 1,183 | `75ba8a031ee0572365185c60629c416e589491c6d25d068b1d3ca29118ea6cc1` |

The source port already covered point construction, range assembly, detachment, fallback, union/intersection, and the transcreated Go cases. The missing production behavior was the pair of Go SQL-condition printers at the end of `ranger.go`: `RangesToString` and `RangeSingleColToString`. They were not test-covered upstream, but are production helpers and therefore cannot be omitted from a whole-package parity claim.

## Change and evidence

- Added native `ranges_to_string` and `range_single_col_to_string` helpers. Composite ranges preserve Go's per-column parentheses, AND/OR shape, last-column-only exclusion flags, point-equality validation, special NULL/sentinel simplifications, and full-range `true` simplification.
- Added typed literal restoration for the value kinds Go's `ValueExpr.Restore` supports (integers, decimals, floats, strings/bytes, binary literals, durations, and temporal values). Unsupported Go kinds return an error, matching the source TODO/error boundary.
- Added focused tests for equality, NULL/sentinel intervals, escaped string literals, composite ranges, full-range simplification, malformed range lengths, and non-point composite bounds.
- Aligned the issue-40997 test fixture with Go's pre-ranger comparison-refinement stage so quoted integral BIGINT constants do not remain Rust-only DOUBLE casts; the full ranger suite now passes this regression.
- Re-exported the helpers from the ranger module and replaced the old “not ported” boundary note with the explicit native-context adaptation.

## Validation

- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner ranger --lib -- --test-threads=1` — **62 passed** (including the three new regressions and issue 40997).
- `PATH=... GOPATH=... go test ./pkg/util/ranger/... -count=1` — package logic ran, but the root package failed only because the repository's `intest` guard requires `-tags=intest` for `TestBinCollationRangeForIndex`; the nested context package passed. The tagged rerun is part of the Ready gate.
- Ready profile: targeted package checks plus `make lint` after the source change. No `make bazel_prepare` is required because no Go/Bazel/go.mod/import files changed.

## Risks and boundaries

- Rust has no mutable Go `StatementContext`; comparison uses typed datum/collation semantics and literal rendering reports errors instead of mutating warning state.
- Non-UTF-8 string/byte literals are rejected by the Rust `String` return type; Go can write arbitrary bytes. This is an explicit representation boundary, not silent replacement.
- Go's plan-cache mutability and testkit warning-surface integration remain outside this crate's type model; they are recorded in the module-level boundary notes and are not claimed as ranger production behavior.

## Follow-up parity batch (2026-09-05)

The complete package inventory above was re-read against Go `master`
(`a0cdff369bd4c7060a840e3943049a79470e8af4`) before the follow-up change.
`pkg/util/ranger/points.go`'s `buildFromBinOp` evaluates a strict constant
operand against an empty row, matching Go's `expr.Eval` path for wrappers such
as `CAST(-1 AS DECIMAL)`. Rust previously accepted only a bare constant, so an
unsigned comparison silently lost the domain fixup and widened to a full scan.

The focused `tidb-planner::ranger::points::tests::bin_op_points_evaluate_wrapped_strict_constants`
regression now pins both directions: `a > CAST(-1 AS DECIMAL)` clamps to
`[0,+inf]`, while `a < ...` has no points. The existing ranger suite and the
source-derived session EXPLAIN regression cover the downstream TableDual
decision.


2026-09-08 Rust ranger WIP: handler-aware index detachment now carries the shared
RangeFallbackHandler through recursive candidate construction and records quota
fallback at construction time. The focused cache-admission regression failed
before wiring and passes afterward. Expanded DNF/recursive coverage exposed a
second mismatch: the DNF accumulator used a hardcoded estimate that omitted
collators and datum payloads, unlike Go Ranges.MemUsage. The composite predicate
(a = 10 and b = 40) or (a = 20 and b = 50), with quota one byte below the full
range footprint, failed before replacing the estimate with ranges_mem_usage.
Both focused tests now pass, including unlimited construction, ordinary residual
DNF without a quota event, and repeated handler calls. Logs:
/tmp/ranger-quota-events-expanded-20260908.log (red),
/tmp/ranger-quota-events-expanded-green-20260908.log (2 passed).
This is WIP: production session/MAX-MIN budget and cache/warning sink integration
remain pending; no Ready claim, commit or push. Documentation-only evidence
continues to remain local under the user's instruction.


2026-09-08 WIP continuation: all 64 ranger unit tests passed before the final
edge-case extensions (/tmp/ranger-suite-20260908.log). Added exact-budget tests
for simple DNF, composite DNF and recursive IN fanout; all retain full access
without warning at equality. Extended shared-handler coverage for forced cache.
The initial expected count of three warnings was incorrect: pinned Go detacher.go
410 and 550 attempt both the IN prefix and the column-condition retry, generating
two fallback events per build. Source inspection confirmed this construction-time
behavior; two builds now assert four risk warnings and one capacity warning,
with cache admission retained. This assertion correction is not a production
bug fix or fail-before evidence. Final focused command:
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-planner range_quota_events --lib -- --test-threads=1
passed both tests (/tmp/ranger-quota-forced-green-20260908.log). git diff --check
passed. Actual executor StmtContext still uses its own first-reason cache marker;
the utility tracker is not yet connected to that admission consumer. Continue
production budget/warning/cache wiring before Ready and publication. No commit
or push; documentation stays local.


2026-09-08 partition quota batch Ready evidence: four SQL regressions cover HASH,
KEY, RANGE, RANGE COLUMNS, LIST and recursive LIST COLUMNS; quota1 preserves rows
and emits one warning, quota0 emits none. Prepared HASH checks changing parameters
and no hit. All four passed in /tmp/partition-sql-ready.log. The original HASH
regression failed before the fix with empty warnings (/tmp/partition-quota-red.log).
All 64 ranger tests passed (/tmp/partition-ranger-ready.log), and both partition
rule tests passed (/tmp/partition-rule-ready.log). Commands use
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked
with -p tidb-session --lib tests_prepared_plan_cache::static_,
-p tidb-planner --lib ranger::, and -p tidb-planner --lib rule_partition_processor.
make lint passed (/tmp/partition-batch-ready-lint.log). rustfmt and diff checks
passed. Final review confirms partition wrapper keeps sort-key conversion and
consecutive-range merging disabled, recursive calls share one statement handler,
and cache rebuild zero-quota paths are unchanged. This batch addresses the
pkg/planner/core/rule partition quota integration; whole-package parity remains
incomplete. It includes Rust changes, so accompanying receipt updates can ship
with the meaningful fix commit under the user's publication instruction.
