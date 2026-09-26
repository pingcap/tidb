# `pkg/planner/cardinality` — Go-master parity receipt

Current status (2026-09-25): **incomplete**. Work is committed on
`hparser-integration`; newer checkpoint sections below supersede historical
failure reports. Ordinary and union estimation share version dispatch,
appended-handle preparation and session estimator options. The subset-index
fixture now exercises the production catalog async queue/cache boundary and
matches all five Go plans with a storage test double. Source/support inventory,
remaining context/loading variants and full workload validation remain open.
No whole-package completion is claimed.

Initial full-package inventory: Go `origin/master` at commit
`94eb995357f34b7bab4889a82f0405797046447d` (2026-09-02). The current live
comparison target is Go `origin/master` at
`633a9e37f1c796ac81c203dc107025e7e65385f0` (2026-09-24). The artifact table
was refreshed from `git ls-tree -r origin/master pkg/planner/cardinality` and
`git cat-file blob`; all 18 blob hashes and 12,070 lines match the previous
inventory at `4f27dad8db3971166156ad79a29c99e5d98dbcd2` because this package
has no upstream changes between those commits. Complete behavior and test
mappings remain open.

Latest source refresh: `git fetch origin master` advanced `origin/master` to
`633a9e37f1c796ac81c203dc107025e7e65385f0`. Comparing the entire cardinality
package against `4f27dad8db3971166156ad79a29c99e5d98dbcd2` produced no changed
paths, so the 18-artifact source inventory and blob hashes are still current.
The Rust work has since been committed and integrated with `hparser-integration`.
Package reference tests still compare against the separately pinned Go master,
not the checked-out branch's Go implementation.

## Latest-master follow-up (2026-09-24)

### Ordering-index fixtures (historical progress; see verified mapping below)

Follow-up: `index_merge_disjuncts_use_their_own_ranges_and_union_overlap`
failed before the correction with 100000 rows per partial and 200000 union rows.
It now passes with the Go fixture's 510 rows per partial and 1017.40 union rows.
Loaded v2 index histograms estimate the actual disjunct ranges instead of reusing
ordinary whole-query path estimates; the covered access DNF estimates overlap.
The helper carries session estimator options, virtual-column flags, per-index
row counts and recursive histogram candidates. Legacy/missing-statistics
fallbacks remain unaudited and are not claimed complete.

Validation commands:
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge_disjuncts_use_their_own_ranges_and_union_overlap -- --nocapture` (fails before, passes after).
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge -- --test-threads=1` (3 pass, 4 explicit pre-existing bootstrap gaps ignored).
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` (log `/private/tmp/tidb-union-planner-tests.log`).
- `make lint` (log `/private/tmp/tidb-union-lint.log`).


The complete `TestOrderingIdxSelectivityRatio` fixture now passes through
`tests_explain::ordering_index_selectivity_ratio_matches_go_fixture`.
`TestOrderingIdxSelectivityThreshold` is active and has one remaining exact
plan mismatch, the OR/index-merge case. Both original Go tests passed via
`GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestOrderingIdxSelectivity(Threshold|Ratio)$' -count=1`.

The index scan schema now contains physical index keys and handles, sharing
schema construction with index merge. Previously it advertised all table
columns, allowing TopN below table lookup on a column absent from the index.
Index-filter selections inherit that physical schema. IndexReader maps its
physical output back to datasource order; identity mappings avoid a projection.
The existing `the_merged_rows_are_the_joins_rows` regression failed with the
physical schema alone and passes with reader output mapping. The new
`index_lookup_limit_and_topn_preserve_table_rows` SQL regression passes.

Open: merge-path selectivity must participate before ordered LIMIT estimates,
union overlap and partial TopN/Limit behavior must match Go, and the broader
analyzed-threshold and six merge-join plan-choice assertions remain unresolved.
An experiment restoring the old full index schema reproduced all six join
plan-choice failures. No whole-package completion or workload-speedup claim.

Validation: `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1`;
`make lint` passed (log `/private/tmp/tidb-ordering-lint-final.log`);
`git diff --check` passed. The broad EXPLAIN run has 92 passes and eight failures
listed above, confirmed after the identity-path/format changes in
`/private/tmp/tidb-session-explain-schema-final.log`.
`cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` passes all 45 tests after test fixtures supply the statement column allocator needed to materialize missing index handles.

### Index-join upper-bound fixture

`TestIndexJoinInnerRowCountUpperBound` now runs through
`tidb_session::tests_explain::index_join_inner_row_count_upper_bound_matches_go`
with Go's 500000-row, NDV-500 histogram fixture. Both complete EXPLAIN outputs
match. Fix44855's default-off upper bound reduces probe index-scan work from
500000000 to 2000000 rows; its separately default-on access floor retains the
existing default/ON/OFF regression. Rust retains initialized column/index NDVs
independently of histogram eviction, selects Go's single-column/exact-index-set/
maximum-column lower bound, and reconstructs each probe count from the relevant
residual filters before applying the bound.

The regression failed first because secondary-index probe candidates were
never enumerated, then because table-filter selectivity and final probe rows
were incorrect. Restoring Go's candidate families and count construction fixes
both failures. Broader tests also identified invalid dominance over nonexistent
index candidates and rejection of runtime-only index access; those admission
checks now follow the enumerated paths and runtime keys. Two duplicate `lock`
fields in executor test fixtures were removed after the upstream refresh.

Validation from the repository root:

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestIndexJoinInnerRowCountUpperBound$' -count=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_join -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib stats_info::tests:: -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
make lint
git diff --check
```

The Go oracle and Rust suites pass (74 session, 27 executor, six statistics,
45 dispatcher tests); failpoint state returns to zero. Lint passes. Full
package mappings, asynchronous statistics loading, original-package validation
and sysbench/TPC-C/TPC-H/YCSB performance runs remain open. This is WIP and has
not been committed or pushed as a completed package.

### Collation-column fixture and lint follow-up

`tidb-session/tests/topn_assisted_string_match.rs` now executes all three
original `TestCollationColumnEstimate` outputs after the original SQL inserts
and version-2 ANALYZE. `SHOW STATS_TOPN` matches both binary general-ci keys
and their frequencies, and the equality and greater-than `aÄa` probes match
every brief EXPLAIN cell, including their 2.00-row estimates. This uses the
existing isolated test process so changing the global collation mode cannot
race other session tests. Go's explicit `LoadNeededHistograms` asynchronous
reload step is still unrepresented; the source placeholder records that gap.

Validation from the repository root:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --test topn_assisted_string_match -- --nocapture --test-threads=1
# PASS: both original 28-plan string-match suites plus the three collation outputs.
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestCollationColumnEstimate$' -count=1
# PASS; failpoint refcount restored to zero.
make lint
# PASS after granting the required dependency/cache access.
```

The initial sandboxed Go invocation stopped before testing because the
failpoint state directory under `.git` was read-only. The successful invocation
used the same command with the necessary filesystem access. Package-wide
source mappings, asynchronous statistics lifecycle, and workload validation
remain open; this fixture is not a package-completion claim.

The refreshed upstream delta touches `row_count_index.go`, `selectivity.go`,
`selectivity_test.go`, and `exponential_test.go`. It includes appended-handle
range damping/point caps, recursive-index error continuation, JSON-column
filtering, and range-length expansion for every appended handle column. The
new planner regressions `TestIndexRangeEstimationWithTruncatedHandleRange`
and `TestIndexRangeEstimationWithPrefixedCommonHandle` are mapped to session
SQL tests. Signed and unsigned truncated-handle cases now match, as do
prefixed common-handle DDL/encoding/decoding, scan ranges, full-value predicate
rechecks, actual tuple-query results, and the forced-index estimate. The
unforced tuple Selection originally estimated 40.80 rows in Rust against Go's
40.00. A Go-master oracle run then established the cause: datasource stats use
already-filled `AllPossibleAccessPaths`, whose appended common-handle range
uses the declared prefix length. The physical DNF ranges merge to two ranges
and estimate 50 rows; Go applies the residual DNF factor 0.8 for 40 rows. The
separate no-filled-path selectivity call builds full-length estimate ranges
and reports 51 rows, matching the Rust result before this fix. Rust datasource
selectivity now receives each filled path's physical appended-handle lengths,
and the path row estimate uses those same lengths. The regression now passes
at 40.00 while the forced-index result remains 50.00.

Go oracle command (Go 1.25.12, with the seven changed package/core sources
from `origin/master` supplied through a temporary Go overlay because the local
Go checkout predates the comparison ref):

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-cardinality-master-overlay/overlay.json -run '^TestIndexRangeEstimationWithPrefixedCommonHandle$' -count=1
# PASS; failpoint wrapper restored refcount to zero.
```

Rust regression command:

```text
cargo test -p tidb-session prefixed_common_handle_ranges_match_go_cardinality_cases -- --nocapture
# 1 passed.
```

The temporary overlay changed no tracked Go files. The package is still
incomplete; latest full inventory/blob refresh, package validation, `make
lint`, and workload checks remain open. Do not commit or push until the whole
package is complete.

## Complete inventory

The package contains 18 tracked artifacts and 12,070 Go/Bazel/fixture lines.
The current inventory was regenerated from `git ls-tree -r origin/master
pkg/planner/cardinality`, with line and declaration counts read from each blob.
This is an artifact inventory, not a claim that all Rust behavior is covered.
There is no generated source, platform-specific variant, benchmark corpus, or
nested Go package.

| Artifact | Lines | Git blob (prefix) | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 101 | `b8ad4b5ed259` | library and 50-shard flaky test target |
| `cross_estimation.go` | 287 | `ca1f9d4244bc` | cross-column and cross-index estimates |
| `exponential.go` | 54 | `22ce6cde6df4` | exponential-backoff estimator |
| `exponential_test.go` | 84 | `f1453e324584` | backoff and zero-repeat regression tests |
| `join.go` | 51 | `db3f172ca3dc` | join cardinality helpers |
| `main_test.go` | 66 | `57c1e1d4fcb3` | test setup, fixture loading, and goleak harness |
| `ndv.go` | 262 | `d144465fab02` | NDV estimation and stats-node graph |
| `ndv_test.go` | 241 | `c8cf0c30f9bf` | NDV and property tests |
| `pseudo.go` | 242 | `d8e8cf450d2b` | pseudo-statistics estimates |
| `row_count_column.go` | 315 | `b6554b37ddf7` | column range and equality estimates |
| `row_count_index.go` | 786 | `6f492efa02af` | index range, recursive, and handle estimates |
| `row_size.go` | 189 | `fd2e9220cb5d` | row-size estimates |
| `row_size_test.go` | 76 | `6e78faee43e5` | row-size tests |
| `selectivity.go` | 1,264 | `6ac355f45a15` | predicate selectivity and stats status |
| `selectivity_test.go` | 3,118 | `16506a54d7b8` | integration and selectivity tests |
| `testdata/cardinality_suite_in.json` | 425 | `d325a11c405b` | recorded input cases |
| `testdata/cardinality_suite_out.json` | 4,419 | `d7afee3258f4` | recorded expected results |
| `trace.go` | 90 | `27a076e336fc` | cardinality trace helpers |

The production files contain 74 function/method declarations. Test and
fixture-support files contain 71 declarations and 57 top-level test/benchmark
entries at the comparison revision. The fixtures are checked-in JSON inputs
and outputs, not generated production code.

## Go-master delta and implementation

The restored production guard is the paired `histCnt > 0` condition in
`equalRowCountOnColumn` and `equalRowCountOnIndex`. A histogram bucket's
`Repeat == 0` means that no point frequency was recorded for its upper bound;
it must not be returned as an exact zero-row estimate. Go master therefore
falls through to the uniform estimate before applying the stale-last-bucket
heuristic. The focused `TestEqualRowCountZeroRepeatFallsBackToUniformEstimate`
constructs a version-2 column histogram with a zero-repeat matching bucket and
asserts the uniform estimate. Before the guard it failed with `0`; afterward
it returns `5` as expected.

Go master also contains larger, dependency-sensitive cardinality additions
that remain explicit follow-up boundaries in this batch: appended-handle
selectivity damping and point caps in `row_count_index.go`, recursive index
estimation error continuation, JSON-column selectivity filtering, appended
handle range-length handling, and their integration/golden test updates in
`selectivity_test.go` and the fixture outputs. This commit does not claim those
unimplemented deltas as package-complete parity.

## Rust ownership and parity result

Rust's `tidb-planner` has partial cardinality and order-planning carriers, but
no dependency-closed owner for Go's statistics histograms, TopN/CMSketch
loading, pseudo-statistics, session risk variables, and testkit integration.
No Rust-only cardinality behavior was found to remove. Adding a detached Rust
estimate would bypass the ordinary Go statistics pipeline, so the remaining
Rust boundary and the unimplemented Go-master deltas stay explicit.

## Validation and risk

Profile: **WIP** for this focused behavior restoration. The package uses
failpoints, so the canonical wrapper enabled and disabled them around both
focused and full package tests.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
./tools/check/failpoint-go-test.sh ./pkg/planner/cardinality \
  -run '^TestEqualRowCountZeroRepeatFallsBackToUniformEstimate$' -count=1
# PASS; failpoints enabled and disabled

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
./tools/check/failpoint-go-test.sh ./pkg/planner/cardinality -count=1
# PASS; 35.203s; failpoints enabled and disabled

make bazel_prepare
# not yet rerun after this edit; required before final staging because the
# existing top-level test file was extended and this checkout has no bazel binary

git diff --check
# PASS before staging
```

The final package batch will run `make bazel_prepare` and `make lint` before
commit. No Rust source changed, so no Rust cargo gate is applicable. Not
verified here: Bazel analysis/sharding, detached Go-master execution, full
planner integration beyond the package suite, live correlated statistics
workloads, Windows execution, and full-workspace tests. Correctness risk is
limited to the zero-repeat fallback; the focused regression and full package
suite cover the changed path. Compatibility/performance risk is low because
the guard only changes an exact-zero histogram fallback and adds no new
runtime work.

## Follow-up: a prefix LIKE estimates from the histogram, not 0.8 (2026-09-09)

Go `cardinality.Selectivity` routes a single-column LIKE through
`GetSelectivityByFilter` -> `GetStrMatchSelectivity`, which samples the
column's histogram/TopN and falls back to
`GetStrMatchDefaultSelectivity` (0.1), never the generic 0.8
`SelectionFactor`. The port's `analyzed_filter_selectivity` charged LIKE 0.8,
so a source inside a subquery block (whose predicate never reaches the
top-level `InitStats` split) kept 80% of its rows.

`subqueries::correlated_sum_predicate_pulls_above_unique_outer_join`'s
`part` source therefore estimated `200000 * 0.8 = 160000` rows, which made
the `part x partsupp` MergeJoin cheaper than the index join. The arm now
recognizes `like(col, const[, escape])`, sums the rows of every histogram
bucket whose lower or upper bound starts with a plain trailing-`%` prefix,
floors at one row, and falls back to 0.1 when the collection has no
histogram.

Regression: new
`logical::rewrite::analyzed_filter_selectivity_tests::a_prefix_like_uses_the_histogram_then_the_string_match_default`
pins the histogram prefix estimate (1 row of 128), the one-row floor for a
prefix no bucket matches, and the 0.1 default without a histogram. The
executor failure `correlated_sum_predicate_pulls_above_unique_outer_join`
passes.

```text
cargo test -p tidb-executor --lib -- --test-threads=1 correlated_sum_predicate
# ok after; FAILED before

cargo test -p tidb-executor --lib -- --test-threads=1
# 1259 passed; 3 failed; no additions to the baseline set

cargo test -p tidb-planner
# 1004 + 268 + 6 + 3 passed; 0 failed

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check
# clean
```

## Current-master follow-up: not-is-null uses the retained HistColl (2026-09-23)

The Go reference is `origin/master` at `56970b286a362b1f0b150c7665453c8f5ff997a9`.
`cardinality.Selectivity` calls `GetRowCountByColumnRanges` with the
histogram collection's `RealtimeCount` and `ModifyCount`, then normalizes by
`RealtimeCount`. `StatsInfo.ScaleByExpectCnt` retains that collection. Rust
initially passed the scaled profile row count instead; the new regression
failed before the correction (`1.0` versus `0.901` for a 100-row expected
count over a 1,000-row histogram). It now uses the retained collection's row
counts for both `not(isnull(col))` and `isnull(col)` selectivity.

```text
env GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality -run TestColumnIndexNullEstimation -count=1
# PASS; failpoints enabled and disabled

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner --lib logical::data_source::tests:: -- --nocapture
# 3 passed, including the regression that failed before the correction
```

This follow-up does not complete `pkg/planner/cardinality`. At this checkpoint,
the risk/objective options flowed through the main logical statistics and
physical scan selectivity paths, while task-root selection still used default
options. The next follow-up closes that task-root gap. The other recorded Go
deltas, package-wide differential coverage, and the current 18-artifact source
inventory remain open. Do not report this package as transcreated.

## Current-master follow-up: thread session options into cardinality (2026-09-23)

`StmtContext::optimizer_cost_env` now captures Go's
`tidb_opt_risk_eq_skew_ratio`, `tidb_opt_risk_range_skew_ratio`, and
`tidb_opt_objective` settings into `CostSessionOpts::estimator_options`.
Logical statistics derivation and the datasource/index-join selectivity paths
carry the same immutable options into the range estimator. The task-root
selection helper still calls the default-options wrapper; that path remains
open and prevents a package-complete parity claim.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib cardinality_estimator_options_follow_session_variables -- --nocapture
# PASS; session settings and old statement snapshot verified

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner --lib \
  logical::rewrite::analyzed_filter_selectivity_tests::range_estimation_uses_the_session_risk_and_objective_options -- --nocapture
# PASS; range skew and determinate objective affect the live selectivity helper

cargo check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner -p tidb-session -p tidb-executor
# PASS; existing workspace warnings remain
```

## Current-master follow-up: carry session options into root-task filters (2026-09-23)

Go `CopTask.handleRootTaskConds` and the MPP root conversion call
`cardinality.Selectivity` with the statement `PlanContext`. Rust previously
re-entered `analyzed_filter_selectivity` through its default-options wrapper
after task construction, dropping the captured `tidb_opt_risk_range_skew_ratio`
and `tidb_opt_objective` values. `CopTask` and `MppTask` now carry the immutable
`EstimatorOptions` snapshot into root conversion; task copies, MPP enforcement,
and composed MPP tasks preserve it. The root selection calls the options-aware
helper and still falls back to `SelectionFactor` only when no estimate is
available.

The new conversion-level test failed before the fix when the options-aware call
was temporarily replaced with the default wrapper: `risk_adjusted > baseline`
was false. After the fix, the risk-adjusted and determinate cases both differ
from the baseline as expected. A second conversion test confirms a contradictory
range keeps Go's minimum one-row selectivity floor (`max(ret, 1/RealtimeCount)`)
rather than expecting an exact zero.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner --lib task::tests:: -- --test-threads=1
# PASS; 22 tests

cargo check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner -p tidb-session -p tidb-executor
# PASS; existing workspace warnings remain

git diff --check
# PASS
```

Package completion remains open: appended-handle damping is now implemented
for the datasource path-estimate boundary described below, but recursive
index-estimation fallback, appended-handle columns in the separate selectivity
index-node path, full package artifact mapping, and package validation remain
incomplete.

## Current-master follow-up: appended-handle index-path estimates (2026-09-23)

Go master adds `AdjustRowCountForAppendedHandleColumns` after building ranges
for eligible non-unique secondary indexes. Rust's executor `InitStats` now
appends the signed integer or complete clustered common-handle suffix for its
estimate-only range construction, estimates the declared index prefix with
the existing index histogram, then estimates each fully bound handle
dimension from its column histogram. Repeated suffix bounds are unioned before
counting. The estimates use Go's ascending-selectivity exponential damping,
retain the independence product as the minimum, preserve the prefix maximum,
and cap all bounds at the number of complete point ranges (using Go's default
`tidb_regard_null_as_point=ON` policy). A V0
new-collation common handle suppresses range appending only when it contains a
non-binary string column, matching `DataSource.HasV0NewCollationStringHandle`.
Estimate-only appended dimensions use the complete column values; physical
scan construction continues to use declared common-handle prefix lengths.

Regression coverage:

- `tidb_planner::cardinality::appended_handle_tests` checks damping, minimum
  independence, point caps, and the sub-one prefix floor.
- `tidb_executor::access_cost::index_async_load_queue_tests::appended_handle_range_damps_the_declared_index_prefix_estimate`
  checks the direct statistics boundary, including unchanged maximum and
  the full-point cap.
- `tidb_session::tests_explain::appended_integer_handle_range_lowers_index_scan_estimate`
  analyzes 100 rows, then verifies a bounded integer handle reduces the live
  `IndexRangeScan` estimate in SQL `EXPLAIN`.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner --lib appended_handle_tests -- --test-threads=1
# PASS; 3 tests

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib appended_handle_range_damps_the_declared_index_prefix_estimate -- --test-threads=1
# PASS; damping and full-point cap

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib appended_integer_handle_range_lowers_index_scan_estimate -- --test-threads=1
# PASS; SQL estimate decreases when an integer handle range is added
```

## Current-master follow-up: appended handles in `Selectivity` and recursive stats (2026-09-23)

The `Selectivity` index-statistics nodes now extend eligible fully matched
declared prefixes with extracted signed-integer or clustered common-handle
columns. These estimate-only range columns use full column lengths, consistent
with Go's `Idx2ColUniqueIDs` range construction. The JSON-column path is also
covered by the existing executor regression
`a_json_column_is_skipped_by_the_selectivity_engine`.

For a multi-column recursive estimate whose position lacks a column histogram,
the estimator now tries alternate loaded index histograms in index-id order,
skips candidates with zero total rows, and uses the first usable estimate. The
focused planner regression proves an empty candidate does not prevent a later
loaded index from estimating the missing column. Rust's estimator currently discards codec errors through
`encode_key(...).unwrap_or_default()`. The codec does return errors, including
unsupported raw datums and decimal encoding failures. A fallible result must
reach the recursive caller and the top-level planning boundary; typed datums
do not prove these failures impossible. That source branch remains open.

SQL `EXPLAIN` tests now verify that appended integer and clustered common-handle
predicates lower a secondary-index scan estimate. The focused executor and
planner tests and the session tests matching `appended_` passed after this
change:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner --lib recursive_index_estimation_skips_empty_index_and_uses_next_candidate -- --test-threads=1
# PASS; later nonempty index candidate supplies the missing column estimate

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib appended_handle_range_damps_the_declared_index_prefix_estimate -- --test-threads=1
# PASS; direct boundary damping and point cap

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib appended_ -- --test-threads=1
# PASS; 3 tests, integer handle, common handle, and tuple comparison

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib a_json_column_is_skipped_by_the_selectivity_engine -- --test-threads=1
# PASS; JSON is excluded from ordinary column-stat collection

rustfmt --edition 2021 --config skip_children=true --check \
  rust/crates/tidb-planner/src/cardinality.rs \
  rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs \
  rust/crates/tidb-executor/src/access_cost.rs \
  rust/crates/tidb-executor/src/driver/planner_bridge.rs \
  rust/crates/tidb-session/src/tests_explain.rs
# PASS

git diff --check
git diff --cached --check
# PASS
```

The package remains WIP. Its full Go-source/support-artifact mapping, original
test and fixture coverage, actual recursive-error behavior, package-level
validation gates, and workload plan/performance checks are still open. Do not
report `pkg/planner/cardinality` as transcreated.

## Current-master audit: leading-column fallback and executable source tests (2026-09-23)

Go `HistColl.GenerateHistCollFromColumnInfo` maps an index only under its first
column in `ColUniqueID2IdxIDs`, sorting each list by index ID. Rust incorrectly
accepted any declared or appended key column as a recursive candidate. The
executor now requires a matching leading column. The regression first failed:
adding an unrelated `(c,b)` index changed the `(a,b)` maximum estimate from 999 to 10.
After the correction the estimate is unchanged; adding an eligible `(b,c)`
index still supplies the missing b-column estimate.

The prior recursive-candidate parameter was missing from five integration-test
calls. Those now explicitly pass an empty candidate list. Building the aggregate
planner test target also exposed three stale test literals: two ordinary source
columns now declare `is_generated: false`, and the CTE rule context carries
default estimator options. These repair test compilation without changing
the original fixture behavior.

Validation from the repository root:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib recursive_index_statistics_only_estimate_the_leading_column -- --test-threads=1
# Failed before the fix (10 versus 999); passes after the fix.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all row_count_estimator_source -- --test-threads=1
# 14 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_ -- --test-threads=1
# 21 passed; 41 explicitly ignored; ignored cases are NOT coverage.
```

The first attempted standalone test targets were unavailable because the crate
uses `autotests = false` and the generated `all` harness. The commands above
are the executable targets. The package remains WIP; codec error propagation,
virtual-column fallback, scaled per-index statistics, full original fixture
coverage, final lint/Go gates, and workload measurements need further audit.

Files changed in this audit: `rust/crates/tidb-executor/src/access_cost.rs`;
planner test files `cardinality_mock_stats_ranges_source.rs`,
`row_count_estimator_source.rs`, `casetest_logicalplan_builder_source.rs`,
`core_logical_cte_topn_prune_source.rs`, and
`tests_pointget_plan_cache_source.rs` under `rust/crates/tidb-planner/tests/`;
this receipt and `rust/docs/planner/cardinality-package-parity-execplan.md`.

The candidate builder now skips index scanning and allocation for positions
with usable column statistics and for single-column-only range sets, following
Go's recursive-lookup guard. No workload speedup is claimed without benchmarks.
After that final change:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# 37 passed, including the new negative/positive leading-column regression.
rustfmt --edition 2021 --config skip_children=true --check rust/crates/tidb-executor/src/access_cost.rs
# PASS.
git diff --check
# PASS.
```

## `TestAvgColLen` source mapping (2026-09-23)

Added a native Rust unit regression for both analysis states in Go
`pkg/planner/cardinality/row_size_test.go::TestAvgColLen`. It pins average
encoded size, `DataInDiskByRows` size, and chunk-format size for INT, VARCHAR,
FLOAT, DATETIME, and a NULL-only VARCHAR after one row and after adding a
second row. In particular, the second VARCHAR sample retains Go's 10.5-byte
encoded average and the rounded `avgSize - log2(avgSize)` disk payload.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib cardinality::row_size::tests::average_column_sizes_match_go_avg_col_len_fixture -- --nocapture
# 1 passed.
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh ./pkg/planner/cardinality -run '^TestAvgColLen$' -count=1
# PASS; failpoints enabled and disabled by the wrapper.
```

Other row-size integration paths, package-wide Rust mapping, lint, and workload
measurements remain open; this is not package completion.

The wider formatting check found existing formatting differences in
`cardinality_mock_stats_ranges_source.rs`; checking the committed HEAD version
reproduces that failure. Its change remains two candidate-list arguments.
Full Go package tests, `make lint`, and workload measurements were not run at
this checkpoint. No package-complete integration/commit is claimed.

## Current-master follow-up: virtual-column fallback and ANALYZE output (2026-09-23)

Go `row_count_index.go::expBackoffEstimation` returns unsuccessful backoff if
an unestimated virtual dimension would otherwise be dropped and the index has
a histogram or TopN. Rust now carries virtual-column flags alongside the
per-column statistics from executor metadata. A successful recursive estimate
still supplies that dimension; ordinary missing columns retain backoff.
Recursive calls pass empty virtual metadata, matching Go's nil `idxCols`.

The native regression covers ordinary versus virtual missing statistics,
histogram and TopN-only fallbacks, an empty index with no fallback statistics,
and a successful recursive estimate. It failed before the implementation at
the virtual-column fallback assertion, then passed after the correction.

The SQL fixture ports `TestVirtualColumnIndexEstimation`: 500 deterministic
rows, an `(a,b,d)` index with virtual `d=c+1`, 8 buckets and no TopN, plus the
ordinary-column control. It checks the same estimated-row thresholds (<25 and
>10) and the actual ten matching rows. Rust uses EXPLAIN and a separate COUNT
query here; this does not claim EXPLAIN ANALYZE runtime-counter coverage.
The test also verifies that composite index statistics exist and that no
virtual-column histogram was published. That last assertion exposed a second
bug: in-process ANALYZE built and published such histograms while Go's
`analyze_col_sampling.go` skips them. Publication now omits virtual columns;
materialized sample values still supply index keys, and stored generated
columns retain their histograms. The shared analyzer still calculates the
unused virtual histogram internally; removing that wasted work is an open
performance follow-up, with no measured workload speedup claimed here.

The pre-existing generated-column ANALYZE test incorrectly required all three
column histograms. Its assertions now require the base and stored-generated
histograms and reject the virtual one. The old empty ignored virtual-case
placeholder in `cardinality_mock_stats_ranges_source.rs` was replaced by an
explicit mapping to the live session and native tests. Its comment had mixed
in the recursive error failpoint from `TestNewIndexWithColumnStats`; that
separate error-propagation obligation remains open.

Files changed in this follow-up: planner `src/cardinality/row_count_estimator.rs`,
`tests/cardinality_mock_stats_ranges_source.rs`, and
`tests/row_count_estimator_source.rs`; executor `src/access_cost.rs` and
`src/analyze/kv.rs`; session `src/tests_analyze.rs` and `src/tests_explain.rs`;
this receipt and the cardinality ExecPlan. These paths are relative to their
respective crates under `rust/crates/`.

Validation commands from the repository root:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib recursive_index_estimation_tests -- --test-threads=1
# 2 passed; the new virtual regression failed before the fix.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib virtual_column_index_estimation_preserves_the_selective_suffix -- --test-threads=1
# 1 passed; histogram-absence assertion failed before the ANALYZE correction.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_analyze:: -- --test-threads=1
# 20 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all row_count_estimator_source -- --test-threads=1
# 14 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# 37 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 9 passed; remaining ignored cases are not coverage.
rustfmt --edition 2021 --config skip_children=true --check rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs rust/crates/tidb-executor/src/access_cost.rs rust/crates/tidb-executor/src/analyze/kv.rs rust/crates/tidb-session/src/tests_explain.rs rust/crates/tidb-session/src/tests_analyze.rs
# PASS.
git diff --check
# PASS.
```

This is supporting evidence within the atomic WIP cardinality package claim.
It does not establish whole-package parity or executor/analyzer package
completion. Codec-error propagation, scaled-index statistics, complete
original fixture coverage, full Go/lint gates, cluster execution, and workload
benchmarks remain unverified at this checkpoint.

## Current-master follow-up: scaled MV-index counts (2026-09-23)

Go `HistColl.GetScaledRealtimeAndModifyCnt` scales fully loaded multi-valued
indexes by their analyzed entry count divided by the analyzed table row count.
Column statistics and ordinary indexes stay in table-row units. Rust formerly
passed the table counts into every index estimator and clamped every result to
the table count, underestimating MV-index work.

`IndexRowCounts` now retains both table and index counts. The executor derives
the index scale only for fully loaded MV indexes with positive analyzed table
and index counts. The analyzed baseline checks fully loaded columns first,
then fully loaded non-MV indexes; the MV index itself cannot supply a table-row
baseline. Index range estimation uses scaled entry counts for growth, modified
entries, and bounds. Exponential column backoff keeps the original table counts.
Each recursive index candidate carries its own counts and normalizes both its
selectivity and `maxSel` by its own scaled realtime index count, matching the
current Go-master `expBackoffEstimation` implementation.

The regression failed before the fix: a 50-entry TopN point returned 15 instead
of 75 when a table grew from 1000 to 1500 rows and its index had 5000 analyzed
entries. It now returns 75 and estimates 7500 entries for the full range; modify
counts scale from 100 to 500 while table counts remain unchanged. Controls
verify ordinary indexes, incomplete index load status, and missing analyzed
table counts stay unscaled. A native recursive test verifies the 75-entry point
and its maximum estimate are normalized by 7500 entries, producing 0.01 rather
than division by the caller's 1500 table rows.

Changed files: executor `src/access_cost.rs`; planner
`src/cardinality/row_count_estimator.rs`, `tests/row_count_estimator_source.rs`,
and `tests/cardinality_mock_stats_ranges_source.rs`; this receipt and the
cardinality ExecPlan. Crate paths are under `rust/crates/`.

Validation from the repository root:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib multivalued_index_counts_scale_with_table_growth -- --test-threads=1
# Failed before fix: 15 versus 75; passed after fix.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# 38 passed, including load-status and missing-baseline controls.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all row_count_estimator_source -- --test-threads=1
# 14 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib recursive_index_estimation_tests -- --test-threads=1
# 7 passed, including exact recursive MV selectivity/maxSel and failed-candidate fallback.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 9 passed; remaining ignored cases are not coverage.
rustfmt --edition 2021 --config skip_children=true --check rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs rust/crates/tidb-executor/src/access_cost.rs
# PASS.
git diff --check
# PASS.
```

The error audit also located the infallible `InitStats` visitor and
`StatisticsLoadRequester::initialize` interface. A complete codec-error fix
must cross those boundaries, handle-range estimation, and selectivity callers;
merely converting failures to pseudo estimates would not establish parity.
This remains WIP. Full Go/lint gates, live MV SQL/cluster execution, original
fixture completeness, and sysbench/TPC-C/TPC-H/YCSB measurements were not run.
No workload speedup or whole-package completion is claimed.

## Error-propagation prerequisite: fallible statistics initialization (2026-09-23)

`StatisticsLoadRequester::initialize` now returns
`Result<LogicalPlan, (LogicalPlan, PlanError)>`, matching the logical-rule
ownership convention without cloning the failed plan. `SyncWaitStatsLoadPoint`
propagates that result; a wait failure still prevents initialization. The
executor's first statistics initialization maps the failure into its existing
fallible planning entry point. Successful initialization and the subsequent
statement index-force stamping are unchanged.

The new test checks exact error identity, retained datasource table identity,
and initialization call counts for both failure stages. It passes with
propagation and fails when the rule is temporarily mutated to discard an
initialization error and return its plan as success. That mutation was removed
and the final suite rerun. This is not a claim of an original end-to-end codec
failure reproduced through SQL: the old API could not represent initialization
failure, and the codec-producing estimator/InitStats visitor are still
infallible. Their connection to this boundary remains required.

Files: planner `src/logical/rule_collect_plan_stats.rs`, executor
`src/driver/planner_bridge.rs`, this receipt, and the cardinality ExecPlan.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib sync_wait_ -- --test-threads=1
# 2 passed before the mutation check.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib sync_wait_preserves_statistics_initialization_errors_and_plan -- --test-threads=1
# FAILED under an error-swallowing mutation; mutation then removed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical::rule_collect_plan_stats::tests:: -- --test-threads=1
# Final code: 11 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib statistics_loads -- --test-threads=1
# 1 passed: shared statistics at the next statement boundary.
rustfmt --edition 2021 --config skip_children=true --check rust/crates/tidb-planner/src/logical/rule_collect_plan_stats.rs rust/crates/tidb-executor/src/driver/planner_bridge.rs
# PASS.
git diff --check
# PASS.
```

The atomic package remains WIP. Codec propagation, recursive-error continuation,
CTE initialization failures, full Go/lint validation, live cluster behavior,
and workload measurements remain unverified. No commit/push or package-complete
claim accompanies this prerequisite change.

### 2026-09-23 index-bound encoding errors (package remains WIP)

The preceding initialization prerequisite is now connected to real index-bound
codec failures. Go reference remains `origin/master` at
`56970b286a362b1f0b150c7665453c8f5ff997a9`: `row_count_index.go` returns errors
from each bound's `codec.EncodeKey`; `exponential.go` skips a failed recursive
candidate; `core/stats.go` uses fixed `cost.SelectionFactor` for failed filter
selectivity. These are distinct caller contracts.

`get_index_row_count_for_stats_v2` now returns the codec error instead of encoding
failure as an empty key. Direct index, appended-prefix, and common-handle
estimates preserve that result. `InitStats::initialize_source` is fallible;
the owned visitor captures its first failure and both outer query and CTE
initialization return it. The outer query retains the logical plan in its
existing error tuple. Nested DNF/row-IN selectivity propagates failures to the
filter-facing wrapper, which applies 0.8 once rather than using a session
selectivity override or independently falling back within each disjunct.

The pre-fix regression observed a fabricated 10-row estimate for Raw bounds.
Its initial expected diagnostic was too specific to a different codec API;
inspection established that `encode_key` returns
`InvalidEncoding("unsupported raw datum")`. The final typed assertions cover
both bounds separately and together. Restoring `unwrap_or_default` temporarily
makes both direct-bound and recursive-error regressions fail (exit 101); the
mutation was removed and the final tests pass. Recursive failure can leave
another column usable, while a failed virtual-column estimate still forces
index histogram fallback. Executor regression assertions also verify direct
and appended-prefix error preservation.

Files changed in this step:
- `rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs`
- `rust/crates/tidb-executor/src/access_cost.rs`
- `rust/crates/tidb-executor/src/handle_range.rs`
- `rust/crates/tidb-executor/src/driver/planner_bridge.rs`
- `rust/crates/tidb-planner/tests/row_count_estimator_source.rs`
- `rust/crates/tidb-planner/tests/cardinality_mock_stats_ranges_source.rs`
- `rust/docs/planner/cardinality-package-parity-execplan.md` and this receipt.

Validation commands from repository root:

```text
cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor
# PASS during migration; final executor/session test builds also pass.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib index_encoding_failure -- --test-threads=1
# Error-swallowing mutation: 2 failed as required, then mutation removed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib recursive_index_estimation_tests -- --test-threads=1
# Final code: 5 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all row_count_estimator_source -- --test-threads=1
# 14 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 9 passed, 34 ignored; ignored cases remain parity gaps.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# Final code: 38 passed, including direct/appended-prefix errors.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib statistics_loads -- --test-threads=1
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain::appended -- --test-threads=1
# 2 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib virtual_column_index_estimation_preserves_the_selective_suffix -- --test-threads=1
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib explain_plan_tree_materialized_cte_is_not_refused -- --test-threads=1
# 1 passed; successful CTE planning, not injected-error coverage.
rustfmt --edition 2021 --config skip_children=true --check rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs rust/crates/tidb-executor/src/access_cost.rs rust/crates/tidb-executor/src/handle_range.rs rust/crates/tidb-executor/src/driver/planner_bridge.rs rust/crates/tidb-planner/tests/row_count_estimator_source.rs
# PASS. Mock source file retains pre-existing formatting differences.
git diff --check
# PASS.
```

This does not close the package error audit: column `encode_datum` remains
infallible, statement-context codec warning handling and range construction/
union errors still need source comparison, and injected SQL/CTE and nested
selectivity failure cases remain unverified. Native recursive tests do not
replace all Go failpoint cases. No performance benefit is claimed or measured.
Full Go validation, `make lint`, live-cluster behavior, and workload gates were
not run in this step. No Go/Bazel files changed, so this step did not trigger
`make bazel_prepare`. No partial-package commit or push was made.

### 2026-09-23 column-bound comparison and encoding errors (WIP)

Continued the same atomic package audit against Go master
`56970b286a362b1f0b150c7665453c8f5ff997a9`. Go `getColumnRowCount` compares
prepared bounds before encoding either bound and returns both comparison and
encoding errors. The Rust estimator previously discarded comparison errors
with `is_ok_and` and encoding errors with `unwrap_or_default`, producing a
one-row estimate for an invalid Raw point. The new regression failed on that
pre-fix result.

`EstimationError` now preserves the original comparison or codec cause. Loaded
column estimates and their pseudo-dispatch wrapper are fallible; pseudo cases
retain the existing source dispatch. Column errors propagate through partial
index statistics, index exponential backoff, integer/common-handle estimation,
and the existing planning error boundary. Recursive index candidates still
skip errors, while a loaded column's failed backoff estimate propagates its
error. Appended-handle suffix estimates skip column failures, matching Go
`AdjustRowCountForAppendedHandleColumns`; the declared prefix remains fallible.

Logical filter helpers now distinguish missing statistics (`None`) from an
estimation error (`Err`) using optional results. Recursive DNF errors propagate
to the outer filter wrapper's fixed 0.8 fallback, rather than allowing an NDV
fallback or multiplying a partial estimate. The logical regression checks a
conjunction and a nested OR with an invalid bound. It fails when the old column
error suppression is temporarily restored. A separate comparison-suppression
mutation fails the native comparison regression. Both mutations were removed.

The final native cases distinguish Raw-key encoding failure from scalar/vector
comparison failure. Raw-to-Raw comparison itself is successful in the current
Go/Rust datum dispatch, so the initial expectation of a comparison error for
that pair was corrected by inspecting the implementation. The scalar/vector
case matches Go `Datum.compareVectorFloat32`'s explicit cast-required error.

Files in this step: `rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs`,
`rust/crates/tidb-planner/src/logical/rewrite.rs`,
`rust/crates/tidb-planner/src/logical/data_source.rs`,
`rust/crates/tidb-executor/src/access_cost.rs`,
`rust/crates/tidb-executor/src/handle_range.rs`,
`rust/crates/tidb-executor/src/driver/planner_bridge.rs`, the two planner source
test files `row_count_estimator_source.rs` and
`cardinality_mock_stats_ranges_source.rs`, this receipt, and the package ExecPlan.

Validation from repository root:

```text
cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor
# PASS during migration; final executor/session test builds also pass.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib loaded_column_estimation_preserves_invalid_bound_errors -- --test-threads=1
# FAILED before fix (one-row estimate); FAILED under comparison-suppression mutation.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib invalid_column_bounds_fall_back_once -- --test-threads=1
# PASS after fix; FAILED under old column-error suppression mutation.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib row_count_estimator:: -- --test-threads=1
# Final restored code: 13 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical::rewrite:: -- --test-threads=1
# 8 passed, including the nested-filter regression.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical::data_source:: -- --test-threads=1
# 3 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all row_count_estimator_source -- --test-threads=1
# 14 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 9 passed, 34 ignored: still-open parity cases.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# 38 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib statistics_loads -- --test-threads=1
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain::appended -- --test-threads=1
# 2 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib virtual_column_index_estimation_preserves_the_selective_suffix -- --test-threads=1
# 1 passed.
```

Statement-context comparison flags, timezone and warning handling still require
comparison with Go's `TypeCtx`/`HandleError`; this change preserves failures
from the current typed codec/comparison APIs, not that whole context contract.
Range construction/union errors, injected SQL/CTE error tests, original Go
failpoint cases, full Go validation, `make lint`, live-cluster checks and
sysbench/TPC-C/TPC-H/YCSB performance measurements remain open. No performance
claim, complete-package claim, commit, or push accompanies this partial step.
No Go/Bazel changes were made, so `make bazel_prepare` was not triggered.

Final formatting/diff checks for the column-error step passed:

```text
rustfmt --edition 2021 --config skip_children=true --check rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs rust/crates/tidb-planner/src/logical/rewrite.rs rust/crates/tidb-planner/src/logical/data_source.rs rust/crates/tidb-executor/src/access_cost.rs rust/crates/tidb-executor/src/handle_range.rs rust/crates/tidb-executor/src/driver/planner_bridge.rs rust/crates/tidb-planner/tests/row_count_estimator_source.rs
git diff --check
```

### 2026-09-23 pruned-prefix range-union errors (WIP)

Go master `56970b286a362b1f0b150c7665453c8f5ff997a9`,
`pkg/planner/core/stats.go:pruneEstimateRange`, returns the `UnionRanges` error,
and its caller returns that error before estimating the declared index prefix.
Rust instead cloned the prefix ranges, attempted union, and reused the unmerged
copy on error. When statistics were missing, the pseudo estimator could then
produce a successful estimate for an unencodable prefix.

Extended the existing appended-handle regression to require the original Raw
encoding error with both loaded and absent statistics. It failed before the
fix and passes after it. The prefix path now propagates union failure and moves
its ranges into the union operation; the full fallback clone is gone. This
removes an allocation/copy by inspection, but no benchmark improvement is
claimed. A suffix-only invalid range still preserves all three successful
prefix estimates (estimate, minimum, maximum), matching Go's separate
skip-on-suffix-error contract.

The shared ranger `union_ranges` now returns its actual `CodecError` directly.
Its only fallible operation is bound encoding. A `From<CodecError>` conversion
preserves the existing `PointBuilderError::Unsupported(error.to_string())`
diagnostic for ranger-building callers; the cardinality caller can retain the
original codec cause. A native test covers each bound and both consecutive-merge
settings, alongside existing range merge/build tests.

Changed files: `rust/crates/tidb-executor/src/access_cost.rs`,
`rust/crates/tidb-planner/src/ranger/ranger.rs`,
`rust/crates/tidb-planner/src/ranger/points.rs`, this receipt and
`rust/docs/planner/cardinality-package-parity-execplan.md`. These ranger changes
support the same atomic cardinality package claim; they are not a separate
completed ranger-package claim.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib appended_handle_range_damps_the_declared_index_prefix_estimate -- --test-threads=1
# FAILED before fix on the newly required error with absent statistics.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib ranger:: -- --test-threads=1
# 68 passed, including low/high bound encoding errors and existing builder/detacher tests.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# Final code: 38 passed, including prefix propagation and suffix-skip control.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain::appended -- --test-threads=1
# 2 passed: integer and common appended handles.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib virtual_column_index_estimation_preserves_the_selective_suffix -- --test-threads=1
# 1 passed.
rustfmt --edition 2021 --config skip_children=true --check rust/crates/tidb-executor/src/access_cost.rs rust/crates/tidb-planner/src/ranger/ranger.rs rust/crates/tidb-planner/src/ranger/points.rs
# PASS.
git diff --check
# PASS.
```

Package still WIP: `InitStats::initialize_source` currently skips failures from
`detach_index_range_with_fallback_handler_in`; inspect that caller against Go's
fallible path-building contract next. Statement context/timezone/error-warning
policy, other range-construction wrappers, injected SQL/CTE failures, original
Go/failpoint coverage, `make lint`, live-cluster tests and workload measurements
remain open. No Go/Bazel files changed, so no `make bazel_prepare` prerequisite
was triggered. No commit/push or completed-package claim in this step.

### 2026-09-23 statistics-initialization range-builder failures (WIP)

Go master `56970b286a362b1f0b150c7665453c8f5ff997a9`,
`core/stats.go:detachCondAndBuildRangeForPath`, returns a failed
`DetachCondAndBuildRangeForIndex` result. Rust's expression-based path in
`InitStats::initialize_source` instead continued to the next index. It now
returns a `PlanError`; the owned visitor retains that exact error for the
existing outer-query and CTE initialization callers.

Evaluation failures keep `PlanError`'s typed evaluation identity. Ranger
`Unsupported` failures now use a dedicated `UnsupportedType` plan error and
reach the driver as MySQL 8108, preserving the diagnostic supplied by the
ranger. This matches Go `plannererrors.ErrUnsupportedType`'s code rather than
recasting it as `ErrInternal` 1815. Comparison and cardinality errors still
use the existing internal-error conversion and need the remaining context/
diagnostic audit; this step does not claim all error messages are Go-identical.

The regression constructs a real catalog, indexed table, logical datasource,
and current-statement parameter, then runs the production range detacher and
owned statistics visitor. Unbound standalone-constant and IN predicates fail;
bound controls build index estimates. It checks retained table identity,
absence of a fabricated path estimate after failure, original error identity,
and the 8108 driver conversion. Reinstating the old `let Ok(...) else {
continue; }` makes this regression fail with no recorded error; the mutation
was removed and the final suite passes.

An initial test expected an unbound binary comparison to fail. Source inspection
showed that Go `points.go:buildFromBinOp` deliberately returns no points when
bound evaluation fails, without assigning the builder error. Rust already
matches that exception. The invalid assertion was replaced with an explicit
empty-range control, and no change was made to that builder behavior. Go's
`buildFromConstant` propagates evaluation errors, while `buildFromIn` reports
`ErrUnsupportedType`; those are the actual regression paths used above.

Files: `rust/crates/tidb-executor/src/driver/planner_bridge.rs`,
`rust/crates/tidb-executor/src/driver.rs`,
`rust/crates/tidb-planner/src/plan_base.rs`, this receipt, and
`rust/docs/planner/cardinality-package-parity-execplan.md`.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_preserves_range_evaluation_error -- --test-threads=1
# Corrected constant/IN regression passes; restoring old skip behavior makes it FAIL.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::planner_bridge:: -- --test-threads=1
# Final code: 5 passed, including direct detacher and visitor regression controls.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical::rule_collect_plan_stats::tests:: -- --test-threads=1
# 11 passed, including the plan-preserving initialization error boundary.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib prepared_explain_ -- --test-threads=1
# 2 passed: current parameters and index probe detail.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib statistics_loads -- --test-threads=1
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain::appended -- --test-threads=1
# 2 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib explain_plan_tree_materialized_cte_is_not_refused -- --test-threads=1
# 1 passed: successful CTE planning, not injected CTE failure coverage.
rustfmt --edition 2021 --config skip_children=true --check rust/crates/tidb-executor/src/driver/planner_bridge.rs rust/crates/tidb-planner/src/plan_base.rs rust/crates/tidb-executor/src/driver.rs
# PASS.
git diff --check
# PASS.
```

The package remains WIP. Remaining error work includes AST-facing range helpers
that represent both unsupported inputs and builder errors as `None`, statement
comparison/codec context and warning policies, complete client diagnostic
mapping, and injected SQL/CTE failures. The native visitor regression is not
an end-to-end SQL/CTE error test. Full Go/failpoint validation, `make lint`,
live-cluster validation and all four workload performance gates remain open.
No Go/Bazel source or dependency changes triggered `make bazel_prepare`.
No partial-package commit or push was made.

### 2026-09-23 prepared parameters in statistics index ranges (WIP)

The statistics index-range path used a private resolver that retained only the
session timezone, then called a context-free ranger detacher. That lost current
prepared parameter values while rewriting and evaluating bounds. It also
converted rewrite and detacher errors into “no range,” allowing selectivity to
continue with a partial estimate. Go builds these ranges under the live
statement context and preserves the distinction between a range that cannot
be used and a failed evaluation.

The helper now forwards the caller's complete expression-resolver context,
including prepared values, timezone and SQL modes, charset, constant folding,
comparison context, and resolver-owned column metadata. It uses the ranger's
context-aware detachers and returns typed builder errors. The existing outer
selectivity error path maps such errors to Go's fixed selection-factor
fallback once. The schema fallback remains available for unqualified columns
in range tests whose standalone resolver does not own a column map.

The regression parses one prepared `IN (?,?)` statement and executes range
construction under two parameter sets. The expected ranges change from
`[2,2], [5,5]` to `[7,7], [9,9]`, the residual set stays empty, and AST
restoration remains unchanged. This regression was run before the production
change and failed because no current-parameter range was produced; it passes
afterward.

Files for this step: `rust/crates/tidb-executor/src/index_range.rs`,
`rust/crates/tidb-executor/src/access_cost.rs`,
`rust/crates/tidb-executor/src/index_range/tests/prepared.rs`,
`rust/crates/tidb-planner/src/ranger/points.rs`,
`rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs`, this
receipt, and `rust/docs/planner/cardinality-package-parity-execplan.md`.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_ranges_use_current_prepared_parameters -- --test-threads=1
# 1 passed; the same test failed before the context-aware range change.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_range::tests:: -- --test-threads=1
# 23 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# 38 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib ranger:: -- --test-threads=1
# 68 passed.
rustfmt --edition 2021 --check rust/crates/tidb-executor/src/index_range.rs rust/crates/tidb-executor/src/index_range/tests/prepared.rs rust/crates/tidb-executor/src/access_cost.rs rust/crates/tidb-planner/src/ranger/points.rs rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs
git diff --check
```

The whole `pkg/planner/cardinality` package remains WIP. AST-facing legacy
range helpers still conflate unsupported expressions and build failures with
`None`; injected SQL/CTE failures, full Go/failpoint and lint gates, complete
package test mapping, and sysbench/TPC-C/TPC-H/YCSB performance checks remain
open. This is not an atomic package-completion claim, so it is not committed or
pushed separately.

### 2026-09-23 column-statistics range conversion errors (WIP)

Go's `ranger.BuildColumnRange` returns point-conversion failures to
`cardinality.getMaskAndRanges`, which returns the error from selectivity. Rust's
column statistics helper previously matched every `points_to_ranges` error by
building an empty node, so the outer fixed `SELECTION_FACTOR` fallback never
saw it. The helper now returns `Result`, the access-cost estimator propagates
the error through `EstimationError::Range`, and its existing top-level caller
uses the fixed 0.8 fallback. Range-quota fallback remains a separate successful
result, as it is in Go.

The regression uses the current prepared value `Datum::Raw("unsupported")`
against an integer column. Before propagation, the direct helper returned an
empty access node with the predicate residual; after the fix it returns the
original typed value-conversion error. An access-cost regression checks that
the error reaches the fallible selectivity entry point and that the public
filter estimate uses 0.8.

Files for this step: `rust/crates/tidb-executor/src/index_range.rs`,
`rust/crates/tidb-executor/src/index_range/tests/prepared.rs`,
`rust/crates/tidb-executor/src/access_cost.rs`,
`rust/crates/tidb-executor/src/ranger_detacher.rs`, this receipt, and
`rust/docs/planner/cardinality-package-parity-execplan.md`.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib column_statistics_ranges_preserve_conversion_errors -- --test-threads=1
# FAIL before the fix: returned Ok with an empty access node; PASS after the fix.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib column_range_conversion_error_reaches_selectivity_fallback -- --test-threads=1
# 1 passed: typed error reaches fallible selectivity and public filter fallback is 0.8.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_range::tests:: -- --test-threads=1
# 24 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# 39 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib ranger_detacher::tests:: -- --test-threads=1
# 6 passed.
```

The fallible column entry catches point-conversion failures after access
conditions have been identified. `points_for_condition` and `constant_value`
still represent some rewrite/evaluation failures as `None`; compare those
branches with Go's `PointBuilder` error policy before closing statement-context
parity. The package inventory, original Go/failpoint tests, `make lint`,
SQL/CTE injected-error paths, and workload performance gates remain open.

### 2026-09-23 column-statistics IN evaluation errors (WIP)

Go's `buildFromIn` reports unsupported-type errors when a candidate IN value
cannot be evaluated; ordinary binary-comparison bound evaluation errors remain
empty ranges. Rust's AST range adapter had returned `None` for both. It now
replays Go's typed point builder only for candidate IN branches whose values
failed the literal fast path, preserving Go's distinct error policy while
leaving successful long IN lists on the allocation-light AST path. Nested
conjunctions validate only branches that can bound this column; a disjunction
is validated only when both arms can bound it, matching Go's access-condition
checker.

The regression covers direct and parenthesized `IN`, comparison, nested `AND`
and `OR`, an unrelated-column OR arm, `a IN (b + 1)`, and
`a IN (?) OR a IN (b + 1)`. Both nonconstant-list cases first failed because
the validator surfaced an unsupported-type error even though Go's checker
excludes those predicates before range building; they pass after both direct
validation and nested-branch eligibility were restricted to Go-constant list
items. LIKE was source-checked separately: Go excludes nonconstant, NULL, and
nonprintable patterns before the column-range builder; the Rust AST path also
declines those inputs, so no broader LIKE error replay was added.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_range::tests:: -- --test-threads=1
# 25 passed, including direct and nested IN/comparison policy cases.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# 39 passed, including fixed selectivity fallback on conversion errors.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib ranger_detacher::tests:: -- --test-threads=1
# 6 passed.
```

These remain package seed changes, not a package-complete claim. AST rewrite
failures outside candidate IN, SQL/CTE injected-error behavior, complete Go
test/support mapping, Go/failpoint and lint gates, and sysbench/TPC-C/TPC-H/YCSB
workload checks remain open. Nothing from this partial package step is
committed or pushed.

### 2026-09-23 column-statistics collation-key LIKE range (WIP)

Go's `BuildColumnRange` builds string points with `convertToSortKey=true`, then
converts the field type to binary before `points2Ranges`. Rust's ANALYZE and
statistics loader likewise store ordinary string histogram bounds as
collation keys. The AST column-range adapter had declined a nonbinary prefix
LIKE as a full range; after enabling sort-key construction, its final
`points_to_ranges` conversion used the original string type and reinterpreted
the key bytes as text. It now uses the same binary-collated range type as Go,
preserving key bytes through range materialization.

The `utf8mb4_general_ci` `s LIKE 'ab%'` regression checks the exact key bounds
`[00 41 00 42, 00 41 00 43)` and confirms the condition is counted as an access
condition. It failed first with no access range and then, once sort-key
construction was enabled, with `Datum::String` bounds instead of
`Datum::Bytes`.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_range::tests:: -- --test-threads=1
# 26 passed, including the new collation-key regression.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost::tests -- --test-threads=1
# 28 passed.
rustfmt --edition 2021 --config skip_children=true --check \
  rust/crates/tidb-executor/src/index_range.rs \
  rust/crates/tidb-executor/src/index_range/tests/prepared.rs
git diff --check
# clean
```

This closes only the nonbinary prefix-LIKE range representation gap in the
AST-facing column-statistics path. Other pattern forms and the complete Go
package, error, integration, and workload gates remain open.

### 2026-09-23 full index-range estimation fast path (WIP)

Go's `GetRowCountByIndexRanges` checks `canSkipIndexEstimation` before
`IndexStatsIsInvalid`. For a full range including NULLs on an ordinary
non-partial, non-multi-valued index, the table's real-time row count is exact;
consulting an evicted histogram would add work and enqueue an unnecessary
asynchronous load. Rust had the normalized range-policy leaf but no live
caller. `access_cost::index_row_count` now applies the policy before its load
queue and returns the table's real-time count. Partial and multi-valued index
metadata come from the existing `KvTable` owners.

The regression first failed because the full range queued the evicted index.
It now verifies the exact 60-row estimate and no queued load. Controls verify
that full-not-null, bounded, and exclusive-NULL ranges still queue, and that
partial and multi-valued full ranges do not take the shortcut.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib full_index_range_skips_evicted_histogram_load -- --test-threads=1
# FAIL before the fix: the full-range call queued the evicted index; PASS after.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# 40 passed, including the full-range shortcut and its exclusion controls.
rustfmt --edition 2021 --config skip_children=true --check \
  rust/crates/tidb-executor/src/access_cost.rs
git diff --check
# clean
```

This removes the redundant histogram/load work only for Go's exact fast-path
case. The rest of the Go package inventory, original test suite, build/lint
gates, and workload measurements remain open.

### 2026-09-23 cross-crate source-test mapping (WIP)

The source suite's empty ignored placeholders for Go
`TestCanSkipIndexEstimation` and
`TestDefaultStringMatchSelectivityZeroImprovesLikeEstimation` are now mapping
notes to production-path regressions in `tidb-executor` and `tidb-session`,
respectively. The full-index test verifies its exact row count and async-load
behavior plus exclusion controls for other range/index shapes. The LIKE test
uses 5 matching and 95 nonmatching rows with analyzed TopN statistics and
checks that the default 0.8 setting estimates 80 rows, while setting
`tidb_default_string_match_selectivity` to zero brings the TableReader estimate
closer to the actual count. The source-mapped planner
suite now has 41 tests: 9 pass and 32 remain ignored; these two cases are
covered by native executor/session tests rather than empty planner
placeholders.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source:: -- --test-threads=1
# 9 passed; 32 ignored.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib default_string_match_selectivity_zero_improves_like_estimates -- --test-threads=1
# 1 passed.
```

The remaining ignored cases still name unverified behavior and are not
removed from the package acceptance inventory. Full source/test mapping,
Go/failpoint, lint, and workload gates remain open.

## Partial-statistics EXPLAIN follow-up (2026-09-23)

The physical EXPLAIN renderer now emits the selected index's source-shaped
partial marker for a non-pseudo table when its index status is not fully
loaded. The session regression covers both fully loaded omission and
`stats:partial[idx_ab:missing]` after removing an analyzed index histogram.
The shared used-stat formatter also now ports `UsedStatsInfoForTable`'s
EXPLAIN ordering, three-entry truncation, grouped remainder count, pseudo
label, and missing-name fallback. This does not yet port Go's statement-local
status collection across every column/index estimate or the complete
`TestPartialStatsInExplain` matrix.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib explain_marks_missing_stats_for_selected_index -- --nocapture --test-threads=1
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --test all used_stats_explain -- --nocapture --test-threads=1
# 2 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1
# 52 passed before the final fully-loaded assertion was added; the focused test passes with that assertion.
git diff --check
# PASS.
```

Earlier Go oracle runs through the failpoint wrapper required elevated
workspace permission because `.git/.failpoint-state` is sandbox read-only; the
wrapper restored its refcount to zero. The focused master-overlay oracle run
and command are recorded in the latest-master follow-up above.

## Statement-scoped partial-statistics EXPLAIN follow-up (2026-09-23)

The partial-statistics EXPLAIN path now records only statistics items consumed
by the current `InitStats` estimator pass. Column and index stats-node creation
callbacks write into `StmtContext`'s per-statement ledger; table and index scan
rendering uses the shared Go-shaped formatter with physical-table-local names.
The session regression covers full-load omission, missing index and column
markers, `unInitialized` for an analyzed column without initialized histogram
data, `allEvicted` for an index, and verifies later statements do not inherit
an earlier statement's statuses. This supersedes the earlier formatter-only and catalog-status-only
notes above, while the complete Go `TestPartialStatsInExplain` matrix and
partition variants remain open.

```text
cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor
# PASS; existing workspace warnings remain
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --test all used_stats_explain -- --nocapture --test-threads=1
# 2 passed
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib explain_marks_missing_stats_for_selected_index -- --nocapture --test-threads=1
# 1 passed, including missing column/index and statement-isolation assertions
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1
# 52 passed after the final column-status and statement-isolation assertions
```

Mutation check: temporarily disabling the column stats-node observer makes the
focused session regression fail with the table scan missing
`stats:partial[b:missing]`; restoring the observer makes the regression pass.

Follow-up correction (2026-09-23): a proposed partition-local regression was
removed after it failed for an invalid expectation. The explicit
`PARTITION (p0)` query retained the logical table stats identity in this
session path, and the matching Go fixture does not expect a partial marker on
those partition scans. This probe is not evidence of partition-local parity.
The partitioned join case from the Go fixture is now covered below.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain::
# 52 passed, 0 failed.
```

## Go `TestPartialStatsInExplain` matrix follow-up (2026-09-23)

Added the source test's five query/status assertions to the Rust SQL EXPLAIN
coverage: an incomplete then full partitioned-table scan, an `allEvicted` join
then a fully loaded join, and the partitioned three-table join with its
`IndexHashJoin` shape and `allEvicted` marker. The in-memory Rust session has
no asynchronous histogram loader, so the test switches the same cached
column/index load metadata around the source queries. This verifies formatting
and selected-plan status behavior, while the actual loader lifecycle remains
open. The new join case exposed that the correlated lookup index was selected
after the cardinality observer ran; physical EXPLAIN now records the selected
index's non-full-load status as a fallback when annotating that scan.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib explain_partial_stats_match_go_plan_matrix -- --nocapture --test-threads=1
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1
# 53 passed, 0 failed.
git diff --check
# PASS.
```

## Group-NDV session ratio root fix (2026-09-24)

The Rust leaf estimator already implemented Go's exponential NDV blend, but
the production estimator over `StatsInfo` returned only the conservative
maximum. It also had no session ratio input. Go reads
`RiskGroupNDVSkewRatio` in `EstimateColsNDVWithMatchedLen` and passes its
statement context at each caller.

The formula now has one profile-based implementation. The statement cost
snapshot exposes the ratio directly; logical joins, projections and
aggregations, Apply-cache costing, index-join probe floors, and MPP
grouping-set statistics carry that value to the estimator. The old
context-free interfaces retain Go's default `0.0`. CTE exposes a ratio-aware
stats method, but recursive CTE stats derivation remains outside the live
logical stats fold and is still an integration gap.

Regression coverage verifies ratio `0` and `1` for multi-column join keys,
GROUP BY, and projection expression NDVs, plus that `SET
tidb_opt_group_ndv_skew_ratio = 0.4` updates new statement snapshots while an
existing statement retains the old value.

```text
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib group_ndv_skew_ratio_is_snapshotted_for_planning
# PASS
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib session_group_ndv_skew_ratio
# 3 passed
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib logical::operator_tests::projection
# PASS; projection regression subset
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib index_join_probe_access_rows_floor
# PASS
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib final_mode_agg::
# 8 passed
cargo test --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_ndv_skew_source
# 3 passed, 2 ignored (existing source mappings)
cargo check --manifest-path rust/Cargo.toml -p tidb-executor
# PASS
```

This closes the production logical/physical ratio plumbing, not the full
`pkg/planner/cardinality` completion claim. Inventory and behavioral gaps,
Go package parity validation, `make lint`, workload plan comparison, and
sysbench/TPC-C/TPC-H/YCSB performance checks remain open. No package commit or
push is due yet.

## Appended integer-handle estimate with partial column statistics (2026-09-24)

Go `TestIndexRangeEstimationWithAppendedHandleColumn` constructs an analyzed
column-statistics-only collection with no index histogram, then explains a
query whose non-unique `(a,b)` index execution range also constrains its signed
integer primary-key handle. Added a Rust session regression that removes the
analyzed index histogram from the cache while retaining the column histograms.
It verifies the physical `[3 3 3,3 3 3]` range estimates 1.00 and the scan
reports `stats:partial[idx_ab:missing]`. This covers the combination of
appended-handle range construction, partial column-stat estimation, and
statement-local EXPLAIN status; the planner source harness now maps this
ignored leaf to the session regression. The sibling truncated-integer and
prefixed-common-handle leaves also map to their production session tests.

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-cardinality-master-overlay/overlay.json -run '^TestIndexRangeEstimationWithAppendedHandleColumn$' -count=1
# PASS; master source fixture, failpoint wrapper refcount returned to zero.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib appended_handle_range_uses_partial_column_statistics -- --nocapture --test-threads=1
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1
# 56 passed, 0 failed.
```

The master-overlay Go package suite passes with
`-skip '^TestVirtualColumnIndexEstimation$'`. Running that test through the
same external overlay did not invoke its `afterRecursiveIndexEstimation`
failpoint: `failpoint-go-test.sh` enables call sites by rewriting Go files in
the repository package tree, while the overlay source is under `/private/tmp`.
The diagnostic callsite did run for index 1, but its callback was not
instrumented, leaving `firstCandidateFailed` false. This is an overlay/test
tooling limitation, not evidence about the source implementation. The original
Go package suite on the checked-out sources and the Rust recursive-estimator
tests remain recorded separately. Full package mapping, complete Go/Rust gates,
lint, and workload validation remain open.

## Unique-index point estimates from `TestEstimationUniqueKeyEqualConds` (2026-09-24)

Activated the source-mapped unique-index case in
`cardinality_mock_stats_ranges_source.rs`. It checks Go's full-length unique
point-range shortcut for values 7 and 6 through Rust's production
`get_index_row_count_for_stats_v2` API; both estimates are exactly one. The Go
test analyzes a CMSketch, but `GetRowCountByIndexRanges` returns from the
unique full-length point branch before reading CMSketch contents, so the Rust
estimator-native fixture preserves the tested behavior without imitating the
unobserved sketch internals. Go's separate primary-key column-range assertions
and the live ANALYZE/statistics lifecycle remain open mappings. The same
regression also checks the Go test's two primary-key handle column probes at
exactly one row each.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 11 passed, 30 ignored.
```

This adds mapped test evidence, not a behavior change or package-completion
claim. Full source/support inventory, remaining test mappings, Go and Rust
package gates, `make lint`, and workload checks remain open. No package commit
or push is due yet.

## New index backed by existing column statistics (2026-09-24)

Mapped Go `TestNewIndexWithColumnStats` to
`tidb_session::tests_explain::newly_created_index_estimates_from_existing_column_statistics`.
The SQL fixture builds two identical 500-row modulo-250 tables, analyzes only
the first, then creates its `(a)` index after analysis. For `a > 5 AND a < 25`,
the first table's new index has no own histogram but uses its column histogram:
EXPLAIN ANALYZE observes 38 rows and estimates within 0.1 of 38. The equivalent
index scan on the table with no stats has a different estimate, matching the
Go test's comparison.

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestNewIndexWithColumnStats$' -count=1
# PASS; failpoint state refcount returned to zero.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib newly_created_index_estimates_from_existing_column_statistics -- --nocapture --test-threads=1
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1
# 59 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 11 passed, 30 ignored.
```

The failpoint wrapper initially could not write to the sandbox's read-only
`.git`; the same targeted command completed with the required temporary write
access and cleaned its state. This is a test mapping and no package-completion
claim: inventory, remaining mappings, complete validation gates, `make lint`,
and workload validation remain open. No package commit or push is due yet.

## Current-master recursive index estimator correction (2026-09-24)

Comparing the Rust recursive backoff path to `origin/master`'s
`pkg/planner/cardinality/row_count_index.go::expBackoffEstimation` found two
stale Rust assumptions. First, Go sets `foundStats` only after a recursive
index estimate succeeds. A failed candidate leaves the virtual column
unestimated, does not lower `maxSel`, and permits the source index histogram or
TopN fallback after all candidates fail. Rust had set the flag before
estimation and forced `maxSel` to zero on error. Second, current Go normalizes
both the candidate estimate and its maximum by that candidate's scaled
real-time index-entry count. Rust divided `maxSel` by the caller's table row
count, which is wrong for scaled multi-valued indexes.

Rust now follows both current-master rules. The regression for the scaled
maximum was run against the Go-master value first and failed before the fix
(Rust returned 0.05 where Go's candidate-index denominator gives 0.01). A new
codec-error regression also failed before the fix because a failed recursive
candidate incorrectly suppressed virtual-column fallback. Existing recursive
error cases now verify that errors do not fabricate zero selectivity, later
dimensions can still contribute, and a virtual dimension with no successful
candidate triggers the source-index fallback.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib recursive_index_uses_scaled_index_count_for_selectivity_and_max -- --nocapture --test-threads=1
# Failed before denominator fix: 0.05 versus 0.01; passes after.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib failed_recursive_estimates_do_not_suppress_virtual_index_fallback -- --nocapture --test-threads=1
# Failed before foundStats fix; passes after.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib recursive_index_estimation_tests -- --test-threads=1
# 7 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib cardinality::row_count_estimator:: -- --test-threads=1
# 14 passed.
```

This closes two estimator discrepancies only. The larger Go package source and
test inventory, package-wide validation, `make lint`, workload checks, and
performance measurements remain open. No package commit or push is due yet.

## NULL column and index range goldens (2026-09-24)

Added `null_column_and_index_ranges_match_cardinality_goldens` for all ten
queries from Go `TestColumnIndexNullEstimation`, using the source's exact
five-row dataset and analyzed `idx_b(b)` / `idx_c_a(c,a)` indexes. It asserts
the recorded root row estimates, index versus table scan, and exact NULL/range
bounds for index scans. These cover 4 rows for `b IS NULL`, 1 for `b IS NOT
NULL`, 4 for the NULL-or-greater DNF, 5 for the forced full index scan, 1 for
`b < 4`, then table estimates 1/4/2/5/3 for the five column/full-scan cases.
The original testdata goldens support all ten assertions. The planner mock
source placeholder now maps to this active session regression.

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestColumnIndexNullEstimation$' -count=1
# PASS; failpoint state refcount returned to zero.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib null_column_and_index_ranges_match_cardinality_goldens -- --nocapture --test-threads=1
# 1 passed; all ten golden queries matched.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1
# 59 passed, including the new-index, NULL-range, composite point-get, and appended-handle cases.
```

This adds coverage rather than changing estimator behavior. Source/support
inventory, other mappings, full package gates, `make lint`, and workload
validation remain open. No package commit or push is due yet.

## Clustered composite primary-key equality (2026-09-24)

Added `clustered_composite_primary_key_equality_matches_go_point_get` for Go
`TestUniqCompEqualEst`: ten rows share `a=1`, the table has clustered
`PRIMARY(a,b)`, and ANALYZE runs before the complete `a=1 AND b=5 AND 1=1`
equality query. Rust produces a 1.00 `Point_Get` using `PRIMARY(a,b)`, matching
the cardinality-suite golden. The source harness placeholder maps to this
session-level SQL/EXPLAIN case.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib clustered_composite_primary_key_equality_matches_go_point_get -- --nocapture --test-threads=1
# 1 passed.
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestUniqCompEqualEst$' -count=1
# PASS; failpoint state refcount returned to zero.
```

The first Rust assertion expected the handle in the wrong EXPLAIN column; the
actual plan row placed the clustered-index metadata in the access-object field,
matching the Go golden. The assertion now checks only the plan data the source
test pins. Package inventory, validation gates, and workload checks remain
open; no package commit or push is due yet.

## JSON selectivity and histogram-load regression (2026-09-24)

The existing JSON test only constructed a JSON-typed table and did not invoke
the behavior named by the test. It now runs `Selectivity` on `j IS NULL` with
the JSON column marked as having evicted/missing statistics. It checks that
the predicate uses Go's generic 0.8 fallback and that the column's
`TableItemID` is not inserted into `ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS`. This
directly verifies the current-master rule to skip JSON columns before ordinary
column-stat collection.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib a_json_column_is_skipped_by_the_selectivity_engine -- --test-threads=1
# 1 passed.
```

The test-only strengthening does not close package inventory, remaining
behavior mappings, Go/Rust package gates, `make lint`, or workload checks. No
package commit or push is due yet.

## Within-bucket range-skew estimate (2026-09-24)

Activated `risk_range_skew_ratio_widens_within_bucket_estimates` with a
single-bucket, no-TopN index histogram matching Go's skewed ten-row fixture.
The production index estimator now has a direct assertion that the closed
`[2,3]` estimate rises strictly across `risk_range_skew_ratio` values 0, 0.5,
and 1. This validates the estimator's same-bucket widening. Go's global/session
variable inheritance checks remain a separate session-settings obligation;
this native fixture directly supplies the immutable estimator options.

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRiskRangeSkewRatioWithinBucket$' -count=1
# PASS; failpoint state refcount returned to zero.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all risk_range_skew_ratio_widens_within_bucket_estimates -- --nocapture --test-threads=1
# 1 passed.
```

This closes the within-bucket estimator math case only. Package source/test
inventory, session/global variable parity, complete validation gates, lint,
and workload checks remain open. No package commit or push is due yet.

### Session/global skew-setting semantics (2026-09-24)

Extended the SQL regression through the source test's session changes and
global change. The Rust test verifies the current session retains its explicit
ratio after `SET GLOBAL`, then verifies `SET SESSION ... = DEFAULT` restores
the compiled-in `0.0` default. Source inspection of Go `pkg/executor/set.go`
shows DEFAULT lookup uses `GlobalSystemVariableInitialValue(sysVar.Name,
sysVar.Value)`; `pkg/sessionctx/vardef/tidb_vars.go` sets the initial value to
`0.0`. The upstream test comment says “inherit the global variable,” but its
assertion only checks that the post-DEFAULT estimate is below the ratio-1
estimate, so it does not establish inheritance. The Rust expectation follows
the implementation contract, not the weaker comment.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib within_bucket_range_skew_setting_changes_analyzed_index_estimate -- --nocapture --test-threads=1
# 1 passed.
```

The package-wide behavior/test mapping, Go/Rust package gates, `make lint`, and
workload checks remain open. No package commit or push is due yet.

Follow-up validation on 2026-09-24:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1
# 60 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 11 passed, 30 ignored.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib cardinality::row_count_estimator:: -- --test-threads=1
# 14 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
# 42 passed.
git diff --check
# clean.
```

The refreshed inventory and focused suites close no remaining ignored Go
cases. Full package parity, `make lint`, and workload plan/performance gates
remain open.

### `TestIssue39593` composite prefix sweep (2026-09-24)

Replaced the ignored placeholder with the source fixture: two uniform
54-NDV/repeat-10 column histograms, a two-column index histogram over the nine
encoded pairs from `[0,3) x [0,3)` at repeat 60, and twenty leading-prefix
point ranges for values 1 through 20. It asserts 462.6 (±1) at 540 realtime
rows and 5400 (±1) after the realtime count grows tenfold.

The initial Rust fixture incorrectly set index StatsVer to 2 and produced
302.6. The Go test constructs `statistics.Index` without setting StatsVer;
`SetIdx` does not supply it, so it remains version 0. At that version Go uses
the legacy index histogram path and does not dispatch v2 exponential backoff.
Using the source's zero value matches both estimates. The source test is
unchanged between the local checkout and `origin/master`.

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestIssue39593$' -count=1
# PASS; failpoint state refcount returned to zero.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all issue_39593_composite_prefix_point_ranges_match_estimates -- --nocapture --test-threads=1
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 12 passed, 29 ignored.
git diff --check
# clean.
```

This maps the Go test without changing production behavior. The other 29
ignored cases, remaining source/test ownership mappings, `make lint`, and
sysbench/TPC-C/TPC-H/YCSB gates remain open. No package commit or push is due.

### `TestLastBucketEndValueHeuristic` (2026-09-24)

Replaced the ignored test with direct calls to the production column and index
estimators using a source-shaped five-bucket histogram for values 1 through
11: values 1–10 contribute 100 rows each, value 11 contributes one row, and
the final bucket carries the low repeat count. The test verifies baseline
value 11 at 1 row, +10 modifications remain within the source's 0.5 tolerance,
+100 modifications lift value 11 to 100.09, and ordinary value 3 scales to
109.99. The same triggering and control estimates are asserted for an encoded
single-column index histogram. No estimator change was needed.

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestLastBucketEndValueHeuristic$' -count=1
# PASS; failpoint wrapper returned its refcount to zero.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all last_bucket_end_value_heuristic_lifts_underrepresented_counts -- --nocapture --test-threads=1
# 1 passed.
```

The source harness now has 13 active tests and 28 ignored mappings. This closes
only this estimator test: full package mapping, Go/Rust gates, `make lint`, and
the four workload checks remain open. Do not commit or push the partial
package.

Revalidated the full source harness after activating both Issue39593 and the
last-bucket case:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 13 passed, 28 ignored.
rustfmt --check --edition 2024 rust/crates/tidb-planner/tests/cardinality_mock_stats_ranges_source.rs
# clean after formatting the in-scope test module.
git diff --check
# clean.
```

### `TestNewIndexWithoutStats` access-path choices (2026-09-24)

Added a session SQL/EXPLAIN regression for the six source choices over 501
rows and indexes `(a)`, `(c,a)`, `(b)`, then `(a,b)`: a new single-column
index without stats loses an equal-coverage tie to analyzed `idxa`; after
ANALYZE it still loses the tie; a newly created `(a,b)` index wins when its
leading columns cover more equality/range predicates despite missing stats;
the final predicate chooses analyzed `idxca` when its equality coverage ties
the new composite index. The test sets the same high full-scan cost factor as
Go and checks the chosen index in brief EXPLAIN output.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib new_index_without_stats_skyline_choice_matches_go -- --nocapture --test-threads=1
# 1 passed.
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestNewIndexWithoutStats$' -count=1
# PASS; failpoint wrapper returned its refcount to zero.
```

This closes the six Go access-path assertions; it does not close the rest of
the package. 28 source harness cases remain ignored, and package-wide
validation, `make lint`, and all four workload checks remain open. No package
commit or push is due yet.

Focused follow-up validation:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1
# 61 passed at the time of this check; a later skyline regression follow-up reran the same subset with 62 passing.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 13 passed, 28 ignored.
rustfmt --check --edition 2024 rust/crates/tidb-session/src/tests_explain.rs
rustfmt --check --edition 2024 rust/crates/tidb-planner/tests/cardinality_mock_stats_ranges_source.rs
git diff --check
# all clean.
```

### `TestIssue57948` new-index skyline choice (2026-09-24)

Added the analyzed-column/new-index SQL regression for Go `TestIssue57948`.
Before the fix Rust used a table full scan despite the valid `idxb` equality
range; a forced-index EXPLAIN confirmed the range candidate was constructible.
The source root cause was four incomplete skyline inputs: Rust did not mark an
unanalyzed index pseudo when its table was analyzed, omitted Go's full-index
match fact, did not have table-path metrics to compare against the index, and
discarded `insert_skyline_candidate`'s missing-stat winner signal. Go only
applies the prefer-range override after comparisons set `idxMissingStats`.
Rust now carries these facts into skyline pruning and evaluates that override
after candidate insertion. The regression asserts `idxb` is chosen and also
retains a forced-index plan diagnostic in assertion output.

```text
cargo test -p tidb-session single_new_index_with_column_stats_is_chosen -- --nocapture
# 1 passed.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session tests_explain:: -- --test-threads=1
# 62 passed.
cargo test -p tidb-planner skyline_tests -- --nocapture
# 4 passed.
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestIssue57948$' -count=1
# PASS; failpoint refcount returned to zero.
rustfmt --check --edition 2024 rust/crates/tidb-session/src/tests_explain.rs
git diff --check
# clean.
```

`rustfmt --check` on `dispatch.rs` reports formatting differences throughout
the already modified file, including unrelated existing planner test code;
no whole-file formatting was applied to avoid unrelated churn. The package is
still not complete: 28 source harness cases are ignored, remaining source/test
mapping and `make lint` are open, and sysbench/TPC-C/TPC-H/YCSB validation has
not run. No commit or push is due for this partial package.

### `TestIndexEstimationCrossValidate` split SQL and estimator coverage (2026-09-24)

Added an active session SQL regression for both Go EXPLAIN cases: after
ANALYZE, a composite `(a,b)` equality returns a one-row index range; after
pre-reading `b` and analyzing only `idx_b`, the empty/invalid index stats do
not replace the five-row table scan estimate. The planner estimator's
cross-validation case now uses a real StatsVer1 `CmsSketch` with 100,000
inserted counts, forcing Go's source branch where cross-validation must beat
a maximally noisy sketch. The SQL outcomes and estimator decision are split
across `tidb-session` and `tidb-planner`, so the source-shaped placeholder
remains ignored but points to both active tests. The lower CMS crate separately
tests the Go failpoint-conversion seam; estimator validation uses real
high-count sketch data rather than a test-only global override through SQL
planning.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib cardinality::row_count_estimator::equal_cond_selectivity_tests:: -- --test-threads=1
# 5 passed, including cross_validation_wins_over_a_maximally_noisy_cms.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-stats --test all source_query_bytes_failpoint_returns_the_scripted_go_int_conversion -- --test-threads=1
# 1 passed; negative and positive Go int conversions match the CMS seam.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session tests_explain:: -- --test-threads=1
# 63 passed after the rename.
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
# 13 passed, 28 ignored; this source test's behavior is mapped across planner/session tests.
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestIndexEstimationCrossValidate$' -count=1
# PASS; failpoint refcount returned to zero.
rustfmt --check --edition 2024 rust/crates/tidb-session/src/tests_explain.rs rust/crates/tidb-planner/tests/cardinality_mock_stats_ranges_source.rs
git diff --check
# clean.
```

The SQL assertions pass in Rust and Go, and a real noisy CMS exercises the
same production estimator decision without a test-only global override.
Package inventory, remaining test mappings, `make lint`, and all workload
checks remain open.

### `TestIssue64137` refreshed stats metadata boundary (2026-09-24)

Go's `TestIssue64137` inserts 2,000 rows after ANALYZE, flushes the session
delta to `mysql.stats_meta`, then calls `StatsHandle.Update` before checking
the out-of-range estimate. The estimator sees a 12,000-row realtime count and
2,000 modified rows against the original analyzed histogram, producing 24
rows for the out-of-range index lookup and 12,000 for the TopN equality.

Rust's SQL-only `Session::new` harness has no domain stats collector or cache
reload worker. Its local `Catalog::flush_stats_delta` only creates metadata
for unanalyzed tables. The parity test therefore explicitly installs the
same refreshed metadata on the analyzed table before checking the two
estimates; the test documents this boundary instead of attributing the
missing worker to cardinality logic. The source-mapped planner harness entry
remains ignored, and full cluster stats-delta lifecycle coverage remains
open.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib small_ndv_out_of_range_index_reader_rows_match_go \
  -- --nocapture --test-threads=1
# 1 passed.

GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality -run '^TestIssue64137$' -count=1
# PASS; failpoint refcount returned to zero.

rustfmt --edition 2024 rust/crates/tidb-session/src/tests_explain.rs
git diff --check -- rust/crates/tidb-session/src/tests_explain.rs
# clean.
```

This maps the cardinality estimator outcome, not the cluster stats-delta
worker. Package-wide source/test coverage, `make lint`, and sysbench/TPC-C/
TPC-H/YCSB validation remain open; no package commit or push is due yet.

### `TestDeriveTablePathStatsNoAccessConds` table-path count (2026-09-24)

Go initializes every table path's `CountAfterAccess` from its datasource row
count; `deriveTablePathStats` leaves that value unchanged when there are no
access conditions. Rust's stats initializer instead left
`table_path_count_after_access` unset unless it found a local ranger range.
`InitStats::initialize_source` now seeds the field with the realtime row
count, and a recognized handle-range estimate may refine it afterward.

The new planner-bridge regression constructs a 1,000-row stats table with no
predicate and asserts `Some(1000.0)`. It failed before the fix with `None`.
The Go source test passes unchanged.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib unfiltered_table_path_count_uses_realtime_row_count \
  -- --nocapture --test-threads=1
# 1 passed; the pre-fix run failed with None instead of Some(1000.0).

GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality -run '^TestDeriveTablePathStatsNoAccessConds$' -count=1
# PASS; failpoint refcount returned to zero.
```

The empty source-harness placeholder now maps to this executor-boundary
regression. Full recursive plan-property comparisons and the remaining
package inventory/gates/workload checks remain open.

### `TestCrossValidationSelectivity` clustered PK range (2026-09-24)

Added the source test's exact two-row clustered primary-key fixture and
predicate. Rust now asserts the `(1 0,1 1000)` `TableRangeScan` estimate of
2.00 and the residual `c > 1000` `Selection` estimate of 1.00. The source
shaped placeholder maps to this SQL `EXPLAIN` test.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib cross_validation_on_clustered_pk_range_matches_go \
  -- --nocapture --test-threads=1
# 1 passed.

GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality -run '^TestCrossValidationSelectivity$' -count=1
# PASS; failpoint refcount returned to zero.
```

The clustered-key plan and residual estimate are covered at the SQL boundary;
the broader Go cardinality suite, remaining source mappings, `make lint`, and
sysbench/TPC-C/TPC-H/YCSB validation remain open.

### `TestIgnoreRealtimeStats` analyzed-count behavior (2026-09-24)

Go `GetStatsTable` copies cached stats in determinate mode, replaces
`RealtimeCount` with the loaded analyzed row count, and zeros `ModifyCount`
before data-source stats/path initialization. Rust already disabled modify
count inside histogram estimators but had kept the realtime count on
`TableStatistics`, so a 15-row cache after four inserts still yielded a
15-row scan instead of Go's frozen 11-row analyzed count.

`InitStats::initialize_source` now applies the same session-local copy before
building `StatsInfo`, histograms, or access-path estimates. It preserves the
shared catalog object for sessions using the moderate objective. A regression
failed before the fix with determinate scan rows at 15, then passed at 11;
moderate mode remains at 15.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib determinate_objective_uses_analyzed_row_count_after_inserts \
  -- --nocapture --test-threads=1
# Passed after; failed before because determinate mode reported 15, not 11.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_explain:: -- --test-threads=1
# 66 passed.

GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality -run '^TestIgnoreRealtimeStats$' -count=1
# PASS; failpoint refcount returned to zero.
```

The regression now covers Go's initial unanalyzed phase as well: moderate mode
uses the realtime count while determinate mode sees the pseudo-stat estimate.
It also covers both post-ANALYZE phases, including determinate mode staying at
the analyzed row count after inserts while moderate mode uses refreshed
metadata. The domain stats-delta/cache refresh lifecycle remains open because
the session harness supplies the refreshed metadata explicitly. Package-wide
mapping, lint, and all four workload checks remain open.

### `TestEstimationForUnknownValuesAfterModify` estimator boundary (2026-09-24)

The Go analyzed fixture has ten values with count 10 each; the default ANALYZE
TopN retains all ten values, leaving an empty bucket histogram with NDV 10 and
100 TopN rows. The Rust source-shaped test preserves that shape and reuses it
with the exact post-insert metadata. It asserts value 5 at 10 rows, unseen
value 11 at the zero-modification fallback of 1, and unseen value 15 after 200
modifications strictly between 1 and 10. An initial histogram-only fixture
returned 10 for value 11; that was a fixture mismatch, not a production
estimator difference. The Go SQL insertion/ANALYZE and stats-handle refresh
lifecycle remain open.

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner --test all \
  cardinality_mock_stats_ranges_source::estimation_for_unknown_values_after_modify_stays_bounded \
  -- --test-threads=1
# 1 passed.

GIT_DIR=/private/tmp/tidb-failpoint-state.git \
GIT_WORK_TREE=/Users/qiliu/projects/tidb \
GOCACHE=/private/tmp/tidb-go-cache GOTOOLCHAIN=go1.25.12 \
  ./tools/check/failpoint-go-test.sh pkg/planner/cardinality \
  -run '^TestEstimationForUnknownValuesAfterModify$' -count=1
# PASS; failpoint refcount returned to zero. The temporary Git metadata/cache
# paths work around this sandbox's read-only .git and Go build-cache mounts.
```

This closes the estimator arithmetic for the source case, not its stats-handle
lifecycle. The package receipt remains partial.

### Committed table-delta lifecycle (2026-09-24)

The SQL stats-delta regression now gets write counts from the same physical
KV-table events that update Go's `TxnCtx.TableDeltaMap`. Rust snapshots that map
for each statement and savepoint, restores it on errors/rollback, and publishes
only after commit. Local mode transfers committed modify counts into the
catalog; cluster mode sends them to its session stats collector. The cluster
transaction test verifies one surviving insert is published after rolling a
second insert back to a savepoint, and that the collector is cleared on commit.

```text
cargo test --offline --locked -p tidb-session --lib flush_stats_delta
# 4 passed.

cargo test --offline --locked -p tidb-session --lib \
  tests_explain::determinate_objective_uses_analyzed_row_count_after_inserts -- --exact
# 1 passed.

cargo test --offline --locked -p tidb-session --lib tests_savepoint
# 14 passed.

cargo test --offline --locked -p tidb-server --lib \
  table_delta_follows_commit_rollback_and_savepoint_lifecycle -- --test-threads=1
# 1 passed outside the sandbox. The sandbox denies Darwin sysctl used by the
# SharedStats fixture; no product code change was needed to work around it.
```

Stats-delta row/modify count behavior is now covered for local table/database
scopes, changed updates, deletes, partitions, transaction/savepoint rollback,
and cluster commit publication. Go warnings for invalid scopes, cascade
deletes, cluster broadcast/remote behavior, full original test mapping, package
lint, and workload checks remain open. The cardinality package is not complete.

### TopN-assisted LIKE EXPLAIN fixtures (2026-09-24)

The production SQL path creates the Go fixture's six string columns and
40-row distribution, runs ANALYZE with three TopN entries, and executes all 28
queries under each collation mode. The comparison checks every recorded plan
row's operator, estimated rows, task, access object, and operator detail;
only generated Rust plan-ID suffixes are normalized. The source-harness
placeholder now points to this executable session test.

```text
cargo test --offline --locked -p tidb-session --test topn_assisted_string_match -- --test-threads=1
# 1 passed; covers 56 recorded plans across both collation modes.

GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality \
  -run '^TestTopNAssistedEstimation(WithoutNewCollation|WithNewCollation)$' \
  -count=1
# PASS; failpoint refcount returned to zero.
```

The remaining package inventory, validation gates, and workload benchmarks
are still open. The original Go transient histogram-bounds test also passes
through the failpoint wrapper:

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality \
  -run '^TestStringMatchSelectivityDoesNotRestoreTransientHistogramBoundsSelection$' \
  -count=1
# PASS; failpoint refcount returned to zero.
```

The transient-bounds invariant is covered natively at the Rust logical-filter
boundary. Go temporarily changes a histogram chunk's selection while
vector-evaluating LIKE; Rust's shared `HistColl` is immutable and has no
analogous chunk-selection state. The regression matches Go's 2/3 estimate for
`LIKE '%R%'` over `BRASS`, `IRON`, and `STEEL` and asserts the shared histogram
buckets are unchanged:

```text
cargo test --offline --locked -p tidb-planner --lib \
  string_match_estimation_does_not_mutate_shared_histogram -- --test-threads=1
# 1 passed.
```

### Unknown-value lifecycle column coverage (2026-09-24)

Mapped the source test's column-estimate lifecycle through a production local
session: analyze an empty table, insert/analyze ten values, publish ten more
committed rows, and assert the source's 2-row out-of-range point plus 4-row
`[9,30]` and `[9,MaxInt64]` estimates. After TRUNCATE, a single analyzed NULL
row estimates one row over `[1,30]`; after recreation with only an index on
`b`, the uncollected `a` column uses the expected 0.001 pseudo estimate.

An initial probe returned 1 for the recreated table because the test helper
incorrectly selected the first present column-stat entry (`b`) while claiming
to test `a`. The Go oracle confirmed that ANALYZE under `PREDICATE` collects
only column `b` there (`TestEstimationForUnknownValues` passes); keying the
Rust lookup by `a`'s actual metadata ID correctly exercises the missing-stats
pseudo path. No product estimator change was warranted. The source placeholder
maps to the active local-session lifecycle test and a direct executor estimator
test for the composite index. The latter uses the Go snapshot shape: ten
analyzed TopN values, then the current row count/modify count of 20/10. Rust
returns 1 for point 30 and 2 for `[9,30]`, matching Go. The placeholder stays
ignored to avoid an empty duplicate test body; both production mappings are
active in their owning Rust crates.

```text
cargo test --offline --locked -p tidb-session --lib \
  unknown_value_estimates_follow_analyze_and_truncate_lifecycle -- --test-threads=1
# 1 passed.

cargo test --offline --locked -p tidb-executor --lib \
  unknown_values_in_composite_index_ranges_match_go -- --test-threads=1
# 1 passed.

GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality -run '^TestEstimationForUnknownValues$' -count=1
# PASS; failpoint refcount returned to zero.
```

The complete cardinality package, remaining source mappings, lint, and
workload performance checks remain open. No package completion or commit/push
is claimed.

### New-index skyline fixture (2026-09-24)

The `TestNewIndexWithoutStats` source test is now explicitly mapped to the
existing Rust SQL regression. It uses the same 501-row distribution and
sequence: a newly-created single-column index loses an equal-coverage tie to
the analyzed index both before and after re-ANALYZE; a new composite index
wins when it covers more predicates despite missing statistics; and the
analyzed `(c,a)` index wins when it ties the composite index's equality count.
Both the Rust EXPLAIN assertions and the focused Go oracle pass.

```text
cargo test --offline --locked -p tidb-session --lib \
  new_index_without_stats_skyline_choice_matches_go -- --test-threads=1
# 1 passed.

GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality -run '^TestNewIndexWithoutStats$' -count=1
# PASS; failpoint refcount returned to zero.
```

This closes that fixture mapping only. Other cardinality source cases, the
package-wide gates, lint, and sysbench/TPC-C/TPC-H/YCSB checks remain open.

### Unknown-value post-analyze delta lifecycle (2026-09-24)

Added a local-session production-path test for `TestEstimationForUnknownValuesAfterModify`.
It analyzes ten integer values with ten rows each, confirms an analyzed value
estimates 10 and an unseen value with no modifications estimates 1, then runs
the same two insert-select statements as Go, flushes stats deltas, and confirms
the unseen value's estimate rises above 1 while remaining below 10. The test
restores the prior global auto-analyze setting after its assertions. This
complements the existing synthetic estimator arithmetic test with the actual
stats-delta and catalog lifecycle.

```text
cargo test --offline --locked -p tidb-session --lib \
  unknown_value_estimates_follow_modify_delta_lifecycle -- --test-threads=1
# 1 passed.

GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality -run '^TestEstimationForUnknownValuesAfterModify$' -count=1
# PASS; failpoint refcount returned to zero.
```

Package completion still requires the remaining cardinality mappings,
package-wide checks, lint, and workload performance validation.

### Global partition delete metadata lifecycle (2026-09-24)

The pinned Go `TestGlobalStatsOutOfRangeEstimationAfterDelete` oracle passes all
13 plans. After deleting half the rows and refreshing stats, Go's logical stats
have `(RealtimeCount, ModifyCount) = (2000, 1000)` while retaining the merged
histogram. Rust's stats-delta flush previously refreshed each physical
partition's metadata but left the logical dynamic-pruning entry at 3,000 rows.
The catalog now refreshes the logical realtime and modification counts from its
physical partition entries while preserving global histogram payloads.

The live Rust session test replays all 13 SQL inputs both before and after
analyzing p4, asserting Go's estimate, partition selection, and 2,000-row
`TableFullScan` count for each plan. The first Go plan estimates 191.04 rows
for `a <= 300`; Rust now matches it and the other twelve plans.

The remaining root cause was partial ANALYZE's global-statistics rebuild. Rust
rescanned the entire table and published p4's new histogram as the logical
global histogram, resetting `ModifyCount` and losing the unchanged partitions'
out-of-range histogram mass. Go's stats handle merges the per-partition stats;
Rust now calls `tidb_stats::merge_partition_stats_item` for complete global
column and index statistics, including histograms, TopN, CMSketch, and FM
sketches. The session test checks all plans before and after analyzing p4, so
the source placeholder now maps to that active production-path test.

```text
cargo test --offline --locked -p tidb-session --lib \
  global_partition_out_of_range_estimates_survive_delete_and_partition_analyze -- \
  --nocapture --test-threads=1
# 1 passed; all 13 Go root estimates, partitions, and scan counts match both
# before and after partial ANALYZE.

GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/planner/cardinality -run '^TestGlobalStatsOutOfRangeEstimationAfterDelete$' \
  -count=1
# PASS; failpoint refcount returned to zero.
```

This closes only this Go source case. The remaining source inventory,
package-wide gates, lint, and workload performance validation remain open.


### Advisory merge ordering follow-up (2026-09-24, partial evidence)

The ordinary OR candidate keeps alternatives through property matching and carries
per-partial advisory order feedback in `CopTask`. Shared TopN attachment now
follows Go master's `handleAdvisorySortItemsForIndexMerge`; the threshold fixture's
`c <= 50` case matches its complete Go plan. The `c <= 100` case still chooses
merge instead of ordered lookup, so the fixture and package remain incomplete.
The ratio fixture passes, as does actual-row forced-merge versus table-scan
comparison for both directions with LIMIT/OFFSET. No golden outputs were changed.

See `rust/docs/planner/access-path-structural-parity-audit.md` for the pinned
source inventory, exact validation commands and remaining shared logical
candidate ownership gap. These changes are transitional evidence, not a package
completion receipt or benchmark claim.


### 2026-09-25 structural dependency update — not package completion

Declared index key slots now exist before statistics loading, as in Go master's
plan builder, and survive `InitStats` refresh. Pruning consumes the effective key
through the same handle-append helper as range construction. This replaces the
old static-only scorer with Go's coverage and redundancy selection, and restores
the sysbench mixed-predicate lookup estimate to the verified 33.33 Go value.
The structural audit records source hashes, failing-before regressions and exact
commands. Logical (399), candidate (47), refresh (2), builder (1), SQL handle (47,
2 ignored), and focused Go reference tests pass; lint passes. The original
ordering-threshold fixture still has one mismatch. Complete source/test mapping,
shared logical ownership of ordinary/merge candidate ranges and workload
performance checks remain open. No package-level commit/push is authorized by
this partial evidence.


### 2026-09-25 logical merge lifecycle — dependency work, package incomplete

Both supported merge forms are generated with logical statistics and retained
with ordinary candidate identities. Physical search consumes these candidates;
merge hints prune ordinary paths before costing. Shared state invalidation,
clone independence, repeated-property reuse and cached-statistics derivation
have active regressions. The structural audit records all exact commands:
399 logical, 47 candidate, 2 refresh, 24 executor merge and 4 session merge tests
pass, plus the Go-master hinted-intersection result reference and lint.
The ordering-threshold mismatch remains. The new AND oracle reveals different
partial-filter placement and estimate stages; matching results do not close
that gap. Complete package mapping, all required variants and workload
benchmarks are still unverified. No package completion or commit/push yet.


### 2026-09-25 ordinary fill and intersection ownership — package incomplete

Ordinary detached ranges and filter partitions now live with the logically
derived index state. Normal-index intersections clone those paths, preserve
prefix-value rechecks and attach pushable filters to the build sides. Ordinary
physical scans reuse the same state. Access-count adjustment and index-filter
counts precede the shared minimum-selectivity calculation, following Go's
stage order; the initializer still produces the underlying access estimates.

The exact handle-expression regression fails before the fix and passes with
Go's two 10-row scans, two 8-row build selections and one-row probe. The prefix
fixture returns rows 1 and 4 and retains a probe recheck. Both Go-master overlay
references pass. The ordinary physical reuse test forbids expression
reevaluation. Final checks pass: 399 logical, 47 candidate, 2 initialization,
24 executor merge and 5 session merge tests (4 bootstrap gaps remain ignored),
plus `make lint` and `git diff --check`. Exact commands, intermediate build
fixes, reference pin and earlier handle/fixture validation limits are in
`rust/docs/planner/access-path-structural-parity-audit.md`.

Residual-filter skyline facts, histogram estimator ownership, general OR/MV
and ordered variants, warning/hint precedence, the single original threshold
plan mismatch, full source/test mapping and workload performance remain open.
This dependency stage does not complete the cardinality package; no atomic
package commit/push is made.


### 2026-09-25 candidate comparison inputs — package incomplete

Removed two dispatcher pruning shortcuts that bypassed Go's full comparison.
The analyzed incomparable-columns regression fails before the fix and now
selects Go's `ic(c)` lookup instead of a full table scan. Filled ordinary paths
retain FullIdxCols separately from the ranger prefix, including missing slots
and handle suffixes; candidate facts include residual index filters, index-stage
counts, exact risk bounds and source DNF equality/full-match rules. Native
coverage checks those facts independently of physical allocation.

Validation: 48 candidate, 399 logical and 19 session hint tests pass; the Go
reference passes with failpoints restored to zero; `make lint` and
`git diff --check` pass. Broader EXPLAIN has 76 passes and the two previously
recorded threshold failures; original goldens are unchanged. Exact commands
and reference details are in the structural audit's candidate-comparison
section. The complete ordinary skyline still runs after physical construction,
and table/merge candidate admission and estimator ownership remain separate.
Those lifecycle gaps, remaining variants, package inventory/gates and workload
measurements prevent package completion or commit/push.


### 2026-09-25 table ownership and early ordinary comparison — package incomplete

Integer/common-handle table paths now share the retained logical owner with
ordinary indexes. Table physical construction reuses ranges, filters and
adjusted access counts; the key layout follows Go's usable-prefix rules and
ranger errors propagate. Ordinary candidate admission and the full skyline
comparison precede physical allocation. The pseudo-winner signal and forced
partial-order marking survive that boundary; the later fold is skipped for
prepared sources.

Mutation evidence: disabling early comparison makes the dominated-index case
allocate 5 plans instead of 2; disabling table-state reuse triggers its
panicking physical evaluator. Restored code passes 48 candidate, 399 logical,
2 initialization and 19 hint tests. Four pinned-master reference tests and
`make lint` pass; `git diff --check` passes. The broader EXPLAIN run still has
76 passes and two known threshold failures, with unchanged Go goldens. Exact
commands, mutation logs and validation timing limits are in the structural
audit. Merge conversion, unfilled/MV fallback, full heuristic/range-preference
scheduling, estimator production, source/variant inventory and benchmark gates
remain open. No package completion or commit/push is claimed.

Final handle coverage for this stage: `cargo test --offline --locked
--manifest-path rust/Cargo.toml -p tidb-session --lib handle -- --test-threads=1`
passes 59 tests with 5 explicitly ignored cases. This includes sysbench handle
read/write shapes; it is correctness coverage, not a throughput measurement.


### 2026-09-25 merge convergence boundary — package incomplete

Ordinary survivors and converged merge alternatives are prepared before
physical allocation. Production union conversion takes only chosen partials;
the old choose-and-build entry point is test-only. Go's unhinted single-index
union rejection and explicit-hint exception now have native fail-before/pass-
after evidence and matching Rust/Go SQL regressions. Candidate preparation
allocates no physical IDs.

Checks: 48 candidate, 20 hint, 24 executor merge and 5 session merge tests pass
(4 session bootstrap gaps remain ignored); the focused Go reference and lint
pass. Broader EXPLAIN remains 76 passes/2 known threshold failures with original
goldens unchanged. Exact commands and source evidence are in the structural
audit's merge-convergence section. That audit also records a newly identified
unfinished-versus-concrete CountAfterIndex/CountAfterAccess distinction for the
next minimum-selectivity verification. Ordered execution, general OR/MV
residuals, source estimator ownership, package inventory and workload gates
remain open. No package completion or commit/push is claimed.

### 2026-09-25 statistics stage ownership — package incomplete

Pinned-master source tests confirm unfinished unions use the unset
CountAfterIndex for the logical minimum. Rust now follows that stage boundary.
Merge scans also preserve the source table profile as Go's common scan
constructors do; losing HistColl had underestimated row sizes and merge cost.
The profile-removal regression fails, and restored code passes 48 candidate,
20 hint, 24 executor merge and 5 session merge tests (4 ignored). Both original
ordering-setting goldens pass unchanged; the EXPLAIN sweep is now 77 passing
with one separate analyzed threshold failure. Exact commands and source cost
evidence are recorded in the structural audit. Full package/source/workload
gates and other structural variants remain open; no completion or package
commit/push is claimed.

`make lint` and `git diff --check` pass for this stage.

### 2026-09-25 natural merge admission — package incomplete

Exact Go master references prove the six merge expectations. Rust's natural
merge enumeration incorrectly required an ordered parent; removing that guard
restores Go's child-order-based admission. The native regression fails before
and passes afterward. Separately, the analyzed threshold expectation was
incorrect: Go also chooses TableReader/TopN at both settings. The replacement
pins six complete Go-verified plans including forced-index alternatives.

Checks: 49 candidate, 101 broad EXPLAIN, 19 session merge-join, 20 hint and 8
executor merge-join tests pass. Three exact Go reference tests pass and restore
failpoints. Full commands and cost evidence are in the structural audit's
natural-merge section. Whole-package/source/workload requirements remain open;
no package completion or partial commit/push is claimed.

`make lint` and `git diff --check` passed after the natural-merge change.

### 2026-09-25 shared merge LIMIT lifecycle — package incomplete

Go's union/intersection task attachment and embedded reader limit are now
represented directly, with native fail-before coverage for partial truncation,
residual filters, schema order and reader sinking. SQL also failed until the
obsolete root-only parent merge-hint override was removed; datasource hint
pruning now remains authoritative. Validation: 98 task, 49 candidate, 21 hint,
101 broad EXPLAIN and 24 executor merge tests pass. The exact Go union LIMIT
reference passes and restores failpoints. Exact commands are in the structural
audit. Ordered conversion metadata, expected-count estimates, table-side work
reduction and whole-package/workload gates remain open. No package completion
or partial commit/push is claimed.

`make lint` and `git diff --check` passed after the shared merge LIMIT changes.

### 2026-09-25 shared union estimate lifecycle — package incomplete

Union overlap now uses ordinary filter dispatch, including Go's uncovered
pseudo DNF recursion and existing ranger same-column grouping. Physical
conversion retains ExpectedCnt and adjusts partial/probe counts without
mutating logical candidates. Fail-before regressions now match the exact Go
reference: 19.99 unlimited rows, 259.75 for grouped DNF, and 2.00/4.00 partial/
probe rows with LIMIT 1,3. Validation passes 101 EXPLAIN, 21 hint, 399 logical,
49 candidate and 24 executor merge tests plus the Go oracle. Exact commands
and remaining metadata/residual/intersection/package/workload gaps are in the
structural audit. No whole-package completion or partial commit/push claimed.

`make lint` and `git diff --check` pass after the shared union estimate changes.

### 2026-09-25 ordered union metadata — package incomplete

Required order now crosses union candidate preparation and cop-to-reader
conversion into the existing ordered executor; advisory order stays separate.
Appended-handle matching borrows the shared normalized layout. Two fail-before
SQL checks pass afterward, and exact Go ASC/DESC/LIMIT/hidden-key/handle-order
plans and outputs agree. Validation: 22 hint, 101 EXPLAIN, 49 candidate, 98 task
and 24 executor merge tests pass, as does the Go reference. Master fetch found
no advance beyond 633a9e37f1. Exact commands and remaining grouped-range,
residual/MV/cache/estimate/package/workload gaps are in the structural audit.
No whole-package completion or partial commit/push is claimed.

`make lint` and `git diff --check` pass for the ordered-union stage.

Retained table-request follow-up (2026-09-25): merge execution now rebuilds the
retained table tree through the shared physical builder, substituting only
the handle scan. Fail-before operator-order regression, task-local LIMIT/TopN
state, 25 executor merge tests, 24 builder tests, 23 hint tests, 101 EXPLAIN
tests, exact Go SQL oracle, and lint pass. Full commands/evidence are in the
structural audit's “Retained merge table request execution” section. Whole
package completion, distributed execution and workload performance remain
unverified; no atomic package claim, commit, or push.

Merge producer follow-up (2026-09-25): initial index task sizing now reuses
ordinary lookup's Go CalculateBatchSize port; direct index and executor-backed
partial sources grow complete tasks to IndexLookupSize independently of
executor chunks. Fail-before cap regression, growth/reopen/key-alignment tests,
26 merge, 25 builder and 23 hint tests, existing sizing regression and lint
pass. See structural audit for exact commands. Remaining producer lifecycle,
full package mapping, distributed and workload gates remain open.

Ordered heap follow-up (2026-09-25): original Go TestIssues70910 exposed
Rust's stale 1024-row retention cap (LIMIT 2000 returned 1024). Rust now
separates bounded initial allocation from full offset+count retention and
uses Go's binary-heap insertion/eviction/drain design. Original Go test,
fail-before/pass-after SQL mapping plus large offsets/descending order,
27 native merge tests, 24 SQL hint tests and lint pass. Exact commands are
in the structural audit. Worker scanned-key/partition lifecycle, whole
package mapping and distributed/workload gates remain open.

Union worker-demand follow-up (2026-09-25): source requests now carry the
remaining raw offset+count across direct, executor-backed and partition
sources before dedup; intersections stay unbounded. Ordered partition refill
is demand-driven. Fail-before six-versus-five regression, 29 merge tests,
26 builder tests, 24 SQL hint tests and lint pass. Exact commands and scope
are in the structural audit. Distributed worker/request/cancellation/memory
lifecycle, complete package inventories and workload gates remain open.

Merge cancellation follow-up (2026-09-25): merge collection and table-task
delivery now poll the existing statement cancellation authority. Fail-before
regression showed successful collection after QueryInterrupted; all three
merge modes now stop and close sources exactly once. Cancellation before
startup opens nothing. 30 merge tests, 24 SQL hint tests and lint pass; exact
commands are in the structural audit. Concurrent/distributed execution,
CPU-phase interruption, handle memory accounting, whole package mapping and
workload gates remain open.

Streaming-union follow-up (2026-09-25): unordered union now retains its
process state and delivers one table task on demand rather than buffering
the entire union. A residual-plus-outer-LIMIT fail-before regression now
reads six handles from the first partial and zero from the second, including
after reopen. Table failures release live sources and preserve the primary
error. 31 merge, 27 builder and 24 SQL hint tests plus lint pass. Exact
commands are in the structural audit. Concurrent worker/channel behavior,
handle quota ownership, package inventories and workload gates remain open.

Handle-map dependency follow-up (2026-09-25): memory-ownership audit exposed
quadratic per-insertion rescans and wrong accounting in txnkv MemAwareHandleMap.
It now uses the shared Go checkpointed map by handle domain/partition. Exact
Go deltas, 3 native handle tests, 38 KV source tests, release microbenchmarks
and lint pass; commands/evidence are in the structural audit. Merge tracking
is still unwired; full package and workload gates remain open.

Intersection ownership follow-up (2026-09-25): intersection membership now
uses the shared per-partition MemAwareHandleMap and Go's batched map/payload/
counter accounting. The fail-before one-byte quota case raises the typed
error, the 32-integer case records a 959-byte peak, and success/error both
release all process-tracker bytes. Common-handle/partition coverage, 33 merge,
27 builder and 24 SQL hint tests plus lint pass. Exact commands are in the
structural audit. Union/ordered accounting, concurrent/distributed ownership,
whole package mapping and workload gates remain open.


Unordered union ownership follow-up (2026-09-25): UnionProcess now retains
a process tracker and charges incoming handle capacity before dedup, matching
Go master fetchLoopUnion. Shared deferred-detach ownership also serves
intersection. The fail-before regression now covers duplicate/slack charges,
quota error and release on EOF, Close, cancellation and Drop. All 34 merge
and 24 SQL hint tests plus make lint pass; exact commands and limitations are
in the structural audit. Ordered-union/task accounting, concurrent workers,
full package mappings and workload gates remain incomplete. No package
commit or push was made.


Shared union identity follow-up (2026-09-25): both union modes now reuse
HandleMap per resolved partition and share handle conversion with intersection.
The characterization test preserves integer/common and partition separation
before/after; 35 merge and 24 SQL hint tests plus lint pass. Exact commands
and ordered taskMap retention mismatch are in the structural audit. This is
seed evidence within the whole-package effort, not package completion.


Ordered merge ownership follow-up (2026-09-25): the heap now references rows
in retained batches, matching Go taskMap retention through duplicate filtering
and eviction. Permutation/retention coverage, 35 merge tests, 24 SQL hint tests,
lint and diff checks pass; commands are in the structural audit. Keys are still
Datum vectors rather than producer chunks, so ordered memory accounting and
full package/workload gates remain open. No package completion is claimed.


Typed ordered-key follow-up (2026-09-25): production partials retain declared
key types in chunks through partition and heap stages. NULL/string partition
coverage and producer capacity checks pass, with 36 merge, 27 builder and
24 SQL hint tests plus lint. Exact commands are in the structural audit.
Go's additional index handle/physical-ID columns, ordered accounting and
whole-package/workload gates remain open. No completion claim is made.


Ordered index layout follow-up (2026-09-25): builder-selected index tasks now
retain keys plus handle fields and any retained physical-ID column; table
tasks keep only keys. Overlapping key/handle slots remain duplicated as in Go.
Mutation proof fails without the suffix; 36 merge tests, 24 SQL hint tests and
lint pass. Audit contains exact commands and open composite/physical-ID,
ordered-accounting and whole-package/workload obligations.


Ordered process accounting follow-up (2026-09-25): retained typed chunks,
shared handle memory (including duplicates), and source heap push/pop deltas
now use the process-owned tracker through table-task construction. Fail-before
quota, exact peak, early eviction and tracker release checks pass; 38 merge
tests, 24 SQL hint tests and lint pass. Audit records exact commands and fixture
corrections. Common/partition quota variants, producer/table task accounting,
concurrency, full package mapping and workload performance remain open.


Partial scratch ownership follow-up (2026-09-25): executor partials now own a
worker tracker and release cumulative per-extraction chunk charges on every
return. The fail-before quota case, exact three-fetch peak and EOF release
checks pass. 38 merge, 27 builder and 24 SQL hint tests plus lint pass; audit
contains exact commands. Direct unordered cursor and final table-task memory,
concurrent ownership and whole-package/workload gates remain incomplete.


Completed table-task follow-up (2026-09-25): table tasks now finish before row
exposure and retain handles/chunks/row references with their tracker until
replacement or Close. Next fills MaxChunkSize across tasks as Go does. The
late-error regression fails before and passes after; quota/final-task retention
checks, 39 merge tests, 27 builder tests, 24 SQL hint tests and lint pass. The
old six-handle LIMIT assertion is superseded by source-derived eighteen-handle
native behavior; audit explains concurrency limits and remaining obligations.
No whole-package completion or workload speedup is claimed.


Table-plan consistency follow-up (2026-09-25): retained physical shape now
controls Go's bare-table handle/row-count invariant. Missing-row fail-before
regression, filtered/limited plans, cleanup error precedence, 40 merge tests,
27 builder tests, 24 SQL hint tests and lint pass; audit contains commands.
Deferred table-reader Close errors are logged. Distributed/concurrent variants,
direct cursor tracking and complete package/workload gates remain open.


Merge comparator follow-up (2026-09-25): ordered merge and partition workers
reuse cached tidb-chunk column comparators instead of decoding typed Datums
for every comparison. NULL/collation/direction differential coverage, 41 merge
tests, 24 SQL hint tests and lint pass. A temporary debug comparison showed
18.5–18.8 ms versus 25.5–26.2 ms for 100,000 long-string comparisons; source
was restored. Exact commands and limits are in the audit. No workload or whole
package completion claim is made.


NDV source mapping follow-up (2026-09-25): exact TestOptScaleNDVSkewRatioSetVar
and TestIssue54812 SQL fixtures failed before (10.20 versus 19.44; 9.18 versus
65.23) and pass after carrying the session scaling authority into initial,
recursive and physical statistics paths. Shared ScaleNDV arithmetic is unchanged.
Original Go tests pass, source blob identity was checked, and 103 EXPLAIN/
411 logical tests plus snapshot isolation and lint pass. Exact commands and
remaining default-scaling/source-lifecycle gaps are in the structural audit.
Whole-package and workload gates remain incomplete; no commit/push was made.

Task-context follow-up (2026-09-25): Go's shared root-selection conversion reads
statement settings from PlanContext. Rust now retains estimator and NDV scaling
settings together across ordinary/merge task dispatch, MPP composition and
exchange enforcement. The fail-before NDV regression passes for table, index,
union, intersection and MPP conversions at three settings. Task 109, candidate
49, exchange 6, EXPLAIN 103, lint and diff checks pass; exact commands/logs are
in the structural audit's statement-context section. The package remains
incomplete; no new Go oracle/workload validation or package commit/push.

Three-stage statistics follow-up (2026-09-25): source review found Expand scaled
NDVs and the following projection/partial aggregate consumed the pre-expansion
profile. The shared profile flow now matches Go adjust3StagePhaseAgg. A physical
tree regression fails before and passes after, checking replicated rows, stable
NDVs/group NDVs/metadata and an unchanged child. Task 109, aggregation 8, original
Go TestMPPMultiDistinct3Stage, lint and diff checks pass. See the structural audit
for exact commands and logs. Full Rust MPP SQL/golden matrix and package/workload
gates remain open; this is not a completed-package claim or commit.

NULL lifecycle follow-up (2026-09-25): physical point/index-lookup selections
no longer apply NULL selectivity a second time to the derived datasource profile.
The unused NULL-only datasource derivation method is removed. Four Go-reference
SQL cases now match (lookup 25, batch-point 10), with execution outputs checked;
the regression failed before at 12.5. All 104 EXPLAIN and 49 candidate tests,
lint and diff checks pass. Exact commands, oracle and logs are in the structural
audit. The full package and workload gates remain incomplete; no commit/push.

## Verified ordering-fixture mapping and remaining cost boundary (2026-09-25)

Both complete source suites are mapped, superseding the historical mismatch
reports above. `ordering_index_selectivity_threshold_matches_go_fixture` checks
all 32 statements (28 EXPLAIN plans and four settings), and
`ordering_index_selectivity_ratio_matches_go_fixture` checks all 21 statements
(15 plans and six settings). `check_cardinality_query_fixture` compares complete
rows directly, with no plan normalization or excluded cases. The shared mock
constructor matches Go's zero-based generateIntDatum, version-2 histogram objects,
zero table StatsVer, per-value repeat counts and full-load flags. The local
output fixture matches master blob `d7afee3258f456e421b0903af5801fe5f8238d8e`.
The old source placeholder incorrectly described these 53 statements as 53
EXPLAIN plans and as unwired; it now names both active session tests.

Verification from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib ordering_index_selectivity_ -- --test-threads=1
    git hash-object pkg/planner/cardinality/testdata/cardinality_suite_out.json
    git rev-parse origin/master:pkg/planner/cardinality/testdata/cardinality_suite_out.json

Three tests pass (the two suites plus statement snapshot isolation). In the
pinned reference `/private/tmp/tidb-go-master-20260923`:

    GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestOrderingIdxSelectivity(Threshold|Ratio|RatioForJoin|RatioForMergeJoin|RatioForApply)$' -count=1

The five original Go tests pass; failpoint refcount returns to zero. Logs:
`/private/tmp/tidb-ordering-mapping-{rust,go}.log`.

The three join-cost suites remain unmapped. Code inspection finds a concrete
cross-layer boundary, not merely absent test setup: executor ExplainFormat::parse
rejects verbose; planner Explain::prepare_schema describes verbose columns but
render_result returns RendererUnavailable; ExplainOperator has no estimated-cost
field. Implementing only format parsing would therefore still be wrong. Required
next work is retained per-operator costs from the same statement CostEnv and task
context, transfer into the shared EXPLAIN representation, and verbose rendering
for plain/ANALYZE plans before translating all join/merge/apply cost assertions.
No zero costs, fresh default cost settings, or root-only replacement is sufficient.
Complete package gates and workload benchmarks remain open; no package commit.

Mapping-update gates: `make lint` passes (log
`/private/tmp/tidb-ordering-mapping-lint.log`) and `git diff --check` is clean.
No product behavior changed in this audit step; Rust join-cost behavior and
workload performance were not newly verified.

Verbose dependency follow-up (2026-09-25): the SQL/parser/renderer/cost-metadata
boundary above is now implemented with the statement CostEnv and a per-render
recursive cache. Exact Go node costs, ANALYZE column placement and DML N/A pass.
Index-join and merge-join source ratio tests are active. The merge test failed
before its child ExpectedCnt/output-statistics correction and now passes with
Go's natural-versus-enforced distinction. EXPLAIN 107, candidate/cost 50, lint
and diff checks pass; exact commands/oracles are in the structural audit.
Apply's mock-statistics test, legacy cost-model validation, full package gates
and workload benchmarks remain open; no whole-package commit or push.

Apply follow-up (2026-09-25): the final ordering-cost source case now runs through
SQL with Go's mock histograms; all five ordering source tests have active Rust
mappings. Shared Apply enumeration now uses outer-input stats for cache admission
and leaves unordered outer requirements unbounded. Both regressions fail before
and pass after; ordered requirements reuse the existing shared helper. Apply 31,
candidates 52, EXPLAIN 108, original Go cache test, lint and diff checks pass.
Exact commands and outstanding runtime-cache/package/workload scope are recorded
in the structural audit. No whole-package completion or commit/push.

Apply runtime follow-up (2026-09-25): full source-shaped LATERAL and upstream
join duplication SQL passes, with cache OFF/90%/90% and correct result counts.
The bridge now respects the published rule blacklist; native Apply publishes
cache counters at Close. Builder 27, EXPLAIN 109 and executor Apply 31 with a
16 MiB thread stack pass, as do lint/diff checks. Default-stack executor sweep
still overflows in multiple_apply_arms_source. Newly registered dormant blacklist
tests have two passes and four failures requiring current-Go verification; they
remain active and block any session-wide completion claim. Details and exact
commands are in the structural audit. Package/workload gates remain incomplete.

### Shared datasource admission follow-up (2026-09-25)

Published blacklist now reaches logical predicate admission with Go Unspecified
store semantics before ordinary/merge candidate derivation. Three previously
failing session tests pass; active blacklist result is 5 pass/1 aggregate failure.
Go oracle confirms a blacklisted aggregate still has a cop scan, and the test
expectation was corrected without hiding the defect. Policy 9, logical 411,
candidates 52, EXPLAIN 109, lint and diff checks pass. Full signature catalog,
physical policy consumers and all prior package/workload gates remain open.
Exact commands, changed files and limitations are recorded in
`rust/docs/planner/access-path-structural-parity-audit.md`, section
"Shared datasource admission policy". This is not package completion.

### Physical admission follow-up (2026-09-25)

Physical dispatch now supplies published policy to shared task attachment and
MPP aggregate enumeration. The active blacklist suite passes 6/6, including
Go's root aggregate/cast projection/cop scan shape. Aggregate 9, task 112,
EXPLAIN 109, MPP enumeration and lint pass. Selection/projection enumeration
and merge residual classification still contain context-free policy calls;
scalar catalog/type/warning and all package/workload gates remain open.
See structural audit section "Published policy during physical attachment"
for changed files and exact commands. No package completion claim.

### Candidate admission and root residual follow-up (2026-09-25)

Published policy reaches selection/projection enumeration and intersection
partial-filter classification. Rejected probe predicates survive as root task
conditions. Shared range estimation now checks actual column access conditions
before BuildColumnRange. Fail-before regressions match Go's root residual
placement, 0.80 estimate and row 4. Physical 59, hints 24, logical 411,
EXPLAIN 109, Go oracle and lint pass. Exact commands and remaining gaps are
in audit section "Candidate policy and intersection root residuals". This does
not establish package or workload completion.

### Ordinary residual follow-up (2026-09-25)

Shared scan-filter splitting now preserves published engine policy and root
residuals for ordinary table, covering-index, lookup and intersection scans.
Root predicate order matches Go. Source-shaped regression fails before and
passes after with Go's 8000/8/8 estimates and results. Blacklist 7, hints 24,
candidates 52, EXPLAIN 109, Go oracle and lint pass. Exact commands and open
mixed/runtime/TiFlash, signature/type/warning, package and workload gates are
in audit section "Ordinary scan residual policy". No completion claim.

### Index/table phase timing correction (2026-09-25)

Index conversion now finishes its index phase before constructing table
selection, preserving Go's CountAfterIndex ratio. Mixed lookup root estimate
failed before at 6.40 and matches Go at 5.12 after; results remain exact.
Blacklists 7, candidates 52, EXPLAIN 109, task 112, Go oracle and lint pass.
See audit section "Index completion precedes table filtering" for exact commands
and superseded previous estimator claim. Remaining package/workload gates are
not satisfied; this is not package completion.

### Root histogram population ownership (2026-09-25)

Root predicate estimation now uses a retained original table profile, then
scales reader output independently. The analyzed regression failed at 2.00
before and matches Go 1.33 after. Snapshot sharing survives task copies and
exchange enforcement, without snapshot allocation for tasks lacking root
filters. Blacklist 8, task 112, enforcement 6, EXPLAIN 109, Go oracle and lint
pass. See audit section "Preserve source population for root selectivity" for
exact commands and open package/workload gates. No completion claim.

### Union residual structural audit (2026-09-25)

A new active Go-shaped union residual regression fails in Rust: its reduced
partial representation loses filter/recheck state and declines the candidate.
Go oracle passes with source OR rechecked on the table probe. Ordinary and
union index derivation now share fill_index_path, but the full metadata
migration remains incomplete. Candidates 52, EXPLAIN 109 and lint pass; the
new hint regression is an explicit failure, superseding any full hint-suite
green implication. Exact commands and next design cut are in audit section
"Union residual alternatives: reproduced structural gap". No completion claim.


### Union branch metadata follow-up (2026-09-25)

The previously failing uncovered-union regression now passes. Logical
alternatives retain shared index filter/key state and table access/residual
metadata through convergence; physical conversion retains branch selections
and source-OR probe/root rechecks. The exact Go estimate exposed a shared
sentinel-bound pseudo estimator dispatch mismatch, now corrected; the changed
UNION ALL golden was independently confirmed against Go master. Hints 25,
candidates 52, logical 411, EXPLAIN 109, focused Go oracles and lint pass.
See audit section "Union lifecycle metadata and residual conversion" for
commands, fail-before evidence and explicit remaining structural/package/
workload gaps. This supersedes the earlier active-failure status, not the
incomplete package receipt. No commit/push or performance claim.


### Shared table derivation and load-state ownership (2026-09-25)

Union integer table alternatives now retain the shared FilledTablePath and
estimate their own typed ranges rather than borrowing the datasource count.
The analyzed regression separately failed at table 4 versus 1 and index 4
versus 2; the latter exposed InitStats discarding TopN-only statistics based
on bucket emptiness. Explicit cache load status fixes that shared boundary.
Both branch counts and query rows now match Go master. Hints 25, candidates
52, logical 411, EXPLAIN 109, initialization 2, access-cost 30, Go oracle and
lint pass. Exact commands and remaining structural/package/workload gaps are
in audit section "Shared integer table derivation and loaded statistics".
Package receipt remains incomplete; no commit or push.


### Candidate-owned index fallback estimates (2026-09-25)

Ordinary executor and union estimates now share the cardinality partial-column
statistics calculation. The new-index union regression failed at 4.00 before
and matches Go at 2.00 after; union no longer borrows an ordinary path count.
The shared pseudo range bridge also preserves string/wide-integer datum
equality rather than replacing unsupported f64 conversions with full ranges.
Fail-before string point estimate 9990 now matches 10. Ranger 69, access-cost
30, candidates 52, hints 25, EXPLAIN 109, source estimator 14, Go oracle and lint
pass. See the corresponding audit section for exact commands and remaining
structural/package/workload scope. Package receipt remains incomplete.


### Unfinished normal-index OR candidate lifecycle (2026-09-25)

Normal union candidates now retain suffix equality/IN filters until top-level
predicates supply missing composite prefixes, and enumerate each top-level OR
separately. Convergence removes only globally covered access predicates and
recomputes cardinality from the selected alternatives. The failed composite
regression now matches Go's union, exact base estimates and rows; the four-case
Go/Rust matrix covers additional residual/OR/LIMIT conditions. Hints 26,
candidates 52, logical 411, EXPLAIN 109, Go oracle and lint pass. Exact commands
and remaining generator/package/workload gates are recorded in the audit's
"Unfinished normal-index OR candidates and top-level predicates" section.
The package receipt is still incomplete; no commit or push.


### Histogram validity across the planner boundary (2026-09-25)

HistColl now preserves column load status independently of retained payloads,
and retains nonzero index payloads even when not fully loaded. Cache/planner
column validity uses one shared rule. The boundary regression failed before
and now matches Go NULL estimates 0.1/100 for positive/zero-NDV evicted column
metadata, with profile scaling preserving validity. Initialization 3,
access-cost 30, correlation 7, logical 411, hints 26, EXPLAIN 109, Go oracle and
lint pass. Exact commands and remaining context/loading/package/workload gaps
are in audit section "Retained statistics versus estimation validity".
No whole-package completion or commit/push claim.


### 2026-09-25: shared V1 enumeration and value-query boundary

Cardinality no longer carries the integer-only EnumRangeValues copy or queries
CMS with an integer-only encoding that swallows failures. It consumes the
statistics implementation, whose QueryValue now calls tablecodec.EncodeValue
through tidb-tablecodec. The equality estimator returns Result and propagates
errors through point, enumerated range and endpoint corrections. New failures
(0 versus 14 point estimate; 60.5 versus 44 range estimate after point-only fix)
are resolved; Go confirms the typed matrix and shared +08:00 timestamp lookup.

545 non-overlapping focused Rust tests, the Go oracle and make lint pass.
See rust/docs/planner/access-path-structural-parity-audit.md, "Shared version-1
statistics dependencies", for exact commands, files, logs and remaining
context requirements. Cardinality still lacks the full V1 index dispatcher and
statement time-zone/error-policy propagation. This is package seed evidence,
not a completed package receipt; no commit/push or workload claim.


### 2026-09-25: preserve ranger ownership of index ranges

IndexRangeDatums now aliases ranger.Range instead of copying only its bounds.
Union and correlation estimates borrow their original ranges; executor adapters
and recursive backoff preserve collators. OnlyPointRange uses the ranger's
point predicate. The distinct Go first-collator prefix rule and per-position
point rule are pinned by failed-before/passed-after Rust and direct Go tests.
804 scoped Rust checks and lint pass; the 27 ignored mock-source mappings,
V1 collection/dispatch, statement context and package/workload gates remain
open. Exact files, commands and evidence are in the structural audit's "One
range representation for cardinality and access paths" entry. No completion,
commit, push or measured performance claim.


Shared V1 dispatch checkpoint (2026-09-25, incomplete package): ordinary,
union, correlation and recursive-index estimates use one version dispatcher.
Raw NDVs, valid column counts, first retained suffix-index policy and V1 cap
ownership now match focused Go evidence. The collection excludes schema-only
indexes but retains invalid payloads, then carries eligible handle mappings as
Go fillIndexPath does. Three appended-handle regressions exposed the missing
second lifecycle step and pass after correction. Recursive backoff uses the
first collator, superseding the earlier per-position claim. Exact commands,
fail-before evidence, changed files and limitations are recorded in the final
shared-dispatch section of rust/docs/planner/access-path-structural-parity-audit.md.
This is seed evidence within the ongoing whole package, not an integration,
completion, commit, push, or workload-performance claim.


Prepared-index lifecycle checkpoint (2026-09-25): union partials now share
ordinary-path prefix pruning, range union and appended-handle damping through
cardinality::estimate_index_path_ranges. Go oracle and fail-before/pass-after
SQL evidence pin partial rows 1.41 instead of 2.00 and unchanged results. The
structural audit records the invalid initial BatchPointGet oracle and corrected
secondary-index fixture, exact commands and remaining package/workload gates.
This remains package-internal evidence rather than completed transcreation.


Statement-options checkpoint (2026-09-25): ordinary access precomputation now
uses the same session estimator options as logical and union paths, including
integer/common handles and appended handles. The strengthened SQL skew fixture
fails before with 4/7.5/10 versus Go 4/6/8, then passes after. Monotonicity-only
evidence was insufficient because a later consistency adjustment hid the missing
snapshot. Exact commands, 180 focused tests and remaining context/package/workload
gaps are in the structural audit. No whole-package completion is claimed.


Async-loading fixture checkpoint (2026-09-25): the new standalone session test
runs the original subset-index SQL setup, drives production catalog queue/cache
loading of evicted statistics, and matches all five original Go plans. The
storage adapter is a test double serving actual ANALYZE payloads; real storage
bootstrap and scheduling are not verified. No production change was necessary.
The original pinned Go test, one new Rust session test, 14 source-mapping tests
(with 27 ignored mappings), make lint and diff checks pass. Exact commands and
limits are in the structural audit's subset-index lifecycle entry. The full Go
package and workload performance gates remain incomplete.


Expression-index prerequisite checkpoint (2026-09-25): the original
TestUninitializedStats SQL exposed ANALYZE's visible-only sample layout, rather
than a missing catalog loading state. Local analysis now samples hidden virtual
values for index keys while omitting virtual-column histograms. Original SQL and
full/selected composite-index TopN regression pass against Go evidence; 110
EXPLAIN tests and lint pass. ANALYZE has 20 passing tests and one identical
baseline partition-global-statistics failure. Cluster virtual evaluation,
special-index NDV collection and virtual histogram publication remain open;
the structural audit records exact code boundaries, commands and limitations.


Cluster virtual-sample checkpoint (2026-09-25): cluster schema projection now
retains expression-index inputs; compiled virtual evaluation, independent index
NDV/null counts, shared histogram suppression and shared statement context follow
Go's ownership. Encoded NULL/populated collectors, nested selected columns,
timezones and paged stored rows pass. Validation: 42 exec, eight shared builder,
42 session expression-index and three isolated storage tests; server check, Go
oracles and lint pass. Session ANALYZE retains its baseline partition failure;
aggregate storage tests remain blocked by unrelated stale DDL warning fields.
Exact commands, temporary test-target procedure and limits are in the structural
audit. Live TiKV loading, memory/error variants and whole-package/workload gates
remain open.

Latest reference refresh for the cluster virtual-sample checkpoint: origin/master
advanced to `8936d7bdcb13a4fc767de42489aace2711c2c6fd`. The entire
`pkg/planner/cardinality` tree and `pkg/executor/analyze_col_sampling.go` are
unchanged from pin `633a9e37f1c796ac81c203dc107025e7e65385f0`; Go oracle
execution above remains on that earlier pinned snapshot.


ANALYZE publication checkpoint (2026-09-25): the prior partition baseline failure
is now resolved. Local global/partial/independent-index results share metadata
publication: histogram formats refresh table StatsVer, real cache state remains
distinct from planner pseudo state, and FM sketches follow their index payloads.
Independent tasks preserve table row/modify counts. Three strengthened cases fail
against the old production code and pass after; 22 ANALYZE, 110 EXPLAIN tests, Go
SQL oracle, lint and diff checks pass. Exact commands and limitations are in the
structural audit. DDL aggregate compilation, live-cluster variants, full-package
inventory and workload gates remain open.


Aggregate gate update (2026-09-25): the stale DDL warning-field blocker above is
resolved. Ordered warning lists and current prefix-index metadata are asserted;
Go-compatible identity rename and resolved-collation admission fixes pass 102
DDL cases, while the real aggregate ANALYZE target passes three storage cases.
Server check, Go reference cases, lint and diff checks pass. Exact commands and
fail-before/pass-after evidence are in the structural audit. This remains seed
and integration evidence, not completion of pkg/ddl or pkg/planner/cardinality;
live-cluster, complete inventory and workload gates remain open.


Unanalyzed-IN initialization checkpoint (2026-09-25): the original fixture now
also runs production DDL/delta statistics writers, update and lite/full startup
loaders, SharedStats publication and StatisticsView conversion against in-memory
storage bytes. Exact plans and unanalyzed existence metadata remain stable.
Two session and 14 source tests, original Go test, lint and diff checks pass.
The structural audit records exact commands and limits: background update
orchestration, live TiKV, full inventory and workload gates remain unverified.
