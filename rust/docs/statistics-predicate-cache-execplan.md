# Match Cached Source Statistics To Current Predicates

This living ExecPlan follows PLANS.md at the repository root.

## Purpose

InitStats in rust/crates/tidb-executor/src/driver/planner_bridge.rs computes
histogram-based estimates from the original SQL WHERE. Logical optimization
can add predicates, notably a NULL filter after eliminating a grouped SUM
over a unique key. Reusing the earlier estimate then prices the wrong input.
Keep that estimate only when its bound predicate matches current pushed
conditions; otherwise the planner derives statistics from current conditions.

## Progress

- [x] Reproduce district Selection 10 instead of Go master 8.
- [x] Reject unconditional stats clearing after six additional regressions.
- [x] Compare bound predicates with associative AND/OR normalization.
- [x] Verify independent SQL regression fails under restored old behavior.
- [x] Collect final full executor and lint results and review.
- [ ] Publish independently after incorporating current remote changes.
- [ ] Continue remaining TPCC failures and overall integration gates.

## Surprises & Discoveries

The global-count OR-of-BETWEEN fixture changes boolean association during
optimization. Structural equality alone unnecessarily discarded its loaded
range estimate (5.75 became 10000). Flattening AND/OR with one-to-one matching
preserves this estimate without ignoring an added condition.

The oracle must initialize identical data. A reused Go database returned
Selection 1/scan 1.25. A fresh database with exact DDL and rows returned 8/10
and d_id=1, matching the new regression. The exact receipt is
/tmp/tpcc-master-oracle.0t3pI6/derived-null-exact.out, using Go master
fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85.

## Decision Log

Reuse existing bound-expression equality for leaves, retaining types and
column identities. Permit only boolean association and ordering changes.
Preserve existing index and handle range estimates. This is a cache-validity
fix, not a replacement for the entire statistics estimator. Existing planner
fallback limitations remain observable through broad tests and differential
gates; do not claim complete package transcreation.

## Milestones

First reproduce the stale cache with the original TPCC query. Then isolate
the aggregate-elimination NULL filter in an independent SQL fixture. Compare
against a fresh Go master database before accepting the expected estimate.
Finally run all executor tests and make lint, commit this root cause separately,
and continue the remaining TPCC failures without weakening assertions.

## Validation

Run in /tmp/tidb-hparser-current:

    RUST_MIN_STACK=33554432 RUSTUP_TOOLCHAIN=1.97 cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib
    make lint
    git diff --check

The independent test is derived_aggregate_null_filter_refreshes_source_statistics.
Restoring the old unconditional cache write makes it fail with 10 instead of 8
(/tmp/predicate-cache-derived-red.log). Full results are recorded in
/tmp/predicate-cache-final-full.log and lint in /tmp/predicate-cache-lint.log.

## Outcomes & Retrospective

The original TPCC query advances past NULL selectivity and join-family checks
to an existing generated-column-number assertion. Condition eleven and all
other outstanding gates remain open. Completion of this cache fix must not be
reported as completion of the full Rust integration objective.

Final verification on 9197fccf9b plus this patch: 1293 executor tests pass and
the two original TPCC tests remain failing. The new isolated NULL-filter test
passes. make lint and git diff --check pass. No temporary probes remain.
