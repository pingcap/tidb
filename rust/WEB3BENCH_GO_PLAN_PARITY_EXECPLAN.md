# Web3Bench Go/Rust parity and performance validation

This ExecPlan is a living record for the Web3Bench work on
`hparser-integration`.  The Go TiDB implementation is the semantic source of
truth: Rust may use a different internal representation, but it must return
the same rows and expose the same physical plan shape for each accepted query.
The acceptance workload is the Web3Bench query set with the data volume
multiplied by ten and one client/concurrency.

## Purpose / Big Picture

After this work, a local TiUP playground can run the Web3Bench queries against
the Go nightly and Rust servers, compare plans with and without statistics,
compare result rows, and measure single-client latency.  The Rust path must not
silently change SQL semantics when it takes an execution shortcut; unsupported
shapes remain on the existing general executor.

## Progress

- [x] (2026-08-25) Rebased `hparser-integration` onto the current
  `origin/hparser-integration`; the remote was already at `a9b0cf0a3b`.
- [x] (2026-08-24) Corrected typed `IS NULL` coprocessor signatures using the
  Go builtin mapping and TiDB protobuf enum values.
- [x] (2026-08-24) Restored common-handle remote scan admission, R25 partial
  aggregation predicates, grouped hash partial aggregation, and the R35 hash
  join residual-candidate correctness.
- [x] (2026-08-24) Added a bounded direct-string hash aggregation path and a
  fail-closed compact count-only join path for the count-only Web3Bench R35
  shape.
- [x] (2026-08-24) Added a streaming executor for direct same-typed `UNION ALL`
  derived sources and exact row-count propagation through a projection and the
  global count aggregate.
- [x] (2026-08-24) Ran the complete Web3Bench ten-times-data result, plan, and
  single-client performance matrix after the rebase, both after `ANALYZE TABLE`
  and after `DROP STATS` on all five benchmark tables. Results were exact for
  every deterministic query; the intentionally unordered R32 may choose a
  different subset/order among tied rows, while `R32det` (with `hash` as a tie
  breaker) is exact.
- [x] (2026-08-24) Removed the temporary `TIDB_DEBUG_COP_SCAN` diagnostics,
  rebuilt the release server, and completed the targeted Rust regression tests
  and diff review.
- [x] (2026-08-24) Deep review found and fixed a real DECIMAL SUM regression:
  `AggState::new` can select the direct MyDecimal state for StreamAgg, so the
  shared StreamAgg/GroupedStreamAgg row updater now consumes that state safely;
  `stream_agg_direct_decimal_sum_uses_the_fast_state_safely` covers the case.
- [x] (2026-08-24) Rebuilt the post-review release binary and reran the
  analyzed-statistics Web3Bench result/EXPLAIN/performance matrix. Results
  remained exact for all deterministic queries and the operator skeletons
  remained aligned; R35's Projection receipt difference is still present.
- [x] (2026-08-25) Rebased the Web3 work onto remote `a9b0cf0a3b`, passed the
  post-rebase Rust checks, and kept the earlier Web3 optimization commits in
  the remote history.
- [x] (2026-08-25) Added a scalar direct-string `SUM`/`FINAL_COUNT` aggregate
  path for the pushed-down R34 root shape. It retains the full DECIMAL
  `AggState` fallback for mixed scales/overflow, preserves first-seen group
  order, and is disabled when a finite quota can trigger spill. The regression
  test `grouped_binary_string_final_count_adds_partial_counts` covers partial
  count accumulation and exact decimal output.
- [x] (2026-08-25) After the final rebase onto `c90c1b6b71`, the release build
  matched Go for every deterministic Web3 query (R1, R21-R25, R31-R35 and
  `R32det`). R32 without a hash tie-breaker may choose a different subset or
  order of tied rows. The alternating one-client medians were R34 `248.702 ms` vs Go
  `155.265 ms` (`1.60x`) and R35 `160.203 ms` vs Go `107.171 ms` (`1.49x`).
- [x] (2026-08-25) Rebuilt the release server from `09725ac0bf`, restarted the
  task-owned Rust endpoint to clear its statistics cache, and reran the
  analyzed/no-stats result, plan, and alternating one-client matrix. Every
  deterministic query (R1, R21-R25, R31, R33-R35, and R32det) is byte-exact;
  R32's tied-row choice is intentionally unspecified. The no-stats Rust/Go
  median ratios stayed within `1.42x` (R1) through `1.95x` (R34), with no
  stable regression versus the prior release receipts. Plan differences remain
  the internal `Column#N`/TopN expression spelling and the R35 derived-table
  Projection; all operator skeletons otherwise match.

## Validation commands

The shared playground currently uses PD `127.0.0.1:14379`, TiKV
`127.0.0.1:32160`, Go TiDB `127.0.0.1:16000`, and Rust TiDB
`127.0.0.1:16003`.  The exact query/result/performance scripts live under
`/tmp` in the current test session.  Targeted Rust checks are run from
`rust/`:

    cargo +nightly-2026-08-22 check -p tidb-exec -p tidb-executor -q
    cargo +nightly-2026-08-22 test -p tidb-executor hash_agg --lib -q
    cargo +nightly-2026-08-22 test -p tidb-executor join --lib -q

Release binaries are rebuilt with:

    cargo +nightly-2026-08-22 build --release -p tidb-server --bin tidb-server

The acceptance matrix must record, for every Web3Bench query, Go/Rust plan
text, result hash/row count, and alternating single-client latency.  The same
matrix is run after `ANALYZE TABLE` and after `DROP STATS` for every table.

Receipts from this final release run are stored under `/tmp`:

    /tmp/web3_rebase_results_go_finalstats.json
    /tmp/web3_rebase_results_rust_finalstats2.json
    /tmp/web3_rebase_plans_go_finalstats.json
    /tmp/web3_rebase_plans_rust_finalstats2.json
    /tmp/web3_rebase_perf_finalstats.json
    /tmp/web3_rebase_results_go_nostats.json
    /tmp/web3_rebase_results_rust_nostats.json
    /tmp/web3_rebase_plans_go_nostats.json
    /tmp/web3_rebase_plans_rust_nostats.json
    /tmp/web3_rebase_perf_nostats.json

Post-review analyzed-statistics receipts (after the StreamAgg fix) are:

    /tmp/web3_rebase_results_go_finalstats_after_review.json
    /tmp/web3_rebase_results_rust_finalstats_after_review.json
    /tmp/web3_rebase_plans_go_finalstats_after_review.json
    /tmp/web3_rebase_plans_rust_finalstats_after_review.json
    /tmp/web3_rebase_perf_finalstats_after_review.json

Current post-rebase scalar R34 receipts are:

    /tmp/web3_rebase_c90_go.json
    /tmp/web3_rebase_c90_rust.json
    /tmp/web3_rebase_c90_plans_go.json
    /tmp/web3_rebase_c90_plans_rust.json
    /tmp/web3_rebase_c90_perf.json

Current fresh-release receipts (commit `09725ac0bf`, after cache reset) are:

    /tmp/web3_current_fresh_go.json
    /tmp/web3_current_fresh_rust.json
    /tmp/web3_current_fresh_plans_go.json
    /tmp/web3_current_fresh_plans_rust.json
    /tmp/web3_current_fresh_perf.json
    /tmp/web3_current_nostats_go.json
    /tmp/web3_current_nostats_rust.json
    /tmp/web3_current_nostats_plans_go.json
    /tmp/web3_current_nostats_plans_rust.json
    /tmp/web3_current_nostats_perf.json

## Surprises & Discoveries

- A generic `IntIsNull` protobuf signature is rejected by TiKV for DECIMAL;
  Go chooses `DecimalIsNull` (and a separate signature for each evaluation
  family), so the Rust lowering must retain that type distinction.
- The Rust derived-UNION execution path previously materialized every term
  before the outer aggregate.  This made a count-only join shortcut unreachable
  even though the EXPLAIN trace contained a Union node.
- The R35 join result was off by one because a full output chunk discarded the
  remaining build candidates for the current probe row.  Candidate position is
  now persisted across `next()` calls and covered by a regression test.

## Decision Log

- Keep the new UNION executor restricted to direct, same-typed `UNION ALL` and
  ordinary execution.  DISTINCT, mixed-type branches, nested/ordered set
  operations, and EXPLAIN ANALYZE retain the materialized path until their Go
  semantics have an equivalent executor.
- Keep compact join admission fail-closed: only inner joins with one binary
  string key, direct DECIMAL comparison residual, bounded declared key widths,
  and a global count parent can use it.  Any other shape returns to the normal
  hash join.
- Do not use debug logging as performance evidence.  Cop-scan diagnostics are
  temporary and must be removed before the final release benchmark.

## Outcomes & Retrospective

At the current checkpoint the R35 result is correct (`161859`). The latest
analyzed-statistics alternating run measured R34 at `1.60x` and R35 at `1.49x`
of Go after the latest remote rebase. This checkpoint records the measured
improvement and the regression test, but it is not a claim that the no-stats
matrix or every EXPLAIN receipt is complete.
