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
- [x] (2026-08-25) Added the bounded direct-string worker path for the pushed
  down R34 `SUM`/`FINAL_COUNT` shape. Rows are partitioned by a cheap
  collation-aware key fingerprint, so equal groups have one owner; workers use
  exact fixed-scale DECIMAL state with the existing arbitrary-precision fallback,
  preserve first-seen order, and batch memory accounting once per window. The
  serial path remains the fallback for one worker, low quotas, and spill-sensitive
  statements. `grouped_binary_string_parallel_workers_merge_exact_decimal`
  covers multi-chunk routing, partial-count merging, exact DECIMAL output, and
  the worker-window receipt.
- [x] (2026-08-25) Rebuilt the release binary and reran the ten-times-data
  Web3Bench matrix with one client (default executor settings). R1, R21-R35,
  and R32det returned exact rows; R32 differs only in the unspecified ordering
  of tied rows. The normalized operator skeleton now matches Go for every
  query, including R35 after eliminating the identity projection for qualified
  wildcard output. The alternating eight-sample medians were R34
  `230.737 ms` vs Go `154.765 ms` (`1.49x`) and R35 `160.028 ms` vs Go
  `113.508 ms` (`1.41x`); the full matrix is in
  `/tmp/web3_final_current_default5_perf.json`.
- [x] (2026-08-25) After rebasing onto the newer remote hash-aggregation and
  result-writer commits, the final release binary exposed an empty-result
  lifecycle bug: an initial empty pull returned before `finish()` and the
  terminal EOF packet, so Web3Bench R23 could hang. The writer now completes
  the source and emits the terminal packet on that path; the existing direct
  writer regression plus `connection_empty_result_finishes_closes_and_emits_terminal_eof`
  cover both layers.
- [x] (2026-08-25) Re-ran the final post-fix ten-times-data matrix against the
  task-owned Go nightly and Rust endpoint. R1, R21-R35, and `R32det` are exact
  (R32 alone is tie-order dependent); every plan operator skeleton is equal.
  In an isolated repeat (no baseline server left running), the post-fix
  default-settings medians were R34 `314.691 ms` vs Go `151.839 ms`
  (`2.07x`) and R35 `163.018 ms` vs Go `110.458 ms` (`1.48x`).
  Against the clean Rust `884e16945ed` baseline, R34 improved from
  `4013.787 ms` to `353.144 ms` and R35 remained within measurement noise
  (`164.660 ms` to `158.905 ms`); the complete receipts are recorded below.
- [x] (2026-08-25) Rebuilt the clean worktree at the final pushed branch after
  the remote `eb9937320cb`/`7f3acc686e8` updates and reran the acceptance
  matrix. Results and all normalized plan skeletons stayed aligned; the
  isolated repeat medians were R34 `321.100 ms` vs Go `154.159 ms` (`2.08x`)
  and R35 `163.627 ms` vs Go `108.207 ms` (`1.51x`).
- [x] (2026-08-25) Optimized the direct R34 worker without weakening exact
  grouping semantics: partitioning now carries the already-computed
  collation-aware fingerprint, and existing groups compare the source column
  bytes directly instead of rebuilding a temporary key. DECIMAL(38,0) worker
  sums use the compact i128 representation, and FIRST_ROW carriers avoid a
  temporary row wrapper. The R35 compact count path likewise reads DECIMAL
  coefficients directly and accesses selected columns without row wrappers.
  The final one-client no-stats matrix returned exact deterministic rows and
  identical normalized operator skeletons. In the repeat receipt, R34 was
  `353.816 ms` vs Go `175.340 ms` (`2.02x`) and R35 `158.309 ms` vs Go
  `109.937 ms` (`1.44x`).
- [x] (2026-08-25) Replaced the R34 worker's stop-and-merge windows with
  bounded streaming queues. The caller continues pulling TiKV chunks while
  persistent fingerprint-partitioned workers aggregate them; each worker's
  map is merged only once at EOF, preserving first-seen order and exact key
  collision checks. The full one-client matrix remained result- and plan-
  aligned with Go (R32 only differs in unspecified tie order). The repeat
  receipt improved R34 to `254.971 ms` vs Go `148.261 ms` (`1.72x`), while
  R35 stayed at `162.565 ms` vs Go `110.591 ms` (`1.47x`).
- [x] (2026-08-25) Rebuilt the clean `d62b8d393b5` release binary and ran two
  alternating eight-sample, one-client matrices over all eleven Web3Bench
  queries plus deterministic `R32det`. Across the combined sixteen samples,
  Rust/Go medians were: R1 `0.685/0.481 ms` (`1.43x`), R21
  `1.978/2.040 ms` (`0.97x`), R22 `0.938/0.812 ms` (`1.15x`), R23
  `1.195/1.333 ms` (`0.90x`), R24 `1.030/0.966 ms` (`1.07x`), R25
  `0.729/0.736 ms` (`0.99x`), R31 `1.753/1.447 ms` (`1.21x`), R32
  `5.288/4.055 ms` (`1.30x`), R33 `143.464/142.517 ms` (`1.01x`), R34
  `221.278/149.611 ms` (`1.48x`), R35 `159.899/105.038 ms` (`1.52x`),
  and R32det `6.024/3.946 ms` (`1.53x`). Every deterministic result is
  exact and every normalized plan operator skeleton is equal; R32's result
  differs only at the unspecified timestamp-tie LIMIT boundary. Receipts:
  `/tmp/web3_d62_clean_all_perf.json`,
  `/tmp/web3_d62_clean_all_perf_repeat.json`,
  `/tmp/web3_d62_clean_go_results.json`,
  `/tmp/web3_d62_clean_rust_results.json`,
  `/tmp/web3_d62_clean_go_plans.json`, and
  `/tmp/web3_d62_clean_rust_plans.json`.
- [x] (2026-08-25) Rebuilt the post-fetch HEAD `385eaf599a8` (including the
  independent grouped count/sum pipeline change) and reran two complete
  matrices. The combined sixteen-sample Rust/Go medians were R1
  `0.489/0.389 ms` (`1.26x`), R21 `1.503/1.536 ms` (`0.98x`), R22
  `1.098/1.032 ms` (`1.06x`), R23 `1.165/1.260 ms` (`0.93x`), R24
  `1.228/1.059 ms` (`1.16x`), R25 `1.047/1.072 ms` (`0.98x`), R31
  `1.793/1.744 ms` (`1.03x`), R32 `5.076/3.546 ms` (`1.43x`), R33
  `142.334/130.063 ms` (`1.09x`), R34 `274.906/159.250 ms` (`1.73x`),
  R35 `153.602/113.474 ms` (`1.35x`), and R32det `6.354/4.237 ms`
  (`1.50x`). All deterministic rows and all plan skeletons remain equal;
  only R32's unspecified tie-order/ LIMIT-boundary choice differs. Receipts:
  `/tmp/web3_head_all_perf.json`, `/tmp/web3_head_all_perf_repeat.json`,
  `/tmp/web3_head_go_results.json`, `/tmp/web3_head_rust_results.json`,
  `/tmp/web3_head_go_plans.json`, and `/tmp/web3_head_rust_plans.json`.

## Validation commands

The shared playground currently uses PD `127.0.0.1:14379`, TiKV
`127.0.0.1:32160`, Go TiDB `127.0.0.1:16000`, and Rust TiDB
`127.0.0.1:16009`.  The exact query/result/performance scripts live under
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

Final post-fix receipts after the remote rebase and empty-result writer fix:

    /tmp/web3_final_rebased_go_results.json
    /tmp/web3_final_rebased_rust_results.json
    /tmp/web3_final_rebased_go_plans.json
    /tmp/web3_final_rebased_rust_plans.json
    /tmp/web3_final_rebased_default5_perf.json
    /tmp/web3_final_rebased_default5_perf_repeat.json
    /tmp/web3_clean_baseline_perf.json
    /tmp/web3_final_clean_default5_perf_isolated.json
    /tmp/web3_final_clean_default5_perf_isolated_repeat.json
    /tmp/web3_final_pushed_go_results.json
    /tmp/web3_final_pushed_rust_results.json
    /tmp/web3_final_pushed_go_plans.json
    /tmp/web3_final_pushed_rust_plans.json
    /tmp/web3_final_pushed_perf.json
    /tmp/web3_final_pushed_perf_repeat.json

Latest key-match optimization receipts are:

    /tmp/web3_keymatch_go_results.json
    /tmp/web3_keymatch_rust_results.json
    /tmp/web3_keymatch_go_plans.json
    /tmp/web3_keymatch_rust_plans.json
    /tmp/web3_optimized_keymatch_perf.json
    /tmp/web3_optimized_keymatch_perf_repeat.json
    /tmp/web3_final_release_go_results.json
    /tmp/web3_final_release_rust_results.json
    /tmp/web3_final_release_go_plans.json
    /tmp/web3_final_release_rust_plans.json
    /tmp/web3_optimized_keymatch_perf_final_release.json
    /tmp/web3_streaming_go_results.json
    /tmp/web3_streaming_rust_results.json
    /tmp/web3_streaming_go_plans.json
    /tmp/web3_streaming_rust_plans.json
    /tmp/web3_streaming_worker_perf.json
    /tmp/web3_streaming_worker_perf_repeat.json

The last performance receipt is an alternating one-client run against the
clean Rust baseline (`884e16945ed`) and the final Rust endpoint. Small-query
outliers are visible in R23 under local process contention; the dedicated R23
probe returned in 6 ms after the fix, and the exact-result/plan receipts are
not affected by those latency samples.

Final default-settings receipts after the qualified-wildcard projection fix:

    /tmp/web3_final_current_default5_go_results.json
    /tmp/web3_final_fixed_rust_results.json
    /tmp/web3_final_fixed_go_plans.json
    /tmp/web3_final_fixed_rust_plans.json
    /tmp/web3_final_current_default5_perf.json

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

At the current checkpoint the R35 result is correct (`161859`). The final
default-settings one-client matrix has exact deterministic rows and matching
operator skeletons for all Web3Bench queries; R32 remains tie-order
non-deterministic by SQL semantics, with R32det exact. The direct-string worker
path improves the pushed-down R34 receipt over the clean Rust baseline while
keeping the exact DECIMAL fallback and a serial path for one-worker execution.
Rust's R34/R35 medians remain above Go's on this local TiKV/CPU run, so the
receipts establish correctness and no regression against the clean Rust
baseline, not identical latency to Go. The result-set writer now also has an
explicit empty-result terminal path, eliminating the R23 liveness failure.
