# Implement Statement-Local Adaptive LIMIT Scan

This document is a living ExecPlan. It follows `PLANS.md` and is updated as
implementation and E2E evidence develop.

## Purpose

Prevent the ordered IndexLookUpJoin LIMIT workload in issue #66658 from
admitting an unbounded amount of speculative work. The implementation learns
within the current statement from actual producer/consumer progress, so the
first execution benefits and parameter distributions cannot poison later runs.

## Progress

- [x] (2026-07-11) Confirmed clean baseline `8be4bd0`.
- [x] (2026-07-11) Located LIMIT, IndexLookUpJoin, and IndexLookUpExecutor
  admission paths.
- [x] (2026-07-11) Added controller tests and recorded the pre-implementation compile failure.
- [x] (2026-07-11) Implemented the controller and default-OFF feature gate.
- [x] (2026-07-11) Wired LIMIT stop and ordered IndexJoin outer admission.
- [x] (2026-07-11) Wired single-range double-read lookup and range admission.
- [x] (2026-07-11) Completed scoped tests, race test, microbenchmark, Bazel regeneration, and build.
- [x] (2026-07-11) Ran and documented ten fresh E2E rounds against the latest
  binary and a 400,000-row issue-shaped dataset. All OFF/ON executions returned
  the expected rows without deadlock; ON reduced scan work and latency in every
  tested scenario.
- [x] (2026-07-11) Self-reviewed correctness, compatibility, and performance
  risk after the fresh E2E cycle. Kept the bounded headroom unchanged because
  every measured latency improved and no regression justified further tuning.

## Design

`LimitExec` creates a controller only when the feature is enabled and the child
tree contains an eligible ordered IndexLookUpJoin. Demand is exactly
`offset + count`. The outer worker reserves rows before reading its child and
commits actual rows afterward. The join main thread records an outer row only
after all of its inner matches are consumed. Join output updates the observed
yield.

The controller maintains separate outer and double-read admission accounting.
The initial window is the executor's requested chunk size. With no observed
output it grows only after admitted input is consumed, avoiding deadlock on low
selectivity. Once output exists, the remaining input estimate derives from the
observed output-per-consumed-outer-row ratio and includes bounded headroom.
Outstanding speculative rows cause the window to shrink. LIMIT completion stops
new reservations and wakes blocked producers.

The existing session values remain ceilings. Already-issued coprocessor
requests are not resized; only future range/task admission changes.

After the first full E2E matrix, the output-aware window was refined in two
validated cycles. Once output exists, the window may shrink below the cold-start
window. Speculative headroom is 25% before 50% completion, 12.5% from 50% to
75%, and zero in the final 25%; estimated remaining input is still admitted as
one concurrent budget. Both refinements completed fresh ten-scenario E2E
matrices. The small-batch scenario reduced outer fetching from 1,058 to 1,024
rows without a latency regression.

A bounded additive-headroom experiment was reverted because its fresh E2E
setup hit a transient schema-lease error and the retry could not obtain local
connection approval. No unverified controller behavior was retained.

The first version crosses only row-preserving Projection executors. It does not
activate for filtering unary operators, partition/grouped-range merge-sort
double reads, unordered IndexLookup, or index-lookup concurrency one. These
restrictions prevent incorrect attribution and partially controlled pipelines.

Join progress is aggregated at task or Next boundaries. Reusable buffered
notification channels wake the single outer and lookup producers without
per-row channel allocation. Controller state resets before every executor Open,
and runtime diagnostics freeze an immutable snapshot during Close.

Outer admission now happens before allocating the lookup-join task, chunk list,
MVMap, and memory tracker, so a blocked or stopped reservation creates no task
buffering objects. The latest binary builds successfully. Ten fresh E2E rounds
cover the default issue case, low selectivity, LIMIT 1, OFFSET, large LIMIT,
small batch, high concurrency, scan concurrency one, small LIMIT, and large
OFFSET. The 25% estimate headroom remains unchanged because all rounds improved
latency and the remaining 23%-29% over-fetch on large demand is bounded;
tightening it without stronger evidence risks reducing pipeline throughput.

## Validation

Add controller unit tests first and record their pre-implementation failure.
Run failpoint-aware targeted executor tests, `make bazel_prepare` when required,
`make`, and Ready-profile lint. Deploy with the user-supplied TiUP command. For
each E2E round capture SQL, row count, full EXPLAIN ANALYZE, key actRows, timing,
and OFF/ON comparison.

## Risks

- Blocking a producer must always be interruptible by statement cancellation.
- Accounting must release reservations on EOF, errors, and partially filled
  batches.
- One outer row may emit multiple join rows; consumption is recorded only after
  that row is fully joined.
- Low selectivity must grow gradually instead of stalling at a tiny window.
- The feature remains OFF by default while first-version eligibility is narrow.
