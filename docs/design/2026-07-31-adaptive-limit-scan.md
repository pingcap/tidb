# Proposal: Adaptive LIMIT Scan

- Author(s): [solotzg](https://github.com/solotzg)
- Tracking Issue: <https://github.com/pingcap/tidb/issues/66658>

## Table of Contents

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Goals and Non-Goals](#goals-and-non-goals)
- [Detailed Design](#detailed-design)
    - [Supported Plan Shape](#supported-plan-shape)
    - [Statement-Local Controller](#statement-local-controller)
    - [Demand and Stage Budgets](#demand-and-stage-budgets)
    - [Outer Row Admission](#outer-row-admission)
    - [Lookup Handle Admission](#lookup-handle-admission)
    - [Runtime Feedback](#runtime-feedback)
    - [Window Adjustment](#window-adjustment)
    - [Batch Size and DistSQL Behavior](#batch-size-and-distsql-behavior)
    - [Stop and Cleanup](#stop-and-cleanup)
    - [Eligibility](#eligibility)
    - [Correctness and Concurrency Invariants](#correctness-and-concurrency-invariants)
    - [Configuration](#configuration)
    - [Observability](#observability)
    - [Current Limitations](#current-limitations)
    - [Compatibility](#compatibility)
- [Test Design](#test-design)
    - [Functional Tests](#functional-tests)
    - [Scenario Tests](#scenario-tests)
    - [Compatibility Tests](#compatibility-tests)
    - [Benchmark Tests](#benchmark-tests)
- [Impacts and Risks](#impacts-and-risks)
- [Investigation and Alternatives](#investigation-and-alternatives)
- [Future Work](#future-work)
- [Unresolved Questions](#unresolved-questions)

## Introduction

`LIMIT` queries need only the first `offset + count` rows, but asynchronous executors below the `LIMIT` can scan, compute, or dispatch much more work before the upper executor stops consuming rows. The multiplication of batching, concurrency, and prefetching can increase TiDB and TiKV CPU, memory, network, I/O, and RU consumption. Ordered execution makes the problem more severe because results completed behind a slow leading task cannot be returned early.

Adaptive LIMIT Scan adds a lightweight controller to one statement execution. The controller translates the remaining LIMIT demand into admission windows for supported execution stages. Workers must reserve capacity before admitting more work, report the actual input and output after that work is consumed, and stop admitting work when the LIMIT is satisfied.

The initial implementation targets the ordered `IndexLookUpJoin` plan involved in [issue #66658](https://github.com/pingcap/tidb/issues/66658) and the related direct ordered `IndexLookUp` plan. The feature can be enabled or disabled through `tidb_enable_adaptive_limit_scan`.

## Motivation or Background

In issue #66658, the query needs 1,000 rows, while different stages of the execution plan process substantially more data:

```text
Limit / IndexJoin:          1,000 rows
IndexJoin outer input:     81,920 rows
IndexRangeScan:           392,857 rows
Selection:                273,763 rows
TableRowIDScan:           273,804 rows
```

These counters describe different stages and are not expected to be equal. The problem is that much of the work is never consumed by the LIMIT.

Each executor independently prefetches work to preserve throughput:

```text
IndexLookUpJoin prefetches outer rows
    |
    v
IndexLookUp extracts handles and dispatches table lookup tasks
    |
    v
DistSQL scans ranges concurrently
```

Without information about the remaining LIMIT demand, each layer may admit a reasonable amount of work locally while the whole pipeline admits far more work than the query can consume.

`keepOrder=true` adds head-of-line blocking. If an early range or lookup task slows down, later tasks may finish but cannot pass the earlier task. Continuing to prefetch at this point consumes more resources, which can further delay the leading task:

```text
transient slowdown
    -> leading ordered task is delayed
    -> more later work accumulates
    -> CPU, I/O, and network pressure increase
    -> leading task is delayed further
```

Reducing a global batch size or concurrency is not sufficient. Selectivity and data distribution vary by SQL statement, parameter, and scan position. A globally small value can reduce normal throughput while still being unable to identify how much work the current LIMIT needs.

The purpose of Adaptive LIMIT Scan is to improve latency and reduce resource consumption by bounding speculative work. Reducing `actRows` is a means to that end, not an independent objective. An implementation that reads fewer rows but regresses latency or RU because it creates too many small tasks is not an improvement.

## Goals and Non-Goals

The goals are:

- propagate the remaining LIMIT demand into supported asynchronous readers;
- bound the amount of speculative work admitted in TiDB;
- adjust that bound from input/output ratios observed in the current execution;
- continue making progress through low-selectivity or changing data regions;
- preserve SQL results, result order, transaction semantics, and user concurrency ceilings;
- keep the feature statement-local and inexpensive on the execution hot path.

The initial implementation does not:

- use historical SQL profiles or digests to select or change a plan;
- change access paths, join keys, predicates, or physical ordering;
- synchronously cancel RPCs or tasks that have already been dispatched;
- impose a hard limit on raw keys, bytes, or time scanned inside one TiKV coprocessor request;
- propagate budgets through blocking implementations such as Sort, HashAgg, or Window implementations that require a complete partition;
- support unordered IndexJoin, IndexHashJoin, Apply, IndexMerge, or MPP.

## Detailed Design

### Supported Plan Shape

The first version recognizes the following executor shapes:

```text
Limit
└─Projection*
  └─IndexLookUpJoin
    └─Projection* (outer)
      └─IndexLookUp (keep order)

Limit
└─Projection*
  └─IndexLookUp (keep order)
```

Only Projection can appear between Limit and the recognized root executor because Projection preserves row cardinality. The IndexLookUp may still contain a table-side Selection inside its coprocessor plan; lookup feedback measures the resulting handle-to-row ratio. An executor-side Selection between Limit and IndexLookUp would need a separate feedback stage. A blocking operator stops budget propagation.

The controller is attached while `LimitExec` is built. One controller is owned by one executor tree and one execution lifecycle.

### Statement-Local Controller

The controller implements a feedback loop:

```text
LIMIT remaining demand
    |
    v
compute per-stage admission windows
    |
    v
workers reserve capacity before admitting work
    |
    v
consume completed work and report actual input/output
    |
    +----------------------> recompute windows
```

No state is shared across SQL executions. This avoids mixing different parameters, plans, schemas, or data distributions and allows the current execution to react to a selectivity change at its current scan position.

All controller state is protected by one mutex. A worker waiting for capacity also listens for context cancellation and the controller stop signal.

### Demand and Stage Budgets

The total demand includes the offset:

```text
demandRows = offset + count
```

Rows before the offset are not returned to the client, but the child executor must still produce them, so they consume demand.

The IndexLookUpJoin shape contains two different input/output transformations:

```text
IndexLookUpJoin outer stage:
    outer rows -> join output rows

IndexLookUp table stage:
    index handles -> table rows
```

These units must not share one counter. For example, 1,000 index handles might produce only 100 table rows after a table-side predicate. Controlling only Join outer rows would not prevent IndexLookUp from dispatching too many low-yield table lookup tasks.

For IndexLookUpJoin, the controller therefore maintains:

- an outer row window, which bounds rows prefetched by IndexLookUpJoin;
- a logical lookup handle window, which estimates how many handles are justified by current LIMIT demand and observed yield;
- a lookup execution batch size, which keeps each table lookup task large enough to avoid inefficient tiny RPCs.

The direct IndexLookUp shape has no Join outer stage. Its controller maintains only the lookup handle window and execution batch size. Fully consumed table lookup tasks report final rows directly toward LIMIT demand.

The physical lookup admission window is derived from the logical window and the execution batch size. A window is an admission bound, not a prediction that the exact number of rows will be needed.

### Outer Row Admission

Before the IndexLookUpJoin outer worker reads another batch, it calls `ReserveOuter`. The admitted amount is bounded by:

```text
outer outstanding =
    fetched but not consumed rows + reserved but not fetched rows

outer outstanding <= outer window
```

After reading, `CommitOuter` converts the reservation into the number of rows actually fetched. If fewer rows are fetched, the unused reservation is released.

When Join finishes consuming outer rows, it reports both completed outer rows and produced Join rows. A single outer row can produce multiple Join rows over multiple `Next` calls. Join output is accumulated immediately for LIMIT progress, but it is paired with an input sample only after that outer row is fully consumed. This avoids treating `(zero completed input rows, multiple output rows)` as an infinite yield.

### Lookup Handle Admission

Before the IndexLookUp index worker creates a table lookup task, it calls `ReserveLookup`. Let `B` be the logical lookup window and `Q` be the execution batch size. The controller derives:

```text
physicalLookupWindow =
    min(maxLookupWindow, ceil(B / Q) * Q)

lookup reserved handles <= physicalLookupWindow
handles in one lookup task <= Q
```

Rounding allows an efficient task when `B` is smaller than one execution batch. Unless the configured maximum truncates the result, the difference between the physical and logical windows is less than `Q`. Concurrent tasks share this one physical window; the rounding allowance is not multiplied by concurrency.

A DistSQL result chunk can contain more handles than the current reservation. The excess handles are stored in `pendingHandles` and are admitted only after a later reservation succeeds. Their retained memory is charged to the index worker memory tracker and released when the handles enter a task or the worker exits.

A lookup task is a feedback sample only after its rows have been fully consumed in result order. At that point, `CompleteLookup` releases its reservation and reports:

```text
lookup input  = handles in the task
lookup output = table rows returned by the task
```

Extraction errors, cancellation, or work that was never dispatched call `AbortLookup`. They release capacity but do not update the yield estimate.

### Runtime Feedback

For each stage, the controller tracks:

- cumulative input and output in the current execution;
- the input and output of the four most recent productive samples;
- input observed during a consecutive zero-output phase.

The cumulative sample reduces sensitivity to one abnormal task. The recent sample detects a change near the current ordered scan position. The controller uses the estimate that requires more input, making it conservative when recent selectivity becomes worse.

The fixed-size recent window keeps update cost and memory usage constant.

### Window Adjustment

For the outer stage, let:

```text
remainingOutput = demandRows - producedOutput

cumulativeEstimate =
    ceil(remainingOutput * totalConsumedOuter / totalOutput)

recentEstimate =
    ceil(remainingOutput * recentConsumedOuter / recentOutput)

estimatedInput = max(cumulativeEstimate, recentEstimate)
```

The target includes tapered headroom:

```text
remaining more than half of total demand:        125% of estimate
remaining between one quarter and one half:      112.5% of estimate
remaining in the final quarter:                  100% of estimate
```

This preserves throughput early in execution while reducing speculative work near LIMIT completion.

The outer window can shrink immediately. It can grow only after new input progress, and one growth step is capped at twice the current window. The maximum is derived from the configured IndexJoin batch size and concurrency.

For IndexLookUpJoin, the lookup stage estimates how many handles are needed to supply the remaining outer window:

```text
lookupTarget =
    ceil(remainingOuterRows * lookupHandles / lookupRows)
```

For a direct IndexLookUp, there is no outer window, so the same lookup yield estimates the handles needed to supply the remaining LIMIT output directly:

```text
lookupTarget =
    ceil(remainingOutput * lookupHandles / lookupRows)
```

The cumulative and recent lookup estimates are computed independently, and the larger result is used. The lookup window follows the same at-most-2x growth rule.

The logical lookup window does not shrink below its statement-derived initial value. The execution batch is tracked independently, so shrinking the logical window does not create row-at-a-time table RPCs. Rounding the logical window to the execution batch intentionally permits less than one batch of physical over-admission.

If a stage produces no output, a ratio cannot estimate the next target. The controller waits until that stage has no outstanding reservation and the zero-output input reaches the current logical window. For the lookup stage, it then doubles both the logical window and execution batch up to their separate configured maxima. This guarantees progress without jumping directly to the maximum after one empty task, while avoiding a long sequence of tiny tasks for a small LIMIT.

### Batch Size and DistSQL Behavior

Initial windows are derived from LIMIT demand and existing session settings:

```text
initialOuterWindow =
    min(max(demandRows, 1), tidb_index_join_batch_size)

maxOuterWindow =
    tidb_index_join_batch_size *
    tidb_index_lookup_join_concurrency

initialLookupWindow =
    min(max(demandRows, 1), tidb_index_lookup_size)

maxLookupWindow =
    tidb_index_lookup_size *
    tidb_index_lookup_concurrency

initialLookupBatchSize =
    min(
        max(initialLookupWindow, tidb_max_chunk_size),
        tidb_index_lookup_size
    )

maxLookupBatchSize = tidb_index_lookup_size
```

For a direct IndexLookUp with paging enabled by the existing plan, `initialLookupBatchSize` is `initialLookupWindow`. This preserves the existing paging behavior for a small LIMIT instead of raising a `LIMIT 1` lookup task to `tidb_max_chunk_size`. Direct non-paging execution and the IndexLookUp under IndexLookUpJoin retain the formula above so productive scans do not start with unnecessarily small table lookup tasks.

Productive input/output feedback adjusts the logical lookup window but does not shrink the execution batch. A fully drained zero-output phase can grow the execution batch by at most 2x. Both the per-task batch and the aggregate physical window remain bounded by existing session settings.

The initial implementation does not change DistSQL scan concurrency. `tidb_distsql_scan_concurrency` and the existing RequestBuilder heuristics keep their original behavior. Lookup yield estimates how many handles should enter future table lookup tasks, but it is not a reliable signal for choosing RPC parallelism. Keeping these controls separate avoids serializing a productive multi-Region scan merely because its lookup window has not grown.

### Stop and Cleanup

The controller stops admission when:

- Join output, or fully consumed direct IndexLookUp output, reaches `offset + count`;
- Limit observes end of input;
- the executor is closed;
- the query is cancelled or fails through the existing error path.

`Stop` is idempotent. It:

- rejects future reservations;
- wakes all workers waiting for outer or lookup capacity;
- records outstanding outer and lookup work for diagnostics;
- clears logical reservations and sets both windows to zero.

Already dispatched tasks and RPCs are not synchronously withdrawn. They follow the existing cancellation and executor close lifecycle.

`Reset` prepares the same executor tree for another Open/Next/Close lifecycle after all producers from the previous lifecycle have exited. It resets counters, recent samples, zero-output state, windows, and notification channels, while preserving immutable configuration.

### Eligibility

The feature first requires all of the following shared conditions:

1. `tidb_enable_adaptive_limit_scan` is enabled;
2. the current physical operator is Limit;
3. Projection is the only executor between Limit and the recognized IndexLookUpJoin or IndexLookUp;
4. the IndexLookUp is keep-order;
5. IndexLookUp concurrency is greater than one;
6. the IndexLookUp is not in partition-table mode;
7. the IndexLookUp does not use grouped ranges or a double-read merge-sort path;
8. IndexLookUp pushdown is not enabled for this reader.

The IndexLookUpJoin shape additionally requires that its outer property preserves order and that its outer child is `Projection* -> IndexLookUp`. The direct shape requires `Projection* -> IndexLookUp` and is skipped when the reader already has a pushed-down LIMIT, because that path already bounds lookup work.

The keep-order requirement narrows the experimental rollout to the high-risk case from #66658. It is not a mathematical requirement of admission control. IndexLookUp concurrency must be greater than one because its index worker and table workers share one worker pool. With only one worker, attaching the controller cannot run index production and table lookup concurrently, so this path keeps the existing executor behavior.

If the feature is disabled or any condition is not met, the executor uses the existing path without a controller.

### Correctness and Concurrency Invariants

The controller changes when and how much work is admitted. It does not change:

- the optimizer-selected access path;
- join keys or predicates;
- result order;
- LIMIT or OFFSET semantics;
- transaction snapshots, locking, or isolation.

The implementation maintains these invariants:

- admission may delay work but must not discard work that the LIMIT may need;
- while LIMIT remains unsatisfied and the child has not reached EOF, a zero-output stage must eventually increase its window and continue;
- every reservation is settled by Complete, Abort, or Stop;
- only fully consumed tasks contribute yield samples;
- errors, cancelled work, and partially consumed tasks do not contribute yield samples;
- Stop wakes all workers waiting for capacity;
- user batch and concurrency settings remain hard upper bounds;
- physical lookup slack is less than one execution batch unless the configured maximum truncates the rounded window;
- normal LIMIT completion returns no error, while context cancellation returns the context error.

Context is checked even when capacity is immediately available. This prevents a cancelled query from being mistaken for a normal LIMIT stop.

### Configuration

The feature is controlled by:

```sql
SET SESSION tidb_enable_adaptive_limit_scan = ON;
```

The variable has GLOBAL and SESSION scope, uses Boolean values, and defaults to ON. It can be disabled to restore the existing executor behavior for subsequent statements. It does not affect plan selection and does not introduce persistent profile state.

Existing batch and concurrency variables are unchanged and remain upper bounds. In particular, enabling this feature does not override or reduce the existing DistSQL request concurrency.

### Observability

For an eligible IndexLookUpJoin, `EXPLAIN ANALYZE` includes a compact runtime summary:

```text
adaptive:{outer:1400/1000, lookup:1000/700, outstanding:400/1000, blocked:outer=1ms,lookup=3ms}
```

The fields are:

| Field | Meaning |
| --- | --- |
| `outer` | fetched outer rows / consumed outer rows |
| `lookup` | lookup handles / returned table rows |
| `outstanding` | outer / lookup capacity outstanding when admission stopped |
| `blocked` | wall-clock time during which at least one worker in the stage was blocked by a full admission budget |

For a direct IndexLookUp, its runtime information contains only the lookup stage:

```text
adaptive:{lookup:1000/700, outstanding:1000, blocked:3ms}
```

Here `outstanding` is lookup capacity outstanding when admission stopped. Direct lookup input/output is sampled only when a lookup task is fully consumed, so a final partially consumed task remains outstanding instead of contributing a misleading yield sample.

LIMIT demand and final output already appear in the Limit and child executor runtime information and are not repeated. Outstanding values are admission accounting: outer outstanding is measured in logical rows, while lookup outstanding is measured in physically reserved handles. Neither value claims that the same amount of already dispatched work was cancelled.

Blocked time is the union of overlapping waits in the corresponding stage. It does not include ordinary executor, coprocessor, or table lookup execution time. A zero value means the controller did not block that stage.

The controller keeps current window values internally for admission control and deterministic unit tests. Window values are intentionally not included in the compact plan output.

### Current Limitations

The current implementation can bound:

- the next batch of outer rows admitted by IndexLookUpJoin;
- handles admitted into future table lookup tasks for IndexLookUpJoin and direct IndexLookUp.

It cannot hard-bound:

- outer rows already read or being read;
- table lookup tasks already dispatched;
- coprocessor RPCs already sent or in flight;
- concurrent coprocessor request dispatch;
- raw keys, bytes, or execution time consumed inside one TiKV request;
- the number of workers already created for a DistSQL request.

Most importantly:

> A lookup handle window is not a TiKV raw scan budget.

A TiKV request may process many raw keys before returning a small number of handles. The current design reduces TiDB pipeline and table-lookup amplification, but it cannot guarantee a maximum `processed_keys`, `IndexRangeScan actRows`, or number of scanned bytes for one request.

Additional limitations are:

- only the recognized keep-order IndexLookUpJoin outer path and direct keep-order IndexLookUp path are supported;
- physical lookup rounding can admit less than one execution batch beyond the logical window near the LIMIT tail;
- DistSQL request concurrency and ordered request dispatch are unchanged;
- runtime stats do not yet include a reason why a plan was not eligible.

### Compatibility

**SQL semantics:** There are no parser or syntax changes. Results, ordering, LIMIT/OFFSET behavior, transaction semantics, and error semantics remain unchanged.

**Planner and plan cache:** The controller is attached while executors are built. It does not select a plan or change plan-cache keys. Cached plans are eligible or ineligible at execution time according to the same executor properties and session switch.

**Mixed versions:** The design changes only TiDB-side execution and does not require a TiKV, PD, TiFlash, or protocol change. During a rolling upgrade, operators can explicitly set the GLOBAL switch to OFF until all desired TiDB instances are upgraded.

**Partitioned tables and unsupported readers:** Unsupported paths skip the feature and preserve current behavior.

**Resource control:** RU accounting and resource groups continue to observe the actual work performed. The controller does not bypass resource control.

**Downgrade and rollback:** The feature creates no persistent metadata. Turning the variable OFF restores the existing execution path for subsequent statements.

## Test Design

### Functional Tests

Controller unit tests cover:

- productive and zero-output window adjustment;
- immediate shrink and at-most-2x growth;
- one growth per progress epoch;
- cumulative and recent yield estimates;
- one-to-many Join output paired with completed outer rows;
- outer and lookup reservation accounting;
- independent lookup budget, execution batch, and rounded physical-window accounting;
- execution-batch reset, ceiling, and zero-output growth;
- Stop, context cancellation, blocked-worker wakeup, and Reset;
- integer saturation and LIMIT edge cases.

Executor unit tests cover:

- exact plan-shape recognition through Projection;
- rejection of Selection and unsupported reader configurations;
- keep-order and concurrency eligibility;
- pending-handle splitting and memory accounting;
- Complete and Abort behavior on success, cancellation, and error;
- feature ON/OFF lifecycle;
- result value and order equivalence;
- direct IndexLookUp paging with a high-selectivity `LIMIT 1`, which must not inflate the initial table lookup task to `tidb_max_chunk_size`;
- compact runtime statistics.

The regression test for #66658 must fail before the fix because the eligible pipeline admits work without the adaptive bound, and pass after the controller is attached. Assertions should use deterministic logical admission and runtime state rather than wall-clock timings.

### Scenario Tests

The complete SQL scenario matrix should include:

- LIMIT with and without OFFSET;
- LIMIT 0 and LIMIT larger than the complete result;
- empty input and consecutive zero-output regions;
- high, low, and changing selectivity;
- `LIMIT 1` with a sparse ordered prefix, including table lookup task count;
- one outer row producing many Join rows over multiple chunks;
- LIMIT completion in the middle of an outer or lookup task;
- normal EOF, cancellation, execution error, and repeated Open/Close;
- ordered results with a delayed leading task;
- multiple ranges and Region boundaries where available.

### Compatibility Tests

Tests verify:

- the switch is GLOBAL/SESSION, Boolean, and ON by default;
- OFF uses the existing executor behavior and omits adaptive runtime stats;
- unsupported plans remain unchanged;
- user batch and concurrency settings remain hard upper bounds;
- prepared and non-prepared execution return identical results;
- partition, grouped-range, merge-sort, and IndexLookUp-pushdown paths are skipped;
- cancellation returns the original context error rather than a normal stop.

### Benchmark Tests

Performance validation compares ON and OFF with alternating rounds and excludes warm-up runs. It records:

- query latency and tail latency;
- throughput;
- RU, TiDB/TiKV CPU, memory, I/O, and network;
- coprocessor task count and processed keys;
- IndexRangeScan, Selection, and TableRowIDScan `actRows`;
- outer and lookup adaptive counters;
- configured and effective concurrency.

The issue-scale local RealTiKV validation used:

- 400,000 rows in each table;
- an ordered range covering 392,857 rows;
- `LIMIT 1000`;
- the same result values and order with the feature ON and OFF.

One observed comparison was:

| Operator | OFF actRows | ON actRows |
| --- | ---: | ---: |
| outer IndexRangeScan | 160,608 | 9,184 |
| Selection | 75,870 | 1,434 |
| TableRowIDScan | 108,384 | 2,048 |

In the same run, latency decreased from 77.60 ms to 42.95 ms and RU decreased from 150.63 to 11.30. The ON plan reported `adaptive:{outer:1000/1000, lookup:1024/718, outstanding:0/1024}`. The final lookup outstanding value reflects one bounded execution batch admitted near LIMIT completion.

Both paths reported a maximum effective DistSQL concurrency of one in this single-TiKV setup. This confirms that enabling the feature does not change the observed request concurrency for this topology, but it is not evidence for multi-Region concurrency behavior.

A second local validation used three TiKV stores. The outer table record keyspace, outer ordering index, and inner join index were each split into 48 Regions. Before measurement, the test required every keyspace to expose all 48 Regions and leaders on all three stores. Each feature state was warmed up, then four measured rounds alternated the ON/OFF order. Every ON/OFF pair returned the same ordered result digest.

The median results were:

| Workload | Feature | Latency (ms) | RU | Outer index actRows | Cop tasks | Effective concurrency |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| #66658, `LIMIT 1000` | OFF | 121.76 | 311.05 | 304,166.5 | 36.5 | 2 |
| #66658, `LIMIT 1000` | ON | 44.77 | 10.07 | 8,333 | 1 | 2 |
| dense, `LIMIT 100000` | OFF | 576.32 | 1,628.84 | 392,857 | 48 | 2 |
| dense, `LIMIT 100000` | ON | 499.91 | 610.73 | 125,000 | 15 | 2 |
| 240,000-row zero-output prefix | OFF | 130.03 | 491.79 | 392,857 | 48 | 2 |
| 240,000-row zero-output prefix | ON | 119.71 | 399.44 | 258,333 | 31 | 2 |

The #66658 workload retained its early-stop benefit. The dense and zero-output prefix workloads crossed multiple Regions, retained an effective DistSQL concurrency of two, and did not regress median latency. An executor request-hook test separately verifies that ON and OFF produce the same positive keep-order `kv.Request.Concurrency`, and that the adaptive path does not install a shared coprocessor request limiter.

This is manual E2E evidence, not a CI guarantee and not proof of a hard per-request scan bound. Acceptance must prioritize result correctness and latency/resource improvement. Lower `actRows` alone is insufficient.

## Impacts and Risks

Expected positive impacts are:

- less speculative work for early-stop ordered queries;
- lower TiDB/TiKV resource usage and RU in affected cases;
- reduced memory accumulated behind an ordered leading task;
- lower latency and tail latency when excess prefetch causes contention.

Intentional trade-offs are:

- workers can wait for admission instead of always prefetching;
- physical-window rounding permits less than one execution batch of tail over-admission to preserve RPC efficiency;
- conservative yield estimates may retain more headroom after an earlier low-selectivity phase.

Risks include:

- windows that are too small can reduce pipeline utilization and increase latency;
- smaller lookup tasks can increase RPC and RU overhead;
- ordered multi-Region execution can still dispatch work behind a slow leading task because there is no strict frontier;
- task-completion feedback can react later than raw scan progress;
- a bug in reservation settlement can deadlock workers or leak logical capacity;
- recent samples may be biased if task completion order does not match the intended ordered consumption contract;
- unchanged DistSQL concurrency can still read ahead inside requests before TiDB-side admission stops.

The GLOBAL/SESSION gate provides an immediate opt-out if a regression is observed. Because the feature defaults to ON, performance evaluation must compare latency, throughput, and RU in addition to scan counters.

## Investigation and Alternatives

### Static Batch or Scan Thresholds

A fixed small batch or concurrency limits prefetch but cannot adapt to different LIMIT values, predicates, or scan positions. It can reduce throughput for normal queries and still over-admit low-yield work. Static session values remain ceilings, while the proposed controller derives the active window from runtime feedback.

### Historical SQL Digest Profiles

A cache keyed by SQL digest, plan digest, reader type, ordering, and LIMIT bucket could provide a better initial estimate on repeated executions. However, different parameters and data positions under one digest can have different yields. Profiles also require invalidation, memory bounds, observability, and ownership semantics.

Historical profiles may be useful later as an initial hint, but they should not replace statement-local correction and stopping.

### Planner-Only LIMIT Pushdown

The planner can push LIMIT into a reader only when doing so preserves semantics. Selection and Join can change cardinality, so blindly pushing `offset + count` below them can return too few rows. The proposed controller does not rewrite the plan; it learns the required lower-stage input while the query executes.

### Always Reduce DistSQL Concurrency to One

This bounds concurrent prefetch but can substantially regress multi-Region latency and throughput. The initial implementation therefore leaves DistSQL concurrency unchanged and limits downstream row and handle admission instead. Any future adaptive RPC concurrency control needs an independent, validated latency or starvation signal.

### Consumer-Starvation-Based Growth

Growing concurrency whenever the result queue is temporarily empty confuses normal pipeline gaps with genuine starvation. During system jitter, this can create the same positive feedback that the feature is intended to avoid. The current design does not adapt DistSQL concurrency. Consumed row progress and drained zero-output phases change only logical admission windows and lookup task sizes.

### Request-Level Scan Budget

A budget enforced within TiKV could bound processed keys, bytes, or execution time inside a single coprocessor request. That is a stronger and more general mechanism, but it requires protocol and TiKV execution changes, continuation or partial-result semantics, and coordination across requests. It is complementary to, rather than replaced by, TiDB-side pipeline admission.

## Future Work

Potential extensions, in increasing implementation complexity, are:

1. IndexReader and TableReader range/request admission;
2. explicit executor-side Selection feedback;
3. unordered IndexJoin, IndexHashJoin, and Apply with task-identified reservations and out-of-order completion;
4. IndexMerge and partition-level budget distribution;
5. MPP, Union, and multi-child budget allocation;
6. strict ordered-task frontier admission;
7. adaptive DistSQL concurrency based on reliable queue and latency signals;
8. request-level raw scan budgets in TiKV;
9. statement and cluster metrics for windows, waits, concurrency, and eligibility reasons.

## Unresolved Questions

The initial implementation intentionally leaves these questions open:

- What signal can safely adjust effective DistSQL concurrency without causing oscillation or head-of-line stalls?
- Should ordered dispatch enforce a strict task frontier rather than only a row-admission window?
- Which metrics and eligibility skip reasons should be exposed for rollout and troubleshooting?
- What performance thresholds should gate expansion beyond the #66658 plan shape?
- Should a future historical profile influence only initial windows, and how should it be invalidated across schema, plan, and statistics changes?
