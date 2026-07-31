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

`LIMIT` queries need only the first `offset + count` rows, but asynchronous
executors below the `LIMIT` can scan, compute, or dispatch much more work before
the upper executor stops consuming rows. The multiplication of batching,
concurrency, and prefetching can increase TiDB and TiKV CPU, memory, network,
I/O, and RU consumption. Ordered execution makes the problem more severe
because results completed behind a slow leading task cannot be returned early.

Adaptive LIMIT Scan adds a lightweight controller to one statement execution.
The controller translates the remaining LIMIT demand into admission windows
for supported execution stages. Workers must reserve capacity before admitting
more work, report the actual input and output after that work is consumed, and
stop admitting work when the LIMIT is satisfied.

The initial implementation targets the ordered `IndexLookUpJoin` plan involved
in [issue #66658](https://github.com/pingcap/tidb/issues/66658). The feature is
guarded by `tidb_enable_adaptive_limit_scan` and is disabled by default.

## Motivation or Background

In issue #66658, the query needs 1,000 rows, while different stages of the
execution plan process substantially more data:

```text
Limit / IndexJoin:          1,000 rows
IndexJoin outer input:     81,920 rows
IndexRangeScan:           392,857 rows
Selection:                273,763 rows
TableRowIDScan:           273,804 rows
```

These counters describe different stages and are not expected to be equal. The
problem is that much of the work is never consumed by the LIMIT.

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

Without information about the remaining LIMIT demand, each layer may admit a
reasonable amount of work locally while the whole pipeline admits far more
work than the query can consume.

`keepOrder=true` adds head-of-line blocking. If an early range or lookup task
slows down, later tasks may finish but cannot pass the earlier task. Continuing
to prefetch at this point consumes more resources, which can further delay the
leading task:

```text
transient slowdown
    -> leading ordered task is delayed
    -> more later work accumulates
    -> CPU, I/O, and network pressure increase
    -> leading task is delayed further
```

Reducing a global batch size or concurrency is not sufficient. Selectivity and
data distribution vary by SQL statement, parameter, and scan position. A
globally small value can reduce normal throughput while still being unable to
identify how much work the current LIMIT needs.

The purpose of Adaptive LIMIT Scan is to improve latency and reduce resource
consumption by bounding speculative work. Reducing `actRows` is a means to that
end, not an independent objective. An implementation that reads fewer rows but
regresses latency or RU because it creates too many small tasks is not an
improvement.

## Goals and Non-Goals

The goals are:

- propagate the remaining LIMIT demand into supported asynchronous readers;
- bound the amount of speculative work admitted in TiDB;
- adjust that bound from input/output ratios observed in the current execution;
- continue making progress through low-selectivity or changing data regions;
- preserve SQL results, result order, transaction semantics, and user
  concurrency ceilings;
- keep the feature statement-local and inexpensive on the execution hot path.

The initial implementation does not:

- use historical SQL profiles or digests to select or change a plan;
- change access paths, join keys, predicates, or physical ordering;
- synchronously cancel RPCs or tasks that have already been dispatched;
- impose a hard limit on raw keys, bytes, or time scanned inside one TiKV
  coprocessor request;
- propagate budgets through blocking implementations such as Sort, HashAgg, or
  Window implementations that require a complete partition;
- support unordered IndexJoin, IndexHashJoin, Apply, IndexMerge, or MPP.

## Detailed Design

### Supported Plan Shape

The first version recognizes the following executor shape:

```text
Limit
└─Projection*
  └─IndexLookUpJoin
    └─Projection* (outer)
      └─IndexLookUp (keep order)
```

Only Projection can appear between the recognized operators because Projection
preserves row cardinality. An operator such as Selection changes the
input/output ratio and would need its own feedback stage. A blocking operator
stops budget propagation.

The controller is attached while `LimitExec` is built. One controller is owned
by one executor tree and one execution lifecycle.

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

No state is shared across SQL executions. This avoids mixing different
parameters, plans, schemas, or data distributions and allows the current
execution to react to a selectivity change at its current scan position.

All controller state is protected by one mutex. A worker waiting for capacity
also listens for context cancellation and the controller stop signal.

### Demand and Stage Budgets

The total demand includes the offset:

```text
demandRows = offset + count
```

Rows before the offset are not returned to the client, but the child executor
must still produce them, so they consume demand.

The target plan contains two different input/output transformations:

```text
IndexLookUpJoin outer stage:
    outer rows -> join output rows

IndexLookUp table stage:
    index handles -> table rows
```

These units must not share one counter. For example, 1,000 index handles might
produce only 100 table rows after a table-side predicate. Controlling only Join
outer rows would not prevent IndexLookUp from dispatching too many low-yield
table lookup tasks.

The controller therefore maintains:

- an outer row window, which bounds rows prefetched by IndexLookUpJoin;
- a lookup handle window, which bounds handles admitted into table lookup.

A window is an admission bound, not a prediction that the exact number of rows
will be needed.

### Outer Row Admission

Before the IndexLookUpJoin outer worker reads another batch, it calls
`ReserveOuter`. The admitted amount is bounded by:

```text
outer outstanding =
    fetched but not consumed rows + reserved but not fetched rows

outer outstanding <= outer window
```

After reading, `CommitOuter` converts the reservation into the number of rows
actually fetched. If fewer rows are fetched, the unused reservation is
released.

When Join finishes consuming outer rows, it reports both completed outer rows
and produced Join rows. A single outer row can produce multiple Join rows over
multiple `Next` calls. Join output is accumulated immediately for LIMIT
progress, but it is paired with an input sample only after that outer row is
fully consumed. This avoids treating `(zero completed input rows, multiple
output rows)` as an infinite yield.

### Lookup Handle Admission

Before the IndexLookUp index worker creates a table lookup task, it calls
`ReserveLookup`. Only the admitted handles can enter that task:

```text
lookup reserved handles <= lookup window
```

A DistSQL result chunk can contain more handles than the current reservation.
The excess handles are stored in `pendingHandles` and are admitted only after a
later reservation succeeds. Their retained memory is charged to the index
worker memory tracker and released when the handles enter a task or the worker
exits.

A lookup task is a feedback sample only after its rows have been fully consumed
in result order. At that point, `CompleteLookup` releases its reservation and
reports:

```text
lookup input  = handles in the task
lookup output = table rows returned by the task
```

Extraction errors, cancellation, or work that was never dispatched call
`AbortLookup`. They release capacity but do not update the yield estimate.

### Runtime Feedback

For each stage, the controller tracks:

- cumulative input and output in the current execution;
- the input and output of the four most recent productive samples;
- input observed during a consecutive zero-output phase.

The cumulative sample reduces sensitivity to one abnormal task. The recent
sample detects a change near the current ordered scan position. The controller
uses the estimate that requires more input, making it conservative when recent
selectivity becomes worse.

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

This preserves throughput early in execution while reducing speculative work
near LIMIT completion.

The outer window can shrink immediately. It can grow only after new input
progress, and one growth step is capped at twice the current window. The
maximum is derived from the configured IndexJoin batch size and concurrency.

The lookup stage estimates how many handles are needed to supply the remaining
outer window:

```text
lookupTarget =
    ceil(remainingOuterRows * lookupHandles / lookupRows)
```

The cumulative and recent lookup estimates are computed independently, and the
larger result is used. The lookup window follows the same at-most-2x growth
rule.

The lookup window does not shrink below its initial value. Very small table
lookup tasks can create many small RPCs and reduce throughput. Keeping the
statement-derived initial window as a floor intentionally permits up to one
initial batch of tail over-admission.

If a stage produces no output, a ratio cannot estimate the next target. The
controller waits until that stage has no outstanding reservation and the
zero-output input reaches the current window, then doubles the window up to its
configured maximum. This guarantees progress without jumping directly to the
maximum after one empty task.

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
```

The current lookup window suggests the next lookup task batch size, bounded by
`tidb_index_lookup_size`.

The initial implementation does not change DistSQL scan concurrency.
`tidb_distsql_scan_concurrency` and the existing RequestBuilder heuristics keep
their original behavior. Lookup yield estimates how many handles should enter
future table lookup tasks, but it is not a reliable signal for choosing RPC
parallelism. Keeping these controls separate avoids serializing a productive
multi-Region scan merely because its lookup window has not grown.

### Stop and Cleanup

The controller stops admission when:

- Join output reaches `offset + count`;
- Limit observes end of input;
- the executor is closed;
- the query is cancelled or fails through the existing error path.

`Stop` is idempotent. It:

- rejects future reservations;
- wakes all workers waiting for outer or lookup capacity;
- records outstanding outer and lookup work for diagnostics;
- clears logical reservations and sets both windows to zero.

Already dispatched tasks and RPCs are not synchronously withdrawn. They follow
the existing cancellation and executor close lifecycle.

`Reset` prepares the same executor tree for another Open/Next/Close lifecycle
after all producers from the previous lifecycle have exited. It resets
counters, recent samples, zero-output state, windows, and notification
channels, while preserving immutable configuration.

### Eligibility

The feature activates only when all of the following are true:

1. `tidb_enable_adaptive_limit_scan` is enabled;
2. the current physical operator is Limit;
3. the Limit child is `Projection* -> IndexLookUpJoin`;
4. the IndexLookUpJoin required physical property for its outer child requires
   order;
5. the outer child is `Projection* -> IndexLookUp`;
6. the IndexLookUp is keep-order;
7. IndexLookUp concurrency is greater than one;
8. the IndexLookUp is not in partition-table mode;
9. the IndexLookUp does not use grouped ranges or a double-read merge-sort path;
10. IndexLookUp pushdown is not enabled for this reader.

The keep-order requirement narrows the experimental rollout to the high-risk
case from #66658. It is not a mathematical requirement of admission control.

If the feature is disabled or any condition is not met, the executor uses the
existing path without a controller.

### Correctness and Concurrency Invariants

The controller changes when and how much work is admitted. It does not change:

- the optimizer-selected access path;
- join keys or predicates;
- result order;
- LIMIT or OFFSET semantics;
- transaction snapshots, locking, or isolation.

The implementation maintains these invariants:

- admission may delay work but must not discard work that the LIMIT may need;
- while LIMIT remains unsatisfied and the child has not reached EOF, a
  zero-output stage must eventually increase its window and continue;
- every reservation is settled by Complete, Abort, or Stop;
- only fully consumed tasks contribute yield samples;
- errors, cancelled work, and partially consumed tasks do not contribute yield
  samples;
- Stop wakes all workers waiting for capacity;
- user batch and concurrency settings remain hard upper bounds;
- normal LIMIT completion returns no error, while context cancellation returns
  the context error.

Context is checked even when capacity is immediately available. This prevents
a cancelled query from being mistaken for a normal LIMIT stop.

### Configuration

The feature is controlled by:

```sql
SET SESSION tidb_enable_adaptive_limit_scan = ON;
```

The variable has GLOBAL and SESSION scope, uses Boolean values, and defaults to
OFF. It does not affect plan selection and does not introduce persistent
profile state.

Existing batch and concurrency variables are unchanged and remain upper bounds.
In particular, enabling this feature does not override or reduce the existing
DistSQL request concurrency.

### Observability

For an eligible IndexLookUpJoin, `EXPLAIN ANALYZE` includes a compact runtime
summary:

```text
adaptive:{outer:1400/1000, lookup:1000/700, outstanding:400/1000}
```

The fields are:

| Field | Meaning |
| --- | --- |
| `outer` | fetched outer rows / consumed outer rows |
| `lookup` | lookup handles / returned table rows |
| `outstanding` | outer / lookup capacity outstanding when admission stopped |

LIMIT demand and final Join output already appear in the Limit and
IndexLookUpJoin runtime information and are not repeated. Outstanding values
are logical admission accounting; they do not claim that the same amount of
physical work was cancelled.

The controller snapshot also retains current window values for deterministic
unit tests, but they are not included in the compact plan output.

### Current Limitations

The current implementation can bound:

- the next batch of outer rows admitted by IndexLookUpJoin;
- handles admitted into future table lookup tasks.

It cannot hard-bound:

- outer rows already read or being read;
- table lookup tasks already dispatched;
- coprocessor RPCs already sent or in flight;
- concurrent coprocessor request dispatch;
- raw keys, bytes, or execution time consumed inside one TiKV request;
- the number of workers already created for a DistSQL request.

Most importantly:

> A lookup handle window is not a TiKV raw scan budget.

A TiKV request may process many raw keys before returning a small number of
handles. The current design reduces TiDB pipeline and table-lookup
amplification, but it cannot guarantee a maximum
`processed_keys`, `IndexRangeScan actRows`, or number of scanned bytes for one
request.

Additional limitations are:

- only one keep-order IndexLookUpJoin outer path is supported;
- the lookup window floor can admit one initial batch near the LIMIT tail;
- DistSQL request concurrency and ordered request dispatch are unchanged;
- runtime stats do not yet include final windows or a reason why a plan was
  not eligible.

### Compatibility

**SQL semantics:** There are no parser or syntax changes. Results, ordering,
LIMIT/OFFSET behavior, transaction semantics, and error semantics remain
unchanged.

**Planner and plan cache:** The controller is attached while executors are
built. It does not select a plan or change plan-cache keys. Cached plans are
eligible or ineligible at execution time according to the same executor
properties and session switch.

**Mixed versions:** The design changes only TiDB-side execution and does not
require a TiKV, PD, TiFlash, or protocol change. A rolling upgrade can leave
the switch OFF until all desired TiDB instances are upgraded.

**Partitioned tables and unsupported readers:** Unsupported paths skip the
feature and preserve current behavior.

**Resource control:** RU accounting and resource groups continue to observe the
actual work performed. The controller does not bypass resource control.

**Downgrade and rollback:** The feature creates no persistent metadata. Turning
the variable OFF restores the existing execution path for subsequent
statements.

## Test Design

### Functional Tests

Controller unit tests cover:

- productive and zero-output window adjustment;
- immediate shrink and at-most-2x growth;
- one growth per progress epoch;
- cumulative and recent yield estimates;
- one-to-many Join output paired with completed outer rows;
- outer and lookup reservation accounting;
- lookup batch size suggestions;
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
- compact runtime statistics.

The regression test for #66658 must fail before the fix because the eligible
pipeline admits work without the adaptive bound, and pass after the controller
is attached. Assertions should use deterministic logical admission and runtime
state rather than wall-clock timings.

### Scenario Tests

The SQL test matrix includes:

- LIMIT with and without OFFSET;
- LIMIT 0 and LIMIT larger than the complete result;
- empty input and consecutive zero-output regions;
- high, low, and changing selectivity;
- one outer row producing many Join rows over multiple chunks;
- LIMIT completion in the middle of an outer or lookup task;
- normal EOF, cancellation, execution error, and repeated Open/Close;
- ordered results with a delayed leading task;
- multiple ranges and Region boundaries where available.

### Compatibility Tests

Tests verify:

- the switch is GLOBAL/SESSION, Boolean, and OFF by default;
- OFF uses the existing executor behavior and omits adaptive runtime stats;
- unsupported plans remain unchanged;
- user batch and concurrency settings remain hard upper bounds;
- prepared and non-prepared execution return identical results;
- partition, grouped-range, merge-sort, and IndexLookUp-pushdown paths are
  skipped;
- cancellation returns the original context error rather than a normal stop.

### Benchmark Tests

Performance validation compares ON and OFF with alternating rounds and excludes
warm-up runs. It records:

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
| outer IndexRangeScan | 210,752 | 9,184 |
| Selection | 104,542 | 1,400 |
| TableRowIDScan | 149,344 | 2,000 |

Both paths reported a maximum effective DistSQL concurrency of one in this
single-TiKV setup. This confirms that enabling the feature does not change the
observed request concurrency for this topology, but it is not evidence for
multi-Region concurrency behavior.

This is manual E2E evidence, not a CI guarantee and not proof of a hard
per-request scan bound. Acceptance must prioritize result correctness and
latency/resource improvement. Lower `actRows` alone is insufficient.

## Impacts and Risks

Expected positive impacts are:

- less speculative work for early-stop ordered queries;
- lower TiDB/TiKV resource usage and RU in affected cases;
- reduced memory accumulated behind an ordered leading task;
- lower latency and tail latency when excess prefetch causes contention.

Intentional trade-offs are:

- workers can wait for admission instead of always prefetching;
- the initial lookup floor permits bounded tail over-admission to preserve RPC
  efficiency;
- conservative yield estimates may retain more headroom after an earlier
  low-selectivity phase.

Risks include:

- windows that are too small can reduce pipeline utilization and increase
  latency;
- smaller lookup tasks can increase RPC and RU overhead;
- ordered multi-Region execution can still dispatch work behind a slow leading
  task because there is no strict frontier;
- task-completion feedback can react later than raw scan progress;
- a bug in reservation settlement can deadlock workers or leak logical
  capacity;
- recent samples may be biased if task completion order does not match the
  intended ordered consumption contract;
- unchanged DistSQL concurrency can still read ahead inside requests before
  TiDB-side admission stops.

The default-OFF gate limits rollout risk. Performance evaluation must compare
latency, throughput, and RU in addition to scan counters.

## Investigation and Alternatives

### Static Batch or Scan Thresholds

A fixed small batch or concurrency limits prefetch but cannot adapt to different
LIMIT values, predicates, or scan positions. It can reduce throughput for
normal queries and still over-admit low-yield work. Static session values remain
ceilings, while the proposed controller derives the active window from runtime
feedback.

### Historical SQL Digest Profiles

A cache keyed by SQL digest, plan digest, reader type, ordering, and LIMIT
bucket could provide a better initial estimate on repeated executions.
However, different parameters and data positions under one digest can have
different yields. Profiles also require invalidation, memory bounds,
observability, and ownership semantics.

Historical profiles may be useful later as an initial hint, but they should not
replace statement-local correction and stopping.

### Planner-Only LIMIT Pushdown

The planner can push LIMIT into a reader only when doing so preserves
semantics. Selection and Join can change cardinality, so blindly pushing
`offset + count` below them can return too few rows. The proposed controller
does not rewrite the plan; it learns the required lower-stage input while the
query executes.

### Always Reduce DistSQL Concurrency to One

This bounds concurrent prefetch but can substantially regress multi-Region
latency and throughput. The initial implementation therefore leaves DistSQL
concurrency unchanged and limits downstream row and handle admission instead.
Any future adaptive RPC concurrency control needs an independent, validated
latency or starvation signal.

### Consumer-Starvation-Based Growth

Growing concurrency whenever the result queue is temporarily empty confuses
normal pipeline gaps with genuine starvation. During system jitter, this can
create the same positive feedback that the feature is intended to avoid.
The current design does not adapt DistSQL concurrency. Consumed row progress
and drained zero-output phases change only logical admission windows and lookup
task sizes.

### Request-Level Scan Budget

A budget enforced within TiKV could bound processed keys, bytes, or execution
time inside a single coprocessor request. That is a stronger and more general
mechanism, but it requires protocol and TiKV execution changes, continuation or
partial-result semantics, and coordination across requests. It is
complementary to, rather than replaced by, TiDB-side pipeline admission.

## Future Work

Potential extensions, in increasing implementation complexity, are:

1. direct `Limit -> Projection* -> IndexLookUp`, using only the lookup-handle
   stage when LIMIT cannot already be pushed into IndexLookUp;
2. IndexReader and TableReader range/request admission;
3. explicit Selection stage feedback;
4. unordered IndexJoin, IndexHashJoin, and Apply with task-identified
   reservations and out-of-order completion;
5. IndexMerge and partition-level budget distribution;
6. MPP, Union, and multi-child budget allocation;
7. strict ordered-task frontier admission;
8. adaptive DistSQL concurrency based on reliable queue and latency signals;
9. request-level raw scan budgets in TiKV;
10. statement and cluster metrics for windows, waits, concurrency, and
    eligibility reasons.

Direct IndexLookUp cannot reuse the current lookup target unchanged because the
current target depends on the Join outer window and consumption. A future
implementation should separate final LIMIT progress from per-stage yield, for
example:

```text
ObserveLimitInput(rows)
ObserveStageProgress(stage, inputRows, outputRows)
```

## Unresolved Questions

The initial implementation intentionally leaves these questions open:

- What signal can safely adjust effective DistSQL concurrency without causing
  oscillation or head-of-line stalls?
- Should ordered dispatch enforce a strict task frontier rather than only a
  row-admission window?
- Should the lookup batch efficiency floor be separated from the logical
  admission window?
- Which metrics and eligibility skip reasons should be exposed before the
  feature is enabled by default?
- What performance thresholds should gate expansion beyond the #66658 plan
  shape?
- Should a future historical profile influence only initial windows, and how
  should it be invalidated across schema, plan, and statistics changes?
