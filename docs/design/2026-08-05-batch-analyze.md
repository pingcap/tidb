# Batch Requests for the `ANALYZE` Statement

- Author(s): [0xPoe](https://github.com/0xPoe)
- Discussion PR: https://github.com/pingcap/tidb/pull/70355
- Tracking Issue: https://github.com/pingcap/tidb/issues/67449

## Table of Contents

* [Introduction](#introduction)
* [Motivation](#motivation)
* [Detailed Design](#detailed-design)
    * [Current Implementation](#current-implementation)
    * [Batched Analyze Requests](#batched-analyze-requests)
        * [Add a Finalizer](#add-a-finalizer)
        * [Explicitly Opt-in](#explicitly-opt-in)
        * [Concurrency Control](#concurrency-control)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
    * [Impacts](#impacts)
    * [Risks](#risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)
* [Future Possibility](#future-possibility)
* [FAQ](#faq)

## Introduction

This document proposes reusing the existing batch-request mechanism in the coprocessor framework to pre-merge the statistics collected from TiKV, so that `ANALYZE` can reduce network traffic and TiDB-side allocation and CPU overhead.

## Motivation

Large tables can contain thousands of regions. In the current statistics-collection process, TiDB sends a coprocessor task to each region, collects the partial statistics, and merges them locally in TiDB.

The partial statistics collected from TiKV include row samples, a row count, null counts, total sizes, and FM sketches. [Reservoir samples](https://github.com/pingcap/tidb/blob/16c97eb67f9558f39535bf63b8c2ffe388fbe391/pkg/statistics/row_sampler.go#L371-L394) are mergeable: TiKV can combine the per-Region weighted samples and retain the global top-K that TiDB needs. [Bernoulli samples](https://github.com/pingcap/tidb/blob/16c97eb67f9558f39535bf63b8c2ffe388fbe391/pkg/statistics/row_sampler.go#L465-L478), however, must be concatenated without dropping selected rows. The remaining collector fields are mergeable in both sampling modes.

A single [FMSketch](https://github.com/pingcap/tidb/blob/16c97eb67f9558f39535bf63b8c2ffe388fbe391/pkg/statistics/fmsketch.go#L36-L66), once serialized to Protobuf, takes at most 160 KiB, and a full-sampling collector contains one for each analyzed column or column group. For a table with 10 billion rows spread across, say, 20,000 regions, transferring just one sketch per region would cost up to 20,000 × 160 KiB ≈ 3 GiB of network traffic.

Since full-sampling collectors are mergeable, TiKV can combine them before sending them back. This document therefore proposes reusing the existing batch-request mechanism to return fewer partial statistics to TiDB, reducing network traffic and TiDB-side allocation and garbage-collection overhead. In cloud environments where network traffic is metered, this reduction can significantly lower operating costs.

## Detailed Design

### Current Implementation

Before diving into the new design, let's take a look at the current batch-request implementation.

Currently, store-batched coprocessor requests are used for handle-based table reads. They combine eligible small Region tasks targeting the same TiKV store into a single unary coprocessor RPC.

The [workflow](https://editor.plantuml.com/uml/lPPDJiCm48NtESM85GY50tI14EL721PLMzW0Ysaoj5OTZsKxG6_Fn0qaaK3Gj7HMJYlvytjlnWcS-O0kb8M6Vwm4WWgQO5WwHoR09B2Zz1n3jg0SXcmTP-GzExZI_BO5PY-LW1NBLAOiYfQ3gReuXnkJq_iTy_BU7W3wvhcqkyIqhHfg9Lv6sdgv8ypjGmTpQNBBgWPzFkm6CoRCOSIiuzxLef-43cOlbNG2Ja_h10OmAMU52X1mfYbz8MbmMVkbXzbAvCuLcyqmTR8jmhLZGVe2jMwsZdRwQghwgMamdmcB538vi24e3RfLfoV6e-6JEInGcNW4E5vFT1pegVpm-7pqBUQhVGHKnIsGwWOevMeW5AjgX-AUIWosSWGvbrvjfYPsKjeHCwPGEXFbEFAb3c39zKZa9pKDQOJP4kS4qHrXMJQU04t-PBecZUl_7f__YcbrGKkFXD5m-bRJ0W9fzVO0BaYhL_5A_4fhlzOE-bwOlLG2XeCahUQB0FoonRNr2sQw8CZgbrf1sGADyeMa8XqxYsyt3y6XaN1SEDa2KurtphARw1AGcpTjZ2lDcFlUkxSmrS17u_wp4ZWJJlt1yG40) is as follows:

![](https://img.plantuml.biz/plantuml/png/lPPDJiCm48NtESM85GY50tI14EL721PLMzW0Ysaoj5OTZsKxG6_Fn0qaaK3Gj7HMJYlvytjlnWcS-O0kb8M6Vwm4WWgQO5WwHoR09B2Zz1n3jg0SXcmTP-GzExZI_BO5PY-LW1NBLAOiYfQ3gReuXnkJq_iTy_BU7W3wvhcqkyIqhHfg9Lv6sdgv8ypjGmTpQNBBgWPzFkm6CoRCOSIiuzxLef-43cOlbNG2Ja_h10OmAMU52X1mfYbz8MbmMVkbXzbAvCuLcyqmTR8jmhLZGVe2jMwsZdRwQghwgMamdmcB538vi24e3RfLfoV6e-6JEInGcNW4E5vFT1pegVpm-7pqBUQhVGHKnIsGwWOevMeW5AjgX-AUIWosSWGvbrvjfYPsKjeHCwPGEXFbEFAb3c39zKZa9pKDQOJP4kS4qHrXMJQU04t-PBecZUl_7f__YcbrGKkFXD5m-bRJ0W9fzVO0BaYhL_5A_4fhlzOE-bwOlLG2XeCahUQB0FoonRNr2sQw8CZgbrf1sGADyeMa8XqxYsyt3y6XaN1SEDa2KurtphARw1AGcpTjZ2lDcFlUkxSmrS17u_wp4ZWJJlt1yG40)

1. During the table-fetch phase of an [`IndexLookUp`](https://docs.pingcap.com/tidb/stable/explain-overview/) query, TiDB divides the handle reads into Region tasks and groups eligible small tasks that target the same TiKV store.

2. TiDB sends each group as a single unary coprocessor RPC, placing one task in the main request and the rest in [`StoreBatchTask`](https://github.com/pingcap/kvproto/blob/683dad8fa3689deb243f2ff8ab5847c97af53e38/proto/coprocessor.proto#L175-L186) entries.

3. TiKV schedules each Region task independently in its read pool and collects the results.

4. TiKV returns the main response together with the corresponding [`StoreBatchTaskResponse`](https://github.com/pingcap/kvproto/blob/683dad8fa3689deb243f2ff8ab5847c97af53e38/proto/coprocessor.proto#L188-L198) entries. TiDB then unpacks them into per-Region results. TiKV never merges the payloads.

The same mechanism can batch `ANALYZE` requests for multiple Regions on the same TiKV store. However, it must be extended to merge the individual statistics payloads on TiKV and return the combined payload in the main response.

### Batched Analyze Requests

#### Add a Finalizer

To fit the requirements of `ANALYZE` requests, the missing piece is that the current batch-request mechanism sends individual `StoreBatchTaskResponse` entries, leaving no opportunity to merge them.

This document proposes adding one extra step before TiKV sends the batched responses back to TiDB: a finalizer that is responsible for merging the sub-task responses into the main response.

The new [workflow](https://editor.plantuml.com/uml/pPRTRjf048NlUOfHRjggeW-GYoeaWLxqHmXGfLRSBFO0ewndtVqX9K_VsJK6EusgOg2ghmmMy-sPSsRmZVFA-b1ekE3Ly-GrmjGRY_rRvRCrvd1dIcioTCvO66dp28_arp15Iqh3y2TcmPDXK9p3Y5XfUbBfYoFHSF6hLqe7AQDIiOUh6TtVmk3BuDEtxyChK0u6SlRj4rw0qkpWdIPadu3LxNae7x-sA4FEIqCxJltXB9G9gXp6nkXKEjwSxii_iBusj_z-r_Phc8P5GHwMGUk-KqMfYLVWbNjmiDYApjNH8Uxj76r0FbWJohhOldZgf8jJraNBxvGXguNChKKNj8S2xIfgYHFfgZ6DPC3zh7RbrKcIvg4KDumGMDbjAtkJyTsSNnUAAi7mjjedQPtCqZdOMqBtfatPfrGuCLR2YEidYAvM-o9KUL3WgS0vHv6b24UxmQGS6wrmBK8AQxybOu8-M4urd_yMIhGTKl0d7NwDi8XjW6MrA8NGCS-LbfhmEPuTNCYY1HBelGIrrKRbRivo89FuoPVTuoCfrSgNKrU58ZElIndEy3nQnYfxKAl3GKl33cEMvzpEqQH-jilJNr5USZ-ggGt93yXEMWxkDyQpaJ8PZjC8hOaKncwnNSCzQjf8e39JB8X5YBmmU7Aoljt_kvRPU9atMgqbGSlp_9kyG9ar3BPW0pkWodRP-tZuIpkD4ZYNwh6gnHy1dN_b6Yit4yUzbKnxz-uKoaYH5dEhNIZkwZZiALx4EMqe3qhl4zOLrOlRFNbpKVTl12DljBy0) is as follows:

![](https://img.plantuml.biz/plantuml/png/pPRTRjf048NlUOfHRjggeW-GYoeaWLxqHmXGfLRSBFO0ewndtVqX9K_VsJK6EusgOg2ghmmMy-sPSsRmZVFA-b1ekE3Ly-GrmjGRY_rRvRCrvd1dIcioTCvO66dp28_arp15Iqh3y2TcmPDXK9p3Y5XfUbBfYoFHSF6hLqe7AQDIiOUh6TtVmk3BuDEtxyChK0u6SlRj4rw0qkpWdIPadu3LxNae7x-sA4FEIqCxJltXB9G9gXp6nkXKEjwSxii_iBusj_z-r_Phc8P5GHwMGUk-KqMfYLVWbNjmiDYApjNH8Uxj76r0FbWJohhOldZgf8jJraNBxvGXguNChKKNj8S2xIfgYHFfgZ6DPC3zh7RbrKcIvg4KDumGMDbjAtkJyTsSNnUAAi7mjjedQPtCqZdOMqBtfatPfrGuCLR2YEidYAvM-o9KUL3WgS0vHv6b24UxmQGS6wrmBK8AQxybOu8-M4urd_yMIhGTKl0d7NwDi8XjW6MrA8NGCS-LbfhmEPuTNCYY1HBelGIrrKRbRivo89FuoPVTuoCfrSgNKrU58ZElIndEy3nQnYfxKAl3GKl33cEMvzpEqQH-jilJNr5USZ-ggGt93yXEMWxkDyQpaJ8PZjC8hOaKncwnNSCzQjf8e39JB8X5YBmmU7Aoljt_kvRPU9atMgqbGSlp_9kyG9ar3BPW0pkWodRP-tZuIpkD4ZYNwh6gnHy1dN_b6Yit4yUzbKnxz-uKoaYH5dEhNIZkwZZiALx4EMqe3qhl4zOLrOlRFNbpKVTl12DljBy0)

1. During a full-sampling `ANALYZE`, TiDB divides the scan into Region tasks and groups the tasks that target the same TiKV store.
2. TiDB enables result merging and serial execution, then sends each group as a single unary coprocessor RPC, placing one task in the main request and the rest in `StoreBatchTask` entries.
3. TiKV runs the Region tasks **one at a time** in its read pool and keeps successful statistics in their mergeable, unserialized form.
4. Once all results are in, TiKV schedules the batch finalizer in the same read pool under the request's execution constraints. The finalizer merges the successful payloads into the main result and serializes it once. This deliberately trades a small, bounded increase in temporary memory for a simpler implementation: merging each result as it arrives would require a separate read-pool submission in the classic TiKV engine, while tests showed no significant TiKV memory pressure from buffering the batch.
5. TiKV returns the merged main response together with the corresponding `StoreBatchTaskResponse` entries. TiDB consumes the merged payload and handles failed or unmerged tasks as before.

The finalizer merges each successful, compatible child result into an error-free, mergeable main result. A child task that fails or produces a non-mergeable result remains a normal `StoreBatchTaskResponse`. If the main result is not mergeable or has an error, child results remain per-task responses. TiDB handles all unmerged results through the existing paths.

Finalizer-level failures, however, are atomic. If the finalizer cannot be scheduled, exceeds its deadline, or fails to serialize a main result that has already absorbed child results, TiKV returns no partial merged data or merge acknowledgments. The entire batch can then be retried safely without losing or double-counting any Region's result.

On the TiKV side, we extend the abstraction so that any request type can use the new mechanism, rather than adding special handling only for `ANALYZE`.

```rust
pub trait MergeableResult: Any + Send {
    fn merge(&mut self, other: Box<dyn MergeableResult>);
    fn into_data(self: Box<Self>) -> Result<Vec<u8>>;
}

/// A coprocessor response together with its response-data memory trace.
pub type TracedResponse = MemoryTraceGuard<coppb::Response>;

/// The output of handling a unary request. It always owns the response and
/// records separately whether its data is ready or still mergeable.
pub struct HandlerOutput {
    response: TracedResponse,
    state: HandlerOutputState,
}

/// Whether the response data is ready or still mergeable and unserialized.
enum HandlerOutputState {
    Ready,
    Mergeable(Box<dyn MergeableResult>),
}
```

A request type that wants to use this mechanism must produce batched results that implement the `MergeableResult` trait. Its handler then returns a `HandlerOutput`, which carries a tracked response together with a state. The state indicates whether the output is a finished result, ready to send as is, or a mergeable result that the finalizer will combine at the end.

#### Explicitly Opt-in

`ANALYZE` is not blocked during a rolling cluster upgrade, so a TiDB instance may send requests to TiKV instances running a different version. The compatibility concern is the wire format rather than whether the statistics are mathematically mergeable: an older TiDB expects each child result in its own [`StoreBatchTaskResponse.data`](https://github.com/pingcap/kvproto/blob/683dad8fa3689deb243f2ff8ab5847c97af53e38/proto/coprocessor.proto#L188-L192) and cannot infer that an empty child payload has been moved into the main response. TiKV must therefore use the merged response shape only when the request explicitly indicates that the client supports it.

```protobuf
message Request {
	...
  // Signals that the client supports merging results from the batched tasks in
  // `tasks` into `Response.data` instead of returning each result in its own
  // `StoreBatchTaskResponse.data`. For example, a batched analyze request may
  // merge per-region sampling results into one result.
  //
  // For every merged task, the store still adds a `StoreBatchTaskResponse`
  // with `data_merged_into_response` set. The store may return some or all task
  // results separately even when this field is set, so the client must handle
  // both merged and per-task results.
  bool allow_batch_task_data_merge = 18;
  // Asks the store to execute the primary task and all batched tasks one at a
  // time, without guaranteeing task order.
  bool execute_batch_tasks_serially = 19;
  ...
}
message Response {
	...
	// StoreBatchTaskResponse is the collection of batch task responses.
  repeated StoreBatchTaskResponse batch_responses = 13;
	...
}

message StoreBatchTaskResponse {
	...
	// Indicates that this task's result was merged into the enclosing
  // `Response.data`, so this message's `data` is empty. The store sets this
  // field only when the client enables `Request.allow_batch_task_data_merge`.
  //
  // This message still identifies the merged task by `task_id` and carries its
  // execution details.
  bool data_merged_into_response = 7;
	...
}
```

Therefore, we add `allow_batch_task_data_merge` and `execute_batch_tasks_serially` to the `Request` proto message. The first negotiates the merged response shape; the second independently requests serial task execution. TiDB sets both for batched full-sampling `ANALYZE`. An old TiDB sets neither, so a new TiKV retains the existing behavior.

For every child result merged into `Response.data`, TiKV retains the corresponding `StoreBatchTaskResponse`, leaves its `data` empty, and sets `data_merged_into_response`. The entry continues to carry the task ID and execution details. A failed or non-mergeable child remains a normal per-task response with the flag unset.

A response may therefore contain both merged and per-task results. TiDB consumes the merged data from the main response, skips the empty payloads marked as merged, and handles unmerged child responses through the existing path.

Both [TiUP](https://github.com/pingcap/tiup/blob/9f6ebb7edc26ca0ba53b9f4a70de22388f865910/pkg/cluster/spec/spec.go#L843-L873) and [TiDB Operator](https://docs.pingcap.com/tidb-in-kubernetes/stable/upgrade-a-tidb-cluster/#rolling-update-introduction) upgrade TiKV before TiDB by default; the following table summarizes the mixed-version scenarios relevant to this protocol, where "old" means a version without these fields and "new" means a version with them.

| TiDB sender | TiKV receiver | When it can occur | Protocol behavior | Why it is safe |
| --- | --- | --- | --- | --- |
| Old | New | While TiKV is being upgraded, after TiKV finishes but before TiDB starts, or from an old TiDB instance while TiDB is rolling | Both request fields default to `false`, so new TiKV retains the existing response and scheduling behavior. | Old TiDB receives the behavior it already understands. |
| New | Old | Not produced by the default TiUP or TiDB Operator upgrade order, but possible under the current TiDB Cloud release model, with pinned component versions, or during a manual upgrade | Old TiKV ignores both request fields, returns every child result separately, and retains the existing scheduling behavior. `data_merged_into_response` reads as `false`. | New TiDB supports the existing response shape and processes every child normally. |

#### Concurrency Control

`tidb_analyze_store_batch_size` is a dedicated `GLOBAL` and `SESSION` variable for full-sampling column `ANALYZE`. Its value is the maximum number of child Region tasks per RPC. It defaults to 4, accepts 0 to 8, and 0 disables batching. It is independent of `tidb_store_batch_size`.

Manual `ANALYZE` uses the session value; Auto Analyze uses the global value.

`tidb_analyze_distsql_scan_concurrency` remains the outer RPC concurrency. **TiDB requests serial execution, so each batched RPC has at most one active Region task and batching does not change this limit.**

## Test Design

### Functional Tests

- TiKV unit tests verify collector merging, serial execution, and finalizer failures.
- TiDB unit tests verify batch-request construction and merged/unmerged response handling.
- Real-TiKV integration tests compare batched and non-batched statistics on the same multi-Region table.

### Scenario Tests

- Cover multi-store tables, uneven Region distribution, Region cache expiry, Region split/move, server-busy responses, cancellation, and timeout.
- Verify that pre-dispatch Region-cache misses rebuild and regroup every original range exactly once. RPC-backed or partial errors retry only unresolved tasks and may use flat requests.

### Compatibility Tests

- **Upgrade and downgrade:** both mixed-version combinations use the existing per-task response format.

### Benchmark Tests

For each scan-concurrency setting, compare every serial batch-size configuration with an otherwise identical run in which batching is disabled. Record elapsed time, RPC count and latency, TiDB and TiKV CPU usage, network traffic, TiDB allocations and GC, RSS, and statement peak memory.

## Impacts & Risks

### Impacts

- The feature trades longer-lived RPCs for fewer RPCs, less network traffic, and substantially less TiDB allocation and GC work, while serial execution preserves Region-task concurrency.

### Risks

- Serial batching can increase RPC tail latency.
- A large batch can increase buffered memory and response size, especially for Bernoulli samples; `tidb_analyze_store_batch_size` remains the explicit bound.


## Investigation & Alternatives

- Reusing `tidb_store_batch_size` couples Analyze to generic store batching, so this design uses a dedicated variable.
- Concurrent child execution can shorten each RPC but multiplies Region-task concurrency, so Analyze requests serial execution.

## Unresolved Questions

- Should batched `ANALYZE` use a longer default RPC timeout than regular coprocessor requests?

## Future Possibility

None

## FAQ

- Does reducing cumulative allocation reduce statement peak memory by the same amount?

  No. It removes temporary decode and merge objects; the final statistics live set remains.

- What does batch size 4 mean, and why can RPC latency increase?

  One outer task can carry up to four child tasks, for five Regions per RPC. A new TiKV runs them one at a time, preserving scan concurrency but extending the RPC across all five tasks.
