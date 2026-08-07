# Batch Requests for the `ANALYZE` Statement

- Author(s): [0xPoe](https://github.com/0xPoe)
- Discussion PR: https://github.com/pingcap/tidb/pull/70355
- Tracking Issue:

## Table of Contents

* [Introduction](#introduction)
* [Motivation](#motivation)
* [Detailed Design](#detailed-design)
    * [Current Implementation](#current-implementation)
    * [Batched Analyze Requests](#batched-analyze-requests)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)
* [Future Possibility](#future-possibility)
* [FAQ](#faq)

## Introduction

This document proposes reusing the existing batch-request mechanism in the coprocessor framework to pre-merge the statistics collected from TiKV, so that `ANALYZE` can reduce network traffic and memory usage on TiDB.

## Motivation

Large tables can contain thousands of regions. In the current statistics-collection process, TiDB sends a coprocessor task to each region, collects the partial statistics, and merges them locally.

The partial statistics collected from TiKV include row samples, a row count, null counts, total sizes, and FM sketches. Samples are simply raw row data, which makes them the most expensive part to transfer over the network and process on TiDB. There is little we can do about that, however, since TiDB needs the raw rows to build the final statistics. FMSketch is a different story: it is also large, but unlike samples, it can be merged before it ever leaves TiKV.

A single FMSketch, once serialized to Protobuf, takes at most 160 KiB. For a table with 2 billion rows spread across, say, 20,000 regions, transferring all of the sketches would cost up to 20,000 × 160 KiB ≈ 3 GiB of network traffic.

Since FMSketch is a mergeable data structure, nothing prevents us from merging sketches on TiKV before sending them back. This document therefore proposes reusing the existing batch-request mechanism in the coprocessor framework to merge sketches locally and return far fewer partial statistics to TiDB, saving both network traffic and memory on TiDB.

## Detailed Design

### Current Implementation

Before diving into the new design, let's take a look at the current batch-request implementation.

Currently, store-batched coprocessor requests are used for handle-based table reads. They combine eligible small Region tasks targeting the same TiKV store into a single unary coprocessor RPC.

The [workflow](https://editor.plantuml.com/uml/lPPDJiCm48NtESM85GY50tI14EL721PLMzW0Ysaoj5OTZsKxG6_Fn0qaaK3Gj7HMJYlvytjlnWcS-O0kb8M6Vwm4WWgQO5WwHoR09B2Zz1n3jg0SXcmTP-GzExZI_BO5PY-LW1NBLAOiYfQ3gReuXnkJq_iTy_BU7W3wvhcqkyIqhHfg9Lv6sdgv8ypjGmTpQNBBgWPzFkm6CoRCOSIiuzxLef-43cOlbNG2Ja_h10OmAMU52X1mfYbz8MbmMVkbXzbAvCuLcyqmTR8jmhLZGVe2jMwsZdRwQghwgMamdmcB538vi24e3RfLfoV6e-6JEInGcNW4E5vFT1pegVpm-7pqBUQhVGHKnIsGwWOevMeW5AjgX-AUIWosSWGvbrvjfYPsKjeHCwPGEXFbEFAb3c39zKZa9pKDQOJP4kS4qHrXMJQU04t-PBecZUl_7f__YcbrGKkFXD5m-bRJ0W9fzVO0BaYhL_5A_4fhlzOE-bwOlLG2XeCahUQB0FoonRNr2sQw8CZgbrf1sGADyeMa8XqxYsyt3y6XaN1SEDa2KurtphARw1AGcpTjZ2lDcFlUkxSmrS17u_wp4ZWJJlt1yG40) is as follows:

![](https://img.plantuml.biz/plantuml/png/lPPDJiCm48NtESM85GY50tI14EL721PLMzW0Ysaoj5OTZsKxG6_Fn0qaaK3Gj7HMJYlvytjlnWcS-O0kb8M6Vwm4WWgQO5WwHoR09B2Zz1n3jg0SXcmTP-GzExZI_BO5PY-LW1NBLAOiYfQ3gReuXnkJq_iTy_BU7W3wvhcqkyIqhHfg9Lv6sdgv8ypjGmTpQNBBgWPzFkm6CoRCOSIiuzxLef-43cOlbNG2Ja_h10OmAMU52X1mfYbz8MbmMVkbXzbAvCuLcyqmTR8jmhLZGVe2jMwsZdRwQghwgMamdmcB538vi24e3RfLfoV6e-6JEInGcNW4E5vFT1pegVpm-7pqBUQhVGHKnIsGwWOevMeW5AjgX-AUIWosSWGvbrvjfYPsKjeHCwPGEXFbEFAb3c39zKZa9pKDQOJP4kS4qHrXMJQU04t-PBecZUl_7f__YcbrGKkFXD5m-bRJ0W9fzVO0BaYhL_5A_4fhlzOE-bwOlLG2XeCahUQB0FoonRNr2sQw8CZgbrf1sGADyeMa8XqxYsyt3y6XaN1SEDa2KurtphARw1AGcpTjZ2lDcFlUkxSmrS17u_wp4ZWJJlt1yG40)

1. During the table-fetch phase of an `IndexLookUp` query, TiDB divides the handle reads into Region tasks and groups eligible small tasks that target the same TiKV store.

2. TiDB sends each group as a single unary coprocessor RPC, placing one task in the main request and the rest in `StoreBatchTask` entries.

3. TiKV schedules each Region task independently in its read pool and collects the results.

4. TiKV returns the main response together with the corresponding `StoreBatchTaskResponse` entries. TiDB then unpacks them into per-Region results. TiKV never merges the payloads.

The same mechanism can batch `ANALYZE` requests for multiple Regions on the same TiKV store. However, it must be extended to merge the individual statistics payloads on TiKV and return the combined payload in the main response.

### Batched Analyze Requests

#### Add a Finalizer

To fit the requirements of `ANALYZE` requests, the missing piece is that the current batch-request mechanism sends individual `StoreBatchTaskResponse` entries, leaving no opportunity to merge them.

This document proposes adding one extra step before TiKV sends the batched responses back to TiDB: a finalizer that is responsible for merging the sub-task responses into the main response.

The new [workflow](https://editor.plantuml.com/uml/jPRTRjf048NlUOfHRjggeW-GYog4G2_w8mIeKYjkrlPWHbdlklj3Ifw-isiRx3HjWW5VWH7ppPbpTkmRwuHnlf1mmMlWo4c4XTC6XxV2fHlCOAnBerEqLXkOIlq03-GsC2Kb93qAHreTIxb8xrr4oSEYb4gX75mjwUuMHjz6Ntxyd5o1i33YtdUFU05AUrXZIQXS15OVpqFv_Br3cQYir5HpcYzdeSnHP33JMiRIuKC_0MtAoHLUHZJc3Z_MOx-6XKyASh3sKWwjA4f9AWSdxBs5PCSwywD3FAWTRK4-6UtBfjY-U9oa3GUgZvQ_AHCb4ZwY1gsN3WeqUT2ovY2uJRZzowCzqluMfSaFZmnL1beXU2NChfEu-MoyKcyBGPLCU5yjZBWlbk465pE4zbsNiiycSqAOc17sYsSrAD9DUt90AiEIE-1AShTIWdifMJAQBKRDs1g2xL1YJ8STANDqtgWevkb_eKJJm___QGas964KiAvM91FwNRMMXl0rz0xMfy46Zl9UWjX9BJAxKjoGZlpeJVtpaPG56efTLuK2CsjAQPNstPbFA3EHtelyq6YfbKLWcrUgcwLvyrkTgLTHddC_g6aCoGx8NbeExhSEPYjbV41E8qHASogPKQ-qQxX3ILiELAgBX1GBuRS2HvONzmkJ-IUbTt-lHoZj-BnHlk44hS_p77c6icmPRC1uPO4gstdZu-5XTXh5yqxghDet5l0tfZw-1wNik3ESQMS_d4AP2f9iRlKGONTbEEmnNhmOQKkP5tAViBweGTZmqPjBNeCRnl1hwny0) is as follows:

![](https://img.plantuml.biz/plantuml/png/jPRTRjf048NlUOfHRjggeW-GYog4G2_w8mIeKYjkrlPWHbdlklj3Ifw-isiRx3HjWW5VWH7ppPbpTkmRwuHnlf1mmMlWo4c4XTC6XxV2fHlCOAnBerEqLXkOIlq03-GsC2Kb93qAHreTIxb8xrr4oSEYb4gX75mjwUuMHjz6Ntxyd5o1i33YtdUFU05AUrXZIQXS15OVpqFv_Br3cQYir5HpcYzdeSnHP33JMiRIuKC_0MtAoHLUHZJc3Z_MOx-6XKyASh3sKWwjA4f9AWSdxBs5PCSwywD3FAWTRK4-6UtBfjY-U9oa3GUgZvQ_AHCb4ZwY1gsN3WeqUT2ovY2uJRZzowCzqluMfSaFZmnL1beXU2NChfEu-MoyKcyBGPLCU5yjZBWlbk465pE4zbsNiiycSqAOc17sYsSrAD9DUt90AiEIE-1AShTIWdifMJAQBKRDs1g2xL1YJ8STANDqtgWevkb_eKJJm___QGas964KiAvM91FwNRMMXl0rz0xMfy46Zl9UWjX9BJAxKjoGZlpeJVtpaPG56efTLuK2CsjAQPNstPbFA3EHtelyq6YfbKLWcrUgcwLvyrkTgLTHddC_g6aCoGx8NbeExhSEPYjbV41E8qHASogPKQ-qQxX3ILiELAgBX1GBuRS2HvONzmkJ-IUbTt-lHoZj-BnHlk44hS_p77c6icmPRC1uPO4gstdZu-5XTXh5yqxghDet5l0tfZw-1wNik3ESQMS_d4AP2f9iRlKGONTbEEmnNhmOQKkP5tAViBweGTZmqPjBNeCRnl1hwny0)

1. During a full-sampling `ANALYZE`, TiDB divides the scan into Region tasks and groups the tasks that target the same TiKV store.
2. TiDB enables result merging and sends each group as a single unary coprocessor RPC, placing one task in the main request and the rest in `StoreBatchTask` entries.
3. TiKV schedules each Region task independently in its read pool and keeps the successful statistics in their mergeable, unserialized form.
4. Once all results are in, TiKV schedules the batch finalizer in the same read pool. The finalizer merges the successful payloads into the main result and serializes it once.
5. TiKV returns the merged main response together with the corresponding `StoreBatchTaskResponse` entries. TiDB consumes the merged payload and handles failed or unmerged tasks as before.

The finalizer merges each successful, compatible child result into an error-free, mergeable main result. A child task that fails or produces a non-mergeable result remains a normal `StoreBatchTaskResponse`. TiDB consumes successful unmerged results through the existing path and retries only failed tasks.

Finalizer-level failures, however, are atomic. If the finalizer cannot be scheduled, exceeds its deadline, or fails to serialize a main result that has already absorbed child results, TiKV returns no partial merged data or merge acknowledgments. TiDB can then safely retry the entire batch without losing or double-counting any Region's result.

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

`ANALYZE` is not blocked during a rolling cluster upgrade, so a TiDB instance may send requests to TiKV instances running a different version. The compatibility concern is the wire format rather than whether the statistics are mathematically mergeable: an older TiDB expects each child result in its own `StoreBatchTaskResponse.data` and cannot infer that an empty child payload has been moved into the main response. TiKV must therefore use the merged response shape only when the request explicitly indicates that the client supports it.

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

Therefore, we add a new field, `allow_batch_task_data_merge`, to the `Request` proto message. It signals that the client can handle merged batched task results, but it does not require TiKV to merge every result. During an upgrade, an old TiDB never sets this field, so TiKV retains the existing per-Region response shape and the old client never receives a merged result it cannot interpret.

For every child result merged into `Response.data`, TiKV retains the corresponding `StoreBatchTaskResponse`, leaves its `data` empty, and sets `data_merged_into_response`. The entry continues to carry the task ID and execution details. A failed or non-mergeable child remains a normal per-task response with the flag unset.

A response may therefore contain both merged and per-task results. TiDB consumes the merged data from the main response, skips the empty payloads marked as merged, and handles unmerged child responses through the existing path.

Both [TiUP](https://github.com/pingcap/tiup/blob/9f6ebb7edc26ca0ba53b9f4a70de22388f865910/pkg/cluster/spec/spec.go#L843-L873) and [TiDB Operator](https://docs.pingcap.com/tidb-in-kubernetes/stable/upgrade-a-tidb-cluster/#rolling-update-introduction) upgrade TiKV before TiDB by default; the following table summarizes the mixed-version scenarios relevant to this protocol, where "old" means a version without the two negotiation fields and "new" means a version with them.

| TiDB sender | TiKV receiver | When it can occur | Protocol behavior | Why it is safe |
| --- | --- | --- | --- | --- |
| Old | New | While TiKV is being upgraded, after TiKV finishes but before TiDB starts, or from an old TiDB instance while TiDB is rolling | `allow_batch_task_data_merge` is absent and defaults to `false`. New TiKV therefore returns every child result separately and does not set `data_merged_into_response`. | Old TiDB receives the response shape it already understands. |
| New | Old | Not produced by the default TiUP or TiDB Operator upgrade order, but possible under the current TiDB Cloud release model, with pinned component versions, or during a manual upgrade | Old TiKV ignores the unknown `allow_batch_task_data_merge` field and returns every child result separately. `data_merged_into_response` is absent and reads as `false`. | New TiDB treats every child response as unmerged and processes it through the existing path. |

#### Concurrency Control





## Test Design

### Functional Tests

### Scenario Tests

### Compatibility Tests

### Benchmark Tests

## Impacts & Risks



## Investigation & Alternatives



## Unresolved Questions



## Future Possibility



## FAQ

-
