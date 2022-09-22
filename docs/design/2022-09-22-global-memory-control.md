# Proposal: Global Memory Control

* Authors: [wshwsh12](https://github.com/wshwsh12), [Xuhuaiyu](https://github.com/Xuhuaiyu)
* Tracking issue: [#37816](https://github.com/pingcap/tidb/issues/37816)

## Abstract

This proposes a design of how to control global memory of TiDB instance.

## Background

Currently, TiDB has a query-level memory control strategy `mem-quota-query`, which triggers Cancel when the memory usage of a single SQL exceeds `mem-quota-query`. However, there is currently no global memory control strategy. 

When TiDB has multiple SQLs whose memory usage does not exceed `mem-quota-query` or memory tracking inaccurate, it will lead to high memory usage or even OOM. 

Therefore, we need an observer to check whether the memory usage of the current system is normal. When there are some problems, try to control TiDB memory not to continue to grow, to reduce the risk of process crashes.

## Goal

- Control the TiDB execution memory within the `tidb_server_memory_limit`

## Design

We need to implement the following three functions to control the memory usage of TiDB:
1. Kill the SQL with the most memory usage in the current system, when `HeapInuse` is larger than `tidb_server_memory_limit`.
2. Take the initiative to trigger `runtime.GC()`, when `HeapInuse` is large than `tidb_server_memory_limit`*`tidb_server_memory_limit_gc_trigger`.
3. Introduce some memory tables to observe the memory status of the current system.

### Kill the SQL with the most memory usage

New variables:
1. Global variable `MemUsageTop1Tracker atomic.Pointer[Tracker]`: Indicates the Tracker with the largest memory usage.
2. The flag `NeedKill atomic.Bool` in the structure `Tracker`: Indicates whether the SQL for the current Tracker needs to be Killed.
3. `SessionID int64` in Structure Tracker: Indicates the Session ID corresponding to the current Tracker.

Implements:

#### How to get the current TiDB memory usage Top 1
When Tracker Consume, judge the following logic. If all are satisfied, update `MemUsageTop1Tracker`.
1. Is it a Session-level Tracker?
2. Whether the flag `NeedKill` is false, that is, the current SQL has not been Canceled
3. Whether the memory usage exceeds the threshold `tidb_server_memory_limit_sess_min_size`(default 128MB, can be dynamically adjusted) of the lowest entry Top 1 
4. Is the memory usage of the current Tracker greater than the current Top 1

#### How to Cancel the current top 1 memory usage and recycle memory usage in time
1. Create a coroutine that calls Golang's `ReadMemStat` interface in a 200 ms cycle. (Get the memory usage of the current TiDB instance)
2. If the heapInUse of the current instance is greater than `tidb_server_memory_limit`, set `MemUsageTop1Tracker`'s `NeedKill` flag to true. (Sends a Kill signal)
3. When the SQL call to Tracker Consume, check its own `NeedKill` flag. If it is true, trigger Panic and exit. (terminates the execution of SQL)
4. Get the SessionID from Tracker and continuously query its status, waiting for it to complete Cancel. When SQL successfully Cancels, explicitly trigger Golang GC to release memory. (Wait for SQL Cancel completely and release memory)

### Take the initiative to trigger GC

Design your own control logic according to the idea of uber-go-gc-tuner:
1. Use the Go1.19 `SetMemoryLimit` feature to set the soft limit to `tidb_server_memory_limit` * `tidb_server_memory_limit_gc_trigger` to ensure that GC can be triggered after reaching a certain threshold
2. After each GC, determine whether the GC is caused by memory_limit. If it is caused by this, temporarily set SetMemoryLimit to infinite, and then set it back to the specified threshold after 1 minute. In this way, the problem of frequent GC caused by heapInUse being larger than the soft limit can be avoided.

### Introduce some memory tables

Introduce `performance_schema.memory_usage` and `performance_schema.memory_usage_ops_history` to display the current system memory usage and historical operations.
This can be achieved by maintaining a set of global data, and reading and outputting directly from the global data when querying.

## 
