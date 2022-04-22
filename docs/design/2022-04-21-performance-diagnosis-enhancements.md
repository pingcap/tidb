- Author(s): [cfzjywxk](https://github.com/cfzjywxk), [you06](https://github.com/you06)
- Discussion PR:
- Tracking Issue: #33963, #34106

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Impacts & Risks](#impacts--risks)

## Introduction

To enhance the diagnosis and tuning of TiDB, this RPC proposes some optimization of metrics, including the supplement of metrics, better display in Grafana.

## Motivation or Background

TiDB collects many metrics, but few of them are frequently used and the metrics of the critical paths are in different panels, which makes the diagnosis a problem. Furthermore, some latency events are not recorded in critical paths.

This RPC proposes some improvements to the metrics collection, and display.

## Detailed Design

### Differences Between Internal and External Requests

The tracking issue of it is #33963, and it should be applied to most of the metrics.

#### Internal Transaction Usage

The main goal of this task is to differentiate the internal and external transactions and mark the snapshots and transactions as internal for internal sessions and tasks.

Some tasks are not running in the internal sessions, however, they are executed directly by `RunInNewTxn` :

- DDL Backfill
- Auto-ID management
- Version check when bootstrapping
- GC placement and label rules
- Load data for cached tables
- Alloc global ID for some operations
    - Write sequence update to binlog
    - Create temporary table

When TiDB bootstrapping, it’ll create some internal sessions, these sessions are used for:

- Bind info read/write
- Privilege read/write
- System variable read/write
- Telemetry report
- Statistic load

The coprocessor request, transaction, and snapshot created by `RunInNewTxn` and the internal sessions need to be marked as internal.

#### Session

Add a flag in `SessionVars`, TiDB creates some internal sessions when bootstrap, and the internal flags are set after bootstrap.

```diff
type SessionVars struct {
	...
+	Internal bool
}
```

#### Coprocessor

```diff
// Request represents a kv request.
type Request struct {
	...
+	Internal bool
}
```

#### KV Client

Add internal flag in transaction and snapshot.

```diff
// KVTxn contains methods to interact with a TiKV transaction.
type KVTxn struct {
	...
+	Internal bool
}
```

```diff
// KVSnapshot implements the tidbkv.Snapshot interface.
type KVSnapshot struct {
	...
+	Internal bool
}
```

#### kvproto

```diff
// Miscellaneous metadata attached to most requests.
message Context {
    reserved 4;
    ...
+   // whether it's from internal request.
+   bool internal = xx;
}
```

The default value of `internal` is `false`(from the user client), and we should mark TiDB’s internal transaction as `true`.

#### Metrics in TiDB

After the internal and external transactions are differentiated, we can attach the flag to metrics of requests and KV client.

Metrics need to be collected in both internal and external ways:

- The metrics in read requests
    - Get
    - BatchGet
    - Coprocessor
    - Scan
    - UnionScan
- The 2PC related metrics
    - Pessimistic Lock
    - Prewrite
    - Commit

Metrics collected for external transactions only:

- UnionScan
- Resolve Lock(but need to be differentiated by read and write)
- Region Cache

Metrics collected together:

- Batch System
- gRPC

### Metrics Enhancement

Since #34106 already listed the metrics to be collected, this RFC only designs the panels where the metrics need to be displayed.

#### Performance Overview Dashboard

- The metrics in common path
- Retry duration(all requests except the last successful one)
    - Get
    - BatchGet
    - Scan
    - Coprocessor
    - Pessimistic Lock
    - Prewrite
    - Commit
- Wait duration
    - Coprocessor
    - Pessimistic Lock
    - Prewrite
    - Commit
- Keys count and KV size
    - Get
    - BatchGet
    - Scan
    - Coprocessor
    - Pessimistic Lock
    - Prewrite
    - Commit
- Region Cache
- TSO by types
    - Sync
    - Async

#### Batch Client Panel

- Batch System
- gRPC

## Impacts & Risks

By collecting more metrics, more traffics will be consumed. We also need to reduce some unused metrics, but it’s not the goal of this RFC.
