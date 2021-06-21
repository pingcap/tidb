# Global Kill

- Author(s):     [pingyu](https://github.com/pingyu) (Ping Yu)
- Last updated:  2021-05-05
- Discussion at: https://github.com/pingcap/tidb/issues/8854

## Abstract

This document introduces the design of global connection id, and the global `KILL <connID>` based on it.

## Background

Currently connection ids are local to TiDB instances, which means that a `KILL x` must be directed to the correct instance, and can not safely be load balanced across the cluster, as discussed [here](https://github.com/pingcap/tidb/issues/8854).

## Proposal

To support "Global Kill", we need:
1. Global connection ids, which are unique among all TiDB instances.
2. Redirect `KILL x` to target TiDB instance, on which the connection `x` is running.
3. Support both 32 & 64 bits `connID`. 32 bits `connID` is used on small clusters (number of TiDB instances less than 2048), to be fully compatible with all clients including legacy 32 bits ones, while 64 bits `connID` is used for big clusters. Bit 0 in `connID` is a markup to distinguish between these two kinds.

## Rationale

#### 1. Structure of `connID`
##### 32 bits
```
                                      31    21 20               1    0
                                     +--------+------------------+------+
                                     |serverID|   local connID   |markup|
                                     | (11b)  |       (20b)      |  =0  |
                                     +--------+------------------+------+
```

##### 64 bits
```
  63 62                 41 40                                   1    0    
 +--+---------------------+--------------------------------------+------+
 |  |      serverID       |             local connID             |markup|
 |=0|       (22b)         |                 (40b)                |  =1  |
 +--+---------------------+--------------------------------------+------+
```

##### Determine 32 or 64 bits
The key factor is `serverID` (see `serverID` section for detail), which depends on number of TiDB instances in cluster.
- Choose 32 bits when number of TiDB instances __less than 2048__. Otherwise choose 64 bits.
- When 32 bits chosen, upgrade to 64 bits when: 1) Fail to acquire `serverID` _(because of occupied)_ continuously __more than 3 times__ _(which will happen when size of cluster is increasing rapidly)_; 2) All `local connID` in 32 bits `connID` are in used (see `local connID` section for detail).
- When 64 bits chosen, downgrade to 32 bits in a gradually manner when cluster scales down from big to small, as TiDB instances keep `serverID` until next restart or lost connection to PD.

#### 2. Bit 63
Bit 63 is always __ZERO__, making `connID` in range of non-negative int64, to be more friendly to exists codes, and some languages don't have primitive type `uint64`.

#### 3. Markup
-  `markup == 0` indicates that the `connID` is just 32 bits long effectively, and high 32 bits should be all zeros. Compatible with legacy 32 bits clients.
-  `markup == 1` indicates that the `connID` is 64 bits long. Incompatible with legacy 32 bits clients.
-  `markup == 1` while __high 32 bits are zeros__, indicates that 32 bits truncation happens. See `Compatibility` section.

#### 4. ServerID
- `serverID` is selected RANDOMLY from `serverIDs pool`_(see next)_ by each TiDB instance on startup, and the uniqueness is guaranteed by PD (etcd). `serverID` should be larger or equal to 1, to ensure that high 32 bits of `connID` is always non-zero, and make it possible to detect truncation.

- `serverIDs pool` is:
  - All __UNUSED__ `serverIDs` within [1, 2047] acquired from [`CLUSTER_INFO`](https://docs.pingcap.com/tidb/stable/information-schema-cluster-info#cluster_info) when 32 bits `connID` chosen.
  - All `serverIDs` within [2048, 2^22-1] when 64 bits `connID` chosen.

- On failure (e.g. fail connecting to PD, or all `serverID` are unavailable when 64 bits `connID` chosen), we block any new connection.
- `serverID` is kept by PD with a lease (defaults to 12 hours, long enough to avoid brutally killing any long-run SQL). If TiDB is disconnected to PD longer than half of the lease (defaults to 6 hours), all connections are killed, and no new connection is accepted, to avoid running with stale/incorrect `serverID`. On connection to PD restored, a new `serverID` is acquired before accepting new connection.
- On single TiDB instance without PD, a `serverID` of `1` is assigned.

#### 5. Local connID
`local connID` is allocated by each TiDB instance on establishing connections:

- For 32 bits `connID`, `local connID` is possible to be integer-overflow and/or used up, especially on system being busy and/or with long running SQL. So we use a __lock-free queue__ to maintain available `local connID`, dequeue on client connecting, and enqueue on disconnecting. When `local connID` exhausted, upgrade to 64 bits.

- For 64 bits `connID`, allocate `local connID` by __auto-increment__. Besides, flip to zero if integer-overflow, and check `local connID` existed or not by [Server.clients](https://github.com/pingcap/tidb/blob/7e1533392030514440d27ba98001c374cdf8808f/server/server.go#L122) for correctness with trivial cost, as the conflict is very unlikely to happen (It needs more than 3 years to use up 2^40 `local connID` in a 1w TPS instance). At last, return _"Too many connections"_ error if exhausted.

#### 6. Global kill
On processing `KILL x` command, first extract `serverID` from `x`. Then if `serverID` aims to a remote TiDB instance, get the address from [`CLUSTER_INFO`](https://docs.pingcap.com/tidb/stable/information-schema-cluster-info#cluster_info), and redirect the command to it by "Coprocessor API" provided by the remote TiDB, along with the original user authentication.

#### 7. Summary
|      | 32 bits | 64 bits |
| ---- | ---- | ---- |
| ServerID pool size | 2^11 | 2^22 - 2^11 |
| ServerID allocation | Random of __Unused__ serverIDs acquired from PD within pool. Retry if unavailable. Upgrade to 64 bits while failed more than 3 times | Random of __All__ serverIDs within pool. Retry if unavailable |
| Local connID pool size | 2^20 | 2^40 |
| Local connID allocation | Using a queue to maintain and allocate available local connID. Upgrade to 64 bits while exhausted | Auto-increment within pool. Flip to zero when overflow. Return "Too many connections" if exhausted |



## Compatibility

- 32 bits `connID` is compatible to well-known clients.

- 64 bits `connID` is __incompatible__ with legacy 32 bits clients. (According to some quick tests by now, MySQL client v8.0.19 supports `KILL` a connection with 64 bits `connID`, while `CTRL-C` does not, as it truncates the `connID` to 32 bits). A warning is set prompting that truncation happened, but user cannot see it, because `CTRL-C` is sent by a new connection in an instant.

- [`KILL TIDB`](https://docs.pingcap.com/tidb/v4.0/sql-statement-kill) syntax and [`compatible-kill-query`](https://docs.pingcap.com/tidb/v4.0/tidb-configuration-file#compatible-kill-query) configuration item are deprecated.

## Test Design

### Prerequisite
Set `small_cluster_size_threshold` and `local_connid_pool_size` to small numbers (e.g. 4) by variable hacking, for easily switch between 32 and 64 bits `connID`.

### Scenario A. 32 bits `connID` with small cluster
1. A TiDB without PD, killed by Ctrl+C, and killed by KILL.
2. One TiDB with PD, killed by Ctrl+C, and killed by KILL.
3. Multiple TiDB nodes, killed {local,remote} by {Ctrl-C,KILL}.

### Scenario B. Upgrade from 32 to 64 bits `connID`
1. Upgrade caused by cluster scaled up from small to big.
2. Upgrade caused by `local connID` used up.

### Scenario C. 64 bits `connID` with big cluster
1. Multiple TiDB nodes, killed {local,remote} by {Ctrl-C,KILL}.

### Scenario D. Downgrade from 64 to 32 bits `connID`
1. Downgrade caused by cluster scaled down from big to small.

### Scenario E. Fault tolerant while disconnected with PD
1. Existing connections are killed after PD lost connection for long time.
2. New connections are not accepted after PD lost connection for long time.
3. New connections are accepted after PD lost connection for long time and then recovered.
4. Connections can be killed after PD lost connection for long time and then recovered.
