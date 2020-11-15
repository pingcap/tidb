# Global Kill

- Author(s):     [pingyu](https://github.com/pingyu) (Ping Yu)
- Last updated:  2020-11-15
- Discussion at: https://github.com/pingcap/tidb/issues/8854

## Abstract

This document introduces the design of global connection id, and the global `KILL <connId>` based on it.

## Background

Currently connection ids are local to TiDB instances, which means that a `KILL x` must be directed to the correct instance, and can not safely be load balanced across the cluster, as discussed [here](https://github.com/pingcap/tidb/issues/8854).

## Proposal

To support "Global Kill", we need:
1. Global connection ids, which are unique among all TiDB instances.
2. Redirect `KILL x` to target TiDB instance, on which the connection `x` is running.
3. Support both 32 & 64 bits `connId`. 32 bits `connId` is used on small clusters (number of TiDB instances less than 2048), to be fully compatible with all clients including legacy 32 bits ones, while 64 bits `connId` is used for big clusters. Bit 0 in `connId` is a markup to distinguish between these two kinds.

## Rationale

#### 1. Structure of `connId`
##### 32 bits
```
                                      31    21 20               1    0
                                     +--------+------------------+------+
                                     |serverId|   local connId   |markup|
                                     | (11b)  |       (20b)      |  =0  |
                                     +--------+------------------+------+
```

##### 64 bits
```
  63 62                 41 40                                   1    0    
 +--+---------------------+--------------------------------------+------+
 |  |      serverId       |             local connId             |markup|
 |=0|       (22b)         |                 (40b)                |  =1  |
 +--+---------------------+--------------------------------------+------+
```

##### Determine 32 or 64 bits
The key factor is `serverId` (see `serverId` section for detail), which depends on number of TiDB instances in cluster.
- Choose 32 bits when number of TiDB instances __less than 2048__. Otherwise choose 64 bits.
- When 32 bits chosen, upgrade to 64 bits when: 1) Fail to acquire `serverId` _(because of occupied)_ continuously __more than 3 times__ _(which will happen when size of cluster is increasing rapidly)_; 2) All `local connId` in 32 bits `connId` are in used (see `local connId` section for detail).
- When 64 bits chosen, downgrade to 32 bits in a gradually manner when cluster scales down from big to small, as TiDB instances keep `serverId` until next restart or lost connection to PD.

#### 2. bit 63
Bit 63 is always __ZERO__, making `connId` in range of non-negative int64, to be more friendly to exists codes, and some languages don't have primitive type `uint64`.

#### 3. markup
-  `markup == 0` indicates that the `connID` is just 32 bits long effectively, and high 32 bits should be all zeros. Compatible with legacy 32 bits clients.
-  `markup == 1` indicates that the `connID` is 64 bits long. Incompatible with legacy 32 bits clients.
-  `markup == 1` while __high 32 bits are zeros__, indicates that 32 bits truncation happens. See `Compatibility` section.

#### 4. serverId
- `serverId` is selected RANDOMLY from `serverIds pool`_(see next)_ by each TiDB instance on startup, and the uniqueness is guaranteed by PD (etcd). `serverId` should be larger or equal to 1, to ensure that high 32 bits of `connId` is always non-zero, and make it possible to detect truncation.

- `serverIds pool` is:
  - All __UNUSED__ `serverIds` within [1, 2047] acquired from [`CLUSTER_INFO`](https://docs.pingcap.com/tidb/stable/information-schema-cluster-info#cluster_info) when 32 bits `connId` chosen.
  - All `serverIds` within [2048, 2^22-1] when 64 bits `connId` chosen.

- On failure (e.g. fail connecting to PD, or all `serverId` are unavailable when 64 bits `connId` chosen), we block any new connection.
- `serverId` is kept by PD with a lease (defaults to 12 hours, long enough to avoid brutally killing any long-run SQL). If TiDB is disconnected to PD longer than half of the lease (defaults to 6 hours), all connections are killed, and no new connection is accepted, to avoid running with stale/incorrect `serverId`. On connection to PD restored, a new `serverId` is acquired before accepting new connection.
- On single TiDB instance without PD, a `serverId` of `1` is assigned.

#### 5. local connId
`local connId` is allocated by each TiDB instance on establishing connections auto-incrementally.

On system being busy and/or with long running SQL, `local connId` is possible to be integer-overflow and/or used up, especially on 32 bits `connId`. So we will check `local connId` existed or not before allocation, upgrade to 64 bits if `local connId` exhausted in 32 bits `connId`, or return _"too many connections"_ error in 64 bits `connId`.

#### 6. global kill
On processing `KILL x` command, first extract `serverId` from `x`. Then if `serverId` aims to a remote TiDB instance, get the address from [`CLUSTER_INFO`](https://docs.pingcap.com/tidb/stable/information-schema-cluster-info#cluster_info), and redirect the command to it by "Coprocessor API" provided by the remote TiDB, along with the original user authentication.

## Compatibility

- 32 bits `connId` is compatible to well-known clients.

- 64 bits `connId` is __incompatible__ with legacy 32 bits clients. (According to some quick tests by now, MySQL client v8.0.19 supports `KILL` a connection with 64 bits `connId`, while `CTRL-C` does not, as it truncates the `connId` to 32 bits). A warning is set prompting that truncation happened, but user cannot see it, because `CTRL-C` is sent by a new connection in an instant.

- [`KILL TIDB`](https://docs.pingcap.com/tidb/v4.0/sql-statement-kill) syntax and [`compatible-kill-query`](https://docs.pingcap.com/tidb/v4.0/tidb-configuration-file#compatible-kill-query) configuration item are deprecated.

