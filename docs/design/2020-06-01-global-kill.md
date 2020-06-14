# Global Kill

- Author(s):     [pingyu](https://github.com/pingyu) (Ping Yu)
- Last updated:  2020-06-15
- Discussion at: https://github.com/pingcap/tidb/issues/8854

## Abstract

This document introduces the design of global connection id, and the global `KILL <connId>` based on it.

## Background

Currently connection ids are local to TiDB instances, which means that a `KILL x` must be directed to the correct instance, and can not safely be load balanced across the cluster, as discussed [here](https://github.com/pingcap/tidb/issues/8854).

## Proposal

To support "Global Kill", we need:
1. Global connection ids, which are unique amount all TiDB instances.
2. Redirect the `KILL x` to the target TiDB instance, on which the connection `x` is running.
3. Make best effort to support legacy 32 bits clients, as well as prevent killing a wrong connection. So we design global connection id refering to [variable-width encoding](https://en.wikipedia.org/wiki/Variable-width_encoding), along with detection of truncation.

## Rationale

#### 1. Structure of `connId`
```
 63                        40 39           32    31    30          24 23                         0
+----------------------------+---------------+--------+--------------+----------------------------+
|          reserved          |    serverId   | markup |   serverId   |        local connId        |
|            (24b)           |    (high 8b)  |  (1b)  |   (low 7b)   |            (24b)           |
+----------------------------+---------------+--------+--------------+----------------------------+
```
#### 2. markup
-  `markup == 0` indicates that the `connID` is just 32 bits long effectively, and high 32 bits should be all zeros. Compatible with legacy 32 bits clients. Happens in small clusters, i.e, number of TiDB instances less than 128.
-  `markup == 1` indicates that the `connID` is 64 bits long. Incompatible with legacy 32 bits clients. Happens in big clusters.
-  `markup == 1` while __high 32 bits are zeros__, indicates that 32 bits truncation happens. See `Compatibility` section.


#### 3. serverId
`serverId` is allocated to each TiDB instance on startup by PD, begin with 1.

TiDB instance should store `serverId` locally, to prevent allocated a new `serverId` on every restart.

When all the 15 bits (32768) `serverId` is used up, TiDB instance will get a `serverId` of `0`, and "Global Kill" be disabled.

On single TiDB instance without PD, a `serverId` of `0` is allocated.

When `serverId == 0`, we deal with `KILL x` as in  [early versions](https://pingcap.com/docs/stable/sql-statements/sql-statement-kill/).


#### 4. local connId
`local connId` is allocated by each TiDB instance on establishing connections.

To prevent used up, `local connId` is recycled and reused.

#### 5. global kill
On processing `KILL x` command, first extract `serverId` from `x`. If `serverId` aims to a remote TiDB instance, get address from cluster info, and then redirect the command to it by "MySQL API", along with the original user authentication.

## Compatibility

- For small clusters (number of TiDB instances adds up to less than 128), `connId` is 32 bits long effectively, and compatible to legacy 32 bits clients.

- For big clusters (number of TiDB instances adds up to not less than 128), `connId` is 64 bits long. TiDB can probe the happening of 32 bits truncation, and deal with a truncated `connId` as [what we do](https://pingcap.com/docs/stable/sql-statements/sql-statement-kill/) in early versions, i.e, run on current instance with `local connId` if `compatible-kill-query = true`, otherwise reject it to prevent killing a wrong connection.

