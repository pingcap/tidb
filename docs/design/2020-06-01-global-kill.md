# Global Kill

- Author(s):     [pingyu](https://github.com/pingyu) (Ping Yu)
- Last updated:  2020-08-12
- Discussion at: https://github.com/pingcap/tidb/issues/8854

## Abstract

This document introduces the design of global connection id, and the global `KILL <connId>` based on it.

## Background

Currently connection ids are local to TiDB instances, which means that a `KILL x` must be directed to the correct instance, and can not safely be load balanced across the cluster, as discussed [here](https://github.com/pingcap/tidb/issues/8854).

## Proposal

To support "Global Kill", we need:
1. Global connection ids, which are unique among all TiDB instances.
2. Redirect `KILL x` to target TiDB instance, on which the connection `x` is running.
3. Support both 32 & 64 bits `connId`, to be compatible with legacy 32 bits clients. In this stage, we only design the 64 bits `connId`, and left a `markup` to distinguish between this two kinds.

## Rationale

#### 1. Structure of `connId`
##### 64 bits version
```
 63                   41 40                                   1    0    
+-----------------------+--------------------------------------+--------+
|       serverId        |             local connId             | markup |
|        (23b)          |                 (40b)                |(1b,==1)|
+-----------------------+--------------------------------------+--------+
```
##### 32 bits version
(To be discussed in another RFC)
```
                                  31                          1    0
                                 +-----------------------------+--------+
                                 |             ???             | markup |
                                 |             ???             |(1b,==0)|
                                 +-----------------------------+--------+
```

#### 2. markup
-  `markup == 0` indicates that the `connID` is just 32 bits long effectively, and high 32 bits should be all zeros. Compatible with legacy 32 bits clients.
-  `markup == 1` indicates that the `connID` is 64 bits long. Incompatible with legacy 32 bits clients.
-  `markup == 1` while __high 32 bits are zeros__, indicates that 32 bits truncation happens. See `Compatibility` section.


#### 3. serverId
`serverId` is selected RANDOMLY by each TiDB instance on startup, and the uniqueness is guaranteed by PD(etcd). `serverId` should be larger or equal to 1, to insure that high 32 bits of `connId` should always be non-zero, and make it possible to detect truncation.

On failure (e.g. fail to connect to PD, or all `serverId` are unavailable), we set `serverId` to `0`. And when `serverId == 0`, we deal with `KILL x` as in [early versions](https://pingcap.com/docs/stable/sql-statements/sql-statement-kill/).

`serverId` is kept by PD with a lease default to 1 hour. If TiDB is disconnected to PD longer than half of the lease, all connections are killed. On connection to PD restored, a new `serverId` is acquired.

On single TiDB instance without PD, a `serverId` of `1` is assigned.

#### 4. local connId
`local connId` is allocated by each TiDB instance on establishing connections incrementally.

Integer overflow is ignored at this stage, as `local connId` should be long enough.

#### 5. global kill
On processing `KILL x` command, first extract `serverId` from `x`. Then if `serverId` aims to a remote TiDB instance, get the address from cluster info, and redirect the command to it by "Coprocessor API" provided by the remote TiDB, along with the original user authentication.

## Compatibility

- Incompatible with legacy 32 bits clients. (According to some quick tests by now, MySQL client v8.0.19 supports `KILL` a connection with 64 bits `connId`, while `CTRL-C` does not, because it truncates the `connId` to 32 bits). A warning is set prompting that truncation happened, but user cannot see it, because `CTRL-C` is sent by a new connection in an instant.
