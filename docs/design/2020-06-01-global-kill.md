# Global Kill

- Author(s):     [pingyu](https://github.com/pingyu) (Ping Yu)
- Last updated:  2020-06-01
- Discussion at: https://github.com/pingcap/tidb/issues/8854

## Abstract

This Document introduces the design of global connection id, and the global `KILL <connId>` based on it.

## Background

Currently connection ids are local to TiDB instances, which means that a `KILL x` must be directed to the correct instance, and can not safely be load balanced across the cluster, as discussed [here](https://github.com/pingcap/tidb/issues/8854).

## Proposal

To support "Global Kill", we need:
1. Global connection ids, which are unique amount all TiDB instances.
2. Redirect the `KILL x` to the proper TiDB instance, on which the connection `x` is running.


## Rationale

1. Assign a `server_id` to each TiDB instance on startup by PD.
2. Extend connection id to 64 bits, which is composed by `server_id` in high-order 32 bits, and local connection id in low-order 32 bits which is allocated by each TiDB instance itself.
3. On processing `KILL x` command, first extract `server_id` from `x`. If `server_id` aims to a remote TiDB instance, get address from cluster info, and then redirect the command to it by "MySQL API", along with the original user authentication.

## Compatibility and Mirgration Plan

None

