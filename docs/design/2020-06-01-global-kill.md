# Global Kill

- Author(s):     [pingyu](https://github.com/pingyu) (Ping Yu)
- Last updated:  2020-06-04
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

1. Assign a `server_id` to each TiDB instance on startup by PD, start from 1.
2. Extend connection id to 64 bits, which is composed by `server_id` in high-order 32 bits, and local connection id in low-order 32 bits which is allocated by each TiDB instance itself.
3. On processing `KILL x` command, first extract `server_id` from `x`. If `server_id` aims to a remote TiDB instance, get address from cluster info, and then redirect the command to it by "MySQL API", along with the original user authentication.
4. As some clients would send `KILL x` command with truncated 32 bits connection ids, if `server_id` equals `0`, we should deal with this circumstance as [what we do](https://pingcap.com/docs/stable/sql-statements/sql-statement-kill/) in older versions, i.e, run on current instance if `compatible-kill-query = true`, otherwise, reject it.

## Compatibility

1. Extend connection ids from 32 to 64 bits would break some clients. Main current clients need to be checked for compatibility.

2. Some clients would probably send `KILL` command with 32 bits connection ids. We should deal with this circumstance. (Ref. [1](https://github.com/pingcap/tidb/issues/8854#issuecomment-637217000) [2](https://github.com/pingcap/tidb/issues/8854#issuecomment-637237183))

3. MySQL had has 64 bits connection ids since 5.6.9 (2012-12-11), as this [article](https://dev.mysql.com/doc/relnotes/mysql/5.6/en/news-5-6-9.html) said. (Ref. [3](https://github.com/pingcap/tidb/issues/8854#issuecomment-636602132))

