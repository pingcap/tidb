# Proposal: Support Global Read Only Mode

- Author(s):    [ichn-hu](https://github.com/ichn-hu)
- Last updated:  Oct. 09, 2020
- Discussion at:  

## Abstract

This document describes the design of the `read_only` global variable that once turned on blocks non-super users from updating the TiDB cluster.


## Motivation or Background

As requested and described in [this issue](https://github.com/pingcap/tidb/issues/18426), such a variable can be beneficial to maintenance, migration and replication, and according to a [blog post](https://pingcap.com/blog/heterogeneous-database-replication-to-tidb#creating-a-read-only-or-archive-database), TiDB can be used as a read-only or archive database, in which case we would like to make the cluster running in a read only mode, while only allowing CDC tools to update the data.


## Detailed Design

Based on the issue description and [MySQL’s description](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_read_only), the `read_only` option of TiDB will act similarly.


### Behavior Description


*   `read_only` is disabled by default.
*   The only way to enable `read_only` mode is to execute `set global read_only = on` with a SuperPriv user.
    *   The enabling attempt will block until the whole cluster enters the `read_only` mode.
    *   And only `set global read_only = off` to turn off the mode.
    *   The `read_only` global variable will have 3 values, the reason for such design is to make the status more intuitive, see section technical issues 4 for a detailed explanation.
        *   off, when the cluster acts in a normal mode
        *   enabling, when the turning on process is going on
        *   on, when the whole cluster is in read only mode
    *   `set global read_only = on` upon return
        *   if there are warnings, meaning there are still some TiDB servers in the cluster that are possibly not read only, the user should check if these TiDB servers are down or have unreleased write lock or have large transactions committing.
            *   The warnings contain all known TiDB servers that have not acknowledged the read only mode.
            *   If the user examines the `read_only` global variable, it will be `enabling`, not `on`.
            *   She could rerun `set global read_only = on` to wait for another period of time (default to 10 seconds, could have exponential backoff behavior).
            *   If some of the TiDB servers reported in the warnings are indeed down, the user should remove them and rerun the enabling command, and expect an immediate return of success.
        *   if there is another session running `set global read_only = off` while waiting for enabling, an error will be returned, marking the fail of the attempt. \
ERROR 1227 (42000):Enabling failed; the enabling attempt is interrupted by another session setting back the read only mode to off.
        *   if there are no warnings or errors, the cluster is then read only. \

*   `read_only` once enabled, all users without SuperPriv will not be able to execute SQL queries that might update the data to the whole cluster (currently not support table level `read_only`, if you want such behavior, use table lock instead).
*   Only users with `SuperPriv` could enable or disable this option, otherwise the following error will be returned. This behavior is implied by the fact that setting global variables requires SUPER privilege level. \
ERROR 1227 (42000): Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation
*   When enabled, writes (updates) from client will not be allowed, and an error will be returned \
ERROR 1290 (HY000): The MySQL server is running with the --read-only option so it cannot execute this statement
    *   Even if the user’s query actually did not update any value, the query might still be denied (tested with MySQL), such as \
insert into t (select * from an_empty_table)
    *   This implies that we could just block the SQL by its type, namely the following kinds of query will be banned if read only is turned on:
        *   INSERT
        *   UPDATE
        *   SELECT * FOR UPDATE
        *   LOCK TABLE FOR WRITE
        *   All DDL
        *   EXPLAIN ANALYZE where the analyzed SQL might update the data.
    *   All other SQL will be allowed and will be executed just as normal.
*   Users with `SuperPriv` privilege level are not affected by the read only mode; they can still write data to the cluster.
*   User identified as a "replication secondary" is also freed from the restraint of this option
    *   A new privilege level called `ReplicationWrite` will be added, in order to allow data migration tools like TiCDC to write data even if the cluster is read only.
    *   Because MySQL’s replication applier is inside the MySQL server, therefore it does not need such a privilege level. For TiCDC and tools alike, the SQL interface is used to apply data, therefore a new privilege level would be a good idea to allow such change.
*   Once we support [temporary tables](https://github.com/pingcap/tidb/issues/1248) in the future, updates to temporary tables should also be allowed, but we could ignore this behavior for now, this is described in [MySQL’s documentation](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_read_only) as well.


### Enabling Attempt

As with MySQL, the read_only mode can be enabled at start using a command option or interactively without a restart. In our case, we might just make it a global variable, and it should be set directly from the SQL interface. We only allow `SET GLOBAL read_only=true` to enable the read only mode, there is no other way to enable it, and it can’t be set from a session level.


#### Case 1, clients have transactions committing

| server 1            | server 2   |
| ---                 | ---        |
| client 1            | client 2   |
|                     | begin tx   |
|                     | working    |
|                     | working    |
|                     | committing |
| turn on read_only   | committing |
| block               | committing |
| block               | committed  |
| read_only turned on |            |

If there are transactions that are committing (i.e. started to commit before enabling attempt), the enabling attempt will be blocked until the commit is finished. This behavior is inline with MySQL’s, according to [MySQL’s documentation](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_read_only). If a transaction has not reached commit point, once the enabling attempt is initiated, the transaction will fail at commit point.


#### Case 2, user who attempts to turn on `read_only` has write lock held


| server 1                              | server 1                              |
| ---                                   | ---                                   |
| user 1 (with SuperPriv), connection 1 | user 1 (with SuperPriv), connection 2 |
| lock table write                      |                                       |
|                                       | turn on read_only                     |
|                                       | read_only turned on                   |
| unlock                                |                                       |


This case is about the same user having more 2 connections (or sessions), and held a write lock in connection 1, and is attempting to enable read only mode in another connection. Note on [MySQL’s doc](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_read_only):

*   The attempt fails and an error occurs if you have any explicit locks (acquired with [LOCK TABLES](https://dev.mysql.com/doc/refman/5.7/en/lock-tables.html)) or have a pending transaction.

If the user herself has an ongoing transaction or table locked for writing, the attempt should fail. However, the client should already have been a SUPER user (otherwise she can’t turn on read only), therefore updating is not affected by turning on `read_only`, **so it is not blocked nor will fail anyway**. This behavior is verified on MySQL 8.0.

#### Case 3, other users have held a write lock

| server 1            | server 2                   |
| ---                 | ---                        |
| client 1            | client 2                   |
|                     | lock table for write       |
| turn on read_only   |                            |
| block               |                            |
| block               | unlock table               |
| read_only turned on |                            |
|                     | lock table for write       |
|                     | error, read_only turned on |

In this case, 2 users are involved, one has a table locked for write, and the other wants to turn on the read only mode, the attempt will be blocked until the write lock is released.


#### Case 4, attempt blocked due to held write lock on t1, while other client update on t2

MySQL's behavior:

| server 1            | server 1            | server 1       |
| ---                 | ---                 | ---            |
| client 1            | client 2            | client 3       |
|                     | lock table t1 write |                |
| turn on read_only   |                     |                |
| block               |                     | insert into t2 |
| block               | unlock table        | block          |
| read_only turned on |                     | insert ok      |

TiDB's behavior (designed):

| server 1            | server 2            | server 3         |
| ---                 | ---                 | ---              |
| client 1            | client 2            | client 3         |
|                     | lock table t1 write |                  |
| turn on read_only   |                     |                  |
| block               |                     | insert into t2   |
| block               | unlock table        | fail immediately |
| read_only turned on |                     |                  |

In this case, MySQL will also block the update, and once the table is unlocked, both update and the enabling attempt will succeed.

However, it would be too difficult to achieve this in a distributed environment, therefore the proposed solution is to fail the update immediately, since the enabling attempt will eventually succeed, it doesn’t matter if we act like the update comes after the enabling of the `read_only` mode.

In a sentence, attempts to turn on `read_only` will never fail, it will block until committing transactions and write locks held from/by unprivileged users before the attempt on all TiDB servers are committed or released, and TiDB server enters a `read_only` mode as long as it receives the update of the global variable, hence no blockage will be caused by turning on `read_only`, queries either fail immediately or get processed.


### Technical Issues


#### How to identify update queries?

As stated above, we will look into the user’s query statement, and if it might change data, then it will be denied. According to the implementation of `lock table for read`, we could detect updates by checking the privilege information in the visitInfo of the query during optimization similarly.


#### How to notify other servers about the enabling of the mode

We can make use of the global variable system table, and the TiDB server could just check the global variable to know if the read only mode is turned on. However, the global variable will only refresh for new connections, to surpass this restriction, for each session, before the execution of a new SQL that might update data, the session should check the `read_only` global variable in the global variable cache. Note that there is at most 2 seconds delay because the system table is cached on each TiDB server, therefore the enabling attempts will wait (block) at least 2 seconds.


#### How do we know that the cluster is already read_only?

Setting the global variable does not mean the cluster is already read only, therefore the `set global read_only = on` will block until the following conditions are met.

If we call the enabler server A, and the rest servers in the cluster B, C, D... etc, A does not know if B, C, D have already flushed their global variable cache, therefore we need another mechanism to ensure that all other servers has acknowledged about the the enabling of the read only mode.

The idea is basically to introduce a new system table (**server_read_only_status, short as SROS in the following paragraph**) that records the status of each running session or TiDB server instance. Each instance or session should insert (or call it “report”) a record into the system table with acknowledgement about the enabling attempt and a timestamp of the last committed transaction before the read only mode is turned on on the instance or session.

The enabling session should actively poll the system table to check if all instances or sessions have acknowledged the enabling attempt. Once it is confirmed, the `set global read_only = on` will return, and the cluster will be guaranteed that it has entered the read only mode.

If the enabling session is disconnected while waiting for the enabling result, the `read_only` mode will continue to take effect, because the enabling session will only poll the system table after it setted the global variable. And the `read_only` global variable will be `enabling`, not `on`. This behavior is the same when the enabling session is forced to terminate (such as the user hits Ctrl+C to cancel), the user only cancels the polling, and the turning on would not be canceled (unless the user hit the Ctrl+C really fast and the global variable is not written yet, which the value of `read_only` will still be `off`). To make sure the cluster enters `read_only` mode, the user could run `set global read_only = on` in a new session, it will return immediately if the cluster is already `read_only` otherwise it will simply continue polling the SROS table, and block until it learned that all server instances has acknowledged the enabling attempt or a configured time interval has passed, e.g. 10 seconds, explanation see below. **Or the user could actively examine the value of `read_only`, if it turns from `enabling` to `on`, it means the whole cluster is then read only. Note the latter approach will have 2 seconds of latency because of global variable cache. **The user could cancel the enabling or turn the mode back to normal by running `set global read_only = off` at any time.

To allow the `read_only` global variable turn from `enabling` to `on` even there is no poller in the cluster (polling canceled or server downed) , the reporter will also check that if she is the last in the cluster to acknowledge, and set the variable to `on` if it is the case.

If the TiDB cluster is scaling out while the `read_only` mode is enabled, the newly added TiDB server will get the `read_only = on` or `read_only = enabling` value from the global variable system table at start, and it will report this in the SROS system table immediately. Also, the newly added TiDB server will register its joining to PD (once [this PR](https://github.com/pingcap/tidb/pull/17649) gets merged), which could be used to track their report status.

If the TiDB cluster is scaling in or there is server shutdown, or even network split , these servers might never acknowledge their status. In case the poller waits forever, the poller will return with warnings containing the TiDB servers in the cluster that have not yet reported read only to the user after waiting 10 seconds.


#### How to identify committing transactions & held locks

As described above, enabling attempts should be blocked when there are committing transactions or held write locks.

It is relatively easy to check held locks, since locks are synchronized by DDL jobs, and we can check the held locks at the same TiDB server and the same session where the read only mode is turned on.

As for committing transactions, we could add a new variable to record if there are any transactions are committing on the session level, by hooking the committing process, we could set the variable when a transaction enters committing, and unset it once it finishes committing. Then we could just check this variable to see if there is any transaction committing. Note we only care about transactions that might have updated user’s data here, for internal transactions, we could just ignore them.

We could make use of the process information to know the status of all sessions in an instance, and the above in committing status could be incorporated in the process information, in this case, we could track all sessions at the domain level, and once all sessions are free of lock and committing transactions, the server could then report its acknowledgement in the SROS table.


### Implementation Design & Plan

The implementation of read only support can be decomposed into the following steps, and they will be proposed as separate PRs to ease the review & testing process.



*   add special handling of SQL `set variable read_only = on`
*   detect potentially offensive SQL and transactions before execution and committing.
*   add a new privilege level for migration tools to bypass the detection.
*   create the system table and integrate it into the above acknowledgement process.
*   add the held locks and committing transactions detection mechanism.
*   finishing the whole process by polling the system table.


## Testing Plan


### Unit test

Unit tests will be added with each step to test the functionalities added.


### Integration test

The integration test will be performed in front of a cluster, and the doc author is still studying how to do so. Will be added once the author figured it out.

A possible direction is to follow the response to [this requested test](https://github.com/pingcap/tidb/pull/17649#issuecomment-698229245).


## Impact & Risks

The polling process might exhaust the TiDB server, but it could be controlled by using a configuration item for polling frequency.


## Alternatives

Alternative designs are considered in the above sections.

But a fundamentally different approach can be, making the read only mode an instance level feature, and leaves cluster level read only management to deployment tools like TiUP, which applies `set instance read_only=1` on all instances. This approach can be much simpler in practice, but creates external dependency on this cluster level feature.
