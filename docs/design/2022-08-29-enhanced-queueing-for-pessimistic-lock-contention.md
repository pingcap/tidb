# Aggressive Locking: Enhanced Queueing for Pessimistic Lock Contention

- Author(s): [MyonKeminta](http://github.com/MyonKeminta)
- Tracking Issue: https://github.com/tikv/tikv/issues/13298

## Abstract

This proposes to make use of the new pessimistic lock waiting model as designed in [TiKV RFC #100](https://github.com/tikv/rfcs/pull/100), which is expected to reduce the tail latency problem in scenarios with frequent pessimistic lock conflicts. The design is currently only applicable for single key point-get locking. 

## Background

As said in [TiKV RFC #100](https://github.com/tikv/rfcs/pull/100), our current implementation of pessimistic locks might be problematic in case there are frequent conflicts. Transactions waiting for the lock on the same key may be granted the lock in random order and may perform too many useless statement retries, which may lead to high tail latency. To solve the problem, we designed a new lock waiting model. We expect the new model can enhance the queueing behavior of concurrent conflicting pessimistic lock operations, so that the conflicting transactions can execute in more serialized order. It's also expected to reduce useless statement retries, which saves CPU cost and RPC calls. Due to the complexity of the implementation, as the first step, we will support the optimized model for pessimistic lock requests that affects only one key.

## Design

### Changes in TiKV side

The majority part of the change is in TiKV side, which is explained in detail in [TiKV RFC #100](https://github.com/tikv/rfcs/pull/100). Briefly speaking, the differences from TiDB's perspective are (only applicable for pessimistic requests that locks only one key):

- By specifying a special parameter, a pessimistic lock request is allowed to lock a key even there is write conflict, in which case TiKV can return the value and `commit_ts` of the latest version, namely `locked_with_conflict_ts`. The actual lock written down in TiKV will have its `for_update_ts` field equal to the latest `commit_ts` on the key.
- By specifying the parameter mentioned above, the pessimistic lock request is also allowed to continue locking the key after waiting for the lock of another transaction, instead of always reporting WriteConflict after being woken up.

### Aggressive Locking

When a key is locked with conflict, the current statement becomes executing at a different snapshot. However, the statement may have already read data in the expired snapshot (as specified by the `for_update_ts` of the statement). In this case, we have no choice but retry the current statement with a new `for_update_ts`.

In our original implementation, when retrying a statement, the pessimistic locks that were already acquired will be rolled back, since it's possible that the keys we need to lock may change after retrying. However, we will choose a different way to handle this case with our new locking mechanism. We expect that in most cases, the keys we need to lock won't change after retrying at a newer snapshot. Therefore, we adopt this way, namely *aggressive locking*:

- When performing statement retry, if there are already some keys locked during the previous attempt, do not roll them back immediately. We denote the set of the keys locked during previous attempt by $S_0$.
- After executing the statement again, it might be found that some keys needs to be locked. We denote the set of the keys need to be locked by $S$.
- Then, we will send requests to lock keys in $S - S_0$, and rollback keys in $S_0 - S$. Those keys that were already locked in the previous attempt don't need to be locked again.

In most cases, we expect that $S_0 \subseteq S$, in which case rolling back the locks and acquire them again like the old implementation may be a waste. It's also possible in the original implementation that the lock is acquired by another transaction between rolling back the lock and acquiring the lock again, causing the statement retry useless. By keeping the lock until it's confirmed that the lock is not needed anymore, we avoid the problem, at the cost of causing more conflict when $S_0 - S$ is not empty.

We plan to support this kind of behavior only for simple point-get queries for now.

To support this behavior, we add some new methods to the `KVTxn` type in client-go:

```go
// Start an aggressive locking session. Usually called when starting a DML statement.
func (*KVTxn) StartAggressiveLocking()
// Start (n+1)-th attempt of the statement (due to pessimistic retry); rollback unnecessary locks locked in (n-1)-th attempt. 
func (*KVTxn) RetryAggressiveLocking()
// Rollback all pessimistic locks acquired during the pessimistic locking session, and exit aggressive locking state.
func (*KVTxn) CancelAggressiveLocking()
// Record keys locked in current (n-th) attempt to MemDB, rollback locks locked in previous (n-1)-th attempt, and exit aggressive locking state.
func (*KVTxn) DoneAggressiveLocking()
```

These functions will implement the behavior stated above, and the basic pattern of using these functions is like
(pseudo code, the actual invocations of `(Retry|Cancel|Done)AggressiveLocking` are put in `OnStmtRetry`, `OnStmtRollback`, `OnStmtCommit` instead of the retry loop):

```go
txn.StartAggressiveLocking()
for {
    result := tryExecuteStatement()
    switch result {
	case PESSIMISTIC_RETRY:
        txn.RetryAggressiveLocking()
		continue;
    case FAIL:
        txn.CancelAggressiveLocking()
		break;
    case SUCCESS: {
        txn.DoneAggressiveLocking()
		break;
    }
}
```

### Performance issue

In the old implementation, when TiKV executes a scheduler command that releases a lock (e.g. `Commit` or `Rollback`), it wakes up the lock-waiting request after executing `process_write` of the command but before writing down the data to the Raft layer. At this time, releasing lock is actually not finished yet. However, in the new design of TiKV as stated in [TiKV RFC #100](https://github.com/tikv/rfcs/pull/100), the pessimistic lock request should be resumed after being woken up, and it can only return to TiDB after it successfully acquire the lock. This difference makes TiDB starts pessimistic retry later than before. In some scenarios, this may make the average latency higher than before, which may also results in lower QPS.

It would be complicated to totally solve the problem, but we have a simpler idea to optimize it for some specific scenarios. When executing a statement and it performs lock-with-conflict, if the statement haven't performed any other read/write, it can actually continue executing, update the `for_update_ts` if necessary, and avoid retrying the whole statement.

### Configurations

We want to avoid introducing new configurations that user must know to make use of the new behavior, but it's a fact that the optimization is very complicated and there's known performance regression in a few scenarios. It would be risky if the new behavior is always enabled unconditionally. We can introduce a hidden system variable which can be used to disable the optimization in case there's any problem.

#### `tidb_pessimistic_txn_aggressive_locking`

Specifies whether the optimization stated above is enabled.

- Scope: global or session
- Value: `0` or `1`
- Default: `1` for new clusters, `0` for clusters upgraded from old version
