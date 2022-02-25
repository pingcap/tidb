# TiDB Design Documents

- Author(s): you06
- Discussion PR: [https://github.com/pingcap/tidb/pull/XXX](https://github.com/pingcap/tidb/pull/XXX)
- Tracking Issue: [https://github.com/pingcap/tidb/issues/XXX](https://github.com/pingcap/tidb/issues/32555)

## Table of Contents

- [Introduction](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
- [Motivation or Background](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
- [Detailed Design](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
- [Test Design](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
    - [Functional Tests](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
    - [Scenario Tests](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
    - [Compatibility Tests](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
    - [Benchmark Tests](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
- [Impacts & Risks](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
- [Investigation & Alternatives](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)
- [Unresolved Questions](https://www.notion.so/Skip-Locked-RFC-37a02a03d42a4a7fa4c6e114d4a01f5a)

## Introduction

The pessimistic lock will make the conflict operations execute in serial order, so in a high contention application, we certainly want to acquire fewer locks and skip more locks. In such scenarios, highly fine-grained lock operations are required.

This design discusses the implementation and compatibility of `SELECT ... FOR UPDATE SKIP LOCKED` in TiDB.

## Motivation or Background

Many scenarios use read-and-lock operations like `SELECT ... FOR UPDATE` and `SELECT ... FOR SHARE`, and when such operations meet a locked row or unique index, they must block until the locks are released. Sometimes, you may want the query to return immediately whether it meets a lock, `SELECT ... FOR UPDATE NOWAIT` does not block and report a locked error when there is a blocking transaction.

Since MySQL 8.0, `SELECT ... FOR UPDATE SKIP LOCKED` syntax is supported, which reads and locks the rows without waiting, and the locked rows will be removed from the resultset. Different from `SELECT ... FOR UPDATE NOWAIT`, this syntax never returns a locked error.

## Detailed Design

Explain the design in enough detail that: it is reasonably clear how the feature would be implemented, corner cases are dissected by example, how the feature is used, etc.

It's better to describe the pseudo-code of the key algorithm, API interfaces, the UML graph, what components are needed to be changed in this section.

Compatibility is important, please also take into consideration, a checklist:

- Compatibility with other features, like partition table, security&privilege, collation&charset, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility

### Planner

The parser of TiDB already supports `SELECT ... FOR UPDATE SKIP LOCKED` syntax, however, such lock type will be processed the same as `SELECT` syntax. The planner should handle this syntax and optimize it as a for-update read.

### Transaction Client

There are 3 ways TiDB acquires pessimistic locks.

- Lock handles after reading
- Read and lock by common handles
- Read and lock by unique index keys

For the first and second cases, we just skip the locked rows in TiKV, however, for the third case, we may lock index keys successfully but meet some locks in row handles, so some locks on index keys need to be released. Will discuss the 3 situations in the protocol design section.

Add a field in the `[LockCtx](https://github.com/tikv/client-go/blob/df187fa79aa1dedc293a1eae37ef8b3a522dba46/kv/kv.go#L56-L78)`, which controls whether the transaction client acquiring pessimistic locks with `SKIP LOCKED` syntax.

```diff
// LockCtx contains information for LockKeys method.
type LockCtx struct {
	...
+	SkipLock bool
+	SkippedKeys map[string]struct{}
}
```

### Protocol

#### Lock handles after reading

Locking from reading results is the basic form of acquiring pessimistic locks.

The current pessimistic lock requests will be queued when it cannot obtain the lock, wait until a timeout, and retry in TiDB. With `SELECT ... FOR UPDATE NOWAIT` syntax, once there is an existing lock, TiKV cancels the rest keys and reports a `KeyIsLocked` error immediately. With `SELECT ... FOR UPDATE SKIP LOCKED` syntax, such requests should not be queued and waited, the lock request should not break when it meets a lock as well.

```diff
message PessimisticLockRequest {
  ...
+ // skip_locked indicate whether TiKV need to skip the exist locks.
+ // If skip_locked is set to true, both optimistic and pessimistic locks will be skipped.
+ bool skip_locked = ...;
}
```

Since TiKV may skip some locks while acquiring pessimistic locks, TiDB cannot take for all pessimistic locks are acquired from a successful pessimistic lock request. TiKV needs to attach the successful locks in the response. Since there is a `KeyError` array field and we can use it.

```protobuf
message PessimisticLockResponse {
    repeated KeyError errors = 2;
}
```

There will be no information for acquired locks, but each skipped key should be corresponding to a `KeyError`. e.g. a transaction tries to lock `key1`, `key2`, and `key3`, in which `key1` and `key2` are already locked by other transactions and should be skipped. The `errors` field in `PessimisticLockResponse` will contain `key1` and `key2` so that TiDB can infer `key3` is locked.

In `[SelectLockExec](https://github.com/tikv/client-go/blob/df187fa79aa1dedc293a1eae37ef8b3a522dba46/txnkv/transaction/txn.go#L577-L750)` executor, such skipped keys should be handled and filtered.

#### Read and lock by common handles

Reading and locking through common handles is similar to lock handles after reading, except set `PessimisticLockRequest.return_values` to true and cache the values in the [pessimistic lock cache](https://github.com/pingcap/tidb/blob/047775fbc89614c1e5a10085351ee78d5842f77a/sessionctx/variable/session.go#L155-L157). So we just handle this case the same as locking handles after reading.

#### Read and lock by unique index keys

This case is more complex, locking unique index and handle should be atomic, that means if lock unique index successfully but the lock on the handle is skipped, we should roll back the lock in the unique index and remove this row from the resultset.

Here is the workflow of how point-get and batch-point-get to read and lock rows by unique index keys in the current TiDB.

Point-get

1. Read handle key by index key.
2. Lock index key if not read consistency.
3. If the handle key does not exist, return.
4. Lock the handle key.
5. Raed the handle key from pessimistic lock cache.

Batch-point-get

1. Read handle keys from index keys.
2. Lock all index and handle keys if no read consistency, else lock existing index and handle keys only.
3. Read values from pessimistic lock cache.

Once there are pessimistic lock failures, we need to roll back the related lock to make the lock consistent across the index and handle. The transaction of TiKV client offers `[asyncPessimisticRollback](https://github.com/tikv/client-go/blob/df187fa79aa1dedc293a1eae37ef8b3a522dba46/txnkv/transaction/txn.go#L768-L803)` method, however, it’s private and should not be exposed because 2 phase lock doesn’t allow to release locks in the growing phase, otherwise the correctness may be broken. We should use this method to roll back the pessimistic locks for the atomic between inside `[LockKeys](https://github.com/tikv/client-go/blob/df187fa79aa1dedc293a1eae37ef8b3a522dba46/txnkv/transaction/txn.go#L577-L750)`.

We can add a concept called atomic group into `LockCtx`, so keys in each atomic group should be locked atomically, any of the keys suffer a locked failure, `asyncPessimisticRollback` will be called to roll back the else keys.

```diff
// LockCtx contains information for LockKeys method.
type LockCtx struct {
	...
+	Key2AtomicGroup map[string]int
+	AtomicGroups [][][]byte
}

+	func (ctx *LockCtx) SetAtomicGroup([][]byte) {...}
```

The workflow of point-get and batch-point-get can be changed into the following description.

Point-get

1. Lock index key if not read consistency.
2. If there is a locked error, return with an empty resultset.
3. Read handle key by index key from pessimistic cache or a transaction snapshot.
4. If the handle key does not exist, return.
5. Set `(index key, handle key)` as an atomic group.
6. Lock the handle key, if this meets a failure, the pessimistic lock on the index key should be rolled back.
7. Raed the handle key from pessimistic lock cache, filter keys from `SkippedKeys`.

Batch-point-get

1. Read handle keys from index keys.
2. Set each `(index key, handle key)` as an atomic group for existing handles.
3. Lock all index and handle keys if no read consistency, else lock existing index and handle keys only.
4. Read values from pessimistic lock cache, filter keys from `SkippedKeys`.

### Deadlock

The `SELECT ... FOR UPDATE SKIP LOCKED` means that it skips all locks it meets, so the deadlock is not expected. Because TiKV avoids sending deadlock-detect requests, there won’t be deadlock errors.

### Primary Key

The primary key is the state of the transaction, however, with `SKIP LOCKED` syntax, the primary key can not be inferred before the first lock is acquired. So there is a limitation before we lock an atomic group successfully, we must repeatably try locking a single atomic group. So the workflow looks like this:

1. Try to lock an atomic group and take the first key of this group as the primary key.
2. If the lock operation failed, async rolls back the atomic group, back to step 1, and try another group with another primary key.
3. Use the primary key as the transaction’s primary key.

## Test Design

A brief description of how the implementation will be tested. Both the integration test and the unit test should be considered.

### Functional Tests

It's used to ensure the basic feature function works as expected. Both the integration test and the unit test should be considered.

### Scenario Tests

It's used to ensure this feature works as expected in some common scenarios.

### Compatibility Tests

A checklist to test compatibility:

- Compatibility with other features, like partition table, security & privilege, charset & collation, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility

### Benchmark Tests

The following two parts need to be measured:

- The performance of this feature under different parameters
- The performance influence on the online workload

## Impacts & Risks

Describe the potential impacts & risks of the design on overall performance, security, k8s, and other aspects. List all the risks or unknowns by far.

Please describe impacts and risks in two sections: Impacts could be positive or negative, and intentional. Risks are usually negative, unintentional, and may or may not happen. E.g., for performance, we might expect a new feature to improve latency by 10% (expected impact), there is a risk that latency in scenarios X and Y could degrade by 50%.

## Investigation & Alternatives

How do other systems solve this issue? What other designs have been considered and what is the rationale for not choosing them?

## Unresolved Questions

What parts of the design are still to be determined?
