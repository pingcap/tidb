# Proposal: Extend `tidb_constraint_check_in_place` to Support Pessimistic Transactions

* Authors: [sticnarf](https://github.com/sticnarf), [ekexium](https://github.com/ekexium)
* Tracking issue: [#36579](https://github.com/pingcap/tidb/issues/36579)

## Abstract

We propose to make lazy uniqueness check usable not only in optimistic transactions, but also in pessimistic transactions. Then, in certain use cases, we can benefit better latency and throughput from deferring the uniqueness constraint checks until the transaction commits while not decreasing the success rate.

Similar to the current `tidb_constraint_check_in_place`, this proposal only involves uniqueness constraints of the primary key and unique keys. Other constraints such as `NOT NULL` and foreign keys are not affected.

## Background

Lazy uniqueness constraint check is a feature enabled by default for optimistic transactions. It is controlled by the system variable `tidb_constraint_check_in_place`. When this variable is off, uniqueness checks are performed when prewriting the mutations:

```sql
CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY);
INSERT INTO t1 VALUES (1), (2);
BEGIN OPTIMISTIC;
INSERT INTO t1 VALUES (1); -- MySQL returns an error here; TiDB returns success.
INSERT INTO t1 VALUES (2);
COMMIT; -- ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'
```

However, the feature is not available for pessimistic transactions now. We developed the pessimistic transaction mode to make the behavior close to MySQL as much as possible. So, the constraint checks are always done at the time of executing the DML and pessimistic locks for the unique keys are always written meanwhile.

Obviously, it is not free. Say we have a transaction that consists of N `INSERT` statements. By deferring the constraint checks, no RPC interactions between TiDB and TiKV are needed before the transaction commits. While in a pessimistic transaction, the RPC number during the 2PC period does not change, but there are extra N `AcquirePessimisticLock` RPCs before `COMMIT`.

Many users use pessimistic transactions only for success rate and they assume their transactions cannot fail. So, if there are DMLs involving uniqueness checks (e.g. `INSERT`), it is _likely_ the users can guarantee the uniqueness checks cannot fail as well. Otherwise, they will break their own assumptions, which means the DMLs may fail due to duplicated unique key even in pessimistic transactions.

So, skipping pessimistic locks for these keys and deferring constraint checks can reduce much latency and cost for these users while not breaking their expectations about the success rate of pessimistic transactions.

## Design

This feature makes it possible to skip acquiring pessimistic locks for the keys that need uniqueness constraint checks in pessimistic transactions. We do constraint checks for these keys during prewrite.

The main affected statements include simple `INSERT` and `UPDATE` that involve primary key or unique keys. `INSERT ON DUPLICATE` and `INSERT IGNORE` are not affected because they don't raise errors for duplicated entries.

A session/global system variable `tidb_constraint_check_in_place_pessimistic` is introduced to control the behavior. The default value is `ON` and the behavior will remain unchanged. When `tidb_constraint_check_in_place_pessimistic` is `OFF`, the new behavior will take effect in pessimistic transactions.

```sql
CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY);
INSERT INTO t1 VALUES (1), (2);

set tidb_constraint_check_in_place_pessimistic = off;
BEGIN PESSIMISTIC;
SELECT * FROM t1 WHERE id = 1 FOR UPDATE; -- SELECT FOR UPDATE locks key as usual.
INSERT INTO t1 VALUES (2); -- Skip acquiring the lock and return success.
COMMIT; -- ERROR 1062 (23000): Duplicate entry '2' for key 'PRIMARY'
```

#### Protocol

Although we skip locking some keys and doing constraint checks before `COMMIT`, it is still a pessimistic transaction. Except for the keys that need constraint checks, we acquire pessimistic locks for other keys as usual.

It is completely a new kind of behavior to do constraint checks during prewrite for keys that don't get locked ahead of time. We need to change the prewrite protocol to describe it.

Previously, there is a `repeated bool` field describing whether each key needs be locked first. Generally, non-unique index keys don't need to be locked and their conflict checks are skipped.

 ```protobuf
message PrewriteRequest {
    // For pessimistic transaction, some mutations don't need to be locked, for example, non-unique index key.
    repeated bool is_pessimistic_lock = 7;
}
 ```

Now, we can reuse this field to describe the keys that need constraint checks during prewrite in a pessimistic transaction:

```protobuf
message PrewriteRequest {
	enum PessimisticAction {
		// The key needn't be locked and no extra write conflict checks are needed.
		SKIP_PESSIMISTIC_CHECK = 0;
		// The key should have been locked at the time of prewrite.
		DO_PESSIMISTIC_CHECK = 1;
		// The key doesn't need a pessimistic lock. But we need to do data constraint checks.
		DO_CONSTRAINT_CHECK = 2;
	}
    // For pessimistic transaction, some mutations don't need to be locked, for example, non-unique index key.
    repeated PessimisticAction pessimistic_actions = 7;
}
```

Because `enum` is compatible with `bool` on the wire, we can seamlessly extend the  `is_pessimistic_lock` field. In a TiDB cluster, TiKV instances are upgraded before TiDB. So, we can make sure TiKV can handle it correctly when TiDB starts using the new protocol.

When `tidb_constraint_check_in_place_pessimistic` is off, all the keys that have `PresumeKeyNotExists` flags don't need to be locked when executing the DML. They will be marked with a new special flag `NeedConstraintCheckInPrewrite`. When the transaction commits, TiDB will set the actions of these keys to `DO_CONSTRAINT_CHECK`. So, TiKV will not check the existence of pessimistic locks of these keys. Instead, constraint and write conflict checks should be done for these keys.

#### Behavior of locking lazy checked keys

Consider the following scenario (from @cfzjywxk):

```sql
/* init */ CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v int);
/* init */ INSERT INTO t1 VALUES (1, 1);

/* s1 */ set tidb_constraint_check_in_place_pessimistic = off;
/* s1 */ BEGIN PESSIMISTIC;
/* s1 */ INSERT INTO t1 VALUES (1, 2); -- Skip acquiring the lock. So the statement would succeed.
/* s1 */ SELECT * FROM t1 FOR UPDATE; -- Here the pessimistic lock on row key '1' would be acquired
```

The `INSERT` statement puts the row with `id = 1` in the transaction write buffer of TiDB without checking the constraint. The later `SELECT FOR UPDATE` will read and lock the row with `id = 1` in TiKV. If the `SELECT FOR UPDATE` succeeded, it would be difficult to decide the result set. Returning `(1, 1), (1, 2)` breaks the unique constraint, while returning `(1, 1)` or `(1, 2)` may be all strange semantically. Using the wrong result set for following operations may even cause data inconsistency.

So, we choose to do the missing constraint check and locking whenever a key that skipped constraint check before is read from the transaction buffer. In union scan or point get executors, if we involve any key in the buffer with a `NeedConstraintCheckInPrewrite` flag, we will reset the flag and add it again to the staging buffer. Later, we can acquire locks and check the constraints for these keys. In this way, the result set will not break any constraint.

This means the read-only statements like the `SELECT FOR UPDATE` above will throw a "duplicate entry" error in the case above. It may be strange that a read-only statement raises errors like this. We should make the user aware of the behavior.

### Applicable Scenarios

When talking about the success rate, we are based on the following assumption:

**The user treats duplicated entry errors returned by DMLs as fatal errors which must abort the transaction.**

A duplicated entry error only makes a single DML statement fail in a pessimistic transaction. The transaction does not abort immediately. If the user can handle the duplicated entry error in their application and continue the transaction, skipping pessimistic locks definitely changes the behavior and lowers the success rate.

While under such an assumption, the general success rate won't change with the feature enabled in most cases.

Suppose that transaction A wants to insert a new row with a unique key, or update a field with a unique key.

1. If a duplicated record exists before the DML, when to check the constraint does not change the success rate. Currently, the duplicated entry error is raised when executing the DML. With lazy constraint check, the error is raised when committing the transaction. 

2. If the duplicated record is written by transaction B between transaction A's executing the DML and committing, let's consider different cases:

   1. If both transactions acquire the lock of the unique key, which is the current behavior, transaction B will be blocked until transaction A commits. After transaction A commits, the DML of transaction B will fail due to the duplicated entry.
   2. If transaction A skips acquiring the lock while transaction B does not, transaction A will fail unless transaction B aborts. When transaction A prewrites, it will either meet the lock or the write record from transaction B. In either case, transaction A will fail to pass the constraint check.
   3. If both transaction A and B skip acquiring the lock, it's similar to the case of an optimistic transaction. The transaction that commits first will succeed.

   Therefore, although the time when the error is raised is different, only one of the transactions will fail.
   
In a special case, the success rate may drop due to the write conflict check:

```sql
/* init */ CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY);
/* init */ INSERT INTO t1 VALUES (1);

/* s1 */ BEGIN PESSIMISTIC;
/* s2 */ DELETE FROM t1 WHERE id = 1;
/* s1 */ INSERT INTO t1 VALUES (1);
/* s1 */ COMMIT;
```

If we check constraints while acquiring the lock, `s1` will succeed because the row with `id = 1` has been deleted when acquiring the lock. However, if we check constraints lazily, to ensure snapshot isolation, we have to do a write conflict check like in optimistic transactions in additional to constraint checks. In the case above, write conflict check will fail because the commit TS of `s2` is bigger than the start TS of `s1`.

#### List of typical use cases

Note that the feature is only needed if part of your transaction still needs to acquire pessimistic locks. Otherwise, use the optimistic transaction mode instead.

* `INSERT` rows to a table without any unique key. TiDB does not generate duplicated implicit row ID, so normally there cannot be unique key conflicts.
* The user guarantees their application does not `INSERT` duplicate entries, such bulk data load.

### Safety

With this feature enabled, we accept slight behavior changes, but we must guarantee the general transaction properties are still preserved.

First, atomicity is not affected. Although we skip the locking phase for some of the keys, we don't skip any conflict and constraint checks that are necessary in the usual 2PC procedure. To be more secure, we can do conflict checks for the non-unique index keys just like optimistic transactions. It avoids the risk of producing duplicated write records when there are RPC retries.

The uniqueness constraints are also preserved. For all the keys with a `PresumeKeyNotExists` flag, we check the constraint either when prewriting them, or when acquiring the pessimistic locks like in the case of [Locking Lazy Checked Keys](#behavior-of-locking-lazy-checked-keys) above. So we can guarantee no duplicated entry exists after committing the transaction. In the case of "rollback to savepoint", some keys that need constraint checks may be unchanged in the end, but we will still check the constraints for them in prewrite to make sure the client does not miss any errors.

Due to the "read committed" semantics of DMLs in pessimistic transactions, the late locking could succeed even if duplicated entries exist at the time of `INSERT` because other transactions remove the duplicated entry after that. From the view of our transaction, it's equivalent to the case when other transactions remove the duplicated entry before our `INSERT`. There will be no data corruption after the transaction commits.

#### Assumptions we make

The safety of the feature depends on these assumptions which are all true in TiDB 6.2.

- TiDB does not acquire pessimistic locks for non-unique index keys.
- TiDB does not mark non-unique index keys as `PresumeKeyNotExists`.
- If a key gets marked as `PresumeKeyNotExists`, it must be in the current statement buffer.

#### Safety with multiple operations in one statement

In current TiDB(<=6.2) implementation, the locking phase of pessimistic DML (except SELECT FOR UPDATE) begins after executors. If there are multiple operations on one key in the execution phase, they may not behave like what we expect. For example in the same statement there are operations:

1. write a key without the NeedConstraintCheckInPrewrite flag. This may be a normal locking request or because of compensating a deferred lock.
2. write the same key with the NeedConstraintCheckInPrewrite flag

In the locking phase we will not acquire pessimistic lock for the key because the NeedConstraintCheckInPrewrite flag is set. This should not happen in the current TiDB implementation, but is noteworthy.