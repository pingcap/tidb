# Proposal: Session Manager

- Author(s): [crazycs520](https://github.com/crazycs520)
- Tracking Issue: https://github.com/pingcap/tidb/issues/6840

## Abstract


This proposes a design of transaction savepoint.

## Background

Some Applications need to use transaction savepoint feature. And in order to be better compatible with some ORM frameworks, such as gorm.

### Goal

- Support transaction savepoint syntax.
- Support transaction savepoint feature, and compatible with MySQL.

### Non-Goals

- Not for implementing savepoint on the entire database, but savepoint on transaction modifications.

## Proposal

### Savepoint Implementation

`SAVEPOINT` is used to set a savepoint of a specified name in the current transaction. If a savepoint with the same name already exists, 
it will be deleted and a new savepoint with the same name will be set.

`SAVEPOINT` need to record a snapshot of current transaction, when users execute `ROLLBACK TO SAVEPOINT` statement, current transaction
will roll back to the specified savepoint.

Specifically, `SAVEPOINT` needs to record the snapshot(checkpoint) of transaction `MemDB`, and the snapshot of `TransactionContext`.

The savepoint data struct is:

```go
// SavepointRecord indicates a transaction's savepoint record.
type SavepointRecord struct {
    // name is the name of the savepoint
    Name string
    // MemDBCheckpoint is the transaction's memdb checkpoint.
    MemDBCheckpoint *tikv.MemDBCheckpoint
    // TxnCtxSavepoint is the savepoint of TransactionContext
    TxnCtxSavepoint TxnCtxNeedToRestore
}

// TxnCtxNeedToRestore stores transaction variables which need to be restored when rolling back to a savepoint.
type TxnCtxNeedToRestore struct {
    // TableDeltaMap is used in the schema validator for DDL changes in one table not to block others.
    // It's also used in the statistics updating.
    // Note: for the partitioned table, it stores all the partition IDs.
    TableDeltaMap map[int64]TableDelta
    
    // pessimisticLockCache is the cache for pessimistic locked keys,
    // The value never changes during the transaction.
    pessimisticLockCache map[string][]byte
    
    // CachedTables is not nil if the transaction write on cached table.
    CachedTables map[int64]interface{}
}

// Transaction Savepoints will be stored in TransactionContext.
type TransactionContext struct {
	...
    Savepoints []SavepointRecord
}
```

The pseudocode about `SAVEPOINT` implementation is following:

```go
func executeSavepoint(savepointName string){
	// If a savepoint with the same name already exists, it will be deleted.
	deleteSavepoint(savepointName)
	memDBCheckpoint := txn.GetMemDBCheckpoint()
	txnCtxSavepoint := txnCtx.GetCurrentSavepoint()
	txnCtx.Savepoints = append(txnCtx.Savepoints, SavepointRecord
	    Name: savepointName, 
	    MemDBCheckpoint: memDBCheckpoint, 
	    TxnCtxSavepoint: txnCtxSavepoint}
    )
}
```

> **Warning**
> Savepoint is not support when TiDB binlog enabled. Since binlog is not recommended after TiDB 4.0.

### ROLLBACK TO SAVEPOINT Implementation

`ROLLBACK TO SAVEPOINT` rolls back a transaction to the savepoint of a specified name and does not terminate the transaction. 
Data changes made to the table data after the savepoint will be reverted in the rollback, and all savepoints after the savepoint are deleted.

Specifically, `ROLLBACK TO SAVEPOINT` needs to roll back following information:

- Transaction `MemDB`
- TransactionContext

The pseudocode about `ROLLBACK TO SAVEPOINT` implementation is following:

```go
func RollbackToSavepoint(savepointName string){
	for idx, sp := range TxnCtx.Savepoints {
		if savepointName == sp.Name {
            TxnCtx.RollbackToSavepoint(sp.TxnCtxSavepoint)
            txn.RollbackMemDBToCheckpoint(sp.MemDBCheckpoint)
			// Delete the later savepoint.
			TxnCtx.Savepoints = sessVars.TxnCtx.Savepoints[:idx+1]
			return nil
		}
	}
	return errSavepointNotExists
}
```

Also need to roll back the `LazyTxn.stagingHandle` information. Since `LazyTxn.stagingHandle` stores the checkpoint of 
transaction `MemDB` when start to execute the current SQL statement, so after `ROLLBACK TO SAVEPOINT` execute, 
`LazyTxn.stagingHandle` should store the roll backed `MemDB` checkpoint handle.

> **Warning**
> `ROLLBACK TO SAVEPOINT` statements won't roll back `AUTO_INCREMENT` and `SEQUENCE`, which means the assigned AUTO_INCREMENT/SEQUENCE ID won't be reclaimed.

### RELEASE SAVEPOINT Implementation

`RELEASE SAVEPOINT` statement removes the named savepoint and **all savepoints** after this savepoint from the current transaction,  
without committing or rolling back the current transaction. If the savepoint of the specified name does not exist, the following error is returned:

```
ERROR 1305 (42000): SAVEPOINT identifier does not exist
```

### MySQL Compatibility

When `ROLLBACK TO SAVEPOINT` is used to roll back a transaction to a specified savepoint, MySQL releases the locks 
held only after the specified savepoint, while in TiDB pessimistic transaction, TiDB does not immediately release the locks 
held after the specified savepoint. Instead, TiDB releases all locks when the transaction is committed or rolled back.
