# Put and Verify Transactions for External Storages

- Author: [Yu Juncen](https://github.com/YuJuncen)
- Tracking Issue: https://github.com/pingcap/tidb/issues/56523

## Background 

Sometimes, we need to control concurrency access to the same backup archive, like:

- When compacting / restoring, we want to block migrating to a new version. 
- When migrating the backup storage to a new version, we want to forbid reading.
- When truncating the storage, we don't want another truncating operation happen.
- When backing up, we don't want another backup uses the same storage.

But external storage locking isn't trivial. Simply putting a lock file isn't safe enough: because after checking there isn't such a lock file, another one may write it immediately. Object locks provide stronger consistency, but also require extra configuration and permissions. Most object storages also support "conditional write", which is lighter-weighted than object locks in the concurrency control scenario. But both object locks and conditional write are focus on "entities", the available conditions are restricted: you cannot say, "if the prefix `/competitor` doesn't contain any file, write `/me`.", at least for now (mid 2024).

This proposal will propose a new procedure for locking / unlocking, which is safe in all object storages that have a *strong consistency* guarantee over its PUT, GET and LIST API. This has been promised in:

- S3: https://aws.amazon.com/cn/s3/consistency/
- Google Cloud Storage: https://cloud.google.com/storage/docs/consistency#strongly_consistent_operations
- Azure Blob Storage: https://github.com/MicrosoftDocs/azure-docs/issues/105331#issuecomment-1450252384 (But no official documents found yet :( ) 

## Spec

A put-and-verify transaction looks like:

```go
type VerifyWriteContext struct {
	context.Context
	Target  string
	Storage ExternalStorage
	TxnID   uuid.UUID
}
 
type ExclusiveWrite struct {
	// Target is the target file of this txn.
	// There shouldn't be other files shares this prefix with this file, or the txn will fail.
	Target string
	// Content is the content that needed to be written to that file.
	Content func(txnID uuid.UUID) []byte
	// Verify allows you add other preconditions to the write.
	// This will be called when the write is allowed and about to be performed.
	// If `Verify()` returns an error, the write will be aborted.
	// 
	// With this, you may add extra preconditions of committing in usecases like RWLock.
	Verify func(ctx VerifyWriteContext) error
}
```

After successfully committing such a PutAndVerify transaction, the following invariants are kept:

- The `Target` should be written exactly once.
- The function `Verify()` returns no error after this transaction committed, if the function `Verify` satisfies:
  - Once it returns non-error, it will always return non-error as long as exactly one file has the Prefix().

A PutAndVerify txn was committed by:

- Put a intention file with a random suffix to the `Target`.
- Check if there is another file in the `Target`.
- If there is, remove our intention file and back off or report error to caller.
- If there isn't, and `Verify()` returns no error, put the `Content()` to the `Target`, without suffix, then remove the intention file.

Here is the detailed code for committing such a transaction:

```go
func (w ExclusiveWrite) CommitTo(ctx context.Context, s ExternalStorage) (uuid.UUID, error) {
	txnID := uuid.New()
	cx := VerifyWriteContext{
		Context: ctx,
		Target:  w.Target,
		Storage: s,
		TxnID:   txnID,
	}
	intentFileName := cx.IntentFileName() // Should be "{Target}.INTENT.{UUID}"
	checkConflict := func() error {
		var err error
		if w.Verify != nil {
			err = multierr.Append(err, w.Verify(cx))
		}
		return multierr.Append(err, cx.assertOnlyMyIntent())
	}

	if err := checkConflict(); err != nil {
		return uuid.UUID{}, errors.Annotate(err, "during initial check")
	}
	if err := s.WriteFile(cx, intentFileName, []byte{}); err != nil {
		return uuid.UUID{}, errors.Annotate(err, "during writing intention file")
	}
	defer s.DeleteFile(cx, intentFileName)
	if err := checkConflict(); err != nil {
		return uuid.UUID{}, errors.Annotate(err, "during checking whether there are other intentions")
	}

	return txnID, s.WriteFile(cx, w.Target, w.Content(txnID))
}
```



An example of the txn aborting, when there are two conflicting txns, the name of intent files are simplified for reading:

| Alice's Txn                              | Bob's Txn                                  |
| ---------------------------------------- | ------------------------------------------ |
| intentFile := "LOCK_Alice"              | intentFile := "LOCK_Bob"                  |
|                                          | Verify() → **OK**                          |
| Verify() → **OK**                        |                                            |
|                                          | Write("LOCK_Bob", "") → **OK**             |
| Write("LOCK_Alice", "") → **OK**         |                                            |
|                                          | Verify() → **Failed! "LOCK_Alice" exists** |
| Verify() → **Failed! "Lock_Bob" exists** |                                            |
|                                          | Delete("LOCK_Bob") → **OK**                |
| Delete("LOCK_Alice") → **OK**            |                                            |
| ABORT                                    | ABORT                                      |

Then, they may retry committing. 

| Alice's Txn                                        | Bob's Txn                                  |
| -------------------------------------------------- | ------------------------------------------ |
| intentFile := "LOCK_Alice"                        | intentFile := "LOCK_Bob"                  |
|                                                    | Verify() → **OK**                          |
| Verify() → **OK**                                  |                                            |
|                                                    | Write("LOCK_Bob", "") → **OK**             |
| Write("LOCK_Alice", "") → **OK**                   |                                            |
|                                                    | Verify() → **Failed! "LOCK_Alice" exists** |
|                                                    | Delete("LOCK_Bob") → **OK**                |
| Verify() → **OK**                                  |                                            |
| Write("LOCK_Alice","Alice owns the lock") → **OK** |                                            |
| COMMITTED                                          | ABORT                                      |

This time, Alice is lucky enough, she committes her transaction, Bob gives up committing when he realizes that there is a conflicting transaction.

## Correctness

### Atomic CAS 

Here is a TLA+ module that describes the algorithm. 

You can find a rendered version [here](imgs/write-and-verify-tla.pdf).

The theorem have been checked by a TLA+ model, [here](imgs/write-and-verfiy-tla-states.pdf) is the state transform graph with 2 clients, you may check it manually.

### Invariant of `Verify()`

It looks trivial: the last call to `Verify` makes sure there should be exactly one file that has the prefix. 
By its definition, it should always return nil before the file is removed.

## Example 1: Mutex

An empty `Verify()` function is enough for implementing a Mutex in the external storage.

```go
func TryLockRemote(ctx context.Context, storage ExternalStorage, path) error {
	writer := ExclusiveWrite{
		Target: path,
		Content: func(_ uuid.UUID) []byte {
			return []byte("I got the lock :D")
		},
	}

	_, err = writer.CommitTo(ctx, storage)
	return err
}
```

## Example 2: RwLock

We can use `Verify()` to check whether there is a conflicting lock.

Before we start, let's involve a helper function of the verify context:

```go
// assertNoOtherOfPrefixExpect asserts that there is no other file with the same prefix than the expect file.
func (cx VerifyWriteContext) assertNoOtherOfPrefixExpect(pfx string, expect string) error {
	fileName := path.Base(pfx)
	dirName := path.Dir(pfx)
	return cx.Storage.WalkDir(cx, &WalkOption{
		SubDir:    dirName,
		ObjPrefix: fileName,
	}, func(path string, size int64) error {
		if path != expect {
			return fmt.Errorf("there is conflict file %s", path)
		}
		return nil
	})
}
```

Then, when adding a write lock, we need to verify that there isn't anyone holds any sort of locks...

Be aware that we are going to put a lock file at `"$path.WRIT"` instead of `"$path"`.

```go
func TryLockRemoteWrite(ctx context.Context, storage ExternalStorage, path string) error {
	target := fmt.Sprintf("%s.WRIT", path)
	writer := ExclusiveWrite{
		Target: target,
		Content: func(txnID uuid.UUID) []byte {
			return []byte("I'm going to write something down :<")
		},
		Verify: func(ctx VerifyWriteContext) error {
			return ctx.assertNoOtherOfPrefixExpect(path, ctx.IntentFileName())
		},
	}

	_, err = writer.CommitTo(ctx, storage)
	return err
}
```

When putting a read lock, we need to check that if there is a write lock...

```go
func TryLockRemoteRead(ctx context.Context, storage ExternalStorage, path string) error
	readID := rand.Int63()
	target := fmt.Sprintf("%s.READ.%016x", path, readID)
	writeLock := fmt.Sprintf("%s.WRIT", target)
	writer := ExclusiveWrite{
		Target: target,
		Content: func(txnID uuid.UUID) []byte {
			return []byte("Guess what we will find today =D")
		},
		Verify: func(ctx VerifyWriteContext) error {
			// Make sure that the write lock doesn't exist.
			return ctx.assertNoOtherOfPrefixExpect(writeLock, "")
		},
	}

	_, err = writer.CommitTo(ctx, storage)
	return err
}
```

Notice that we are putting a read lock with a random suffix. So read locks won't conflict with each other.
