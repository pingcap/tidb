# Optimize GC Advance For Internal Transactions

- Author(s): [Michael](https://github.com/TonsnakeLin) 
- Tracking Issue: https://github.com/pingcap/tidb/issues/32725

## Introduction

This document describes the design of the feature "optimize gc advance for internal transaction", which adds the startTS(start timestamp) of all internal transactions to the gc(garbage collect) safepoint calculation process.

## Motivation or Background

TiDB advances gc safepoint every `tidb_gc_run_interval` time with the value `now - tidb_gc_life_time`. If there is a long time transaction from user client lives more than `tidb_gc_life_time`, the safepoint can't be advanced until the transaction is finished or lives over 24 hours. This mechanism ensures the continuous advancement of gc safepoint and ensures that the data active transactions need to access will not be cleared.

However, Internal transactions run in TiDB don't comply with the mechanism above. If the internal transaction lives more than `tidb_gc_life_time`, it may fail because the data it needs to access was cleared. This design aims to resolve the problem that internal transaction run failed because data is cleared by the gc mechanism.

## Detailed Design

`Design Summary` describes the main processes of safepoint calculation and what job this design did.

### Design Summary

Currently here are two kinds of internal transactions. one is run by internal session, the other is run by function `RunInNewTxn`. This design aims to take account of all internal transactions when calculate gc safepoint.

The design stores internal sessions to session manager and stores the internal transactions to `globalInnerTxnTsBox` so that it can get startTS of internal transactions when calculates gc safepoint.

#### Calculate GC SafePoint

Currently, the processes TiDB calculates safepoint is as bellow, it is implemented in `(is *InfoSyncer) ReportMinStartTS`

- Get current timestamp from PD, save it to variable `now`,	 get timestamp that is 24 hours earlier than `now`, save it to variable `startTSLowerLimit`
- Get all user client sessions, save them to slice `processlist`
- Traverse `processlist` to get startTS from every client session and compare the `startTS` with `now` to get the minimum timestamp which is after `startTSLowerLimit` , save it as minStartTS which is gc safepoint.

This design add extra processes in `(is *InfoSyncer) ReportMinStartTS` to take account of internal transactions startTS when calculates gc safepoint. The new processes TiDB calculates safepoint is as bellow.
  - Get current timestamp from PD, save it to variable `now`, get timestamp that is earlier than `now` which is specified by system variable `tidb_gc_txn_max_wait_time`, save it to variable `startTSLowerLimit`
- Get all user client sessions, save them to slice `processlist`
- Get the startTS of every internal transaction run by internal session, save it to slice `InnerSessionStartTSList`
- Get the startTS of every internal transaction run by `RunInNewTxn`, save it to map `innerTxnStartTsMap`
- Traverse `processlist` to get startTS from every client session and compare the `startTS` with `now` to get the minimum timestamp which is after `startTSLowerLimit` , save it as `minStartTS`.
- Traverse `InnerSessionStartTSList` to get startTS and compare with `minStartTS` to get the minimum timestamp which is after `startTSLowerLimit` , save it also as `minStartTS`.
- Traverse `innerTxnStartTsMap` to get startTS and compare with `minStartTS` to get the minimum timestamp which is after `startTSLowerLimit` , save it also as `minStartTS` which is gc safepoint.

#### Store And Delete Internal Sessions

TiDB gets an internal session from a session pool implemented in `(s *session) getInternalSession`, when the transaction finished,it puts the session to the pool. It stores the session to session manager when gets an internal session, delete the session from session manager when returns the session to a pool.

#### Store And Delete Internal Transactions

The internal transactions here are executed by function `RunInNewTxn`. It stores the transaction to `globalInnerTxnTsBox` at the transaction's beginning and deletes the transaction at the end.
### Data Structure Design

#### SessionManager 

TiDB manages client sessions by SessionManager interface. This design expands the SessionManager interface to manage internal sessions too.

```go
type SessionManager interface {
	// Put the internal session pointer to the map in the SessionManager
	StoreInternalSession(se interface{})
	// Delete the internal session pointer from the map in the SessionManager
	DeleteInternalSession(se interface{})
	// Get all startTS of every transactions running in the current internal sessions
	GetInternalSessionStartTSList() []uint64
}
```

#### innerTxnStartTsBox 

This design defines a global variable `globalInnerTxnTsBox` to manage internal transactions run by function `RunInNewTxn`. The map `innerTxnStartTsBox.innerTxnStartTsMap` stores all the internal transaction startTS. 

```go
var globalInnerTxnTsBox = innerTxnStartTsBox{
	innerTSLock:		sync.Mutex{},
	innerTxnStartTsMap: make(map[uint64]struct{}, 256),
}

type innerTxnStartTsBox struct {
	innerTSLock		   sync.Mutex
	innerTxnStartTsMap map[uint64]struct{}
}
```

### Functional design

#### Store And Delete Internal Sessions

TiDB gets an internal session from a session pool implemented in `(s *session) getInternalSession`, when the transaction finished,it puts the session to the pool. This design stores the internal session to session manger in the function `(s *session) getInternalSession` and deletes the internal session from session manager in the callback returned by `getInternalSession`. It calls the function `infosync.DeleteInternalSession()` to delete the internal session from session manager and calls the function `infosync.StoreInternalSession` to add the internal session to session manger.

```go
func (s *session) getInternalSession(execOption sqlexec.ExecOption) (*session, func(), error) {
	tmp, err := s.sysSessionPool().Get()
	se := tmp.(*session)
	// Put the internal session to the map of SessionManager
	infosync.StoreInternalSession(se)
	return se, func() {
		// Delete the internal session to the map of SessionManager
		infosync.DeleteInternalSession(se)
		s.sysSessionPool().Put(tmp)
	}
}
```

#### Store And Delete Internal Transactions

The internal transactions here are executed by function `RunInNewTxn`. It stores the internal transaction startTS by function  `wrapStoreInterTxnTS()` and deletes the transaction startTS by function `wrapDeleteInterTxnTS()`.

```go
// RunInNewTxn will run the f in a new transaction environment.
func RunInNewTxn(ctx context.Context, store Storage, retryable bool, f func(ctx context.Context, txn Transaction) error) error {
	defer func() {
		wrapDeleteInterTxnTS(originalTxnTS)
	}()
	for i := uint(0); i < maxRetryCnt; i++ {
		txn, err = store.Begin()
		if i == 0 {
			originalTxnTS = txn.StartTS()
			wrapStoreInterTxnTS(originalTxnTS)
		}
		err = txn.Commit(ctx)
		...
	}
}
```

#### Calculate GC Safepoint

`ReportMinStartTS` calculates the minimum startTS of ongoing transactions (except the ones that exceed the limit), and it's result is useful for calculating the safepoint, which is gc_worker's work.This design add some code in the  function `ReportMinStartTS` to consider internal transactions when calculates the minimum startTS.

```go
func (is *InfoSyncer) ReportMinStartTS(store kv.Storage) {}
```

## Compatibility

This feature does not affect compatibility.

## Unresolved Questions

Currently, It prints a log message such as "An internal transaction running by RunInNewTxn lasts more than 1 minute" when an internal transaction lasts more than 1 minute. In the future, we should print more detail messages in the log and add some monitor data showed on the grafana panel.
