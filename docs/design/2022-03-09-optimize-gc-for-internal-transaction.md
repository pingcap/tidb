# Optimize GC Advance For Internal Transactions

- Author(s): [Michael](https://github.com/TonsnakeLin) 
- Tracking Issue: https://github.com/pingcap/tidb/issues/32725

## Introduction

This document describes the design of the feature "optimize gc advance for internal transaction", which adds the startTS(start time stamp) of all internal transactions to the gc(garbage collect) safe point calculation process.

## Motivation or Background

TiDB advances gc safe point every `tidb_gc_run_interval` time with step `tidb_gc_life_time`. If there is a long time transaction from user client lives more than `tidb_gc_life_time`, the safe point can't be advanced until the transaction is finished or lives over 24 hours. This mechanism ensures the continuous advancement of gc safe point and ensures that the data active transactions need to access will not be cleared.

However, Internal transactions run in TiDB don't comply with the mechanism above. If the internal transaction lives more than `tidb_gc_life_time`, maybe failed because the data it needs to access was cleared. This design aims to resolve the problem that internal transaction run failed because data is cleared by the gc mechanism.

## Detailed Design

`Design Summary` describes the main processes of safe point calculation and what job this design did.

### Design Summary

Currently here are two kinds of internal transactions. one is run by internal session, the other is run by function `RunInNewTxn`. This design aims to take account of all internal transactions when calculate gc safe point.

The design stores internal sessions to session manager and stores the internal transactions to `globalInnerTxnTsBox` so that it can get startTS of internal transactions when calculates gc safe point.

#### Calculate GC Safe Point

Currently, the processes TiDB calculates safe point is as bellow, it is implemented in `(is *InfoSyncer) ReportMinStartTS`

- Get current time stamp from PD, save it to variable `now`,  get time stamp that is 24 hours earlier than `now`, save it to variable `startTSLowerLimit`
- Get all user client sessions, save them to slice `processlist`
- Traverse `processlist` to get startTS from every client session and compare the `startTS` with `now` to get the minimum time stamp which is after `startTSLowerLimit` , save it as minStartTS which is gc safe point.

This design add extra processes  in `(is *InfoSyncer) ReportMinStartTS` to take account of internal transactions startTS when calculates gc safe point . The new processes TiDB calculates safe point is as bellow.

- Get current time stamp from PD, save it to variable `now`,  get time stamp that is earlier than `now` which is specified by system variable `tidb_gc_txn_max_wait_time`, save it to variable `startTSLowerLimit`
- Get all user client sessions, save them to slice `processlist`
- Get the startTS of every internal transaction run by internal session, save it to slice `InnerSessionStartTSList`
- Get the startTS of every internal transaction run by `RunInNewTxn`, save it to map `innerTxnStartTsMap`
- Traverse `processlist` to get startTS from every client session and compare the `startTS` with `now` to get the minimum time stamp which is after `startTSLowerLimit` , save it as `minStartTS`.
- Traverse `InnerSessionStartTSList` to get startTS and compare with `minStartTS` to get the minimum time stamp which is after `startTSLowerLimit` , save it also as `minStartTS`.
- Traverse `innerTxnStartTsMap` to get startTS and compare with `minStartTS` to get the minimum time stamp which is after `startTSLowerLimit` , save it also as `minStartTS` which is gc safe point.

#### Store And Delete Internal Sessions

TiDB gets an internal session from a session pool implemented in `(s *session) getInternalSession`, when the transaction finished,it puts the session to the pool. It stores the session to session manager when gets an internal session, delete the session from session manager when returns the session to a pool.

#### Store And Delete Internal Transactions

The internal transactions here are executed by function `RunInNewTxn`. It stores the transaction to  `globalInnerTxnTsBox`  at transaction begin and deletes the transaction at end.

### Data Structure Design

#### SessionManager 

TiDB manages client sessions by SessionManager interface. This design expands the SessionManager interface to manage internal sessions too.

```go
type SessionManager interface {
	// Put the internal session pointer to the map in the SessionManager
	StoreInternalSession(addr unsafe.Pointer)
	// Delete the internal session pointer from the map in the SessionManager
	DeleteInternalSession(addr unsafe.Pointer)
	// Get all startTS of every transactions running in the current internal sessions
	GetInternalSessionStartTSList() []uint64
}
```



#### innerTxnStartTsBox 

This design defines a global variable `globalInnerTxnTsBox` to manage internal transactions run by function `RunInNewTxn`. The map `innerTxnStartTsBox.innerTxnStartTsMap` stores all the internal transaction startTS. 

```go
var globalInnerTxnTsBox atomic.Value

type innerTxnStartTsBox struct {
	wg                  sync.WaitGroup
	chanToStoreStartTS  chan uint64
	chanToDeleteStartTS chan uint64

	innerTxnTsBoxRun atomic.Value
	
	innerTSLock        sync.Mutex
	innerTxnStartTsMap map[uint64]uint64
	
	// `storeInnerTxnStartTsLoop` and `deleteInnerTxnStartTsLoop` run asynchronously,It can't ensure  
    //`storeInnerTxnStartTsLoop` receives startTS before `deleteInnerTxnStartTsLoop`,though 
    // `RunInNewTxn`send startTS to `storeInnerTxnStartTsLoop` firstly. If `deleteInnerTxnStartTsLoop`
	// recevied startTS before `storeInnerTxnStartTsLoop`, the `startTS` couldn't be deleted from 
    // `innerTxnStartTsMap`,and GC safepoint can't be advanced properly.
	undeletedTsLock        sync.Mutex
	chanToProcUndelStartTS chan uint64
	undeletedStartTsMap    map[uint64]uint64

}
```



### Functional design

#### Initialize Internal Transaction Global Management Box

This design initializes the global variable `globalInnerTxnTsBox` and starts 3 goroutines in the `main()` function to manage internal transactions run by function `RunInNewTxn()`. The initialization is wrapped in the function `InitInnerTxnStartTsBox()`. It closes the relevant goroutines in the function `cleanup()`

```go
func main() {
	kv.InitInnerTxnStartTsBox()
    signal.SetupSignalHandler(func(graceful bool) {
        svr.Close()
        cleanup(svr, storage, dom, graceful)
        cpuprofile.StopCPUProfiler()
        close(exited)
    })
}
```



the main code for `InitInnerTxnStartTsBox()` is as follower. It starts `storeInnerTxnStartTsLoop` goroutine to store an internal transaction startTS in `globalInnerTxnTsBox.innerTxnStartTsMap`, starts `deleteInnerTxnStartTsLoop` goroutine to delete an internal transaction startTS from `globalInnerTxnTsBox.innerTxnStartTsMap`. In particular, It starts `processUndeletedStartTsLoop` to delete the startTS for some rarely scenes.

```go
func InitInnerTxnStartTsBox() {
	iTxnTsBox := &innerTxnStartTsBox{
		chanToStoreStartTS:     make(chan uint64, chanBufferSize),
		chanToDeleteStartTS:    make(chan uint64, chanBufferSize),
		innerTxnStartTsMap:     make(map[uint64]uint64, chanBufferSize),
		chanToProcUndelStartTS: make(chan uint64, undeletedStartTsBufferSize),
		undeletedStartTsMap:    make(map[uint64]uint64, undeletedStartTsBufferSize),
	}
	globalInnerTxnTsBox.Store(iTxnTsBox)
	iTxnTsBox.wg.Add(3)
	iTxnTsBox.innerTxnTsBoxRun.Store(true)
	go iTxnTsBox.storeInnerTxnStartTsLoop()
	go iTxnTsBox.deleteInnerTxnStartTsLoop()
	go iTxnTsBox.processUndeletedStartTsLoop()
}
```



#### Store And Delete Internal Sessions

TiDB gets an internal session from a session pool implemented in `(s *session) getInternalSession`, when the transaction finished,it puts the session to the pool. This design stores the internal session to session manger and deletes the internal session from session manager in the function  `(s *session) getInternalSession`. It calls the function `infosync.DeleteInternalSession()` to delete the internal session from session manager and calls the function `infosync.StoreInternalSession` to add the internal session to session manger.

```go
func (s *session) getInternalSession(execOption sqlexec.ExecOption) (*session, func(), error) {
	tmp, err := s.sysSessionPool().Get()
	se := tmp.(*session)
	// Put the internal session to the map of SessionManager
	infosync.StoreInternalSession(unsafe.Pointer(se))
	return se, func() {
		// Delete the internal session to the map of SessionManager
		infosync.DeleteInternalSession(unsafe.Pointer(se))
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
        ..........................................
    }
}
```

#### Calculate GC Safe Point

Currently, TiDB calculates gc safe point in the function `(is *InfoSyncer) ReportMinStartTS`. This design add some code in the 

function `ReportMinStartTS` to consider internal transactions when calculates gc safe point.

```go
func (is *InfoSyncer) ReportMinStartTS(store kv.Storage) {
}
```



## Compatibility

This feature does not affect compatibility.



## Unresolved Questions

Currently, It prints a log message such as "An internal transaction running by RunInNewTxn lasts more than 1 minute" when an internal transaction lasts more than 1 minute. In the future, we should print more detail messages in the log and add some monitor data showed on the grafana panel.
