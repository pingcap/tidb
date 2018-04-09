package tikv

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/store/tikv/latch"
	binlog "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"sync"
)

type txnCommiter struct {
	commiter *twoPhaseCommitter
	ch       chan error
	ctx      context.Context
	lock     latch.Lock
}

func (txn *txnCommiter) execute(timeout bool) (commitTs uint64) {
	commitTs = uint64(0)
	if timeout {
		err := errors.Errorf("startTs %d timeout", txn.commiter.startTS)
		log.Warn(txn.commiter.connID, err)
		txn.ch <- errors.Annotate(err, txnRetryableMark)
		return
	}

	err := txn.commiter.execute(txn.ctx)

	if err != nil {
		txn.commiter.writeFinishBinlog(binlog.BinlogType_Rollback, 0)
	} else {
		commitTs = txn.commiter.commitTS
		txn.commiter.writeFinishBinlog(binlog.BinlogType_Commit, int64(commitTs))
	}
	txn.ch <- errors.Trace(err)
	log.Debug(txn.commiter.connID, "finish txn with startTs:", txn.commiter.startTS, " commitTs:", commitTs, " error:", err)
	return commitTs
}

type txnStore struct {
	latches latch.Latches
	txns    map[uint64]*txnCommiter
	sync.RWMutex
}

func newTxnStore() txnStore {
	return txnStore{
		latch.NewLatches(1024000), //TODO
		make(map[uint64]*txnCommiter),
		sync.RWMutex{},
	}
}

func (store *txnStore) execute(ctx context.Context, txn *tikvTxn, connID uint64) (err error) {
	commiter, err := newTwoPhaseCommitter(txn, connID)
	if err != nil || commiter == nil {
		return errors.Trace(err)
	}
	ch := make(chan error)
	lock := store.latches.GenLock(commiter.startTS, commiter.keys)
	store.putTxn(&txnCommiter{
		commiter,
		ch,
		ctx,
		lock,
	})

	go store.run(commiter.startTS)
	err = errors.Trace(<-ch)
	if err == nil {
		txn.commitTS = commiter.commitTS
	}
	return err
}

func (store *txnStore) getTxn(startTs uint64) (*txnCommiter, bool) {
	store.RLock()
	defer store.RUnlock()
	c, ok := store.txns[startTs]
	return c, ok
}

func (store *txnStore) putTxn(txn *txnCommiter) {
	store.Lock()
	defer store.Unlock()
	if _, ok := store.txns[txn.commiter.startTS]; ok {
		panic(fmt.Sprintf("The startTs %d shouldn't used in two transaction", txn.commiter.startTS))
	}
	store.txns[txn.commiter.startTS] = txn
}

func (store *txnStore) run(startTs uint64) {
	txn, ok := store.getTxn(startTs)
	if !ok {
		panic(startTs)
	}
	acquired, timeout := store.latches.Acquire(&txn.lock)
	if !timeout && !acquired {
		// wait for next wakeup
		return
	}
	commitTs := txn.execute(timeout)
	wakeupList := store.latches.Release(&txn.lock, commitTs)
	for _, s := range wakeupList {
		go store.run(s)
	}
	store.Lock()
	defer store.Unlock()
	delete(store.txns, startTs)
}
