// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv/latch"
	binlog "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type txnCommitter struct {
	twoPC *twoPhaseCommitter
	ch    chan error
	ctx   context.Context
	lock  *latch.Lock
}

func newTxnCommiter(ctx context.Context, committer *twoPhaseCommitter) *txnCommitter {
	return &txnCommitter{
		twoPC: committer,
		ctx:   ctx,
	}
}

func (tc *txnCommitter) commit(stale bool) uint64 {
	if stale {
		err := errors.Errorf("startTS %d is stale", tc.twoPC.startTS)
		log.Debug(tc.twoPC.connID, err)
		tc.ch <- errors.Annotate(err, txnRetryableMark)
		return uint64(0)
	}

	commitTS, err := tc.execute()
	tc.ch <- errors.Trace(err)
	log.Debug(tc.twoPC.connID, " finish txn with startTS:", tc.twoPC.startTS, " commitTS:", commitTS, " error:", err)
	return commitTS
}

func (tc *txnCommitter) execute() (commitTS uint64, err error) {
	err = tc.twoPC.execute(tc.ctx)
	if err != nil {
		tc.twoPC.writeFinishBinlog(binlog.BinlogType_Rollback, 0)
	} else {
		commitTS = tc.twoPC.commitTS
		tc.twoPC.writeFinishBinlog(binlog.BinlogType_Commit, int64(commitTS))
	}
	return
}

type txnMap struct {
	txns map[uint64]*txnCommitter
	sync.RWMutex
}

func (t *txnMap) put(startTS uint64, txn *txnCommitter) {
	t.Lock()
	defer t.Unlock()
	if t.txns == nil {
		t.txns = make(map[uint64]*txnCommitter)
	}
	if _, ok := t.txns[startTS]; ok {
		panic(fmt.Sprintf("The startTS %d shouldn't be used in two transactions.", txn.twoPC.startTS))
	}
	t.txns[startTS] = txn
}

func (t *txnMap) get(startTS uint64) *txnCommitter {
	t.RLock()
	defer t.RUnlock()
	return t.txns[startTS]
}

func (t *txnMap) delete(startTS uint64) {
	t.Lock()
	defer t.Unlock()
	delete(t.txns, startTS)
}

// txnScheduler is the scheduler to commit transactions.
type txnScheduler struct {
	latches *latch.Latches
	txnMaps []txnMap
}

const (
	txnSchedulerLatchesSize = 102400
	txnSchedulerMapsCount   = 256
)

func newTxnScheduler(enableLatches bool) *txnScheduler {
	if !enableLatches {
		return &txnScheduler{
			latches: nil,
			txnMaps: nil,
		}
	}
	return &txnScheduler{
		latches: latch.NewLatches(txnSchedulerLatchesSize),
		txnMaps: make([]txnMap, txnSchedulerMapsCount),
	}
}

func (scheduler *txnScheduler) execute(ctx context.Context, txn *twoPhaseCommitter, connID uint64) error {
	newCommitter := newTxnCommiter(ctx, txn)
	// latches disabled
	if scheduler.latches == nil {
		_, err := newCommitter.execute()
		return errors.Trace(err)
	}
	// latches enabled
	newCommitter.lock = scheduler.latches.GenLock(txn.startTS, txn.keys)
	// for transactions not retryable, commit directly.
	if !sessionctx.GetRetryable(ctx) {
		err := scheduler.runForUnRetryAble(newCommitter)
		return errors.Trace(err)
	}
	// for transactions which need to acquire latches
	ch := make(chan error, 1)
	newCommitter.ch = ch
	scheduler.putTxn(txn.startTS, newCommitter)
	scheduler.run(txn.startTS)

	err := errors.Trace(<-ch)
	return errors.Trace(err)
}

func (scheduler *txnScheduler) getMap(startTS uint64) *txnMap {
	id := int(startTS % uint64(len(scheduler.txnMaps)))
	return &scheduler.txnMaps[id]
}

func (scheduler *txnScheduler) getTxn(startTS uint64) *txnCommitter {
	return scheduler.getMap(startTS).get(startTS)
}

func (scheduler *txnScheduler) deleteTxn(startTS uint64) {
	scheduler.getMap(startTS).delete(startTS)
}

func (scheduler *txnScheduler) putTxn(startTS uint64, txn *txnCommitter) {
	scheduler.getMap(startTS).put(startTS, txn)
}

// run the transaction directly which is not retryable.
func (scheduler *txnScheduler) runForUnRetryAble(txn *txnCommitter) error {
	commitTS, err := txn.execute()
	if err == nil {
		scheduler.latches.RefreshCommitTS(txn.lock.RequiredSlots(), commitTS)
	}
	log.Debug(txn.twoPC.connID, " finish txn with startTS:", txn.twoPC.startTS, " commitTS:", commitTS, " error:", err)
	return errors.Trace(err)
}

// run a normal transaction, it needs to acquire latches before 2pc.
func (scheduler *txnScheduler) run(startTS uint64) {
	txn := scheduler.getTxn(startTS)
	if txn == nil {
		log.Error("Transaction with StartTS:", startTS, " is not exist")
		return
	}
	acquired, stale := scheduler.latches.Acquire(txn.lock)
	if !stale && !acquired {
		// wait for next wakeup
		return
	}
	commitTS := txn.commit(stale)
	scheduler.deleteTxn(startTS)
	wakeupList := scheduler.latches.Release(txn.lock, commitTS)
	for _, s := range wakeupList {
		go scheduler.run(s)
	}
}
