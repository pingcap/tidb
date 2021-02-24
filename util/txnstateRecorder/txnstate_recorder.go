// Copyright 2020 PingCAP, Inc.
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

// Package txnstateRecorder is for recording the transaction running state on current tidb instance
// so we can display them in `information_schema.TIDB_TRX`
package txnstateRecorder

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/parser/mysql"

	"github.com/pingcap/tidb/types"
)

// TxnRunningState is the current state of a transaction
type TxnRunningState = int

const (
	// TxnRunningNormal means the transaction is running normally
	TxnRunningNormal TxnRunningState = iota
	// TxnLockWaiting means the transaction is blocked on a lock
	TxnLockWaiting TxnRunningState = iota
	// TxnCommitting means the transaction is (at least trying to) committing
	TxnCommitting TxnRunningState = iota
	// TxnRollingBack means the transaction is rolling back
	TxnRollingBack TxnRunningState = iota
)

// txnStateEntry is an entry to be stored in `information_schema.TIDB_TRX`
type txnStateEntry struct {
	// start timestamp of the transaction
	// also worked as transaction id
	startTs uint64
	// todo: Shall we parse startTs to get it?
	// Pros: Save memory, some how is the "global" timestamp in a cluster(so for the CLUSTER_TIDB_TRX, this field would be more useful)
	// Cons: May different with result of "NOW()"
	humanReadableStartTime time.Time
	// digest of SQL current running
	currentSQLDigest string
	// current executing state
	state TxnRunningState
	// how many times the transaction tries to commit
	commitCount uint64
	// last trying of commit start time, nil if commitCount is 0
	commitStartTime *time.Time
	// last trying to block start time
	// todo: currently even if stmtState is not Blocking, blockStartTime is not nil (showing last block), is it the preferred behaviour?
	blockStartTime *time.Time
}

// storage place for `information_schema.TIDB_TRX`
// todo: is it necessary to port executor/concurrent_map.go here?
type stateMap struct {
	sync.Mutex
	items map[uint64]*txnStateEntry
}

var stateStorage = stateMap{
	items: map[uint64]*txnStateEntry{},
}

func (e *txnStateEntry) onStatementStartExecute(sqlDigest string) {
	e.currentSQLDigest = sqlDigest
}

func (e *txnStateEntry) onRollbackStarted() {
	e.state = TxnRollingBack
}

func (e *txnStateEntry) onCommitStarted() {
	e.state = TxnCommitting
	now := time.Now()
	e.commitStartTime = &now
	e.commitCount++
}

func (e *txnStateEntry) onBlocked() {
	e.state = TxnLockWaiting
	now := time.Now()
	e.blockStartTime = &now
}

func (e *txnStateEntry) onUnblocked() {
	e.state = TxnRunningNormal
}

func (e *txnStateEntry) toDatum() []types.Datum {
	var commitStartTime interface{}
	if e.commitStartTime == nil {
		commitStartTime = nil
	} else {
		commitStartTime = types.NewTime(types.FromGoTime(*e.commitStartTime), mysql.TypeTimestamp, 0)
	}
	var blockStartTime interface{}
	if e.blockStartTime == nil {
		blockStartTime = nil
	} else {
		blockStartTime = types.NewTime(types.FromGoTime(*e.blockStartTime), mysql.TypeTimestamp, 0)
	}
	return types.MakeDatums(
		e.startTs,
		types.NewTime(types.FromGoTime(e.humanReadableStartTime), mysql.TypeTimestamp, 0),
		e.currentSQLDigest,
		e.state,
		e.commitCount,
		commitStartTime,
		blockStartTime)
}

// ReportTxnStart is expected to be called when a transaction starts
func ReportTxnStart(txnID uint64) {
	stateStorage.Lock()
	defer stateStorage.Unlock()
	stateStorage.items[txnID] = &txnStateEntry{
		startTs:                txnID,
		humanReadableStartTime: time.Now(),
		currentSQLDigest:       "",
		state:                  TxnRunningNormal,
		commitCount:            0,
		commitStartTime:        nil,
		blockStartTime:         nil,
	}
}

// ReportStatementStartExecute is expected to be called when a statement starts to run in a transaction
func ReportStatementStartExecute(txnID uint64, sqlDigest string) {
	stateStorage.Lock()
	defer stateStorage.Unlock()
	if item, ok := stateStorage.items[txnID]; ok {
		item.onStatementStartExecute(sqlDigest)
	}
}

// ReportRollbackStarted is expected to be called when a transaction starts to rollback
func ReportRollbackStarted(txnID uint64) {
	stateStorage.Lock()
	defer stateStorage.Unlock()
	if item, ok := stateStorage.items[txnID]; ok {
		item.onRollbackStarted()
	}
}

// ReportCommitStarted is expected to be called when a transaction starts to commit (call once for each try)
func ReportCommitStarted(txnID uint64) {
	stateStorage.Lock()
	defer stateStorage.Unlock()
	if item, ok := stateStorage.items[txnID]; ok {
		item.onCommitStarted()
	}
}

// ReportBlocked is expected to be called when a transaction is blocked when trying to acquiring a pessimistic lock
func ReportBlocked(txnID uint64) {
	stateStorage.Lock()
	defer stateStorage.Unlock()
	if item, ok := stateStorage.items[txnID]; ok {
		item.onBlocked()
	}
}

// ReportUnblocked is expected to be called when a transaction is unblocked
func ReportUnblocked(txnID uint64) {
	stateStorage.Lock()
	defer stateStorage.Unlock()
	if item, ok := stateStorage.items[txnID]; ok {
		item.onUnblocked()
	}
}

// ReportTxnEnd is expected to be called when a transaction end
func ReportTxnEnd(txnID uint64) {
	stateStorage.Lock()
	defer stateStorage.Unlock()
	if _, ok := stateStorage.items[txnID]; ok {
		delete(stateStorage.items, txnID)
	}
}

// Datums is used to read all the txn states to Datums, for querying by SQL
func Datums() [][]types.Datum {
	stateStorage.Lock()
	defer stateStorage.Unlock()
	var result [][]types.Datum
	for _, status := range stateStorage.items {
		log.Info("stateStorage", zap.Any("item", status))
		if status != nil {
			result = append(result, status.toDatum())
		}
	}
	return result
}
