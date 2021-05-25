// Copyright 2021 PingCAP, Inc.
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

package deadlockhistory

import (
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/parser/mysql"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/resourcegrouptag"
	"go.uber.org/zap"
)

// WaitChainItem represents an entry in a deadlock's wait chain.
type WaitChainItem struct {
	TryLockTxn     uint64
	SQLDigest      string
	Key            []byte
	AllSQLs        []string
	TxnHoldingLock uint64
}

// DeadlockRecord represents a deadlock events, and contains multiple transactions' information.
type DeadlockRecord struct {
	// The ID doesn't need to be set manually and it's set when it's added into the DeadlockHistory by invoking its Push
	// method.
	ID          uint64
	OccurTime   time.Time
	IsRetryable bool
	WaitChain   []WaitChainItem
}

// DeadlockHistory is a collection for maintaining recent several deadlock events.
type DeadlockHistory struct {
	sync.RWMutex

	deadlocks []*DeadlockRecord

	// The `head` and `size` makes the `deadlocks` array behaves like a deque. The valid elements are
	// deadlocks[head:head+size], or deadlocks[head:] + deadlocks[:head+size-len] if `head+size` exceeds the array's
	// length.
	head int
	size int

	// currentID is used to allocate IDs for deadlock records pushed to the queue that's unique in the deadlock
	// history queue instance.
	currentID uint64
}

// NewDeadlockHistory creates an instance of DeadlockHistory
func NewDeadlockHistory(capacity int) *DeadlockHistory {
	return &DeadlockHistory{
		deadlocks: make([]*DeadlockRecord, capacity),
		currentID: 1,
	}
}

// GlobalDeadlockHistory is the global instance of DeadlockHistory, which is used to maintain recent several recent
// deadlock events globally.
// TODO: Make the capacity configurable
var GlobalDeadlockHistory = NewDeadlockHistory(10)

// Push pushes an element into the queue. It will set the `ID` field of the record, and add the pointer directly to
// the collection. Be aware that do not modify the record's content after pushing.
func (d *DeadlockHistory) Push(record *DeadlockRecord) {
	d.Lock()
	defer d.Unlock()

	capacity := len(d.deadlocks)
	if capacity == 0 {
		return
	}

	record.ID = d.currentID
	d.currentID++

	if d.size == capacity {
		// The current head is popped and it's cell becomes the latest pushed item.
		d.deadlocks[d.head] = record
		d.head = (d.head + 1) % capacity
	} else if d.size < capacity {
		d.deadlocks[(d.head+d.size)%capacity] = record
		d.size++
	} else {
		panic("unreachable")
	}
}

// GetAll gets all collected deadlock events.
func (d *DeadlockHistory) GetAll() []*DeadlockRecord {
	d.RLock()
	defer d.RUnlock()

	res := make([]*DeadlockRecord, 0, d.size)
	capacity := len(d.deadlocks)
	if d.head+d.size <= capacity {
		res = append(res, d.deadlocks[d.head:d.head+d.size]...)
	} else {
		res = append(res, d.deadlocks[d.head:]...)
		res = append(res, d.deadlocks[:(d.head+d.size)%capacity]...)
	}
	return res
}

// GetAllDatum gets all collected deadlock events, and make it into datum that matches the definition of the table
// `INFORMATION_SCHEMA.DEADLOCKS`.
func (d *DeadlockHistory) GetAllDatum() [][]types.Datum {
	records := d.GetAll()
	rowsCount := 0
	for _, rec := range records {
		rowsCount += len(rec.WaitChain)
	}

	rows := make([][]types.Datum, 0, rowsCount)

	row := make([]interface{}, 8)
	for _, rec := range records {
		row[0] = rec.ID
		row[1] = types.NewTime(types.FromGoTime(rec.OccurTime), mysql.TypeTimestamp, types.MaxFsp)
		row[2] = rec.IsRetryable

		for _, item := range rec.WaitChain {
			row[3] = item.TryLockTxn

			row[4] = nil
			if len(item.SQLDigest) > 0 {
				row[4] = item.SQLDigest
			}

			row[5] = nil
			if len(item.Key) > 0 {
				row[5] = strings.ToUpper(hex.EncodeToString(item.Key))
			}

			row[6] = nil
			if item.AllSQLs != nil {
				row[6] = "[" + strings.Join(item.AllSQLs, ", ") + "]"
			}

			row[7] = item.TxnHoldingLock

			rows = append(rows, types.MakeDatums(row...))
		}
	}

	return rows
}

// Clear clears content from deadlock histories
func (d *DeadlockHistory) Clear() {
	d.Lock()
	defer d.Unlock()
	for i := 0; i < len(d.deadlocks); i++ {
		d.deadlocks[i] = nil
	}
	d.head = 0
	d.size = 0
}

// ErrDeadlockToDeadlockRecord generates a DeadlockRecord from the information in a `tikverr.ErrDeadlock` error.
func ErrDeadlockToDeadlockRecord(dl *tikverr.ErrDeadlock) *DeadlockRecord {
	waitChain := make([]WaitChainItem, 0, len(dl.WaitChain))
	for _, rawItem := range dl.WaitChain {
		sqlDigest, err := resourcegrouptag.DecodeResourceGroupTag(rawItem.ResourceGroupTag)
		if err != nil {
			logutil.BgLogger().Warn("decoding resource group tag encounters error", zap.Error(err))
		}
		waitChain = append(waitChain, WaitChainItem{
			TryLockTxn:     rawItem.Txn,
			SQLDigest:      hex.EncodeToString(sqlDigest),
			Key:            rawItem.Key,
			AllSQLs:        nil,
			TxnHoldingLock: rawItem.WaitForTxn,
		})
	}
	rec := &DeadlockRecord{
		OccurTime:   time.Now(),
		IsRetryable: dl.IsRetryable,
		WaitChain:   waitChain,
	}
	return rec
}
