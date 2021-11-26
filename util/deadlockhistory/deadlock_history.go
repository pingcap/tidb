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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deadlockhistory

import (
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/resourcegrouptag"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
)

const (
	// ColDeadlockIDStr is the name of the DEADLOCK_ID column in INFORMATION_SCHEMA.DEADLOCKS and INFORMATION_SCHEMA.CLUSTER_DEADLOCKS table.
	ColDeadlockIDStr = "DEADLOCK_ID"
	// ColOccurTimeStr is the name of the OCCUR_TIME column in INFORMATION_SCHEMA.DEADLOCKS and INFORMATION_SCHEMA.CLUSTER_DEADLOCKS table.
	ColOccurTimeStr = "OCCUR_TIME"
	// ColRetryableStr is the name of the RETRYABLE column in INFORMATION_SCHEMA.DEADLOCKS and INFORMATION_SCHEMA.CLUSTER_DEADLOCKS table.
	ColRetryableStr = "RETRYABLE"
	// ColTryLockTrxIDStr is the name of the TRY_LOCK_TRX_ID column in INFORMATION_SCHEMA.DEADLOCKS and INFORMATION_SCHEMA.CLUSTER_DEADLOCKS table.
	ColTryLockTrxIDStr = "TRY_LOCK_TRX_ID"
	// ColCurrentSQLDigestStr is the name of the CURRENT_SQL_DIGEST column in INFORMATION_SCHEMA.DEADLOCKS and INFORMATION_SCHEMA.CLUSTER_DEADLOCKS table.
	ColCurrentSQLDigestStr = "CURRENT_SQL_DIGEST"
	// ColCurrentSQLDigestTextStr is the name of the CURRENT_SQL_DIGEST_TEXT column in INFORMATION_SCHEMA.DEADLOCKS and INFORMATION_SCHEMA.CLUSTER_DEADLOCKS table.
	ColCurrentSQLDigestTextStr = "CURRENT_SQL_DIGEST_TEXT"
	// ColKeyStr is the name of the KEY column in INFORMATION_SCHEMA.DEADLOCKS and INFORMATION_SCHEMA.CLUSTER_DEADLOCKS table.
	ColKeyStr = "KEY"
	// ColKeyInfoStr is the name of the KEY_INFO column in INFORMATION_SCHEMA.DEADLOCKS and INFORMATION_SCHEMA.CLUSTER_DEADLOCKS table.
	ColKeyInfoStr = "KEY_INFO"
	// ColTrxHoldingLockStr is the name of the TRX_HOLDING_LOCK column in INFORMATION_SCHEMA.DEADLOCKS and INFORMATION_SCHEMA.CLUSTER_DEADLOCKS table.
	ColTrxHoldingLockStr = "TRX_HOLDING_LOCK"
)

// WaitChainItem represents an entry in a deadlock's wait chain.
type WaitChainItem struct {
	TryLockTxn     uint64
	SQLDigest      string
	Key            []byte
	AllSQLDigests  []string
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

var columnValueGetterMap = map[string]func(rec *DeadlockRecord, waitChainIdx int) types.Datum{
	ColDeadlockIDStr: func(rec *DeadlockRecord, waitChainIdx int) types.Datum {
		return types.NewDatum(rec.ID)
	},
	ColOccurTimeStr: func(rec *DeadlockRecord, waitChainIdx int) types.Datum {
		return types.NewDatum(types.NewTime(types.FromGoTime(rec.OccurTime), mysql.TypeTimestamp, types.MaxFsp))
	},
	ColRetryableStr: func(rec *DeadlockRecord, waitChainIdx int) types.Datum {
		return types.NewDatum(rec.IsRetryable)
	},
	ColTryLockTrxIDStr: func(rec *DeadlockRecord, waitChainIdx int) types.Datum {
		return types.NewDatum(rec.WaitChain[waitChainIdx].TryLockTxn)
	},
	ColCurrentSQLDigestStr: func(rec *DeadlockRecord, waitChainIdx int) types.Datum {
		digest := rec.WaitChain[waitChainIdx].SQLDigest
		if len(digest) == 0 {
			return types.NewDatum(nil)
		}
		return types.NewDatum(digest)
	},
	ColKeyStr: func(rec *DeadlockRecord, waitChainIdx int) types.Datum {
		key := rec.WaitChain[waitChainIdx].Key
		if len(key) == 0 {
			return types.NewDatum(nil)
		}
		return types.NewDatum(strings.ToUpper(hex.EncodeToString(key)))
	},
	ColTrxHoldingLockStr: func(rec *DeadlockRecord, waitChainIdx int) types.Datum {
		return types.NewDatum(rec.WaitChain[waitChainIdx].TxnHoldingLock)
	},
}

// ToDatum creates the datum for the specified column of `INFORMATION_SCHEMA.DEADLOCKS` table. Usually a single deadlock
// record generates multiple rows, one for each item in wait chain. The first parameter `waitChainIdx` specifies which
// wait chain item is to be used.
func (r *DeadlockRecord) ToDatum(waitChainIdx int, columnName string) types.Datum {
	res, ok := columnValueGetterMap[columnName]
	if !ok {
		return types.NewDatum(nil)
	}
	return res(r, waitChainIdx)
}

// DeadlockHistory is a collection for maintaining recent several deadlock events. All its public APIs are thread safe.
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
func NewDeadlockHistory(capacity uint) *DeadlockHistory {
	return &DeadlockHistory{
		deadlocks: make([]*DeadlockRecord, capacity),
		currentID: 1,
	}
}

// GlobalDeadlockHistory is the global instance of DeadlockHistory, which is used to maintain recent several recent
// deadlock events globally.
// The real size of the deadlock history table should be initialized with `Resize`
// in `setGlobalVars` in tidb-server/main.go
var GlobalDeadlockHistory = NewDeadlockHistory(0)

// Resize update the DeadlockHistory's table max capacity to newCapacity
func (d *DeadlockHistory) Resize(newCapacity uint) {
	d.Lock()
	defer d.Unlock()
	if newCapacity != uint(len(d.deadlocks)) {
		current := d.getAll()
		d.head = 0
		if uint(len(current)) < newCapacity {
			// extend deadlocks
			d.deadlocks = make([]*DeadlockRecord, newCapacity)
			copy(d.deadlocks, current)
		} else {
			// shrink deadlocks, keep the last len(current)-newCapacity items
			// use append here to force golang to realloc the underlying array to save memory
			d.deadlocks = append([]*DeadlockRecord{}, current[uint(len(current))-newCapacity:]...)
			d.size = int(newCapacity)
		}
	}
}

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
	return d.getAll()
}

// getAll is a thread unsafe version of GetAll() for internal use
func (d *DeadlockHistory) getAll() []*DeadlockRecord {
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
			AllSQLDigests:  nil,
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
