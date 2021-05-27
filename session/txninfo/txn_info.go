// Copyright 2021 PingCAP, Inc.

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

package txninfo

import (
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
)

// TxnRunningState is the current state of a transaction
type TxnRunningState = int32

const (
	// TxnRunningNormal means the transaction is running normally
	TxnRunningNormal TxnRunningState = iota
	// TxnLockWaiting means the transaction is blocked on a lock
	TxnLockWaiting
	// TxnCommitting means the transaction is (at least trying to) committing
	TxnCommitting
	// TxnRollingBack means the transaction is rolling back
	TxnRollingBack
)

// TxnRunningStateStrs is the names of the TxnRunningStates
var TxnRunningStateStrs = []string{
	"Normal", "LockWaiting", "Committing", "RollingBack",
}

// TxnInfo is information about a running transaction
// This is supposed to be the datasource of `TIDB_TRX` in infoschema
type TxnInfo struct {
	// The following fields are immutable and can be safely read across threads.

	StartTS uint64
	// Digest of SQL currently running
	CurrentSQLDigest string
	// Digests of all SQLs executed in the transaction.
	AllSQLDigests []string

	// The following fields are mutable and needs to be read or written by atomic operations. But since only the
	// transaction's thread can modify its value, it's ok for the transaction's thread to read it without atomic
	// operations.

	// Current execution state of the transaction.
	State TxnRunningState
	// Last trying to block start time. Invalid if State is not TxnLockWaiting. It's an unsafe pointer to time.Time or nil.
	BlockStartTime unsafe.Pointer
	// How many entries are in MemDB
	EntriesCount uint64
	// MemDB used memory
	EntriesSize uint64

	// The following fields will be filled in `session` instead of `LazyTxn`

	// Which session this transaction belongs to
	ConnectionID uint64
	// The user who open this session
	Username string
	// The schema this transaction works on
	CurrentDB string
}

// ShallowClone shallow clones the TxnInfo. It's safe to call concurrently with the transaction.
// Note that this function doesn't do deep copy and some fields of the result may be unsafe to write. Use it at your own
// risk.
func (info *TxnInfo) ShallowClone() *TxnInfo {
	return &TxnInfo{
		StartTS:          info.StartTS,
		CurrentSQLDigest: info.CurrentSQLDigest,
		AllSQLDigests:    info.AllSQLDigests,
		State:            atomic.LoadInt32(&info.State),
		BlockStartTime:   atomic.LoadPointer(&info.BlockStartTime),
		EntriesCount:     atomic.LoadUint64(&info.EntriesCount),
		EntriesSize:      atomic.LoadUint64(&info.EntriesSize),
		ConnectionID:     info.ConnectionID,
		Username:         info.Username,
		CurrentDB:        info.CurrentDB,
	}
}

// ToDatum Converts the `TxnInfo` to `Datum` to show in the `TIDB_TRX` table.
func (info *TxnInfo) ToDatum() []types.Datum {
	// TODO: The timezone represented to the user is not correct and it will be always UTC time.
	humanReadableStartTime := time.Unix(0, oracle.ExtractPhysical(info.StartTS)*1e6).UTC()

	var currentDigest interface{}
	if len(info.CurrentSQLDigest) != 0 {
		currentDigest = info.CurrentSQLDigest
	}

	var blockStartTime interface{}
	if t := (*time.Time)(atomic.LoadPointer(&info.BlockStartTime)); t == nil {
		blockStartTime = nil
	} else {
		blockStartTime = types.NewTime(types.FromGoTime(*t), mysql.TypeTimestamp, types.MaxFsp)
	}

	e, err := types.ParseEnumValue(TxnRunningStateStrs, uint64(info.State+1))
	if err != nil {
		panic("this should never happen")
	}

	allSQLs := "[" + strings.Join(info.AllSQLDigests, ", ") + "]"

	state := types.NewMysqlEnumDatum(e)

	datums := types.MakeDatums(
		info.StartTS,
		types.NewTime(types.FromGoTime(humanReadableStartTime), mysql.TypeTimestamp, types.MaxFsp),
		currentDigest,
	)
	datums = append(datums, state)
	datums = append(datums, types.MakeDatums(
		blockStartTime,
		info.EntriesCount,
		info.EntriesSize,
		info.ConnectionID,
		info.Username,
		info.CurrentDB,
		allSQLs)...)
	return datums
}
