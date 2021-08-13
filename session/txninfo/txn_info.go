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
	"encoding/json"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// TxnRunningState is the current state of a transaction
type TxnRunningState = int32

const (
	// TxnIdle means the transaction is idle, i.e. waiting for the user's next statement
	TxnIdle TxnRunningState = iota
	// TxnRunning means the transaction is running, i.e. executing a statement
	TxnRunning
	// TxnLockWaiting means the transaction is blocked on a lock
	TxnLockWaiting
	// TxnCommitting means the transaction is (at least trying to) committing
	TxnCommitting
	// TxnRollingBack means the transaction is rolling back
	TxnRollingBack
)

const (
	// IDStr is the column name of the TIDB_TRX table's ID column.
	IDStr = "ID"
	// StartTimeStr is the column name of the TIDB_TRX table's StartTime column.
	StartTimeStr = "START_TIME"
	// CurrentSQLDigestStr is the column name of the TIDB_TRX table's CurrentSQLDigest column.
	CurrentSQLDigestStr = "CURRENT_SQL_DIGEST"
	// CurrentSQLDigestTextStr is the column name of the TIDB_TRX table's CurrentSQLDigestText column.
	CurrentSQLDigestTextStr = "CURRENT_SQL_DIGEST_TEXT"
	// StateStr is the column name of the TIDB_TRX table's State column.
	StateStr = "STATE"
	// WaitingStartTimeStr is the column name of the TIDB_TRX table's WaitingStartTime column.
	WaitingStartTimeStr = "WAITING_START_TIME"
	// MemBufferKeysStr is the column name of the TIDB_TRX table's MemBufferKeys column.
	MemBufferKeysStr = "MEM_BUFFER_KEYS"
	// MemBufferBytesStr is the column name of the TIDB_TRX table's MemBufferBytes column.
	MemBufferBytesStr = "MEM_BUFFER_BYTES"
	// SessionIDStr is the column name of the TIDB_TRX table's SessionID column.
	SessionIDStr = "SESSION_ID"
	// UserStr is the column name of the TIDB_TRX table's User column.
	UserStr = "USER"
	// DBStr is the column name of the TIDB_TRX table's DB column.
	DBStr = "DB"
	// AllSQLDigestsStr is the column name of the TIDB_TRX table's AllSQLDigests column.
	AllSQLDigestsStr = "ALL_SQL_DIGESTS"
)

// TxnRunningStateStrs is the names of the TxnRunningStates
var TxnRunningStateStrs = []string{
	"Idle", "Running", "LockWaiting", "Committing", "RollingBack",
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
	// Last trying to block start time. Invalid if State is not TxnLockWaiting.
	BlockStartTime struct {
		Valid bool
		time.Time
	}
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

var columnValueGetterMap = map[string]func(*TxnInfo) types.Datum{
	IDStr: func(info *TxnInfo) types.Datum {
		return types.NewDatum(info.StartTS)
	},
	StartTimeStr: func(info *TxnInfo) types.Datum {
		humanReadableStartTime := time.Unix(0, oracle.ExtractPhysical(info.StartTS)*1e6)
		return types.NewDatum(types.NewTime(types.FromGoTime(humanReadableStartTime), mysql.TypeTimestamp, types.MaxFsp))
	},
	CurrentSQLDigestStr: func(info *TxnInfo) types.Datum {
		if len(info.CurrentSQLDigest) != 0 {
			return types.NewDatum(info.CurrentSQLDigest)
		}
		return types.NewDatum(nil)
	},
	StateStr: func(info *TxnInfo) types.Datum {
		e, err := types.ParseEnumValue(TxnRunningStateStrs, uint64(info.State+1))
		if err != nil {
			panic("this should never happen")
		}

		state := types.NewMysqlEnumDatum(e)
		return state
	},
	WaitingStartTimeStr: func(info *TxnInfo) types.Datum {
		if !info.BlockStartTime.Valid {
			return types.NewDatum(nil)
		}
		return types.NewDatum(types.NewTime(types.FromGoTime(info.BlockStartTime.Time), mysql.TypeTimestamp, types.MaxFsp))
	},
	MemBufferKeysStr: func(info *TxnInfo) types.Datum {
		return types.NewDatum(info.EntriesCount)
	},
	MemBufferBytesStr: func(info *TxnInfo) types.Datum {
		return types.NewDatum(info.EntriesSize)
	},
	SessionIDStr: func(info *TxnInfo) types.Datum {
		return types.NewDatum(info.ConnectionID)
	},
	UserStr: func(info *TxnInfo) types.Datum {
		return types.NewDatum(info.Username)
	},
	DBStr: func(info *TxnInfo) types.Datum {
		return types.NewDatum(info.CurrentDB)
	},
	AllSQLDigestsStr: func(info *TxnInfo) types.Datum {
		allSQLDigests := info.AllSQLDigests
		// Replace nil with empty array
		if allSQLDigests == nil {
			allSQLDigests = []string{}
		}
		res, err := json.Marshal(allSQLDigests)
		if err != nil {
			logutil.BgLogger().Warn("Failed to marshal sql digests list as json", zap.Uint64("txnStartTS", info.StartTS))
			return types.NewDatum(nil)
		}
		return types.NewDatum(string(res))
	},
}

// ToDatum Converts the `TxnInfo`'s specified column to `Datum` to show in the `TIDB_TRX` table.
func (info *TxnInfo) ToDatum(column string) types.Datum {
	res, ok := columnValueGetterMap[column]
	if !ok {
		return types.NewDatum(nil)
	}
	return res(info)
}
