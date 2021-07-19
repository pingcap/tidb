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
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/v2/oracle"
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

const (
	IDStr                   = "ID"
	StartTimeStr            = "START_TIME"
	CurrentSQLDigestStr     = "CURRENT_SQL_DIGEST"
	CurrentSQLDigestTextStr = "CURRENT_SQL_DIGEST_TEXT"
	StateStr                = "STATE"
	WaitingStartTimeStr     = "WAITING_START_TIME"
	MemBufferKeysStr        = "MEM_BUFFER_KEYS"
	MemBufferBytesStr       = "MEM_BUFFER_BYTES"
	SessionIDStr            = "SESSION_ID"
	UserStr                 = "USER"
	DBStr                   = "DB"
	AllSQLDigestsStr        = "ALL_SQL_DIGESTS"
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
		res, err := json.Marshal(info.AllSQLDigests)
		if err != nil {
			logutil.BgLogger().Warn("Failed to marshal sql digests list as json", zap.Uint64("txnStartTS", info.StartTS))
			return types.NewDatum(nil)
		}
		return types.NewDatum(string(res))
	},
}

// ToDatum Converts the `TxnInfo`'s specified column to `Datum` to show in the `TIDB_TRX` table.
func (info *TxnInfo) ToDatum(column string) types.Datum {
	//humanReadableStartTime := time.Unix(0, oracle.ExtractPhysical(info.StartTS)*1e6)
	//
	//var currentDigest interface{}
	//if len(info.CurrentSQLDigest) != 0 {
	//	currentDigest = info.CurrentSQLDigest
	//}
	//
	//var blockStartTime interface{}
	//if !info.BlockStartTime.Valid {
	//	blockStartTime = nil
	//} else {
	//	blockStartTime = types.NewTime(types.FromGoTime(info.BlockStartTime.Time), mysql.TypeTimestamp, types.MaxFsp)
	//}
	//
	//e, err := types.ParseEnumValue(TxnRunningStateStrs, uint64(info.State+1))
	//if err != nil {
	//	panic("this should never happen")
	//}
	//
	//allSQLs := "[" + strings.Join(info.AllSQLDigests, ", ") + "]"
	//
	//state := types.NewMysqlEnumDatum(e)
	//
	//datums := types.MakeDatums(
	//	info.StartTS,
	//	types.NewTime(types.FromGoTime(humanReadableStartTime), mysql.TypeTimestamp, types.MaxFsp),
	//	currentDigest,
	//)
	//datums = append(datums, state)
	//datums = append(datums, types.MakeDatums(
	//	blockStartTime,
	//	info.EntriesCount,
	//	info.EntriesSize,
	//	info.ConnectionID,
	//	info.Username,
	//	info.CurrentDB,
	//	allSQLs)...)
	//return datums
	res, ok := columnValueGetterMap[column]
	if !ok {
		return types.NewDatum(nil)
	}
	return res(info)
}
