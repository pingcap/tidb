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
	"time"

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
	StartTS uint64
	// digest of SQL current running
	CurrentSQLDigest string
	// current executing State
	State TxnRunningState
	// last trying to block start time
	BlockStartTime *time.Time
	// How many entries are in MemDB
	EntriesCount uint64
	// MemDB used memory
	EntriesSize uint64

	// the following fields will be filled in `session` instead of `LazyTxn`

	// Which session this transaction belongs to
	ConnectionID uint64
	// The user who open this session
	Username string
	// The schema this transaction works on
	CurrentDB string
}

// ToDatum Converts the `TxnInfo` to `Datum` to show in the `TIDB_TRX` table
func (info *TxnInfo) ToDatum() []types.Datum {
	humanReadableStartTime := time.Unix(0, oracle.ExtractPhysical(info.StartTS)*1e6)
	var blockStartTime interface{}
	if info.BlockStartTime == nil {
		blockStartTime = nil
	} else {
		blockStartTime = types.NewTime(types.FromGoTime(*info.BlockStartTime), mysql.TypeTimestamp, 0)
	}
	e, err := types.ParseEnumValue(TxnRunningStateStrs, uint64(info.State+1))
	if err != nil {
		panic("this should never happen")
	}
	state := types.NewMysqlEnumDatum(e)
	datums := types.MakeDatums(
		info.StartTS,
		types.NewTime(types.FromGoTime(humanReadableStartTime), mysql.TypeTimestamp, 0),
		info.CurrentSQLDigest,
	)
	datums = append(datums, state)
	datums = append(datums, types.MakeDatums(
		blockStartTime,
		info.EntriesCount,
		info.EntriesSize,
		info.ConnectionID,
		info.Username,
		info.CurrentDB)...)
	return datums
}
