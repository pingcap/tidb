// Copyright 2017 PingCAP, Inc.
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

package util

import (
	"fmt"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// ProcessInfo is a struct used for show processlist statement.
type ProcessInfo struct {
	ID                        uint64
	User                      string
	Host                      string
	DB                        string
	Command                   byte
	Plan                      interface{}
	Time                      time.Time
	State                     uint16
	Info                      string
	CurTxnStartTS             uint64
	StmtCtx                   *stmtctx.StatementContext
	StatsInfo                 func(interface{}) map[string]uint64
	ExceedExpensiveTimeThresh bool
	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the query takes too long, kill it.
	MaxExecutionTime uint64
}

// ToRowForShow returns []interface{} for the row data of "SHOW [FULL] PROCESSLIST".
func (pi *ProcessInfo) ToRowForShow(full bool) []interface{} {
	var info interface{}
	if len(pi.Info) > 0 {
		if full {
			info = pi.Info
		} else {
			info = fmt.Sprintf("%.100v", pi.Info)
		}
	}
	t := uint64(time.Since(pi.Time) / time.Second)
	var db interface{}
	if len(pi.DB) > 0 {
		db = pi.DB
	}
	return []interface{}{
		pi.ID,
		pi.User,
		pi.Host,
		db,
		mysql.Command2Str[pi.Command],
		t,
		fmt.Sprintf("%d", pi.State),
		info,
	}
}

func (pi *ProcessInfo) txnStartTs(tz *time.Location) (txnStart string) {
	if pi.CurTxnStartTS > 0 {
		physicalTime := oracle.GetTimeFromTS(pi.CurTxnStartTS)
		txnStart = fmt.Sprintf("%s(%d)", physicalTime.In(tz).Format("01-02 15:04:05.000"), pi.CurTxnStartTS)
	}
	return
}

// ToRow returns []interface{} for the row data of
// "SELECT * FROM INFORMATION_SCHEMA.PROCESSLIST".
func (pi *ProcessInfo) ToRow(tz *time.Location) []interface{} {
	return append(pi.ToRowForShow(true), pi.StmtCtx.MemTracker.BytesConsumed(), pi.txnStartTs(tz))
}

// SessionManager is an interface for session manage. Show processlist and
// kill statement rely on this interface.
type SessionManager interface {
	ShowProcessList() map[uint64]*ProcessInfo
	GetProcessInfo(id uint64) (*ProcessInfo, bool)
	Kill(connectionID uint64, query bool)
}
