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
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// ProcessInfo is a struct used for show processlist statement.
type ProcessInfo struct {
	ID              uint64
	User            string
	Host            string
	DB              string
	Plan            interface{}
	PlanExplainRows [][]string
	Time            time.Time
	Info            string
	CurTxnStartTS   uint64
	StmtCtx         *stmtctx.StatementContext
	StatsInfo       func(interface{}) map[string]uint64
	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the query takes too long, kill it.
	MaxExecutionTime uint64

	State                     uint16
	Command                   byte
	ExceedExpensiveTimeThresh bool
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
		serverStatus2Str(pi.State),
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
	bytesConsumed := int64(0)
	if pi.StmtCtx != nil && pi.StmtCtx.MemTracker != nil {
		bytesConsumed = pi.StmtCtx.MemTracker.BytesConsumed()
	}
	return append(pi.ToRowForShow(true), bytesConsumed, pi.txnStartTs(tz))
}

// ascServerStatus is a slice of all defined server status in ascending order.
var ascServerStatus = []uint16{
	mysql.ServerStatusInTrans,
	mysql.ServerStatusAutocommit,
	mysql.ServerMoreResultsExists,
	mysql.ServerStatusNoGoodIndexUsed,
	mysql.ServerStatusNoIndexUsed,
	mysql.ServerStatusCursorExists,
	mysql.ServerStatusLastRowSend,
	mysql.ServerStatusDBDropped,
	mysql.ServerStatusNoBackslashEscaped,
	mysql.ServerStatusMetadataChanged,
	mysql.ServerStatusWasSlow,
	mysql.ServerPSOutParams,
}

// mapServerStatus2Str is the map for server status to string.
var mapServerStatus2Str = map[uint16]string{
	mysql.ServerStatusInTrans:            "in transaction",
	mysql.ServerStatusAutocommit:         "autocommit",
	mysql.ServerMoreResultsExists:        "more results exists",
	mysql.ServerStatusNoGoodIndexUsed:    "no goods index used",
	mysql.ServerStatusNoIndexUsed:        "no index used",
	mysql.ServerStatusCursorExists:       "cursor exists",
	mysql.ServerStatusLastRowSend:        "last row send",
	mysql.ServerStatusDBDropped:          "db dropped",
	mysql.ServerStatusNoBackslashEscaped: "no backslash escaped",
	mysql.ServerStatusMetadataChanged:    "metadata changed",
	mysql.ServerStatusWasSlow:            "was slow",
	mysql.ServerPSOutParams:              "ps out params",
}

// serverStatus2Str convert server status to string.
// Param state is a bit-field. (e.g. 0x0003 = "in transaction; autocommit").
func serverStatus2Str(state uint16) string {
	// l collect server status strings.
	var l []string
	// check each defined server status, if match, append to collector.
	for _, s := range ascServerStatus {
		if state&s == 0 {
			continue
		}
		l = append(l, mapServerStatus2Str[s])
	}
	return strings.Join(l, "; ")
}

// SessionManager is an interface for session manage. Show processlist and
// kill statement rely on this interface.
type SessionManager interface {
	ShowProcessList() map[uint64]*ProcessInfo
	GetProcessInfo(id uint64) (*ProcessInfo, bool)
	Kill(connectionID uint64, query bool)
	UpdateTLSConfig(cfg *tls.Config)
}
