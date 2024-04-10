// Copyright 2022 PingCAP, Inc.
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

package util

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/tikv/client-go/v2/oracle"
)

// OOMAlarmVariablesInfo is a struct for OOM alarm variables.
type OOMAlarmVariablesInfo struct {
	SessionAnalyzeVersion         int
	SessionEnabledRateLimitAction bool
	SessionMemQuotaQuery          int64
}

// ProcessInfo is a struct used for show processlist statement.
type ProcessInfo struct {
	Time                  time.Time
	ExpensiveLogTime      time.Time
	ExpensiveTxnLogTime   time.Time
	CurTxnCreateTime      time.Time
	Plan                  any
	StmtCtx               *stmtctx.StatementContext
	RefCountOfStmtCtx     *stmtctx.ReferenceCount
	MemTracker            *memory.Tracker
	DiskTracker           *disk.Tracker
	StatsInfo             func(any) map[string]uint64
	RuntimeStatsColl      *execdetails.RuntimeStatsColl
	User                  string
	Digest                string
	Host                  string
	DB                    string
	Info                  string
	Port                  string
	ResourceGroupName     string
	SessionAlias          string
	RedactSQL             string
	IndexNames            []string
	TableIDs              []int64
	PlanExplainRows       [][]string
	OOMAlarmVariablesInfo OOMAlarmVariablesInfo
	ID                    uint64
	CurTxnStartTS         uint64
	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the query takes too long, kill it.
	MaxExecutionTime uint64
	State            uint16
	Command          byte
}

// Clone return a shallow clone copy of this processInfo.
func (pi *ProcessInfo) Clone() *ProcessInfo {
	cp := *pi
	return &cp
}

// ToRowForShow returns []interface{} for the row data of "SHOW [FULL] PROCESSLIST".
func (pi *ProcessInfo) ToRowForShow(full bool) []any {
	var info any
	if len(pi.Info) > 0 {
		if full {
			info = pi.Info
		} else {
			info = fmt.Sprintf("%.100v", pi.Info)
		}
	}
	t := uint64(time.Since(pi.Time) / time.Second)
	var db any
	if len(pi.DB) > 0 {
		db = pi.DB
	}
	var host string
	if pi.Port != "" {
		host = net.JoinHostPort(pi.Host, pi.Port)
	} else {
		host = pi.Host
	}
	return []any{
		pi.ID,
		pi.User,
		host,
		db,
		mysql.Command2Str[pi.Command],
		t,
		serverStatus2Str(pi.State),
		info,
	}
}

func (pi *ProcessInfo) String() string {
	rows := pi.ToRowForShow(false)
	return fmt.Sprintf("{id:%v, user:%v, host:%v, db:%v, command:%v, time:%v, state:%v, info:%v}", rows...)
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
func (pi *ProcessInfo) ToRow(tz *time.Location) []any {
	bytesConsumed := int64(0)
	diskConsumed := int64(0)
	if pi.StmtCtx != nil {
		if pi.MemTracker != nil {
			bytesConsumed = pi.MemTracker.BytesConsumed()
		}
		if pi.DiskTracker != nil {
			diskConsumed = pi.DiskTracker.BytesConsumed()
		}
	}
	return append(pi.ToRowForShow(true), pi.Digest, bytesConsumed, diskConsumed, pi.txnStartTs(tz), pi.ResourceGroupName, pi.SessionAlias)
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
	mysql.ServerStatusNoGoodIndexUsed:    "no good index used",
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
	//nolint: prealloc
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
	ShowTxnList() []*txninfo.TxnInfo
	GetProcessInfo(id uint64) (*ProcessInfo, bool)
	Kill(connectionID uint64, query bool, maxExecutionTime bool)
	KillAllConnections()
	UpdateTLSConfig(cfg *tls.Config)
	ServerID() uint64
	// GetAutoAnalyzeProcID returns processID for auto analyze
	GetAutoAnalyzeProcID() uint64
	// StoreInternalSession puts the internal session pointer to the map in the SessionManager.
	StoreInternalSession(se any)
	// DeleteInternalSession deletes the internal session pointer from the map in the SessionManager.
	DeleteInternalSession(se any)
	// GetInternalSessionStartTSList gets all startTS of every transactions running in the current internal sessions.
	GetInternalSessionStartTSList() []uint64
	// CheckOldRunningTxn checks if there is an old transaction running in the current sessions
	CheckOldRunningTxn(job2ver map[int64]int64, job2ids map[int64]string)
	// KillNonFlashbackClusterConn kill all non flashback cluster connections.
	KillNonFlashbackClusterConn()
	// GetConAttrs gets the connection attributes
	GetConAttrs(user *auth.UserIdentity) map[uint64]map[string]string
}
