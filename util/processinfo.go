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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
	"github.com/tikv/client-go/v2/oracle"
)

// ProtectedTSList holds a list of timestamps that should delay GC.
type ProtectedTSList interface {
	// HoldTS holds the timestamp to prevent its data from being GCed.
	HoldTS(ts uint64) (unhold func())
	// GetMinProtectedTS returns the minimum protected timestamp that greater than `lowerBound` (0 if no such one).
	GetMinProtectedTS(lowerBound uint64) (ts uint64)
}

// OOMAlarmVariablesInfo is a struct for OOM alarm variables.
type OOMAlarmVariablesInfo struct {
	SessionAnalyzeVersion         int
	SessionEnabledRateLimitAction bool
	SessionMemQuotaQuery          int64
}

// ProcessInfo is a struct used for show processlist statement.
type ProcessInfo struct {
	ProtectedTSList
	Time                  time.Time
	ExpensiveLogTime      time.Time
	Plan                  interface{}
	StmtCtx               *stmtctx.StatementContext
	RefCountOfStmtCtx     *stmtctx.ReferenceCount
	MemTracker            *memory.Tracker
	DiskTracker           *disk.Tracker
	StatsInfo             func(interface{}) map[string]uint64
	RuntimeStatsColl      *execdetails.RuntimeStatsColl
	DB                    string
	Digest                string
	Host                  string
	User                  string
	Info                  string
	Port                  string
	ResourceGroupName     string
	PlanExplainRows       [][]string
	OOMAlarmVariablesInfo OOMAlarmVariablesInfo
	ID                    uint64
	CurTxnStartTS         uint64
	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the query takes too long, kill it.
	MaxExecutionTime uint64
	State            uint16
	Command          byte
	RedactSQL        bool
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
	var host string
	if pi.Port != "" {
		host = fmt.Sprintf("%s:%s", pi.Host, pi.Port)
	} else {
		host = pi.Host
	}
	return []interface{}{
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
func (pi *ProcessInfo) ToRow(tz *time.Location) []interface{} {
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
	return append(pi.ToRowForShow(true), pi.Digest, bytesConsumed, diskConsumed, pi.txnStartTs(tz), pi.ResourceGroupName)
}

// GetMinStartTS returns the minimum start-ts (used to delay GC) that greater than `lowerBound` (0 if no such one).
func (pi *ProcessInfo) GetMinStartTS(lowerBound uint64) (ts uint64) {
	if pi == nil {
		return
	}
	if thisTS := pi.CurTxnStartTS; thisTS > lowerBound && (thisTS < ts || ts == 0) {
		ts = thisTS
	}
	if pi.ProtectedTSList == nil {
		return
	}
	if thisTS := pi.GetMinProtectedTS(lowerBound); thisTS > lowerBound && (thisTS < ts || ts == 0) {
		ts = thisTS
	}
	return
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
	Kill(connectionID uint64, query bool)
	KillAllConnections()
	UpdateTLSConfig(cfg *tls.Config)
	ServerID() uint64
	// StoreInternalSession puts the internal session pointer to the map in the SessionManager.
	StoreInternalSession(se interface{})
	// DeleteInternalSession deletes the internal session pointer from the map in the SessionManager.
	DeleteInternalSession(se interface{})
	// GetInternalSessionStartTSList gets all startTS of every transactions running in the current internal sessions.
	GetInternalSessionStartTSList() []uint64
	// CheckOldRunningTxn checks if there is an old transaction running in the current sessions
	CheckOldRunningTxn(job2ver map[int64]int64, job2ids map[int64]string)
	// KillNonFlashbackClusterConn kill all non flashback cluster connections.
	KillNonFlashbackClusterConn()
	// GetMinStartTS returns the minimum start-ts (used to delay GC) that greater than `lowerBound` (0 if no such one).
	GetMinStartTS(lowerBound uint64) uint64
}

// GlobalConnID is the global connection ID, providing UNIQUE connection IDs across the whole TiDB cluster.
// 64 bits version:
/*
  63 62                 41 40                                   1   0
 +--+---------------------+--------------------------------------+------+
 |  |      serverId       |             local connId             |markup|
 |=0|       (22b)         |                 (40b)                |  =1  |
 +--+---------------------+--------------------------------------+------+
 32 bits version(coming soon):
  31                          1   0
 +-----------------------------+------+
 |             ???             |markup|
 |             ???             |  =0  |
 +-----------------------------+------+
*/
type GlobalConnID struct {
	ServerIDGetter func() uint64
	ServerID       uint64
	LocalConnID    uint64
	Is64bits       bool
}

// NewGlobalConnID creates GlobalConnID with serverID
func NewGlobalConnID(serverID uint64, is64Bits bool) GlobalConnID {
	return GlobalConnID{ServerID: serverID, Is64bits: is64Bits, LocalConnID: reservedLocalConns}
}

// NewGlobalConnIDWithGetter creates GlobalConnID with serverIDGetter
func NewGlobalConnIDWithGetter(serverIDGetter func() uint64, is64Bits bool) GlobalConnID {
	return GlobalConnID{ServerIDGetter: serverIDGetter, Is64bits: is64Bits, LocalConnID: reservedLocalConns}
}

const (
	// MaxServerID is maximum serverID.
	MaxServerID = 1<<22 - 1
)

func (g *GlobalConnID) makeID(localConnID uint64) uint64 {
	var (
		id       uint64
		serverID uint64
	)
	if g.ServerIDGetter != nil {
		serverID = g.ServerIDGetter()
	} else {
		serverID = g.ServerID
	}
	if g.Is64bits {
		id |= 0x1
		id |= localConnID & 0xff_ffff_ffff << 1 // 40 bits local connID.
		id |= serverID & MaxServerID << 41      // 22 bits serverID.
	} else {
		// TODO: update after new design for 32 bits version.
		id |= localConnID & 0x7fff_ffff << 1 // 31 bits local connID.
	}
	return id
}

// ID returns the connection id
func (g *GlobalConnID) ID() uint64 {
	return g.makeID(g.LocalConnID)
}

// NextID returns next connection id
func (g *GlobalConnID) NextID() uint64 {
	localConnID := atomic.AddUint64(&g.LocalConnID, 1)
	return g.makeID(localConnID)
}

// ParseGlobalConnID parses an uint64 to GlobalConnID.
//
//	`isTruncated` indicates that older versions of the client truncated the 64-bit GlobalConnID to 32-bit.
func ParseGlobalConnID(id uint64) (g GlobalConnID, isTruncated bool, err error) {
	if id&0x80000000_00000000 > 0 {
		return GlobalConnID{}, false, errors.New("Unexpected connectionID excceeds int64")
	}
	if id&0x1 > 0 {
		if id&0xffffffff_00000000 == 0 {
			return GlobalConnID{}, true, nil
		}
		return GlobalConnID{
			Is64bits:    true,
			LocalConnID: (id >> 1) & 0xff_ffff_ffff,
			ServerID:    (id >> 41) & MaxServerID,
		}, false, nil
	}
	// TODO: update after new design for 32 bits version.
	return GlobalConnID{
		Is64bits:    false,
		LocalConnID: (id >> 1) & 0x7fff_ffff,
		ServerID:    0,
	}, false, nil
}

const (
	reservedLocalConns  = 200
	reservedConnAnalyze = 1
)

// GetAutoAnalyzeProcID returns processID for auto analyze
// TODO support IDs for concurrent auto-analyze
func GetAutoAnalyzeProcID(serverIDGetter func() uint64) uint64 {
	globalConnID := NewGlobalConnIDWithGetter(serverIDGetter, true)
	return globalConnID.makeID(reservedConnAnalyze)
}
