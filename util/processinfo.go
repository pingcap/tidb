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
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/tikv/client-go/v2/oracle"
)

// ProcessInfo is a struct used for show processlist statement.
type ProcessInfo struct {
	ID               uint64
	User             string
	Host             string
	Port             string
	DB               string
	Digest           string
	Plan             interface{}
	PlanExplainRows  [][]string
	RuntimeStatsColl *execdetails.RuntimeStatsColl
	Time             time.Time
	Info             string
	CurTxnStartTS    uint64
	StmtCtx          *stmtctx.StatementContext
	StatsInfo        func(interface{}) map[string]uint64
	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the query takes too long, kill it.
	MaxExecutionTime uint64

	State                     uint16
	Command                   byte
	ExceedExpensiveTimeThresh bool
	RedactSQL                 bool
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
		if pi.StmtCtx.MemTracker != nil {
			bytesConsumed = pi.StmtCtx.MemTracker.BytesConsumed()
		}
		if pi.StmtCtx.DiskTracker != nil {
			diskConsumed = pi.StmtCtx.DiskTracker.BytesConsumed()
		}
	}
	return append(pi.ToRowForShow(true), pi.Digest, bytesConsumed, diskConsumed, pi.txnStartTs(tz))
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
	var l []string // nolint: prealloc
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
	// Put the internal session pointer to the map in the SessionManager
	StoreInternalSession(se interface{})
	// Delete the internal session pointer from the map in the SessionManager
	DeleteInternalSession(se interface{})
	// Get all startTS of every transactions running in the current internal sessions
	GetInternalSessionStartTSList() []uint64
}

// GlobalConnID is the global connection ID, providing UNIQUE connection IDs across the whole TiDB cluster.
// 64 bits version:
//  63 62                 41 40                                   1   0
// +--+---------------------+--------------------------------------+------+
// |  |      serverId       |             local connId             |markup|
// |=0|       (22b)         |                 (40b)                |  =1  |
// +--+---------------------+--------------------------------------+------+
// 32 bits version(coming soon):
//  31                          1   0
// +-----------------------------+------+
// |             ???             |markup|
// |             ???             |  =0  |
// +-----------------------------+------+
type GlobalConnID struct {
	ServerID       uint64
	LocalConnID    uint64
	Is64bits       bool
	ServerIDGetter func() uint64
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
//   `isTruncated` indicates that older versions of the client truncated the 64-bit GlobalConnID to 32-bit.
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
