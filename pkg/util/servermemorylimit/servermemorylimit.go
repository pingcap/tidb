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

package servermemorylimit

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Process global Observation indicators for memory limit.
var (
	MemoryMaxUsed                 = atomicutil.NewUint64(0)
	SessionKillLast               = atomicutil.NewTime(time.Time{})
	SessionKillTotal              = atomicutil.NewInt64(0)
	IsKilling                     = atomicutil.NewBool(false)
	GlobalMemoryOpsHistoryManager = &memoryOpsHistoryManager{}
)

// Handle is the handler for server memory limit.
type Handle struct {
	exitCh chan struct{}
	sm     atomic.Value
}

// NewServerMemoryLimitHandle builds a new server memory limit handler.
func NewServerMemoryLimitHandle(exitCh chan struct{}) *Handle {
	return &Handle{exitCh: exitCh}
}

// SetSessionManager sets the SessionManager which is used to fetching the info
// of all active sessions.
func (smqh *Handle) SetSessionManager(sm util.SessionManager) *Handle {
	smqh.sm.Store(sm)
	return smqh
}

// Run starts a server memory limit checker goroutine at the start time of the server.
// This goroutine will obtain the `heapInuse` of Golang runtime periodically and compare it with `tidb_server_memory_limit`.
// When `heapInuse` is greater than `tidb_server_memory_limit`, it will set the `needKill` flag of `MemUsageTop1Tracker`.
// When the corresponding SQL try to acquire more memory(next Tracker.Consume() call), it will trigger panic and exit.
// When this goroutine detects the `needKill` SQL has exited successfully, it will immediately trigger runtime.GC() to release memory resources.
func (smqh *Handle) Run() {
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := smqh.sm.Load().(util.SessionManager)
	sessionToBeKilled := &sessionToBeKilled{}
	for {
		select {
		case <-ticker.C:
			killSessIfNeeded(sessionToBeKilled, memory.ServerMemoryLimit.Load(), sm)
		case <-smqh.exitCh:
			return
		}
	}
}

type sessionToBeKilled struct {
	isKilling      bool
	sqlStartTime   time.Time
	sessionID      uint64
	sessionTracker *memory.Tracker

	killStartTime time.Time
	lastLogTime   time.Time
}

func (s *sessionToBeKilled) reset() {
	s.isKilling = false
	s.sqlStartTime = time.Time{}
	s.sessionID = 0
	s.sessionTracker = nil
	s.killStartTime = time.Time{}
	s.lastLogTime = time.Time{}
}

func killSessIfNeeded(s *sessionToBeKilled, bt uint64, sm util.SessionManager) {
	if s.isKilling {
		if info, ok := sm.GetProcessInfo(s.sessionID); ok {
			if info.Time == s.sqlStartTime {
				if time.Since(s.lastLogTime) > 5*time.Second {
					logutil.BgLogger().Warn(fmt.Sprintf("global memory controller failed to kill the top-consumer in %ds",
						time.Since(s.killStartTime)/time.Second),
						zap.Uint64("conn", info.ID),
						zap.String("sql digest", info.Digest),
						zap.String("sql text", fmt.Sprintf("%.100v", info.Info)),
						zap.Int64("sql memory usage", info.MemTracker.BytesConsumed()))
					s.lastLogTime = time.Now()
				}
				return
			}
		}
		s.reset()
		IsKilling.Store(false)
		memory.MemUsageTop1Tracker.CompareAndSwap(s.sessionTracker, nil)
		//nolint: all_revive,revive
		runtime.GC()
		logutil.BgLogger().Warn("global memory controller killed the top1 memory consumer successfully")
	}

	if bt == 0 {
		return
	}
	failpoint.Inject("issue42662_2", func(val failpoint.Value) {
		if val.(bool) {
			bt = 1
		}
	})
	instanceStats := memory.ReadMemStats()
	if instanceStats.HeapInuse > MemoryMaxUsed.Load() {
		MemoryMaxUsed.Store(instanceStats.HeapInuse)
	}
	limitSessMinSize := memory.ServerMemoryLimitSessMinSize.Load()
	if instanceStats.HeapInuse > bt {
		t := memory.MemUsageTop1Tracker.Load()
		if t != nil {
			sessionID := t.SessionID.Load()
			memUsage := t.BytesConsumed()
			// If the memory usage of the top1 session is less than tidb_server_memory_limit_sess_min_size, we do not need to kill it.
			if uint64(memUsage) < limitSessMinSize {
				memory.MemUsageTop1Tracker.CompareAndSwap(t, nil)
				t = nil
			} else if info, ok := sm.GetProcessInfo(sessionID); ok {
				logutil.BgLogger().Warn("global memory controller tries to kill the top1 memory consumer",
					zap.Uint64("conn", info.ID),
					zap.String("sql digest", info.Digest),
					zap.String("sql text", fmt.Sprintf("%.100v", info.Info)),
					zap.Uint64("tidb_server_memory_limit", bt),
					zap.Uint64("heap inuse", instanceStats.HeapInuse),
					zap.Int64("sql memory usage", info.MemTracker.BytesConsumed()),
				)
				s.sessionID = sessionID
				s.sqlStartTime = info.Time
				s.isKilling = true
				s.sessionTracker = t
				t.Killer.SendKillSignal(sqlkiller.ServerMemoryExceeded)

				killTime := time.Now()
				SessionKillTotal.Add(1)
				SessionKillLast.Store(killTime)
				IsKilling.Store(true)
				GlobalMemoryOpsHistoryManager.recordOne(info, killTime, bt, instanceStats.HeapInuse)
				s.lastLogTime = time.Now()
				s.killStartTime = time.Now()
			}
		}
		// If no one larger than tidb_server_memory_limit_sess_min_size is found, we will not kill any one.
		if t == nil {
			if s.lastLogTime.IsZero() {
				s.lastLogTime = time.Now()
			}
			if time.Since(s.lastLogTime) < 5*time.Second {
				return
			}
			logutil.BgLogger().Warn("global memory controller tries to kill the top1 memory consumer, but no one larger than tidb_server_memory_limit_sess_min_size is found", zap.Uint64("tidb_server_memory_limit_sess_min_size", limitSessMinSize))
			s.lastLogTime = time.Now()
		}
	}
}

type memoryOpsHistoryManager struct {
	mu      sync.Mutex
	infos   []memoryOpsHistory
	offsets int
}

type memoryOpsHistory struct {
	killTime         time.Time
	memoryLimit      uint64
	memoryCurrent    uint64
	processInfoDatum []types.Datum // id,user,host,db,command,time,state,info,digest,mem,disk,txnStart
}

func (m *memoryOpsHistoryManager) init() {
	m.infos = make([]memoryOpsHistory, 50)
	m.offsets = 0
}

func (m *memoryOpsHistoryManager) recordOne(info *util.ProcessInfo, killTime time.Time, memoryLimit uint64, memoryCurrent uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	op := memoryOpsHistory{killTime: killTime, memoryLimit: memoryLimit, memoryCurrent: memoryCurrent, processInfoDatum: types.MakeDatums(info.ToRow(time.UTC)...)}
	sqlInfo := op.processInfoDatum[7]
	sqlInfo.SetString(fmt.Sprintf("%.256v", sqlInfo.GetString()), mysql.DefaultCollationName) // Truncated
	// Only record the last 50 history ops
	m.infos[m.offsets] = op
	m.offsets++
	if m.offsets >= 50 {
		m.offsets = 0
	}
}

func (m *memoryOpsHistoryManager) GetRows() [][]types.Datum {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := make([][]types.Datum, 0, len(m.infos))
	getRowFromInfo := func(info memoryOpsHistory) {
		killTime := types.NewTime(types.FromGoTime(info.killTime), mysql.TypeDatetime, 0)
		op := "SessionKill"
		rows = append(rows, []types.Datum{
			types.NewDatum(killTime),           // TIME
			types.NewDatum(op),                 // OPS
			types.NewDatum(info.memoryLimit),   // MEMORY_LIMIT
			types.NewDatum(info.memoryCurrent), // MEMORY_CURRENT
			info.processInfoDatum[0],           // PROCESSID
			info.processInfoDatum[9],           // MEM
			info.processInfoDatum[10],          // DISK
			info.processInfoDatum[2],           // CLIENT
			info.processInfoDatum[3],           // DB
			info.processInfoDatum[1],           // USER
			info.processInfoDatum[8],           // SQL_DIGEST
			info.processInfoDatum[7],           // SQL_TEXT
		})
	}
	var zeroTime = time.Time{}
	for i := 0; i < len(m.infos); i++ {
		pos := (m.offsets + i) % len(m.infos)
		info := m.infos[pos]
		if info.killTime.Equal(zeroTime) {
			continue
		}
		getRowFromInfo(info)
	}
	return rows
}

func init() {
	GlobalMemoryOpsHistoryManager.init()
}
