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
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
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
	isKilling    bool
	sqlStartTime time.Time
	sessionID    uint64
}

func killSessIfNeeded(s *sessionToBeKilled, bt uint64, sm util.SessionManager) {
	if s.isKilling {
		if info, ok := sm.GetProcessInfo(s.sessionID); ok {
			if info.Time == s.sqlStartTime {
				return
			}
		}
		s.isKilling = false
		//nolint: all_revive,revive
		runtime.GC()
	}

	if bt == 0 {
		return
	}
	instanceStats := &runtime.MemStats{}
	runtime.ReadMemStats(instanceStats)
	if instanceStats.HeapInuse > bt {
		t := memory.MemUsageTop1Tracker.Load()
		if t != nil {
			if info, ok := sm.GetProcessInfo(t.SessionID); ok {
				s.sessionID = t.SessionID
				s.sqlStartTime = info.Time
				s.isKilling = true
				t.NeedKill.Store(true)
			}
		}
	}
}
