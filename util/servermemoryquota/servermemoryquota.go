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

package servermemoryquota

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
)

// Handle is the handler for server memory quota.
type Handle struct {
	exitCh chan struct{}
	sm     atomic.Value
}

// NewServerMemoryQuotaHandle builds a new server memory quota handler.
func NewServerMemoryQuotaHandle(exitCh chan struct{}) *Handle {
	return &Handle{exitCh: exitCh}
}

// SetSessionManager sets the SessionManager which is used to fetching the info
// of all active sessions.
func (smqh *Handle) SetSessionManager(sm util.SessionManager) *Handle {
	smqh.sm.Store(sm)
	return smqh
}

// Run starts a server memory quota checker goroutine at the start time of the server.
func (smqh *Handle) Run() {
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := smqh.sm.Load().(util.SessionManager)
	serverMemoryQuota := &serverMemoryQuota{}
	for {
		select {
		case <-ticker.C:
			serverMemoryQuota.CheckQuotaAndKill(memory.ServerMemoryQuota.Load(), sm)
		case <-smqh.exitCh:
			return
		}
	}
}

type serverMemoryQuota struct {
	sqlStartTime time.Time
	sessionID    uint64
}

func (s *serverMemoryQuota) CheckQuotaAndKill(bt uint64, sm util.SessionManager) {
	if s.sessionID != 0 {
		if info, ok := sm.GetProcessInfo(s.sessionID); ok {
			if info.Time == s.sqlStartTime {
				return // Wait killing finished.
			}
		}
		s.sessionID = 0
		s.sqlStartTime = time.Time{}
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
		if t != nil && uint64(t.BytesConsumed()) > memory.ServerMemoryLimitSessMinSize.Load() {
			if info, ok := sm.GetProcessInfo(t.SessionID); ok {
				s.sessionID = t.SessionID
				s.sqlStartTime = info.Time
				t.IsKilled.Store(true)
			}
		}
	}
}
