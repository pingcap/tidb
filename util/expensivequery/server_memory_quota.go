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

package expensivequery

import (
	"runtime"
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
)

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
