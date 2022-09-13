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

	"github.com/pingcap/tidb/util/memory"
)

type serverMemoryQuota struct {
}

func (s *serverMemoryQuota) CheckQuotaAndKill(bt uint64) {
	if bt == 0 {
		return
	}
	instanceStats := &runtime.MemStats{}
	runtime.ReadMemStats(instanceStats)
	if instanceStats.HeapInuse > bt {
		t := memory.MemUsageTop1Tracker.Load()
		if t != nil {
			t.IsKilled.Store(true)
		}
	}
}
