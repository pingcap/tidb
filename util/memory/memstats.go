// Copyright 2018 PingCAP, Inc.
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

package memory

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
)

var stats atomic.Pointer[globalMstats]

// ReadMemInterval controls the interval to read memory stats.
const ReadMemInterval = 300 * time.Millisecond

// ReadMemStats read the mem stats from runtime.ReadMemStats
func ReadMemStats() (memStats *runtime.MemStats) {
	s := stats.Load()
	if s != nil {
		memStats = &s.m
	} else {
		memStats = ForceReadMemStats()
	}
	failpoint.Inject("ReadMemStats", func(val failpoint.Value) {
		injectedSize := val.(int)
		memStats.HeapInuse += uint64(injectedSize)
	})
	return
}

// ForceReadMemStats is to force read memory stats.
func ForceReadMemStats() *runtime.MemStats {
	var g globalMstats
	g.ts = time.Now()
	runtime.ReadMemStats(&g.m)
	stats.Store(&g)
	return &g.m
}

type globalMstats struct {
	ts time.Time
	m  runtime.MemStats
}
