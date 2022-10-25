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
	"sync"
	"time"
)

var stats globalMstats

// ReadMemStats
// The returned memory allocator statistics are up to date as of the
// call to ReadMemStats. This is in contrast with a heap profile,
// which is a snapshot as of the most recently completed garbage
// collection cycle.
func ReadMemStats() runtime.MemStats {
	return stats.readMemStats()
}

// ForceReadMemStats is to force read memory stats.
func ForceReadMemStats() runtime.MemStats {
	return stats.forceReadMemStats()
}

type globalMstats struct {
	ts time.Time
	m  runtime.MemStats
	mu sync.RWMutex
}

func (g *globalMstats) readMemStats() (result runtime.MemStats) {
	g.mu.RLock()
	if time.Since(g.ts) < 200*time.Millisecond {
		result = g.m
		g.mu.RUnlock()
		return result
	}
	g.mu.RUnlock()

	g.mu.Lock()
	defer g.mu.Unlock()
	g.ts = time.Now()
	runtime.ReadMemStats(&g.m)
	result = g.m
	return result
}

func (g *globalMstats) forceReadMemStats() (result runtime.MemStats) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ts = time.Now()
	runtime.ReadMemStats(&g.m)
	result = g.m
	return result
}
