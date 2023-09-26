// Copyright 2023 PingCAP, Inc.
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

package usage

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/parser/model"
)

// StatsUsage maps (tableID, columnID) to the last time when the column stats are used(needed).
// All methods of it are thread-safe.
type StatsUsage struct {
	usage map[model.TableItemID]time.Time
	lock  sync.RWMutex
}

// NewStatsUsage creates a new StatsUsage.
func NewStatsUsage() *StatsUsage {
	return &StatsUsage{
		usage: make(map[model.TableItemID]time.Time),
	}
}

// Reset resets the StatsUsage.
func (m *StatsUsage) Reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.usage = make(map[model.TableItemID]time.Time)
}

// GetUsageAndReset gets the usage and resets the StatsUsage.
func (m *StatsUsage) GetUsageAndReset() map[model.TableItemID]time.Time {
	m.lock.Lock()
	defer m.lock.Unlock()
	ret := m.usage
	m.usage = make(map[model.TableItemID]time.Time)
	return ret
}

// Merge merges the usageMap into the StatsUsage.
func (m *StatsUsage) Merge(other map[model.TableItemID]time.Time) {
	if len(other) == 0 {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	for id, t := range other {
		if mt, ok := m.usage[id]; !ok || mt.Before(t) {
			m.usage[id] = t
		}
	}
}
