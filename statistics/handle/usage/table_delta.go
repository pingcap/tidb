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

	"github.com/pingcap/tidb/sessionctx/variable"
)

// TableDelta is used to collect tables' change information.
// All methods of it are thread-safe.
type TableDelta struct {
	delta map[int64]variable.TableDelta // map[tableID]delta
	lock  sync.Mutex
}

// NewTableDelta creates a new TableDelta.
func NewTableDelta() *TableDelta {
	return &TableDelta{
		delta: make(map[int64]variable.TableDelta),
	}
}

// Reset resets the TableDelta.
func (m *TableDelta) Reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.delta = make(map[int64]variable.TableDelta)
}

// GetDeltaAndReset gets the delta and resets the TableDelta.
func (m *TableDelta) GetDeltaAndReset() map[int64]variable.TableDelta {
	m.lock.Lock()
	defer m.lock.Unlock()
	ret := m.delta
	m.delta = make(map[int64]variable.TableDelta)
	return ret
}

// Update updates the delta of the table.
func (m *TableDelta) Update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	UpdateTableDeltaMap(m.delta, id, delta, count, colSize)
}

// Merge merges the deltaMap into the TableDelta.
func (m *TableDelta) Merge(deltaMap map[int64]variable.TableDelta) {
	if len(deltaMap) == 0 {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	for id, item := range deltaMap {
		UpdateTableDeltaMap(m.delta, id, item.Delta, item.Count, &item.ColSize)
	}
}

// UpdateTableDeltaMap updates the delta of the table.
func UpdateTableDeltaMap(m map[int64]variable.TableDelta, id int64, delta int64, count int64, colSize *map[int64]int64) {
	item := m[id]
	item.Delta += delta
	item.Count += count
	if item.ColSize == nil {
		item.ColSize = make(map[int64]int64)
	}
	if colSize != nil {
		for key, val := range *colSize {
			item.ColSize[key] += val
		}
	}
	m[id] = item
}
