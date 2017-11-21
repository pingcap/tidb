// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package update

// TableDelta stands for the changed count for one table.
type TableDelta struct {
	Delta int64
	Count int64
}

// Updater is used to feed runtime statistics information back to statistics.
type Updater map[int64]TableDelta

// NewUpdater returns a new updater.
func NewUpdater() Updater {
	return make(Updater)
}

// UpdateDelta update delta for table.
func (m Updater) UpdateDelta(id int64, delta int64, count int64) {
	item := m[id]
	item.Delta += delta
	item.Count += count
	m[id] = item
}

// RelatedTableIDs returns the related table IDs in the updater.
func (m Updater) RelatedTableIDs() []int64 {
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}

// Merge merges two updater.
func (m Updater) Merge(rMap Updater) {
	for id, item := range rMap {
		m.UpdateDelta(id, item.Delta, item.Count)
	}
}
