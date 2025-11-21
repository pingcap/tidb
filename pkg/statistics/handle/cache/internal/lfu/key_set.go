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

package lfu

import (
	"maps"
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/statistics"
)

type tableWithCost struct {
	table *statistics.Table
	cost  int64
}

type keySet struct {
	set map[int64]*tableWithCost
	mu  sync.RWMutex
}

func (ks *keySet) Remove(key int64) int64 {
	var cost int64
	ks.mu.Lock()
	if entry, ok := ks.set[key]; ok {
		if entry != nil {
			cost = entry.cost
		}
		delete(ks.set, key)
	}
	ks.mu.Unlock()
	return cost
}

func (ks *keySet) Keys() []int64 {
	ks.mu.RLock()
	result := slices.Collect(maps.Keys(ks.set))
	ks.mu.RUnlock()
	return result
}

func (ks *keySet) Len() int {
	ks.mu.RLock()
	result := len(ks.set)
	ks.mu.RUnlock()
	return result
}

func (ks *keySet) AddKeyValue(key int64, value *statistics.Table, cost int64) {
	ks.mu.Lock()
	ks.set[key] = &tableWithCost{
		table: value,
		cost:  cost,
	}
	ks.mu.Unlock()
}

func (ks *keySet) Get(key int64) (*statistics.Table, bool) {
	ks.mu.RLock()
	entry, ok := ks.set[key]
	ks.mu.RUnlock()
	if !ok || entry == nil {
		return nil, ok
	}
	return entry.table, ok
}

func (ks *keySet) GetWithCost(key int64) (*statistics.Table, int64, bool) {
	ks.mu.RLock()
	entry, ok := ks.set[key]
	ks.mu.RUnlock()
	if !ok || entry == nil {
		return nil, 0, ok
	}
	return entry.table, entry.cost, ok
}

func (ks *keySet) Clear() {
	ks.mu.Lock()
	ks.set = make(map[int64]*tableWithCost)
	ks.mu.Unlock()
}
