// Copyright 2024 PingCAP, Inc.
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

package asyncload

import (
	"sync"

	"github.com/pingcap/tidb/pkg/parser/model"
)

// AsyncLoadHistogramNeededItems stores the columns/indices whose Histograms need to be loaded from physical kv layer.
// Currently, we only load index/pk's Histogram from kv automatically. Columns' are loaded by needs.
var AsyncLoadHistogramNeededItems = newNeededStatsMap()

type neededStatsInternalMap struct {
	// the bool value indicates whether is a full load or not.
	items map[model.TableItemID]bool
	m     sync.RWMutex
}

func (n *neededStatsInternalMap) AllItems() []model.StatsLoadItem {
	n.m.RLock()
	keys := make([]model.StatsLoadItem, 0, len(n.items))
	for key, val := range n.items {
		keys = append(keys, model.StatsLoadItem{
			TableItemID: key,
			FullLoad:    val,
		})
	}
	n.m.RUnlock()
	return keys
}

func (n *neededStatsInternalMap) Insert(col model.TableItemID, fullLoad bool) {
	n.m.Lock()
	cur := n.items[col]
	if cur {
		// If the existing one is full load. We don't need to update it.
		n.m.Unlock()
		return
	}
	n.items[col] = fullLoad
	// Otherwise, we could safely update it.
	n.m.Unlock()
}

func (n *neededStatsInternalMap) Delete(col model.TableItemID) {
	n.m.Lock()
	delete(n.items, col)
	n.m.Unlock()
}

func (n *neededStatsInternalMap) Length() int {
	n.m.RLock()
	defer n.m.RUnlock()
	return len(n.items)
}

const shardCnt = 128

type neededStatsMap struct {
	items [shardCnt]neededStatsInternalMap
}

func getIdx(tbl model.TableItemID) int64 {
	var id int64
	if tbl.ID < 0 {
		id = -tbl.ID
	} else {
		id = tbl.ID
	}
	return id % shardCnt
}

func newNeededStatsMap() *neededStatsMap {
	result := neededStatsMap{}
	for i := 0; i < shardCnt; i++ {
		result.items[i] = neededStatsInternalMap{
			items: make(map[model.TableItemID]bool),
		}
	}
	return &result
}

func (n *neededStatsMap) AllItems() []model.StatsLoadItem {
	var result []model.StatsLoadItem
	for i := 0; i < shardCnt; i++ {
		keys := n.items[i].AllItems()
		result = append(result, keys...)
	}
	return result
}

func (n *neededStatsMap) Insert(col model.TableItemID, fullLoad bool) {
	n.items[getIdx(col)].Insert(col, fullLoad)
}

func (n *neededStatsMap) Delete(col model.TableItemID) {
	n.items[getIdx(col)].Delete(col)
}

func (n *neededStatsMap) Length() int {
	var result int
	for i := 0; i < shardCnt; i++ {
		result += n.items[i].Length()
	}
	return result
}
