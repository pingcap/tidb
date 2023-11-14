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
	"github.com/pingcap/tidb/pkg/statistics"
)

const keySetCnt = 256

type keySetShard struct {
	resultKeySet [keySetCnt]keySet
}

func newKeySetShard() *keySetShard {
	result := keySetShard{}
	for i := 0; i < keySetCnt; i++ {
		result.resultKeySet[i] = keySet{
			set: make(map[int64]*statistics.Table),
		}
	}
	return &result
}

func (kss *keySetShard) Get(key int64) (*statistics.Table, bool) {
	return kss.resultKeySet[key%keySetCnt].Get(key)
}

func (kss *keySetShard) AddKeyValue(key int64, table *statistics.Table) {
	kss.resultKeySet[key%keySetCnt].AddKeyValue(key, table)
}

func (kss *keySetShard) Remove(key int64) {
	kss.resultKeySet[key%keySetCnt].Remove(key)
}

func (kss *keySetShard) Keys() []int64 {
	result := make([]int64, 0, len(kss.resultKeySet))
	for idx := range kss.resultKeySet {
		result = append(result, kss.resultKeySet[idx].Keys()...)
	}
	return result
}

func (kss *keySetShard) Len() int {
	result := 0
	for idx := range kss.resultKeySet {
		result += kss.resultKeySet[idx].Len()
	}
	return result
}

func (kss *keySetShard) Clear() {
	for idx := range kss.resultKeySet {
		kss.resultKeySet[idx].Clear()
	}
}
