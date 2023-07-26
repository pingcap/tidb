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

const keySetCnt = 128

type keySetShard struct {
	resultKeySet [keySetCnt]keySet
}

func newKeySetShard() *keySetShard {
	result := keySetShard{}
	for i := 0; i < keySetCnt; i++ {
		result.resultKeySet[i] = keySet{
			set: make(map[int64]struct{}),
		}
	}
	return &result
}

func (kss *keySetShard) Add(key int64) {
	kss.resultKeySet[key%keySetCnt].Add(key)
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
