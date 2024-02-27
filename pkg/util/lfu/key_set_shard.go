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

const keySetCnt = 256

type keySetShard[k K, v V] struct {
	resultKeySet [keySetCnt]keySet[k, v]
}

func newKeySetShard[k K, v V]() *keySetShard[k, v] {
	result := keySetShard[k, v]{}
	for i := 0; i < keySetCnt; i++ {
		result.resultKeySet[i] = keySet[k, v]{
			set: make(map[k]v),
		}
	}
	return &result
}

func (kss *keySetShard[K, V]) Get(key K) (V, bool) {
	return kss.resultKeySet[KeyToHash(key)%keySetCnt].Get(key)
}

func (kss *keySetShard[K, V]) AddKeyValue(key K, table V) {
	kss.resultKeySet[KeyToHash(key)%keySetCnt].AddKeyValue(key, table)
}

func (kss *keySetShard[K, V]) Remove(key K) {
	kss.resultKeySet[KeyToHash(key)%keySetCnt].Remove(key)
}

func (kss *keySetShard[K, V]) Keys() []K {
	result := make([]K, 0, len(kss.resultKeySet))
	for idx := range kss.resultKeySet {
		result = append(result, kss.resultKeySet[idx].Keys()...)
	}
	return result
}

func (kss *keySetShard[K, V]) Len() int {
	result := 0
	for idx := range kss.resultKeySet {
		result += kss.resultKeySet[idx].Len()
	}
	return result
}

func (kss *keySetShard[K, V]) Clear() {
	for idx := range kss.resultKeySet {
		kss.resultKeySet[idx].Clear()
	}
}
