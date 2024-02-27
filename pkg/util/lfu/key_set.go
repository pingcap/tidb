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
	"sync"

	"golang.org/x/exp/maps"
)

type K interface {
	~uint64 | ~string | ~int | ~int32 | ~uint32 | ~int64
}
type V interface {
	DeepCopy() any
	TotalTrackingMemUsage() int64
}

type keySet[k K, v V] struct {
	set map[k]v
	mu  sync.RWMutex
}

func (ks *keySet[K, V]) Remove(key K) int64 {
	var cost int64
	ks.mu.Lock()
	if table, ok := ks.set[key]; ok {
		// if table is nil, it still return 0.
		cost = table.TotalTrackingMemUsage()
		delete(ks.set, key)
	}
	ks.mu.Unlock()
	return cost
}

func (ks *keySet[K, V]) Keys() []K {
	ks.mu.RLock()
	result := maps.Keys(ks.set)
	ks.mu.RUnlock()
	return result
}

func (ks *keySet[K, V]) Len() int {
	ks.mu.RLock()
	result := len(ks.set)
	ks.mu.RUnlock()
	return result
}

func (ks *keySet[K, V]) AddKeyValue(key K, value V) {
	ks.mu.Lock()
	ks.set[key] = value
	ks.mu.Unlock()
}

func (ks *keySet[K, V]) Get(key K) (V, bool) {
	ks.mu.RLock()
	value, ok := ks.set[key]
	ks.mu.RUnlock()
	return value, ok
}

func (ks *keySet[K, V]) Clear() {
	ks.mu.Lock()
	ks.set = make(map[K]V)
	ks.mu.Unlock()
}
