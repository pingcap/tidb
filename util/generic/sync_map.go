// Copyright 2022 PingCAP, Inc.
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

package generic

import "sync"

// SyncMap is the generic version of the sync.Map.
type SyncMap[K comparable, V any] struct {
	item map[K]V
	mu   sync.RWMutex
}

// NewSyncMap returns a new SyncMap.
func NewSyncMap[K comparable, V any](capacity int) SyncMap[K, V] {
	return SyncMap[K, V]{
		item: make(map[K]V, capacity),
	}
}

// Store stores a value.
func (m *SyncMap[K, V]) Store(key K, value V) {
	m.mu.Lock()
	m.item[key] = value
	m.mu.Unlock()
}

// Load loads a key value.
func (m *SyncMap[K, V]) Load(key K) (V, bool) {
	m.mu.RLock()
	val, exist := m.item[key]
	m.mu.RUnlock()
	return val, exist
}

// Delete deletes the value for a key, returning the previous value if any.
// The exist result reports whether the key was present.
func (m *SyncMap[K, V]) Delete(key K) (val V, exist bool) {
	m.mu.Lock()
	val, exist = m.item[key]
	if exist {
		delete(m.item, key)
	}
	m.mu.Unlock()
	return val, exist
}

// Keys returns all the keys in the map.
func (m *SyncMap[K, V]) Keys() []K {
	ret := make([]K, 0, len(m.item))
	m.mu.RLock()
	for k := range m.item {
		ret = append(ret, k)
	}
	m.mu.RUnlock()
	return ret
}
