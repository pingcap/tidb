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

package lightning

import "sync"

// resourceManager is a thread-safe manager for resource.
type resourceManager[T any] struct {
	item map[string]*T
	mu   sync.RWMutex
}

func (m *resourceManager[T]) init(size int) {
	m.item = make(map[string]*T, size)
}

// Store stores a resource.
func (m *resourceManager[T]) Store(key string, bc *T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.item[key] = bc
}

// Load loads a resource.
func (m *resourceManager[T]) Load(key string) (*T, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bc, exist := m.item[key]
	if !exist {
		return nil, exist
	}
	return bc, exist
}

// Drop drops a resource.
func (m *resourceManager[T]) Drop(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.item, key)
}
