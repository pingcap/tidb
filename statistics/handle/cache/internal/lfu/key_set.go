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

type keySet struct {
	set map[int64]struct{}
	mu  sync.RWMutex
}

func (ks *keySet) Add(key int64) {
	ks.mu.Lock()
	ks.set[key] = struct{}{}
	ks.mu.Unlock()
}

func (ks *keySet) Remove(key int64) {
	ks.mu.Lock()
	delete(ks.set, key)
	ks.mu.Unlock()
}

func (ks *keySet) Keys() []int64 {
	ks.mu.RLock()
	result := maps.Keys(ks.set)
	ks.mu.RUnlock()
	return result
}

func (ks *keySet) Len() int {
	ks.mu.RLock()
	result := len(ks.set)
	ks.mu.RUnlock()
	return result
}
