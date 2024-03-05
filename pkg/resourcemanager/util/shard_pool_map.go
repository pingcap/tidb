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

package util

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/intest"
)

const shard = 8

func hash(key string) int {
	return int(key[0]) % shard
}

// ShardPoolMap is a map with shard
type ShardPoolMap struct {
	pools [shard]poolMap
}

// NewShardPoolMap creates a shard pool map
func NewShardPoolMap() *ShardPoolMap {
	var result ShardPoolMap
	for i := 0; i < shard; i++ {
		result.pools[i] = newPoolMap()
	}
	return &result
}

// Add adds a pool to the map
func (s *ShardPoolMap) Add(key string, pool *PoolContainer) error {
	return s.pools[hash(key)].Add(key, pool)
}

// Del deletes a pool to the map.
func (s *ShardPoolMap) Del(key string) {
	s.pools[hash(key)].Del(key)
}

// Iter iterates the map
func (s *ShardPoolMap) Iter(fn func(pool *PoolContainer)) {
	for i := 0; i < shard; i++ {
		s.pools[i].Iter(fn)
	}
}

type poolMap struct {
	mu      sync.RWMutex
	poolMap map[string]*PoolContainer
}

func newPoolMap() poolMap {
	return poolMap{poolMap: make(map[string]*PoolContainer)}
}

func (p *poolMap) Add(key string, pool *PoolContainer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, contain := p.poolMap[key]; contain && !intest.InTest {
		return errors.New("pool is already exist")
	}
	p.poolMap[key] = pool
	return nil
}

func (p *poolMap) Del(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.poolMap, key)
}

func (p *poolMap) Iter(fn func(pool *PoolContainer)) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pool := range p.poolMap {
		fn(pool)
	}
}
