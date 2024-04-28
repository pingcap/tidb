// Copyright 2018 PingCAP, Inc.
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

package chunk

import (
	"sync"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/syncutil"
)

var (
	globalChunkPoolMutex syncutil.RWMutex
	// globalChunkPool is a chunk pool, the key is the init capacity.
	globalChunkPool = make(map[int]*Pool)
)

// getChunkFromPool gets a Chunk from the Pool. In fact, initCap is the size of the bucket in the histogram.
// so it will not have too many difference value.
func getChunkFromPool(initCap int, fields []*types.FieldType) *Chunk {
	globalChunkPoolMutex.RLock()
	pool, ok := globalChunkPool[initCap]
	globalChunkPoolMutex.RUnlock()
	if ok {
		return pool.GetChunk(fields)
	}
	globalChunkPoolMutex.Lock()
	defer globalChunkPoolMutex.Unlock()
	globalChunkPool[initCap] = NewPool(initCap)
	return globalChunkPool[initCap].GetChunk(fields)
}

func putChunkFromPool(initCap int, fields []*types.FieldType, chk *Chunk) {
	globalChunkPoolMutex.RLock()
	pool, ok := globalChunkPool[initCap]
	globalChunkPoolMutex.RUnlock()
	if ok {
		pool.PutChunk(fields, chk)
		return
	}
	globalChunkPoolMutex.Lock()
	defer globalChunkPoolMutex.Unlock()
	globalChunkPool[initCap] = NewPool(initCap)
	globalChunkPool[initCap].PutChunk(fields, chk)
}

// Pool is the Column pool.
// NOTE: Pool is non-copyable.
type Pool struct {
	initCap int

	varLenColPool   *sync.Pool
	fixLenColPool4  *sync.Pool
	fixLenColPool8  *sync.Pool
	fixLenColPool16 *sync.Pool
	fixLenColPool40 *sync.Pool
}

// NewPool creates a new Pool.
func NewPool(initCap int) *Pool {
	return &Pool{
		initCap:         initCap,
		varLenColPool:   &sync.Pool{New: func() any { return newVarLenColumn(initCap) }},
		fixLenColPool4:  &sync.Pool{New: func() any { return newFixedLenColumn(4, initCap) }},
		fixLenColPool8:  &sync.Pool{New: func() any { return newFixedLenColumn(8, initCap) }},
		fixLenColPool16: &sync.Pool{New: func() any { return newFixedLenColumn(16, initCap) }},
		fixLenColPool40: &sync.Pool{New: func() any { return newFixedLenColumn(40, initCap) }},
	}
}

// GetChunk gets a Chunk from the Pool.
func (p *Pool) GetChunk(fields []*types.FieldType) *Chunk {
	chk := new(Chunk)
	chk.capacity = p.initCap
	chk.columns = make([]*Column, len(fields))
	for i, f := range fields {
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			chk.columns[i] = p.varLenColPool.Get().(*Column)
		case 4:
			chk.columns[i] = p.fixLenColPool4.Get().(*Column)
		case 8:
			chk.columns[i] = p.fixLenColPool8.Get().(*Column)
		case 16:
			chk.columns[i] = p.fixLenColPool16.Get().(*Column)
		case 40:
			chk.columns[i] = p.fixLenColPool40.Get().(*Column)
		}
	}
	return chk
}

// PutChunk puts a Chunk back to the Pool.
func (p *Pool) PutChunk(fields []*types.FieldType, chk *Chunk) {
	for i, f := range fields {
		c := chk.columns[i]
		c.reset()
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			p.varLenColPool.Put(c)
		case 4:
			p.fixLenColPool4.Put(c)
		case 8:
			p.fixLenColPool8.Put(c)
		case 16:
			p.fixLenColPool16.Put(c)
		case 40:
			p.fixLenColPool40.Put(c)
		}
	}
	chk.columns = nil // release the Column references.
}
