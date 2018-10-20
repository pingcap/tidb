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
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"container/list"
	"math/rand"
	"sync"

	"github.com/pingcap/tidb/types"
)

type Pool struct {
	varLenColPool   *colPool
	fixLenColPool4  *colPool
	fixLenColPool8  *colPool
	fixLenColPool16 *colPool
	fixLenColPool40 *colPool
}

func NewPool(maxChunkSize int) *Pool {
	numShards := 8
	return &Pool{
		varLenColPool:   newColPool(numShards, varElemLen),
		fixLenColPool4:  newColPool(numShards, 4),
		fixLenColPool8:  newColPool(numShards, 8),
		fixLenColPool16: newColPool(numShards, 16),
		fixLenColPool40: newColPool(numShards, 40),
	}
}

func (p *Pool) GetChunk(fields []*types.FieldType, cap int) *Chunk {
	chk := new(Chunk)
	chk.columns = make([]*column, 0, len(fields))
	chk.capacity = cap

	for _, f := range fields {
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			chk.columns = append(chk.columns, p.varLenColPool.get(cap))
		case 4:
			chk.columns = append(chk.columns, p.fixLenColPool4.get(cap))
		case 8:
			chk.columns = append(chk.columns, p.fixLenColPool8.get(cap))
		case 16:
			chk.columns = append(chk.columns, p.fixLenColPool16.get(cap))
		case 40:
			chk.columns = append(chk.columns, p.fixLenColPool40.get(cap))
		}
	}
	return chk
}

func (p *Pool) PutChunk(fields []*types.FieldType, chk *Chunk) {
	for i, f := range fields {
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			p.varLenColPool.put(chk.columns[i])
		case 4:
			p.fixLenColPool4.put(chk.columns[i])
		case 8:
			p.fixLenColPool8.put(chk.columns[i])
		case 16:
			p.fixLenColPool16.put(chk.columns[i])
		case 40:
			p.fixLenColPool40.put(chk.columns[i])
		}
	}
}

type colPool struct {
	shards  []colPoolShard
	elemLen int
}

func newColPool(numShards int, elemLen int) *colPool {
	return &colPool{
		shards:  make([]colPoolShard, numShards),
		elemLen: elemLen,
	}
}

func (cp *colPool) put(col *column) {
	ordinal := rand.Int() % len(cp.shards)
	cp.shards[ordinal].put(col)
}

func (cp *colPool) get(cap int) *column {
	ordinal := rand.Int() % len(cp.shards)
	col := cp.shards[ordinal].get()
	if col != nil {
		return col
	}

	if cp.elemLen == varElemLen {
		return newVarLenColumn(cap, nil)
	}
	return newFixedLenColumn(cp.elemLen, cap)
}

type colPoolShard struct {
	sync.Mutex
	cols *list.List
}

func (ps *colPoolShard) put(col *column) {
	ps.Lock()
	defer ps.Unlock()

	ps.cols.PushFront(col)
}

func (ps *colPoolShard) get() *column {
	ps.Lock()
	defer ps.Unlock()

	if ps.cols.Len() > 0 {
		head := ps.cols.Front()
		return ps.cols.Remove(head).(*column)
	}
	return nil
}
