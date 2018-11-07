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
	"sync"

	"github.com/pingcap/tidb/types"
)

// Pool is the column pool.
type Pool struct {
	initCap int

	varLenColPool   *colPool
	fixLenColPool4  *colPool
	fixLenColPool8  *colPool
	fixLenColPool16 *colPool
	fixLenColPool40 *colPool
}

// NewPool creates a new Pool.
func NewPool(initCap int) *Pool {
	return &Pool{
		initCap:         initCap,
		varLenColPool:   newColPool(varElemLen, initCap),
		fixLenColPool4:  newColPool(4, initCap),
		fixLenColPool8:  newColPool(8, initCap),
		fixLenColPool16: newColPool(16, initCap),
		fixLenColPool40: newColPool(40, initCap),
	}
}

// GetChunk gets a Chunk from the Pool.
func (p *Pool) GetChunk(fields []*types.FieldType) *Chunk {
	chk := new(Chunk)
	chk.capacity = p.initCap
	chk.columns = make([]*column, len(fields))
	for i, f := range fields {
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			chk.columns[i] = p.varLenColPool.Get().(*column)
		case 4:
			chk.columns[i] = p.fixLenColPool4.Get().(*column)
		case 8:
			chk.columns[i] = p.fixLenColPool8.Get().(*column)
		case 16:
			chk.columns[i] = p.fixLenColPool16.Get().(*column)
		case 40:
			chk.columns[i] = p.fixLenColPool40.Get().(*column)
		}
	}
	return chk
}

// PutChunk puts a Chunk back to the Pool.
func (p *Pool) PutChunk(fields []*types.FieldType, chk *Chunk) {
	for i, f := range fields {
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			p.varLenColPool.Put(chk.columns[i])
		case 4:
			p.fixLenColPool4.Put(chk.columns[i])
		case 8:
			p.fixLenColPool8.Put(chk.columns[i])
		case 16:
			p.fixLenColPool16.Put(chk.columns[i])
		case 40:
			p.fixLenColPool40.Put(chk.columns[i])
		}
	}
	chk.columns = nil // release the column references.
}

type colPool struct {
	sync.Pool
}

func newColPool(elemLen, initCap int) *colPool {
	pool := &colPool{}
	if elemLen != varElemLen {
		pool.New = func() interface{} { return newFixedLenColumn(elemLen, initCap) }
	} else {
		pool.New = func() interface{} { return newVarLenColumn(initCap, nil) }
	}
	return pool
}
