// Copyright 2024 PingCAP, Inc.
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

package partitionedhashjoin

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"hash/fnv"
	"unsafe"
)

type probeProcessInfo struct {
	chunk              *chunk.Chunk
	matchedRowsHeaders []unsafe.Pointer // the start address of each matched rows
	currentRowsPos     []unsafe.Pointer // the current address of each matched rows
	serializedKeys     [][]byte         // used for save serialized keys
	filterVector       []bool           // if there is filter before probe, filterVector saves the filter result
	nullKeyVector      []bool           // nullKeyVector[i] = true if any of the key is null
	currentProbeRow    int
	init               bool
}

type probeMode int

const (
	OneInt64 probeMode = iota
	OneFloat64
	SerializeKey
)

type probeCtx struct {
	keyIndex           []int
	columnTypes        []*types.FieldType
	filter             expression.CNFExprs
	postProbeCondition expression.CNFExprs
	keyIsNullable      bool
	joinHashTable      *joinHashTable
	hashTableMeta      *tableMeta
	typeCtx            types.Context
	probeMode          probeMode
}

func (probeCtx *probeCtx) hasPostProbeCondition() bool {
	return probeCtx.postProbeCondition != nil
}

func (ppi *probeProcessInfo) initForCurrentChunk(ctx *probeCtx) error {
	if ppi.init {
		return nil
	}
	ppi.init = true
	rows := ppi.chunk.NumRows()
	ppi.currentProbeRow = 0
	if cap(ppi.matchedRowsHeaders) >= rows {
		ppi.matchedRowsHeaders = ppi.matchedRowsHeaders[:rows]
	} else {
		ppi.matchedRowsHeaders = make([]unsafe.Pointer, rows)
	}
	if ctx.filter != nil {
		if cap(ppi.filterVector) >= rows {
			ppi.filterVector = ppi.filterVector[:rows]
		} else {
			ppi.filterVector = make([]bool, rows)
		}
	}
	if ctx.keyIsNullable {
		if cap(ppi.nullKeyVector) >= rows {
			ppi.nullKeyVector = ppi.nullKeyVector[:rows]
		} else {
			ppi.nullKeyVector = make([]bool, rows)
		}
	}
	if cap(ppi.serializedKeys) >= rows {
		ppi.serializedKeys = ppi.serializedKeys[:rows]
	} else {
		ppi.serializedKeys = make([][]byte, rows)
	}
	// todo support filter
	if ctx.filter != nil {
		return errors.New("Probe side filter is not supported yet")
	}

	// generate serialized key
	for _, keyIndex := range ctx.keyIndex {
		// todo set ignoreSign to false for unsigned key join signed key
		err := codec.SerializeKeys(ctx.typeCtx, ppi.chunk, ctx.columnTypes[keyIndex], keyIndex, ppi.filterVector, ppi.nullKeyVector, true, ppi.serializedKeys)
		if err != nil {
			return err
		}
	}
	// generate hash value
	hash := fnv.New64()
	for i := 0; i < rows; i++ {
		if (ppi.filterVector != nil && !ppi.filterVector[i]) || (ppi.nullKeyVector != nil && ppi.nullKeyVector[i]) {
			continue
		}
		hash.Reset()
		// As the golang doc described, `Hash.Write` never returns an error.
		// See https://golang.org/pkg/hash/#Hash
		_, _ = hash.Write(ppi.serializedKeys[i])
		ppi.matchedRowsHeaders[i] = ctx.joinHashTable.lookup(hash.Sum64())
	}
	return nil
}

type joinProbe interface {
	doProbe(chk *chunk.Chunk, info *probeProcessInfo) (err error)
}

type baseJoinProbe struct {
	ctx              *probeCtx
	maxChunkSize     int
	rightAsBuildSide bool
	// lUsed/rUsed show which columns are used by father for left child and right child.
	// NOTE:
	// 1. all columns are used if lUsed/rUsed is nil.
	// 2. no columns are used if lUsed/rUsed is not nil but the size of lUsed/rUsed is 0.
	lUsed, rUsed []int
}

func (baseJoinProbe *baseJoinProbe) getNext(chk *chunk.Chunk, info *probeProcessInfo) (err error) {
	if chk.NumRows() >= baseJoinProbe.maxChunkSize {
		return nil
	}
	if !info.init {
		err = info.initForCurrentChunk(baseJoinProbe.ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

type innerJoinProbe struct {
	baseJoinProbe
}

func (innerJoinProbe *innerJoinProbe) doProbeOneInt64(chk *chunk.Chunk, info *probeProcessInfo) (err error) {
	keyOffset := SizeOfNextPtr + innerJoinProbe.ctx.hashTableMeta.nullMapLength
	for !chk.IsFull() && info.currentProbeRow < info.chunk.NumRows() {
		if info.matchedRowsHeaders[info.currentProbeRow] != nil {
			if *(*int64)(unsafe.Pointer(&info.serializedKeys[info.currentProbeRow][0])) == *(*int64)(unsafe.Add(info.matchedRowsHeaders[info.currentProbeRow], keyOffset)) {
				// key matched, construct result block
				if innerJoinProbe.rightAsBuildSide {

				} else {

				}
			}
			info.matchedRowsHeaders[info.currentProbeRow] = getNextRowAddress(info.matchedRowsHeaders[info.currentProbeRow])
		} else {
			info.currentProbeRow++
		}
	}
	return
}

func (innerJoinProbe *innerJoinProbe) doProbe(chk *chunk.Chunk, info *probeProcessInfo) (err error) {
	if innerJoinProbe.ctx.hasPostProbeCondition() {
		return errors.New("join with other condition is not supported yet")
	}
	switch innerJoinProbe.ctx.probeMode {
	case OneInt64:
		err = innerJoinProbe.doProbeOneInt64(chk, info)
	case OneFloat64, SerializeKey:
		err = errors.New("unsupported")
	}
	return
}
