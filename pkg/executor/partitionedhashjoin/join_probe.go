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
	hashValues         []uint64         // the hash value of each rows
	serializedKeys     [][]byte         // used for save serialized keys
	filterVector       []bool           // if there is filter before probe, filterVector saves the filter result
	nullKeyVector      []bool           // nullKeyVector[i] = true if any of the key is null
	init               bool
}

type probeCtx struct {
	keyIndex       []int
	columnTypes    []*types.FieldType
	filter         expression.CNFExprs
	keyIsNullable  bool
	buildTableMeta *tableMeta
	typeCtx        types.Context
}

func (ppi *probeProcessInfo) initForCurrentChunk(info *probeCtx) error {
	if ppi.init {
		return nil
	}
	ppi.init = true
	rows := ppi.chunk.NumRows()
	if cap(ppi.matchedRowsHeaders) >= rows {
		ppi.matchedRowsHeaders = ppi.matchedRowsHeaders[:rows]
	} else {
		ppi.matchedRowsHeaders = make([]unsafe.Pointer, rows)
	}
	if cap(ppi.hashValues) >= rows {
		ppi.hashValues = ppi.hashValues[:rows]
	} else {
		ppi.hashValues = make([]uint64, rows)
	}
	if info.filter != nil {
		if cap(ppi.filterVector) >= rows {
			ppi.filterVector = ppi.filterVector[:rows]
		} else {
			ppi.filterVector = make([]bool, rows)
		}
	}
	if info.keyIsNullable {
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
	if info.filter != nil {
		return errors.New("Probe side filter is not supported yet")
	}

	// generate serialized key
	for _, keyIndex := range info.keyIndex {
		// todo set ignoreSign to false for unsigned key join signed key
		err := codec.SerializeKeys(info.typeCtx, ppi.chunk, info.columnTypes[keyIndex], keyIndex, ppi.filterVector, ppi.nullKeyVector, true, ppi.serializedKeys)
		if err != nil {
			return err
		}
	}
	// generate hash value
	hash := fnv.New64()
	for i := 0; i < rows; i++ {
		if (ppi.filterVector != nil && ppi.filterVector[i]) || (ppi.nullKeyVector != nil && ppi.nullKeyVector[i]) {
			continue
		}
		hash.Reset()
		// As the golang doc described, `Hash.Write` never returns an error.
		// See https://golang.org/pkg/hash/#Hash
		_, _ = hash.Write(ppi.serializedKeys[i])
		ppi.hashValues[i] = hash.Sum64()
	}
	return nil
}

type joinProbe interface {
}

type baseJoinProbe struct {
}
