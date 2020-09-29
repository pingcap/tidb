// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"fmt"
	"hash"
	"hash/fnv"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
)

func (s *pkgTestSuite) testHashTables(c *C) {
	var ht baseHashTable
	test := func() {
		ht.Put(1, chunk.RowPtr{ChkIdx: 1, RowIdx: 1})
		c.Check(ht.Get(1), DeepEquals, []chunk.RowPtr{{ChkIdx: 1, RowIdx: 1}})

		rawData := map[uint64][]chunk.RowPtr{}
		for i := uint64(0); i < 10; i++ {
			for j := uint64(0); j < initialEntrySliceLen*i; j++ {
				rawData[i] = append(rawData[i], chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)})
			}
		}
		// put all rawData into ht vertically
		for j := uint64(0); j < initialEntrySliceLen*9; j++ {
			for i := 9; i >= 0; i-- {
				i := uint64(i)
				if !(j < initialEntrySliceLen*i) {
					break
				}
				ht.Put(i, rawData[i][j])
			}
		}
		// check
		totalCount := 0
		for i := uint64(0); i < 10; i++ {
			totalCount += len(rawData[i])
			c.Check(ht.Get(i), DeepEquals, rawData[i])
		}
		c.Check(ht.Len(), Equals, uint64(totalCount))
	}
	// test unsafeHashTable
	ht = newUnsafeHashTable(0)
	test()
	// test ConcurrentMapHashTable
	ht = newConcurrentMapHashTable()
	test()
}

func initBuildChunk(numRows int) (*chunk.Chunk, []*types.FieldType) {
	numCols := 6
	colTypes := make([]*types.FieldType, 0, numCols)
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeNewDecimal})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeJSON})

	oldChk := chunk.NewChunkWithCapacity(colTypes, numRows)
	for i := 0; i < numRows; i++ {
		str := fmt.Sprintf("%d.12345", i)
		oldChk.AppendNull(0)
		oldChk.AppendInt64(1, int64(i))
		oldChk.AppendString(2, str)
		oldChk.AppendString(3, str)
		oldChk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		oldChk.AppendJSON(5, json.CreateBinary(str))
	}
	return oldChk, colTypes
}

func initProbeChunk(numRows int) (*chunk.Chunk, []*types.FieldType) {
	numCols := 3
	colTypes := make([]*types.FieldType, 0, numCols)
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarchar})

	oldChk := chunk.NewChunkWithCapacity(colTypes, numRows)
	for i := 0; i < numRows; i++ {
		str := fmt.Sprintf("%d.12345", i)
		oldChk.AppendNull(0)
		oldChk.AppendInt64(1, int64(i))
		oldChk.AppendString(2, str)
	}
	return oldChk, colTypes
}

type hashCollision struct {
	count int
}

func (h *hashCollision) Sum64() uint64 {
	h.count++
	return 0
}
func (h hashCollision) Write(p []byte) (n int, err error) { return len(p), nil }
func (h hashCollision) Reset()                            {}
func (h hashCollision) Sum(b []byte) []byte               { panic("not implemented") }
func (h hashCollision) Size() int                         { panic("not implemented") }
func (h hashCollision) BlockSize() int                    { panic("not implemented") }

func (s *pkgTestSerialSuite) TestHashRowContainer(c *C) {
	hashFunc := func() hash.Hash64 {
		return fnv.New64()
	}
	rowContainer := s.testHashRowContainer(c, hashFunc, false)
	c.Assert(rowContainer.stat.probeCollision, Equals, 0)
	// On windows time.Now() is imprecise, the elapse time may equal 0
	c.Assert(rowContainer.stat.buildTableElapse >= 0, IsTrue)

	rowContainer = s.testHashRowContainer(c, hashFunc, true)
	c.Assert(rowContainer.stat.probeCollision, Equals, 0)
	c.Assert(rowContainer.stat.buildTableElapse >= 0, IsTrue)

	h := &hashCollision{count: 0}
	hashFuncCollision := func() hash.Hash64 {
		return h
	}
	rowContainer = s.testHashRowContainer(c, hashFuncCollision, false)
	c.Assert(h.count > 0, IsTrue)
	c.Assert(rowContainer.stat.probeCollision > 0, IsTrue)
	c.Assert(rowContainer.stat.buildTableElapse >= 0, IsTrue)
}

func (s *pkgTestSerialSuite) testHashRowContainer(c *C, hashFunc func() hash.Hash64, spill bool) *hashRowContainer {
	sctx := mock.NewContext()
	var err error
	numRows := 10

	chk0, colTypes := initBuildChunk(numRows)
	chk1, _ := initBuildChunk(numRows)

	hCtx := &hashContext{
		allTypes:  colTypes,
		keyColIdx: []int{1, 2},
	}
	hCtx.hasNull = make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		hCtx.hashVals = append(hCtx.hashVals, hashFunc())
	}
	rowContainer := newHashRowContainer(sctx, 0, hCtx)
	tracker := rowContainer.GetMemTracker()
	tracker.SetLabel(memory.LabelForBuildSideResult)
	if spill {
		tracker.SetBytesLimit(1)
		rowContainer.rowContainer.ActionSpillForTest().Action(tracker)
	}
	err = rowContainer.PutChunk(chk0, nil)
	c.Assert(err, IsNil)
	err = rowContainer.PutChunk(chk1, nil)
	c.Assert(err, IsNil)
	rowContainer.ActionSpill().(*chunk.SpillDiskAction).WaitForTest()
	c.Assert(rowContainer.alreadySpilledSafeForTest(), Equals, spill)
	c.Assert(rowContainer.GetMemTracker().BytesConsumed() == 0, Equals, spill)
	c.Assert(rowContainer.GetMemTracker().BytesConsumed() > 0, Equals, !spill)
	if rowContainer.alreadySpilledSafeForTest() {
		c.Assert(rowContainer.GetDiskTracker(), NotNil)
		c.Assert(rowContainer.GetDiskTracker().BytesConsumed() > 0, Equals, true)
	}

	probeChk, probeColType := initProbeChunk(2)
	probeRow := probeChk.GetRow(1)
	probeCtx := &hashContext{
		allTypes:  probeColType,
		keyColIdx: []int{1, 2},
	}
	probeCtx.hasNull = make([]bool, 1)
	probeCtx.hashVals = append(hCtx.hashVals, hashFunc())
	matched, _, err := rowContainer.GetMatchedRowsAndPtrs(hCtx.hashVals[1].Sum64(), probeRow, probeCtx)
	c.Assert(err, IsNil)
	c.Assert(len(matched), Equals, 2)
	c.Assert(matched[0].GetDatumRow(colTypes), DeepEquals, chk0.GetRow(1).GetDatumRow(colTypes))
	c.Assert(matched[1].GetDatumRow(colTypes), DeepEquals, chk1.GetRow(1).GetDatumRow(colTypes))
	return rowContainer
}
