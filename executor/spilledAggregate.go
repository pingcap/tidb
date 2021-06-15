// Copyright 2021 PingCAP, Inc.
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
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
)

type SpilledHashAggExec struct {
	*HashAggExec
	childDrained bool
	drained      bool
	spillMode    uint32

	// spill
	list         *chunk.ListInDisk
	lastChunkNum int
	idx          int
	tmpChk       *chunk.Chunk
	haveData     bool
}

func (e *SpilledHashAggExec) Close() error {
	if err := e.HashAggExec.Close(); err != nil {
		return err
	}
	if e.list != nil {
		if err := e.list.Close(); err != nil {
			return err
		}
	}
	e.tmpChk = nil
	return nil
}

func (e *SpilledHashAggExec) Open(ctx context.Context) error {
	if err := e.HashAggExec.Open(ctx); err != nil {
		return err
	}
	e.initForExec()
	return nil
}

func (e *SpilledHashAggExec) initForExec() {
	e.groupSet, _ = set.NewStringSetWithMemoryUsage()
	e.partialResultMap = make(aggPartialResultMapper)
	e.groupKeyBuffer = make([][]byte, 0, 8)
	e.childResult = newFirstChunk(e.children[0])
	e.tmpChk = newFirstChunk(e.children[0])
	e.list = chunk.NewListInDisk(retTypes(e.children[0]))
	e.idx = 0
	e.haveData = false
	e.drained = false
	e.childDrained = false
	e.spillMode = 0
}

func (e *SpilledHashAggExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	return e.spilledExec(ctx, req)
}

func (e *SpilledHashAggExec) spilledExec(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for {
		if e.prepared {
			for ; e.cursor4GroupKey < len(e.groupKeys); e.cursor4GroupKey++ {
				partialResults := e.getPartialResults(e.groupKeys[e.cursor4GroupKey])
				if len(e.PartialAggFuncs) == 0 {
					chk.SetNumVirtualRows(chk.NumRows() + 1)
				}
				for i, af := range e.PartialAggFuncs {
					if err := af.AppendFinalResult2Chunk(e.ctx, partialResults[i], chk); err != nil {
						return nil
					}
				}
				if chk.IsFull() {
					e.cursor4GroupKey++
					return nil
				}
			}
			e.cursor4GroupKey = 0
			e.groupKeys = e.groupKeys[:0]
			e.partialResultMap = make(aggPartialResultMapper)
			e.prepared = false
			e.spillMode = 0
			if e.tmpChk.NumRows() > 0 {
				err := e.list.Add(e.tmpChk)
				if err != nil {
					return err
				}
				e.tmpChk.Reset()
			}
			e.drained = !(e.lastChunkNum < e.list.NumChunks())
			e.lastChunkNum = e.list.NumChunks()
		}
		if e.drained {
			return nil
		}
		if err := e.prepare(ctx); err != nil {
			return err
		}
		if !e.haveData {
			if (len(e.groupSet.StringSet) == 0) && len(e.GroupByItems) == 0 {
				// If no groupby and no data, we should add an empty group.
				// For example:
				// "select count(c) from t;" should return one row [0]
				// "select count(c) from t group by c1;" should return empty result set.
				e.groupKeys = append(e.groupKeys, "")
			}
			e.prepared = true
		}
	}
}

func (e *SpilledHashAggExec) prepare(ctx context.Context) (err error) {
	for {
		if err := e.getNextChunk(ctx); err != nil {
			return err
		}
		if e.childResult.NumRows() == 0 {
			e.prepared = true
			return nil
		}
		e.haveData = true
		e.groupKeyBuffer, err = getGroupKey(e.ctx, e.childResult, e.groupKeyBuffer, e.GroupByItems)
		if err != nil {
			return err
		}
		for j := 0; j < e.childResult.NumRows(); j++ {
			groupKey := string(e.groupKeyBuffer[j])
			if !e.groupSet.Exist(groupKey) {
				if atomic.LoadUint32(&e.spillMode) == 1 {
					e.tmpChk.Append(e.childResult, j, j+1)
					if e.tmpChk.IsFull() {
						err = e.list.Add(e.tmpChk)
						if err != nil {
							return err
						}
						e.tmpChk.Reset()
					}
					continue
				} else {
					_ = e.groupSet.Insert(groupKey)
					e.groupKeys = append(e.groupKeys, groupKey)
					e.randomOOMForTest()
				}
			}
			partialResults := e.getPartialResults(groupKey)
			for i, af := range e.PartialAggFuncs {
				_, err := af.UpdatePartialResult(e.ctx, []chunk.Row{e.childResult.GetRow(j)}, partialResults[i])
				if err != nil {
					return err
				}
			}
		}
	}
}

func (e *SpilledHashAggExec) randomOOMForTest() {
	if rand.Int31n(200) < 1 {
		e.spillMode = 1
	}
}

func (e *SpilledHashAggExec) getNextChunk(ctx context.Context) (err error) {
	e.childResult.Reset()
	if !e.childDrained {
		if err := Next(ctx, e.children[0], e.childResult); err != nil {
			return err
		}
		if e.childResult.NumRows() == 0 {
			e.childDrained = true
		} else {
			return nil
		}
	}
	if e.idx < e.lastChunkNum {
		e.childResult, err = e.list.GetChunk(e.idx)
		if err != nil {
			return err
		}
		e.idx++
	}
	e.drained = true
	return nil
}
