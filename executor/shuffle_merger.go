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
	"container/heap"
	"context"

	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ shuffleMerger = (*shuffleRandomMerger)(nil)
)

// shuffleMerger is the results merger of Shuffle executor.
type shuffleMerger interface {
	// Open initializes merger.
	Open(ctx context.Context) error
	// Prepare for executing. Should prepare merger before worker/splitter.
	Prepare(ctx context.Context, finishCh <-chan struct{})
	// Close de-initializes merger. Should close merger after worker/splitter.
	Close() error
	// Next retrievals next result. Each merger should implement `Next` for different merge algorithm.
	Next(ctx context.Context, chk *chunk.Chunk) error
	// PushToMerger is called by workers to push results or error to merger.
	PushToMerger(workerIdx int, data *shuffleData)
	// WorkerFinished signals that a worker is finished. Notice that it would not be called if not prepared.
	WorkerFinished(workerIdx int)
	// WorkersAllFinished signals that all workers are finished. Notice that it would not be called if not prepared.
	WorkersAllFinished()
}

type baseShuffleMerger struct {
	shuffle *ShuffleExec

	// fanIn is number of input channels. Should be equal to number of workers.
	fanIn int
	// isSingleDataCh indicates the merger need single(e.g shuffleRandomMerger) or multiple(e.g ShuffleMergeSortMerger) output channels.
	isSingleDataCh bool

	prepared bool
	finishCh <-chan struct{}
	dataCh   []chan *shuffleData
}

func (m *baseShuffleMerger) Open(ctx context.Context) error {
	m.prepared = false
	return nil
}

func (m *baseShuffleMerger) Prepare(ctx context.Context, finishCh <-chan struct{}) {
	m.finishCh = finishCh
	if m.isSingleDataCh {
		m.dataCh = make([]chan *shuffleData, 1)
		m.dataCh[0] = make(chan *shuffleData, m.fanIn)
	} else {
		m.dataCh = make([]chan *shuffleData, m.fanIn)
		for i := range m.dataCh {
			m.dataCh[i] = make(chan *shuffleData, 1)
		}
	}
	m.prepared = true
}

func (m *baseShuffleMerger) Close() error {
	if m.prepared {
		// waiting workers exit and close `dataCh` here.
		for _, ch := range m.dataCh {
			for range ch {
			}
		}
	}
	return nil
}

func (m *baseShuffleMerger) PushToMerger(workerIdx int, data *shuffleData) {
	if m.isSingleDataCh {
		m.dataCh[0] <- data
	} else {
		m.dataCh[workerIdx] <- data
	}
}

func (m *baseShuffleMerger) WorkerFinished(workerIdx int) {
	if !m.isSingleDataCh {
		close(m.dataCh[workerIdx])
	}
}

func (m *baseShuffleMerger) WorkersAllFinished() {
	if m.isSingleDataCh {
		close(m.dataCh[0])
	}
}

func (m *baseShuffleMerger) fetchData(ctx context.Context, partitionIdx int, chk *chunk.Chunk) error {
	chk.Reset()

	select {
	case <-m.finishCh:
		return nil
	case result, ok := <-m.dataCh[partitionIdx]:
		if !ok {
			return nil
		}
		if result.err != nil {
			return result.err
		}

		chk.SwapColumns(result.chk) // worker will not send an empty chunk
		result.giveBackCh <- result.chk
		return nil
	}
}

// shuffleRandomMerger merges results by the whole chunk, resulting random order.
type shuffleRandomMerger struct {
	baseShuffleMerger
}

// newShuffleRandomMerger creates shuffleRandomMerger.
func newShuffleRandomMerger(shuffle *ShuffleExec, fanIn int) *shuffleRandomMerger {
	return &shuffleRandomMerger{baseShuffleMerger{
		shuffle:        shuffle,
		fanIn:          fanIn,
		isSingleDataCh: true,
	},
	}
}

// Next implements shuffleMerger interface.
func (m *shuffleRandomMerger) Next(ctx context.Context, chk *chunk.Chunk) error {
	return m.fetchData(ctx, 0, chk)
}

// shuffleMergeSortMerger merges results by merge-sort.
// See also executor/sort.go, `multiWayMerge`.
// See also https://en.wikipedia.org/wiki/K-way_merge_algorithm.
type shuffleMergeSortMerger struct {
	baseShuffleMerger

	byItems     []property.Item
	keyCmpFuncs []chunk.CompareFunc
	keyColumns  []int

	multiWayMerge *multiWayMerge
	partitionList []*chunk.Chunk
}

// newShuffleMergeSortMerger creates shuffleMergeSortMerger
func newShuffleMergeSortMerger(shuffle *ShuffleExec, fanIn int, byItems []property.Item) *shuffleMergeSortMerger {
	return &shuffleMergeSortMerger{
		baseShuffleMerger: baseShuffleMerger{
			shuffle:        shuffle,
			fanIn:          fanIn,
			isSingleDataCh: false,
		},
		byItems: byItems,
	}
}

func (m *shuffleMergeSortMerger) initCompareFuncs() {
	m.keyCmpFuncs = make([]chunk.CompareFunc, len(m.byItems))
	for i := range m.byItems {
		keyType := m.byItems[i].Col.GetType()
		m.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (m *shuffleMergeSortMerger) buildKeyColumns() {
	m.keyColumns = make([]int, 0, len(m.byItems))
	for _, by := range m.byItems {
		m.keyColumns = append(m.keyColumns, by.Col.Index)
	}
}

func (m *shuffleMergeSortMerger) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range m.keyColumns {
		cmpFunc := m.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if m.byItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

func (m *shuffleMergeSortMerger) Prepare(ctx context.Context, finishCh <-chan struct{}) {
	m.baseShuffleMerger.Prepare(ctx, finishCh)

	m.initCompareFuncs()
	m.buildKeyColumns()
}

// Next implements shuffleMerger interface.
func (m *shuffleMergeSortMerger) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if m.multiWayMerge == nil {
		m.multiWayMerge = &multiWayMerge{m.lessRow, make([]partitionPointer, 0, m.fanIn)}
		m.partitionList = make([]*chunk.Chunk, m.fanIn)
		for i := 0; i < m.fanIn; i++ {
			chk := newFirstChunk(m.shuffle)
			if err := m.fetchData(ctx, i, chk); err != nil {
				return err
			}
			if chk.NumRows() > 0 {
				m.partitionList[i] = chk
				row := m.partitionList[i].GetRow(0)
				m.multiWayMerge.elements = append(m.multiWayMerge.elements, partitionPointer{row: row, partitionID: i, consumed: 0})
			}
		}
		heap.Init(m.multiWayMerge)
	}

	req.Reset()

	for !req.IsFull() && m.multiWayMerge.Len() > 0 {
		ptr := m.multiWayMerge.elements[0]
		req.AppendRow(ptr.row)
		ptr.consumed++

		if ptr.consumed >= m.partitionList[ptr.partitionID].NumRows() {
			if err = m.fetchData(ctx, ptr.partitionID, m.partitionList[ptr.partitionID]); err != nil {
				return err
			}
			if m.partitionList[ptr.partitionID].NumRows() == 0 {
				m.partitionList[ptr.partitionID] = nil
				heap.Remove(m.multiWayMerge, 0)
				continue
			}
			ptr.consumed = 0
		}

		ptr.row = m.partitionList[ptr.partitionID].GetRow(ptr.consumed)
		m.multiWayMerge.elements[0] = ptr
		heap.Fix(m.multiWayMerge, 0)
	}

	return nil
}
