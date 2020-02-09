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
	"context"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/spaolacci/murmur3"
)

var (
	_ shuffleSplitter = (*shuffleRandomSplitter)(nil)
	_ shuffleSplitter = (*shuffleHashSplitter)(nil)
)

// shuffleSplitter is the input splitter of Shuffle executor.
type shuffleSplitter interface {
	// Open initializes splitter.
	Open(ctx context.Context) error
	// Prepare for executing.
	Prepare(ctx context.Context, finishCh <-chan struct{})
	// Close de-initializes splitter.
	Close() error
	// PushResult pushs result to splitter.
	PushResult(ctx context.Context, chk *chunk.Chunk) (finished bool, err error)
	// SplitByRow splits input into partitions row by row. Override this method to implement different splitter scheme.
	SplitByRow(ctx context.Context, input *chunk.Chunk, partitionIndices []int) ([]int, error)
}

type baseShuffleSplitter struct {
	workerIdx int
	shuffle   *ShuffleExec

	// fanOut is number of merger. Can be NOT equal to number of workers.
	fanOut int
	// self is the concrete shuffleSplitter itself.
	self shuffleSplitter

	finishCh <-chan struct{}
	holderCh []chan *chunk.Chunk

	// temporary variables
	results          []*chunk.Chunk
	partitionIndices []int
}

func (s *baseShuffleSplitter) Open(ctx context.Context) error {
	return nil
}

func (s *baseShuffleSplitter) Close() error {
	return nil
}

func (s *baseShuffleSplitter) Prepare(ctx context.Context, finishCh <-chan struct{}) {
	s.finishCh = finishCh
	s.holderCh = make([]chan *chunk.Chunk, s.fanOut)
	for i := range s.holderCh {
		s.holderCh[i] = make(chan *chunk.Chunk, 1)
		s.holderCh[i] <- newFirstChunk(s.shuffle)
	}
}

func (s *baseShuffleSplitter) PushResult(ctx context.Context, chk *chunk.Chunk) (finished bool, err error) {
	if s.results == nil {
		s.results = make([]*chunk.Chunk, s.fanOut)
	}

	numRows := chk.NumRows()
	if numRows > 0 {
		s.partitionIndices, err = s.self.SplitByRow(ctx, chk, s.partitionIndices)
		if err != nil {
			return false, err
		}
		for i := 0; i < numRows; i++ {
			partitionIdx := s.partitionIndices[i]

			if s.results[partitionIdx] == nil {
				select {
				case <-s.finishCh:
					return true, nil
				case s.results[partitionIdx] = <-s.holderCh[partitionIdx]:
					break
				}
			}
			s.results[partitionIdx].AppendRow(chk.GetRow(i))
			if s.results[partitionIdx].IsFull() {
				data := &shuffleData{chk: s.results[partitionIdx], giveBackCh: s.holderCh[partitionIdx]}
				s.shuffle.PushToMerger(partitionIdx, s.workerIdx, data)
				s.results[partitionIdx] = nil
			}
		}
		return false, nil
	}

	for partitionIdx := 0; partitionIdx < s.fanOut; partitionIdx++ {
		if s.results[partitionIdx] != nil {
			data := &shuffleData{chk: s.results[partitionIdx], giveBackCh: s.holderCh[partitionIdx]}
			s.shuffle.PushToMerger(partitionIdx, s.workerIdx, data)
			s.results[partitionIdx] = nil
		}
	}
	return true, nil
}

// shuffleRandomSplitter splits data source by the whole chunk, resulting random order.
type shuffleRandomSplitter struct {
	baseShuffleSplitter
	partitionIdx int
}

// newShuffleRandomSplitter creates shuffleRandomSplitter
func newShuffleRandomSplitter(workerIdx int, shuffle *ShuffleExec, fanOut int) *shuffleRandomSplitter {
	splitter := &shuffleRandomSplitter{}
	splitter.baseShuffleSplitter = baseShuffleSplitter{
		workerIdx: workerIdx,
		shuffle:   shuffle,
		fanOut:    fanOut,
		self:      splitter,
	}
	return splitter
}

// PushResult implements shuffleSplitter interface.
func (s *shuffleRandomSplitter) PushResult(ctx context.Context, result *chunk.Chunk) (finished bool, err error) {
	if result.NumRows() > 0 {
		select {
		case <-s.finishCh:
			return true, nil
		case chk := <-s.holderCh[s.partitionIdx]:
			chk.SwapColumns(result)
			data := &shuffleData{chk: chk, giveBackCh: s.holderCh[s.partitionIdx]}
			s.shuffle.PushToMerger(s.partitionIdx, s.workerIdx, data)
			s.partitionIdx = (s.partitionIdx + 1) % s.fanOut
			return false, nil
		}
	}
	return true, nil
}

// SplitByRow is just a placeholder.
func (s *shuffleRandomSplitter) SplitByRow(ctx context.Context, input *chunk.Chunk, workerIndices []int) ([]int, error) {
	panic("Should not reach here")
}

// shuffleHashSplitter splits data by hash
type shuffleHashSplitter struct {
	baseShuffleSplitter
	byItems  []expression.Expression
	hashKeys [][]byte
}

// newShuffleHashSplitter creates shuffleHashSplitter
func newShuffleHashSplitter(workerIdx int, shuffle *ShuffleExec, fanOut int, byItems []*expression.Column) *shuffleHashSplitter {
	items := make([]expression.Expression, len(byItems))
	for i, col := range byItems {
		items[i] = col
	}
	splitter := &shuffleHashSplitter{byItems: items}
	splitter.baseShuffleSplitter = baseShuffleSplitter{
		workerIdx: workerIdx,
		shuffle:   shuffle,
		fanOut:    fanOut,
		self:      splitter,
	}
	return splitter
}

// SplitByRow implements shuffleSplitter SplitByRow interface.
func (s *shuffleHashSplitter) SplitByRow(ctx context.Context, input *chunk.Chunk, partitionIndices []int) ([]int, error) {
	var err error
	s.hashKeys, err = getGroupKey(s.shuffle.ctx, input, s.hashKeys, s.byItems)
	if err != nil {
		return partitionIndices, err
	}
	partitionIndices = partitionIndices[:0]
	numRows := input.NumRows()
	for i := 0; i < numRows; i++ {
		partitionIndices = append(partitionIndices, int(murmur3.Sum32(s.hashKeys[i]))%s.fanOut)
	}
	return partitionIndices, nil
}
