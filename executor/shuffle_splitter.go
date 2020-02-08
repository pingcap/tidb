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
	"github.com/pingcap/tidb/sessionctx"
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
	Open(ctx context.Context, sctx sessionctx.Context, finishCh <-chan struct{}) error
	// Prepare for executing. It also signals that workers will be running right after now.
	Prepare(ctx context.Context)
	// Close de-initializes splitter.
	Close() error
	// PushResult pushs result to splitter.
	PushResult(ctx context.Context, chk *chunk.Chunk) (finished bool, err error)
	// SplitByRow splits input into partitions row by row.
	SplitByRow(ctx context.Context, input *chunk.Chunk, partitionIndices []int) ([]int, error)
}

type baseShuffleSplitter struct {
	shuffle *ShuffleExec
	// fanOut is number of merger. Can be NOT equal to number of workers.
	fanOut int
	// self is the concrete shuffleSplitter itself.
	self shuffleSplitter

	prepared bool

	sctx     sessionctx.Context
	finishCh <-chan struct{}
	holderCh []chan *chunk.Chunk

	// temporary variables
	results          []*chunk.Chunk
	partitionIndices []int
}

func (s *baseShuffleSplitter) Open(ctx context.Context, sctx sessionctx.Context, finishCh <-chan struct{}) error {
	s.prepared = false

	s.sctx = sctx
	s.finishCh = finishCh
	s.holderCh = make([]chan *chunk.Chunk, s.fanOut)
	for i := range s.holderCh {
		s.holderCh[i] = make(chan *chunk.Chunk, 1)
		s.holderCh[i] <- newFirstChunk(s.shuffle)
	}

	return nil
}

func (s *baseShuffleSplitter) Close() error {
	if !s.prepared {
		for _, ch := range s.holderCh {
			close(ch)
		}
	}
	return nil
}

func (s *baseShuffleSplitter) Prepare(ctx context.Context) {
	s.prepared = true
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

			if results[partitionIdx] == nil {
				select {
				case <-s.finishCh:
					return true, nil
				case results[partitionIdx] = <-s.holderCh[partitionIdx]:
					break
				}
			}
			results[partitionIdx].AppendRow(chk.GetRow(i))
			if results[partitionIdx].IsFull() {
				data := &shuffleData{chk: results[partitionIdx], giveBackCh: s.holderCh[partitionIdx]}
				s.shuffle.PushToMerger(partitionIdx, data)
				results[partitionIdx] = nil
			}
		}
		return false, nil
	}

	for partitionIdx := 0; partitionIdx < s.fanOut; partitionIdx++ {
		if results[partitionIdx] != nil {
			data := &shuffleData{chk: results[partitionIdx], giveBackCh: s.holderCh[partitionIdx]}
			s.shuffle.PushToMerger(partitionIdx, data)
			results[partitionIdx] = nil
		}
	}
	return true, nil
}

// shuffleRandomSplitter splits data source by the whole chunk, resulting random order.
type shuffleRandomSplitter struct {
	baseShuffleSplitter
	partitionIdx int
}

// newShuffleSimpleSplitter creates shuffleRandomSplitter
func newShuffleRandomSplitter(shuffle *ShuffleExec, fanOut int) *shuffleRandomSplitter {
	splitter := &shuffleRandomSplitter{}
	splitter.baseShuffleSplitter = baseShuffleSplitter{
		shuffle: shuffle,
		fanOut:  fanOut,
		self:    splitter,
	}
	return splitter
}

// Split implements shuffleSplitter interface.
func (s *shuffleRandomSplitter) PushResult(ctx context.Context, result *chunk.Chunk) (finished bool, err error) {
	if chk.NumRows() > 0 {
		select {
		case <-s.finishCh:
			return true, nil
		case chk = <-s.holderCh[s.partitionIdx]:
			break
		}
		chk.SwapColumns(result)
		data := &shuffleData{chk: chk, giveBackCh: s.holderCh[s.partitionIdx]}
		s.shuffle.PushToMerger(s.partitionIdx, data)
		s.partitionIdx = (s.partitionIdx + 1) % s.fanOut
		return false, nil
	}
	return true, nil
}

// SplitByRow is just a placeholder.
func (s *shuffleRandomSplitter) SplitByRow(ctx context.Context, input *chunk.Chunk, workerIndices []int) ([]int, error) {
	panic("Should not reach here")
}

// shuffleHashSplitter splits data source by hash
type shuffleHashSplitter struct {
	baseShuffleSplitter
	byItems  []expression.Expression
	hashKeys [][]byte
}

// newShuffleHashSplitter creates shuffleHashSplitter
func newShuffleHashSplitter(shuffle *ShuffleExec, fanOut int, byItems []*expression.Column) *shuffleHashSplitter {
	items := make([]expression.Expression, len(byItems))
	for i, col := range byItems {
		items[i] = col
	}
	splitter := &shuffleHashSplitter{byItems: items}
	splitter.baseShuffleSplitter = baseShuffleSplitter{
		shuffle: shuffle,
		fanOut:  fanOut,
		self:    splitter,
	}
	return splitter
}

// SplitByRow implements shuffleSplitter SplitByRow interface.
func (s *shuffleHashSplitter) SplitByRow(ctx context.Context, input *chunk.Chunk, partitionIndices []int) ([]int, error) {
	var err error
	s.hashKeys, err = getGroupKey(s.sctx, input, s.hashKeys, s.byItems)
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
