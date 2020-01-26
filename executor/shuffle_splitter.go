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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

var (
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
	// Split input into partitions. Each splitter should implement `Split` for different spliting algorithm.
	Split(ctx sessionctx.Context, input *chunk.Chunk, workerIndices []int) ([]int, error)
	// WorkerInput is called by workers to get input from splitter.
	WorkerGetInput(workerIdx int) (chk *chunk.Chunk, finished bool, err error)
	// WorkerReturnHolderChunk is called by workers to return holder chunk.
	WorkerReturnHolderChunk(workerIdx int, chk *chunk.Chunk)
}

type baseShuffleSplitter struct {
	// shuffle is the parent Shuffle executor.
	shuffle *ShuffleExec
	// fanOut is number of shuffle worker input channels. Should be equal to number of workers.
	fanOut int
	// self is the concrete shuffleSplitter itself.
	self shuffleSplitter

	prepared bool

	sctx          sessionctx.Context
	finishCh      <-chan struct{}
	inputCh       []chan *shuffleOutput
	inputHolderCh []chan *chunk.Chunk
}

func (s *baseShuffleSplitter) Open(ctx context.Context, sctx sessionctx.Context, finishCh <-chan struct{}) error {
	s.prepared = false

	s.sctx = sctx
	s.finishCh = finishCh
	s.inputCh = make([]chan *shuffleOutput, s.fanOut)
	s.inputHolderCh = make([]chan *chunk.Chunk, s.fanOut)
	for i := range s.inputCh {
		s.inputCh[i] = make(chan *shuffleOutput, 1)
		s.inputHolderCh[i] = make(chan *chunk.Chunk, 1)
		s.inputHolderCh[i] <- newFirstChunk(s.shuffle)
	}

	return nil
}

func (s *baseShuffleSplitter) Close() error {
	if !s.prepared {
		for _, ch := range s.inputCh {
			close(ch)
		}
		for _, ch := range s.inputHolderCh {
			close(ch)
		}
	}
	for _, ch := range s.inputCh {
		for range ch {
		}
	}

	return nil
}

func (s *baseShuffleSplitter) Prepare(ctx context.Context) {
	go s.fetchDataAndSplit(ctx)
	s.prepared = true
}

func (s *baseShuffleSplitter) WorkerGetInput(workerIdx int) (chk *chunk.Chunk, finished bool, err error) {
	select {
	case <-s.finishCh:
		return nil, true, nil
	case result, ok := <-s.inputCh[workerIdx]:
		if !ok {
			return nil, true, nil
		}
		return result.chk, false, result.err
	}
}

func (s *baseShuffleSplitter) WorkerReturnHolderChunk(workerIdx int, chk *chunk.Chunk) {
	s.inputHolderCh[workerIdx] <- chk
}

func (s *baseShuffleSplitter) sendError(err error) {
	s.inputCh[0] <- &shuffleOutput{err: err} // workerIdx is not cared for.
}

func (s *baseShuffleSplitter) fetchDataAndSplit(ctx context.Context) {
	var (
		err           error
		workerIndices []int
	)
	results := make([]*chunk.Chunk, s.fanOut)
	chk := newFirstChunk(s.shuffle)

	defer func() {
		if r := recover(); r != nil {
			err := errors.Errorf("%v", r)
			s.sendError(err)
			logutil.BgLogger().Error("shuffle splitter panicked", zap.Error(err))
		}
		for _, ch := range s.inputCh {
			close(ch)
		}
	}()

	for {
		err = Next(ctx, s.shuffle, chk)
		if err != nil {
			s.sendError(err)
			return
		}
		if chk.NumRows() == 0 { // Should not send an empty chunk to worker.
			break
		}

		workerIndices, err = s.self.Split(s.sctx, chk, workerIndices)
		if err != nil {
			s.sendError(err)
			return
		}
		numRows := chk.NumRows()
		for i := 0; i < numRows; i++ {
			workerIdx := workerIndices[i]

			if results[workerIdx] == nil {
				select {
				case <-s.finishCh:
					return
				case results[workerIdx] = <-s.inputHolderCh[workerIdx]:
					break
				}
			}
			results[workerIdx].AppendRow(chk.GetRow(i))
			if results[workerIdx].IsFull() {
				s.inputCh[workerIdx] <- &shuffleOutput{chk: results[workerIdx]}
				results[workerIdx] = nil
			}
		}
	}
	for i, ch := range s.inputCh {
		if results[i] != nil {
			ch <- &shuffleOutput{chk: results[i]}
			results[i] = nil
		}
	}
}

// shuffleHashSplitter splits data source by hash
type shuffleHashSplitter struct {
	baseShuffleSplitter
	byItems  []expression.Expression
	hashKeys [][]byte
}

// newShuffleHashSplitter creates shuffleHashSplitter
func newShuffleHashSplitter(shuffle *ShuffleExec, fanOut int, byItems []expression.Expression) *shuffleHashSplitter {
	splitter := &shuffleHashSplitter{byItems: byItems}
	splitter.baseShuffleSplitter = baseShuffleSplitter{
		shuffle: shuffle,
		fanOut:  fanOut,
		self:    splitter,
	}
	return splitter
}

// Split implements shuffleSplitter Split interface.
func (s *shuffleHashSplitter) Split(ctx sessionctx.Context, input *chunk.Chunk, workerIndices []int) ([]int, error) {
	var err error
	s.hashKeys, err = getGroupKey(ctx, input, s.hashKeys, s.byItems)
	if err != nil {
		return workerIndices, err
	}
	workerIndices = workerIndices[:0]
	numRows := input.NumRows()
	for i := 0; i < numRows; i++ {
		workerIndices = append(workerIndices, int(murmur3.Sum32(s.hashKeys[i]))%s.fanOut)
	}
	return workerIndices, nil
}
