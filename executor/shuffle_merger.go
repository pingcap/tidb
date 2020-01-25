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

	"github.com/pingcap/tidb/util/chunk"
)

// ShuffleMerger is the results merger of Shuffle executor.
type ShuffleMerger interface {
	// Open initializes merger.
	Open(ctx context.Context, finishCh <-chan struct{}) error
	// WorkersPrepared signals workers are prepared (i.e. running).
	WorkersPrepared(ctx context.Context)
	// Close de-initializes merger.
	Close() error
	// Next retrievals next result. Each merger should implement `Next` for different merge algorithm.
	Next(ctx context.Context, chk *chunk.Chunk) error
	// WorkerOutput is called by workers to return results or error to merger.
	WorkerOutput(workerIdx int, chk *chunk.Chunk, err error)
	// WorkerGetHolderChunk is called by workers to get holder chunk.
	WorkerGetHolderChunk(workerIdx int) (chk *chunk.Chunk, finished bool)
	// WorkerFinished signals that a worker is finished. Notice that it would not be called if not prepared.
	WorkerFinished(workerIdx int)
	// WorkersAllFinished signals that all workers are finished. Notice that it would not be called if not prepared.
	WorkersAllFinished()
}

type baseShuffleMerger struct {
	// shuffle is the parent Shuffle executor.
	shuffle *ShuffleExec
	// fanIn is number of input channels. Should be equal to number of workers.
	fanIn int
	// isSingleOutputCh indicates the merger need single(e.g ShuffleSimpleMerger) or multiple(e.g ShuffleMergeSortMerger) output channels.
	isSingleOutputCh bool

	prepared bool
	executed bool

	finishCh       <-chan struct{}
	outputCh       []chan *shuffleOutput
	outputHolderCh []chan *chunk.Chunk
}

func (m *baseShuffleMerger) Open(ctx context.Context, finishCh <-chan struct{}) error {
	m.prepared = false

	m.finishCh = finishCh
	if m.isSingleOutputCh {
		m.outputCh = make([]chan *shuffleOutput, 1)
		m.outputCh[0] = make(chan *shuffleOutput, m.fanIn)
	} else {
		m.outputCh = make([]chan *shuffleOutput, m.fanIn)
		for i := range m.outputCh {
			m.outputCh[i] = make(chan *shuffleOutput, 1)
		}
	}
	m.outputHolderCh = make([]chan *chunk.Chunk, m.fanIn)
	for i := range m.outputHolderCh {
		m.outputHolderCh[i] = make(chan *chunk.Chunk, 1)
		m.outputHolderCh[i] <- newFirstChunk(m.shuffle)
	}

	return nil
}

func (m *baseShuffleMerger) WorkersPrepared(ctx context.Context) {
	m.prepared = true
}

func (m *baseShuffleMerger) Close() error {
	if !m.prepared {
		for _, ch := range m.outputHolderCh {
			close(ch)
		}
		for _, ch := range m.outputCh {
			close(ch)
		}
	}
	for _, ch := range m.outputCh { // workers exit before `outputCh` is closed.
		for range ch {
		}
	}
	m.executed = false

	return nil
}

func (m *baseShuffleMerger) WorkerOutput(workerIdx int, chk *chunk.Chunk, err error) {
	var out *shuffleOutput
	if err != nil {
		out = &shuffleOutput{err: err}
	} else {
		out = &shuffleOutput{chk: chk, giveBackCh: m.outputHolderCh[workerIdx]}
	}

	if m.isSingleOutputCh {
		m.outputCh[0] <- out
	} else {
		m.outputCh[workerIdx] <- out
	}
}

func (m *baseShuffleMerger) WorkerGetHolderChunk(workerIdx int) (chk *chunk.Chunk, finished bool) {
	select {
	case <-m.finishCh:
		return nil, true
	case chk := <-m.outputHolderCh[workerIdx]:
		return chk, false
	}
}

func (m *baseShuffleMerger) WorkerFinished(workerIdx int) {
	if !m.isSingleOutputCh {
		close(m.outputCh[workerIdx])
	}
}

func (m *baseShuffleMerger) WorkersAllFinished() {
	if m.isSingleOutputCh {
		close(m.outputCh[0])
	}
}

var (
	_ ShuffleMerger = (*ShuffleSimpleMerger)(nil)
)

// ShuffleSimpleMerger merges results without any other process.
type ShuffleSimpleMerger struct {
	baseShuffleMerger
}

// NewShuffleSimpleMerger creates ShuffleSimpleMerger.
func NewShuffleSimpleMerger(shuffle *ShuffleExec, fanIn int) *ShuffleSimpleMerger {
	return &ShuffleSimpleMerger{baseShuffleMerger{
		shuffle:          shuffle,
		fanIn:            fanIn,
		isSingleOutputCh: true,
	},
	}
}

// Next implements ShuffleMerger Next interface.
func (sm *ShuffleSimpleMerger) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if sm.executed {
		return nil
	}

	result, ok := <-sm.outputCh[0]
	if !ok {
		sm.executed = true
		return nil
	}
	if result.err != nil {
		return result.err
	}

	chk.SwapColumns(result.chk) // worker will not send an empty chunk
	result.giveBackCh <- result.chk
	return nil
}
