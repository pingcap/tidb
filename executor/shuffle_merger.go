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
	executed bool

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
		m.executed = false
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

// Next implements shuffleMerger Next interface.
func (m *shuffleRandomMerger) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if m.executed {
		return nil
	}

	result, ok := <-m.dataCh[0]
	if !ok {
		m.executed = true
		return nil
	}
	if result.err != nil {
		return result.err
	}

	chk.SwapColumns(result.chk) // worker will not send an empty chunk
	result.giveBackCh <- result.chk
	return nil
}
