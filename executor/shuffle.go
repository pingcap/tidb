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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)

// ShuffleExec is the executor to run other executors in a parallel manner.
//  1. It fetches chunks from `DataSource`.
//  2. It splits tuples from `DataSource` into N partitions (Only "split by hash" is implemented so far).
//  3. It invokes N workers in parallel, assign each partition as input to each worker and execute child executors.
//  4. It collects outputs from each worker, then sends outputs to its parent.
//
//                                +-------------+
//                        +-------| Main Thread |
//                        |       +------+------+
//                        |              ^
//                        |              |
//                        |              +
//                        v             +++
//                 outputHolderCh       | | outputCh (1 x Concurrency)
//                        v             +++
//                        |              ^
//                        |              |
//                        |      +-------+-------+
//                        v      |               |
//                 +--------------+             +--------------+
//          +----- |    worker    |   .......   |    worker    |  worker (N Concurrency): child executor, eg. WindowExec (+SortExec)
//          |      +------------+-+             +-+------------+
//          |                 ^                 ^
//          |                 |                 |
//          |                +-+  +-+  ......  +-+
//          |                | |  | |          | |
//          |                ...  ...          ...  inputCh (Concurrency x 1)
//          v                | |  | |          | |
//    inputHolderCh          +++  +++          +++
//          v                 ^    ^            ^
//          |                 |    |            |
//          |          +------o----+            |
//          |          |      +-----------------+-----+
//          |          |                              |
//          |      +---+------------+------------+----+-----------+
//          |      |              Partition Splitter              |
//          |      +--------------+-+------------+-+--------------+
//          |                             ^
//          |                             |
//          |             +---------------v-----------------+
//          +---------->  |    fetch data from DataSource   |
//                        +---------------------------------+
//
////////////////////////////////////////////////////////////////////////////////////////
type ShuffleExec struct {
	baseExecutor
	concurrency int
	workers     []*shuffleWorker

	prepared bool
	executed bool

	splitter   partitionSplitter
	dataSource Executor

	finishCh chan struct{}
	outputCh chan *shuffleOutput
}

type shuffleOutput struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *ShuffleExec) Open(ctx context.Context) error {
	if err := e.dataSource.Open(ctx); err != nil {
		return err
	}
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.finishCh = make(chan struct{}, 1)
	e.outputCh = make(chan *shuffleOutput, e.concurrency)

	for _, w := range e.workers {
		w.finishCh = e.finishCh

		w.inputCh = make(chan *chunk.Chunk, 1)
		w.inputHolderCh = make(chan *chunk.Chunk, 1)
		w.outputCh = e.outputCh
		w.outputHolderCh = make(chan *chunk.Chunk, 1)

		if err := w.childExec.Open(ctx); err != nil {
			return err
		}

		w.inputHolderCh <- newFirstChunk(e.dataSource)
		w.outputHolderCh <- newFirstChunk(e)
	}

	return nil
}

// Close implements the Executor Close interface.
func (e *ShuffleExec) Close() error {
	if !e.prepared {
		for _, w := range e.workers {
			close(w.inputHolderCh)
			close(w.inputCh)
			close(w.outputHolderCh)
		}
		close(e.outputCh)
	}
	close(e.finishCh)
	for _, w := range e.workers {
		for range w.inputCh {
		}
	}
	for range e.outputCh { // workers exit before `e.outputCh` is closed.
	}
	e.executed = false

	if e.runtimeStats != nil {
		runtimeStats := &execdetails.RuntimeStatsWithConcurrencyInfo{BasicRuntimeStats: e.runtimeStats}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("ShuffleConcurrency", e.concurrency))
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id.String(), runtimeStats)
	}

	err := e.dataSource.Close()
	err1 := e.baseExecutor.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(err1)
}

func (e *ShuffleExec) prepare4ParallelExec(ctx context.Context) {
	go e.fetchDataAndSplit(ctx)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(e.workers))
	for _, w := range e.workers {
		go w.run(ctx, waitGroup)
	}

	go e.waitWorkerAndCloseOutput(waitGroup)
}

func (e *ShuffleExec) waitWorkerAndCloseOutput(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.outputCh)
}

// Next implements the Executor Next interface.
func (e *ShuffleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.prepared {
		e.prepare4ParallelExec(ctx)
		e.prepared = true
	}

	failpoint.Inject("shuffleError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("ShuffleExec.Next error"))
		}
	})

	if e.executed {
		return nil
	}

	result, ok := <-e.outputCh
	if !ok {
		e.executed = true
		return nil
	}
	if result.err != nil {
		return result.err
	}
	req.SwapColumns(result.chk) // `shuffleWorker` will not send an empty `result.chk` to `e.outputCh`.
	result.giveBackCh <- result.chk

	return nil
}

func recoveryShuffleExec(output chan *shuffleOutput, r interface{}) {
	err := errors.Errorf("%v", r)
	output <- &shuffleOutput{err: errors.Errorf("%v", r)}
	logutil.BgLogger().Error("shuffle panicked", zap.Error(err), zap.Stack("stack"))
}

func (e *ShuffleExec) fetchDataAndSplit(ctx context.Context) {
	var (
		err           error
		workerIndices []int
	)
	results := make([]*chunk.Chunk, len(e.workers))
	chk := newFirstChunk(e.dataSource)

	defer func() {
		if r := recover(); r != nil {
			recoveryShuffleExec(e.outputCh, r)
		}
		for _, w := range e.workers {
			close(w.inputCh)
		}
	}()

	for {
		err = Next(ctx, e.dataSource, chk)
		if err != nil {
			e.outputCh <- &shuffleOutput{err: err}
			return
		}
		if chk.NumRows() == 0 {
			break
		}

		workerIndices, err = e.splitter.split(e.ctx, chk, workerIndices)
		if err != nil {
			e.outputCh <- &shuffleOutput{err: err}
			return
		}
		numRows := chk.NumRows()
		for i := 0; i < numRows; i++ {
			workerIdx := workerIndices[i]
			w := e.workers[workerIdx]

			if results[workerIdx] == nil {
				select {
				case <-e.finishCh:
					return
				case results[workerIdx] = <-w.inputHolderCh:
					break
				}
			}
			results[workerIdx].AppendRow(chk.GetRow(i))
			if results[workerIdx].IsFull() {
				w.inputCh <- results[workerIdx]
				results[workerIdx] = nil
			}
		}
	}
	for i, w := range e.workers {
		if results[i] != nil {
			w.inputCh <- results[i]
			results[i] = nil
		}
	}
}

var _ Executor = &shuffleWorker{}

// shuffleWorker is the multi-thread worker executing child executors within "partition".
type shuffleWorker struct {
	baseExecutor
	childExec Executor

	finishCh <-chan struct{}
	executed bool

	// Workers get inputs from dataFetcherThread by `inputCh`,
	//   and output results to main thread by `outputCh`.
	// `inputHolderCh` and `outputHolderCh` are "Chunk Holder" channels of `inputCh` and `outputCh` respectively,
	//   which give the `*Chunk` back, to implement the data transport in a streaming manner.
	inputCh        chan *chunk.Chunk
	inputHolderCh  chan *chunk.Chunk
	outputCh       chan *shuffleOutput
	outputHolderCh chan *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *shuffleWorker) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.executed = false
	return nil
}

// Close implements the Executor Close interface.
func (e *shuffleWorker) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
// It is called by `Tail` executor within "shuffle", to fetch data from `DataSource` by `inputCh`.
func (e *shuffleWorker) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.executed {
		return nil
	}
	select {
	case <-e.finishCh:
		e.executed = true
		return nil
	case result, ok := <-e.inputCh:
		if !ok || result.NumRows() == 0 {
			e.executed = true
			return nil
		}
		req.SwapColumns(result)
		e.inputHolderCh <- result
		return nil
	}
}

func (e *shuffleWorker) run(ctx context.Context, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			recoveryShuffleExec(e.outputCh, r)
		}
		waitGroup.Done()
	}()

	for {
		select {
		case <-e.finishCh:
			return
		case chk := <-e.outputHolderCh:
			if err := Next(ctx, e.childExec, chk); err != nil {
				e.outputCh <- &shuffleOutput{err: err}
				return
			}

			// Should not send an empty `chk` to `e.outputCh`.
			if chk.NumRows() == 0 {
				return
			}
			e.outputCh <- &shuffleOutput{chk: chk, giveBackCh: e.outputHolderCh}
		}
	}
}

var _ partitionSplitter = &partitionHashSplitter{}

type partitionSplitter interface {
	split(ctx sessionctx.Context, input *chunk.Chunk, workerIndices []int) ([]int, error)
}

type partitionHashSplitter struct {
	byItems    []expression.Expression
	numWorkers int
	hashKeys   [][]byte
}

func (s *partitionHashSplitter) split(ctx sessionctx.Context, input *chunk.Chunk, workerIndices []int) ([]int, error) {
	var err error
	s.hashKeys, err = getGroupKey(ctx, input, s.hashKeys, s.byItems)
	if err != nil {
		return workerIndices, err
	}
	workerIndices = workerIndices[:0]
	numRows := input.NumRows()
	for i := 0; i < numRows; i++ {
		workerIndices = append(workerIndices, int(murmur3.Sum32(s.hashKeys[i]))%s.numWorkers)
	}
	return workerIndices, nil
}
