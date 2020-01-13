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
	"github.com/pingcap/tidb/util/logutil"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

// PartitionExec is the executor to run other executor in a parallel manner.
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
//          |      |             Partition  Execution             |
//          |      +--------------+-+------------+-+--------------+
//          |                             ^
//          |                             |
//          |             +---------------v-----------------+
//          +---------->  |    fetch data from DataSource   |
//                        +---------------------------------+
//
////////////////////////////////////////////////////////////////////////////////////////
type PartitionExec struct {
	baseExecutor
	concurrency int
	workers     []*partitionWorker

	prepared bool
	executed bool

	splitter   partitionSplitter
	dataSource Executor
	//childrenExec []Executor

	finishCh chan struct{}
	outputCh chan *partitionOutput
}

type partitionSplitter interface {
	split(ctx sessionctx.Context, input *chunk.Chunk, workerIndices []int) ([]int, error)
}

var _ partitionSplitter = &hashPartitionSplitter{}

type hashPartitionSplitter struct {
	byItems    []expression.Expression
	numWorkers int
	hashKeys   [][]byte
}

func (s *hashPartitionSplitter) split(ctx sessionctx.Context, input *chunk.Chunk, workerIndices []int) ([]int, error) {
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

type partitionOutput struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// Open implements the Executor Open interface
func (e *PartitionExec) Open(ctx context.Context) error {
	if err := e.dataSource.Open(ctx); err != nil {
		return err
	}
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.finishCh = make(chan struct{}, 1)
	e.outputCh = make(chan *partitionOutput, e.concurrency)

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

// Close implements the Executor Close interface
func (e *PartitionExec) Close() error {
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
	for range e.outputCh {
	}
	e.executed = false

	if e.runtimeStats != nil {
		e.runtimeStats.SetConcurrencyInfo("PartitionConcurrency", e.concurrency)
	}

	err := e.dataSource.Close()
	err1 := e.baseExecutor.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(err1)
}

func (e *PartitionExec) prepare4ParallelExec(ctx context.Context) {
	go e.execDataFetcherThread(ctx)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(e.workers))
	for _, w := range e.workers {
		go w.run(ctx, waitGroup)
	}

	go e.waitWorkerAndCloseOutput(waitGroup)
}

func (e *PartitionExec) waitWorkerAndCloseOutput(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.outputCh)
}

// Next implements the Executor Next interface.
func (e *PartitionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.prepared {
		e.prepare4ParallelExec(ctx)
		e.prepared = true
	}

	failpoint.Inject("partitionError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("PartitionExec.Next error"))
		}
	})

	if e.executed {
		return nil
	}
	for {
		result, ok := <-e.outputCh
		if !ok {
			e.executed = true
			return nil
		}
		if result.err != nil {
			return result.err
		}
		req.SwapColumns(result.chk)
		result.giveBackCh <- result.chk
		if req.NumRows() > 0 {
			return nil
		}
	}
}

func recoveryPartitionExec(output chan *partitionOutput, r interface{}) {
	err := errors.Errorf("%v", r)
	output <- &partitionOutput{err: errors.Errorf("%v", r)}
	logutil.BgLogger().Error("partition panicked", zap.Error(err))
}

func (e *PartitionExec) execDataFetcherThread(ctx context.Context) {
	var (
		err           error
		workerIndices []int
	)
	results := make([]*chunk.Chunk, len(e.workers))
	chk := newFirstChunk(e.dataSource)

	defer func() {
		if r := recover(); r != nil {
			recoveryPartitionExec(e.outputCh, r)
		}
		for _, w := range e.workers {
			close(w.inputCh)
		}
	}()

	for {
		err = Next(ctx, e.dataSource, chk)
		if err != nil {
			e.outputCh <- &partitionOutput{err: err}
			return
		}
		if chk.NumRows() == 0 {
			break
		}

		workerIndices, err = e.splitter.split(e.ctx, chk, workerIndices)
		if err != nil {
			e.outputCh <- &partitionOutput{err: err}
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

var _ Executor = &partitionWorker{}

type partitionWorker struct {
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
	outputCh       chan *partitionOutput
	outputHolderCh chan *chunk.Chunk
}

func (e *partitionWorker) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.executed = false
	return nil
}

func (e *partitionWorker) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

func (e *partitionWorker) Next(ctx context.Context, req *chunk.Chunk) error {
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

func (e *partitionWorker) run(ctx context.Context, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			recoveryPartitionExec(e.outputCh, r)
		}
		waitGroup.Done()
	}()

	for {
		select {
		case <-e.finishCh:
			return
		case chk := <-e.outputHolderCh:
			if err := Next(ctx, e.childExec, chk); err != nil {
				e.outputCh <- &partitionOutput{err: err}
				return
			}
			if chk.NumRows() == 0 {
				return
			}
			e.outputCh <- &partitionOutput{chk: chk, giveBackCh: e.outputHolderCh}
		}
	}
}
