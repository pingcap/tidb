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

// WindowParallelExec is the executor for window functions in a parallel manner.
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
//          +----- |    worker    |   .......   |    worker    |  worker (x Concurrency): SortExec + WindowExec
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
//          |      |                     hash                     |
//          |      +--------------+-+------------+-+--------------+
//          |                             ^
//          |                             |
//          |             +---------------v-----------------+
//          +---------->  |          fetch data             |
//                        +---------------------------------+
//
////////////////////////////////////////////////////////////////////////////////////////
type WindowParallelExec struct {
	baseExecutor

	prepared bool
	executed bool

	concurrency int
	finishCh    chan struct{}
	outputCh    chan *windowParallelOutput
	workers     []windowParallelWorker

	groupByItems []expression.Expression
	orderByCols  []*expression.Column

	dataFetchers []*windowDataFetcherExec
	windowExecs  []*WindowExec
	sortExecs    []*SortExec
}

type windowParallelOutput struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// Open implements the Executor Open interface
func (e *WindowParallelExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.finishCh = make(chan struct{}, 1)
	e.outputCh = make(chan *windowParallelOutput, e.concurrency)

	e.workers = make([]windowParallelWorker, e.concurrency)
	for i := range e.workers {
		e.workers[i] = windowParallelWorker{
			ctx:      e.ctx,
			e:        e,
			finishCh: e.finishCh,

			inputCh:        make(chan *chunk.Chunk, 1),
			inputHolderCh:  make(chan *chunk.Chunk, 1),
			outputCh:       e.outputCh,
			outputHolderCh: make(chan *chunk.Chunk, 1),

			dataFetcher: e.dataFetchers[i],
			sortExec:    e.sortExecs[i],
			windowExec:  e.windowExecs[i],
		}
		e.workers[i].dataFetcher.finishCh = e.finishCh
		e.workers[i].dataFetcher.inputCh = e.workers[i].inputCh
		e.workers[i].dataFetcher.giveBackCh = e.workers[i].inputHolderCh
		if err := e.workers[i].windowExec.Open(ctx); err != nil {
			return err
		}

		e.workers[i].inputHolderCh <- newFirstChunk(e.children[0])
		e.workers[i].outputHolderCh <- newFirstChunk(e)
	}

	return nil
}

// Close implements the Executor Close interface
func (e *WindowParallelExec) Close() error {
	if !e.prepared {
		for i := range e.workers {
			close(e.workers[i].inputHolderCh)
			close(e.workers[i].inputCh)
			close(e.workers[i].outputHolderCh)
		}
		close(e.outputCh)
	}
	close(e.finishCh)
	for i := range e.workers {
		for range e.workers[i].inputCh {
		}
	}
	for range e.outputCh {
	}
	e.executed = false

	if e.runtimeStats != nil {
		e.runtimeStats.SetConcurrencyInfo("WindowParallelConcurrency", e.concurrency)
	}

	return errors.Trace(e.baseExecutor.Close())
}

func recoveryWindowParallelExec(output chan *windowParallelOutput, r interface{}) {
	err := errors.Errorf("%v", r)
	output <- &windowParallelOutput{err: errors.Errorf("%v", r)}
	logutil.BgLogger().Error("window parallel panicked", zap.Error(err))
}

func (e *WindowParallelExec) execDataFetcherThread(ctx context.Context) {
	var (
		ok        bool
		err       error
		groupKeys [][]byte
	)
	results := make([]*chunk.Chunk, len(e.workers))

	defer func() {
		if r := recover(); r != nil {
			recoveryWindowParallelExec(e.outputCh, r)
		}
		for i := range e.workers {
			close(e.workers[i].inputCh)
		}
	}()
	for {
		chk := newFirstChunk(e.children[0])
		err = Next(ctx, e.children[0], chk)
		if err != nil {
			e.outputCh <- &windowParallelOutput{err: err}
			return
		}
		if chk.NumRows() == 0 {
			break
		}

		groupKeys, err = getGroupKey(e.ctx, chk, groupKeys, e.groupByItems)
		if err != nil {
			e.outputCh <- &windowParallelOutput{err: err}
			return
		}
		numRows := chk.NumRows()
		for i := 0; i < numRows; i++ {
			workerIdx := int(murmur3.Sum32(groupKeys[i])) % len(e.workers)
			w := &e.workers[workerIdx]

			if results[workerIdx] == nil {
				select {
				case <-e.finishCh:
					return
				case results[workerIdx], ok = <-w.inputHolderCh:
					if !ok {
						return
					}
				}
			}
			results[workerIdx].AppendRow(chk.GetRow(i))
			if results[workerIdx].IsFull() {
				w.inputCh <- results[workerIdx]
				results[workerIdx] = nil
			}
		}
	}
	for i := range e.workers {
		if results[i] != nil {
			e.workers[i].inputCh <- results[i]
			results[i] = nil
		}
	}
}

func (e *WindowParallelExec) prepare4ParallelExec(ctx context.Context) {
	go e.execDataFetcherThread(ctx)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(e.workers))
	for i := range e.workers {
		go e.workers[i].run(ctx, waitGroup)
	}

	go e.waitWorkerAndCloseOutput(waitGroup)
}

func (e *WindowParallelExec) waitWorkerAndCloseOutput(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.outputCh)
}

// Next implements the Executor Next interface.
func (e *WindowParallelExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.prepared {
		e.prepare4ParallelExec(ctx)
		e.prepared = true
	}

	failpoint.Inject("windowParallelError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("WindowParallelExec.Next error"))
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

type windowDataFetcherExec struct {
	baseExecutor
	finishCh   <-chan struct{}
	inputCh    <-chan *chunk.Chunk
	giveBackCh chan<- *chunk.Chunk

	executed bool
}

func (e *windowDataFetcherExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.executed = false
	return nil
}

func (e *windowDataFetcherExec) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

func (e *windowDataFetcherExec) Next(ctx context.Context, req *chunk.Chunk) error {
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
		result.Reset()
		e.giveBackCh <- result
		return nil
	}
}

type windowParallelWorker struct {
	ctx      sessionctx.Context
	e        *WindowParallelExec
	finishCh <-chan struct{}

	// Workers get inputs from dataFetcherThread by `inputCh`,
	//   and output results to main thread by `outputCh`.
	// `inputHolderCh` and `outputHolderCh` are "Chunk Holder" channels of `inputCh` and `outputCh` respectively,
	//   which give the `*Chunk` back, to implement the data transport in a streaming manner.
	inputCh        chan *chunk.Chunk
	inputHolderCh  chan *chunk.Chunk
	outputCh       chan *windowParallelOutput
	outputHolderCh chan *chunk.Chunk

	dataFetcher *windowDataFetcherExec
	sortExec    *SortExec
	windowExec  *WindowExec
}

func (w *windowParallelWorker) run(ctx context.Context, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			recoveryWindowParallelExec(w.outputCh, r)
		}
		waitGroup.Done()
	}()

	for {
		select {
		case <-w.finishCh:
			return
		case chk, ok := <-w.outputHolderCh:
			if !ok {
				return
			}
			if err := Next(ctx, w.windowExec, chk); err != nil {
				w.outputCh <- &windowParallelOutput{err: err}
				return
			}
			if chk.NumRows() == 0 {
				return
			}
			w.outputCh <- &windowParallelOutput{chk: chk, giveBackCh: w.outputHolderCh}
		}
	}
}
