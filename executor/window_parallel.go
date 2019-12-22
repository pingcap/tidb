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
//                            +-------------+
//                            | Main Thread |
//                            +------+------+
//                                   ^
//                                   |
//                                   +
//                                  +++
//                                  | |  outputCh (1 x Concurrency)
//                                  +++
//                                   ^
//                                   |
//                               +---+-----------+
//                               |               |
//                 +--------------+             +--------------+
//                 |    worker    |     ......  |    worker    |  worker (x Concurrency): SortExec + WindowExec
//                 +------------+-+             +-+------------+
//                              ^                 ^
//                              |                 |
//                             +-+  +-+  ......  +-+
//                             | |  | |          | |
//                             ...  ...          ...    inputChs (Concurrency x 1)
//                             | |  | |          | |
//                             +++  +++          +++
//                              ^    ^            ^
//                              |    |            |
//                     +--------o----+            |
//                     |        +-----------------+---+
//                     |                              |
//                 +---+------------+------------+----+-----------+
//                 |                     hash                     |
//                 +--------------+-+------------+-+--------------+
//                                        ^
//                                        |
//                        +---------------v-----------------+
//                        |          fetch data             |
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

type windowParallelWorker struct {
	ctx      sessionctx.Context
	e        *WindowParallelExec
	finishCh <-chan struct{}

	inputCh        chan *chunk.Chunk
	inputHolderCh  chan *chunk.Chunk
	outputCh       chan *windowParallelOutput
	outputHolderCh chan *chunk.Chunk

	dataFetcher windowDataFetcherExec
	sortExec    SortExec
	windowExec  WindowExec
}

// Open implements the Executor Open interface
func (e *WindowParallelExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	sessionVars := e.ctx.GetSessionVars()
	e.concurrency = sessionVars.WindowConcurrency

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
		}
		if err := e.workers[i].buildExecutors(); err != nil {
			return err
		}
		e.workers[i].inputHolderCh <- newFirstChunk(e)
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

	if e.runtimeStats != nil { // TODO: why ?
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
		}
	}
}

func (e *WindowParallelExec) prepare4ParallelExec(ctx context.Context) {
	go e.execDataFetcherThread(ctx)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(e.workers))
	for i := range e.workers {
		go e.workers[i].run(e.ctx, waitGroup)
	}

	go e.waitWorkerAndCloseOutput(waitGroup)
}

func (e *WindowParallelExec) waitWorkerAndCloseOutput(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.outputCh)
}

// Next implements the Executor Next interface.
func (e *WindowParallelExec) Next(ctx context.Context, chk *chunk.Chunk) error {
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
		chk.SwapColumns(result.chk)
		result.chk.Reset()
		result.giveBackCh <- result.chk
		if chk.NumRows() > 0 {
			return nil
		}
	}
}

type windowDataFetcherExec struct {
	baseExecutor
}

func (w *windowParallelWorker) buildExecutors() error {
	return nil
}

func (w *windowParallelWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup) {
	return
}
