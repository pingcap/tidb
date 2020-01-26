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
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
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

	splitter shuffleSplitter
	merger   shuffleMerger
	child    *ShuffleExec

	finishCh chan struct{}
}

type shuffleOutput struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// IsParallel indicates Shuffle running in a parallel manner or not.
// (Shuffle providing splitter only is running in serialization.)
func (e *ShuffleExec) IsParallel() bool {
	return e.concurrency > 1
}

// Open implements the Executor Open interface.
func (e *ShuffleExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.finishCh = make(chan struct{}, 1)
	if e.splitter != nil {
		if err := e.splitter.Open(ctx, e.ctx, e.finishCh); err != nil {
			return err
		}
	}

	if e.IsParallel() {
		if err := e.merger.Open(ctx, e.finishCh); err != nil {
			return err
		}
		for _, w := range e.workers {
			w.childSplitter = e.child.splitter
			w.merger = e.merger
			if err := w.childExec.Open(ctx); err != nil {
				return err
			}
		}
	}

	e.prepared = false
	return nil
}

// Close implements the Executor Close interface.
func (e *ShuffleExec) Close() error {
	close(e.finishCh)

	if e.runtimeStats != nil {
		e.runtimeStats.SetConcurrencyInfo("ShuffleConcurrency", e.concurrency)
	}

	var errs []error
	errs = append(errs, e.baseExecutor.Close())
	if e.IsParallel() {
		errs = append(errs, e.merger.Close())
	}
	if e.splitter != nil {
		errs = append(errs, e.splitter.Close())
	}
	for _, err := range errs {
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Prepare4ParallelExec prepares threads for parallel executing.
func (e *ShuffleExec) Prepare4ParallelExec(ctx context.Context) {
	if e.prepared {
		return
	}

	if e.splitter != nil {
		e.splitter.Prepare(ctx)
	}

	if e.IsParallel() {
		e.child.Prepare4ParallelExec(ctx)

		waitGroup := &sync.WaitGroup{}
		waitGroup.Add(len(e.workers))
		for _, w := range e.workers {
			go w.run(ctx, waitGroup)
		}
		go e.waitWorker(waitGroup)

		e.merger.Prepare(ctx)
	}
	e.prepared = true
}

func (e *ShuffleExec) waitWorker(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	e.merger.WorkersAllFinished()
}

// Next implements the Executor Next interface.
func (e *ShuffleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	e.Prepare4ParallelExec(ctx)

	failpoint.Inject("shuffleError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("ShuffleExec.Next error"))
		}
	})

	if e.IsParallel() {
		return e.merger.Next(ctx, req)
	}
	return Next(ctx, e.children[0], req)
}

// shuffleWorker is the multi-thread worker executing child executors within "shuffle".
type shuffleWorker struct {
	workerIdx int
	baseExecutor
	childExec Executor

	finishCh <-chan struct{}
	executed bool

	// Workers get input from child splitter, and output results to merger.
	childSplitter shuffleSplitter
	merger        shuffleMerger
}

var _ Executor = (*shuffleWorker)(nil)

// Open implements the Executor Open interface.
func (w *shuffleWorker) Open(ctx context.Context) error {
	if err := w.baseExecutor.Open(ctx); err != nil {
		return err
	}
	w.executed = false
	return nil
}

// Close implements the Executor Close interface.
func (w *shuffleWorker) Close() error {
	return errors.Trace(w.baseExecutor.Close())
}

// Next implements the Executor Next interface.
// It is called by `Tail` executor within "shuffle", to fetch data from splitter.
func (w *shuffleWorker) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if w.executed {
		return nil
	}

	result, finished, err := w.childSplitter.WorkerGetInput(w.workerIdx)
	if err != nil {
		return err
	}
	if finished {
		w.executed = true
		return nil
	}
	req.SwapColumns(result)
	w.childSplitter.WorkerReturnHolderChunk(w.workerIdx, result)
	return nil
}

func recoveryShuffleWorker(merger shuffleMerger, r interface{}) {
	err := errors.Errorf("%v", r)
	merger.WorkerOutput(0, nil, err) // workerIdx is not cared for.
	logutil.BgLogger().Error("shuffle panicked", zap.Error(err))
}

func (w *shuffleWorker) run(ctx context.Context, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			recoveryShuffleWorker(w.merger, r)
		}
		w.merger.WorkerFinished(w.workerIdx)
		waitGroup.Done()
	}()

	for {
		chk, finished := w.merger.WorkerGetHolderChunk(w.workerIdx)
		if finished {
			return
		}

		if err := Next(ctx, w.childExec, chk); err != nil {
			w.merger.WorkerOutput(w.workerIdx, nil, err)
			return
		}
		// Should not send an empty `chk` to merger.
		if chk.NumRows() == 0 {
			return
		}
		w.merger.WorkerOutput(w.workerIdx, chk, nil)
	}
}
