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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ShuffleExec is the executor to run other executors in a parallel manner.
// (Takes Window operator as example)
// 1. Shuffle(child): Fetches chunks from data source.
// 2. Shuffle(child): Splits tuples from data source into N partitions by `shuffleHashSplitter`.
// 3. Shuffle(the upper one): Invokes N workers in parallel, assigns each partition as input to each worker and executes child executors.
// 4. Shuffle(the upper one): Collects outputs from each worker by `shuffleSimpleMerger`, then sends outputs to its parent.
//
//                                              Parent
//                                                |
//                                 +--------------v--------------+
//                                 |   Shuffle.Splitter(serial)  |
//                                 +--------------^--------------+
//                                                |
//                                 +--------------v--------------+
//                         +-------|    Shuffle.Merger(simple)   |
//                         |       +--------------+--------------+
//                         |                      ^
//                         |                      |
//                         |              +-------+
//                         v             +++
//                  outputHolderCh       | | outputCh (1 x Concurrency)
//                         v             +++
//                         |              ^
//                         |              |
//                         |      +-------+-------+
//                         v      |               |
//                  +--------------+             +--------------+
//           +----- |    worker    |   .......   |    worker    |  workers (N Concurrency)
//           |      +----------+---+             +--------------+   i.e. WindowExec (+SortExec)
//           |                 ^                 ^
//           |                 |                 |
//           |                +-+  +-+  ......  +-+
//           |                | |  | |          | |
//           |                ...  ...          ...  inputCh (Concurrency x 1)
//           v                | |  | |          | |
//     inputHolderCh          +++  +++          +++
//           v                 ^    ^            ^
//           |                 |    |            |
//           |          +------o----+            |
//           |          |      +-----------------+-----+
//           |          |                              |
//           |      +---+------------+------------+----+-----------+
//           +----> |         Shuffle(child).Splitter(hash)        |
//                  |           Splits Partitions by Hash          |
//                  +--------------+-+------------+-+--------------+
//                                         ^
//                                         |
//                         +---------------v-----------------+
//                         |  Shuffle(child).Merger(serial)  |
//                         |   fetch data from data source   |
//                         +---------------------------------+
//
////////////////////////////////////////////////////////////////////////////////////////
type ShuffleExec struct {
	baseExecutor

	concurrency  int
	workers      []*shuffleWorker
	childShuffle *ShuffleExec

	fanOut  int
	mergers []*shuffleMerger

	prepared bool
	finishCh chan struct{}
}

type shuffleData struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// IsParallel indicates Shuffle running in a parallel manner or not.
// (Shuffle providing splitter only is running in serialization.)
/*func (e *ShuffleExec) IsParallel() bool {
	return e.concurrency > 1
}*/

// Open implements the Executor Open interface.
func (e *ShuffleExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.finishCh = make(chan struct{}, 1)

	for _, w := range e.workers {
		if err := w.childExec.Open(ctx); err != nil {
			return err
		}
	}
	for _, merger := range e.mergers {
		if err := merger.Open(ctx, e.finishCh); err != nil {
			return err
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
	e.workersAllFinished()
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

// WorkerNext is the `Next` for parent Shuffle's workers.
func (e *ShuffleExec) WorkerNext(ctx context.Context, workerIdx int, req *chunk.Chunk) error {
	return e.mergers[workerIdx].Next(ctx, req)
}

// shuffleWorker is the multi-thread worker executing child executors within Shuffle.
type shuffleWorker struct {
	workerIdx int
	baseExecutor
	shuffle      *ShuffleExec
	childShuffle *ShuffleExec

	executed bool

	splitter shuffleSplitter
}

var _ Executor = (*shuffleWorker)(nil)

// Open implements the Executor Open interface.
func (w *shuffleWorker) Open(ctx context.Context, sctx sessionctx.Context, finishCh <-chan struct{}) error {
	if err := w.baseExecutor.Open(ctx); err != nil {
		return err
	}
	if err := w.splitter.Open(ctx, sctx, w.finishCh); err != nil {
		return err
	}
	w.executed = false
	return nil
}

// Close implements the Executor Close interface.
func (w *shuffleWorker) Close() error {
	err := w.baseExecutor.Close()
	err1 := w.splitter.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(err1)
}

// Next implements the Executor Next interface.
// It is called by `Tail` executor within "shuffle", to fetch data from child Shuffle.
func (w *shuffleWorker) Next(ctx context.Context, req *chunk.Chunk) error {
	return w.childShuffle.WorkerNext(ctx, req, w.workerIdx)
}

func (w *shuffleWorker) reportError(err error) {
	data := &shuffleData{err: err}
	w.shuffle.PushToMerger(0, data)
}

func (w *shuffleWorker) run(ctx context.Context, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			err := errors.Errorf("%v", r)
			logutil.BgLogger().Error("shuffle worker panicked", zap.Error(err))
			w.reportError(err)
		}
		w.shuffle.WorkerFinished(w.workerIdx)
		waitGroup.Done()
	}()

	chk := newFirstChunk(w.children[0])
	for {
		if err := Next(ctx, w.children[0], req); err != nil {
			w.reportError(err)
			return
		}
		finished, err := w.splitter.PushResult(ctx, chk)
		if err != nil {
			w.reportError(err)
			return
		}
		if finished {
			return
		}
	}
}
