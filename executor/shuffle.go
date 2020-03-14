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
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ShuffleExec is the executor of Shuffle.
type ShuffleExec struct {
	baseExecutor

	concurrency   int
	workers       []*shuffleWorker
	childShuffles []*childShuffle

	fanOut  int
	mergers []shuffleMerger

	prepared bool
	finishCh chan struct{}
}

type childShuffle struct {
	parent   plannercore.PhysicalPlan
	childIdx int
	plan     *plannercore.PhysicalShuffle
	exec     *ShuffleExec
}

type shuffleData struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *ShuffleExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	for _, child := range e.childShuffles {
		if err := child.exec.Open(ctx); err != nil {
			return err
		}
	}

	// open mergers before workers, to get data channels ready.
	for _, merger := range e.mergers {
		if err := merger.Open(ctx); err != nil {
			return err
		}
	}
	for _, w := range e.workers {
		if err := w.Open(ctx); err != nil {
			return err
		}
	}

	e.finishCh = make(chan struct{}, 1)
	e.prepared = false
	return nil
}

// Close implements the Executor Close interface.
func (e *ShuffleExec) Close() error {
	// Notify workers to terminate.
	close(e.finishCh)

	if e.runtimeStats != nil {
		e.runtimeStats.SetConcurrencyInfo("ShuffleConcurrency", e.concurrency)
	}

	var errs []error

	// Closes mergers first.
	// Mergers wait for dataCh to be closed, which is closed by workers.
	for _, merger := range e.mergers {
		errs = append(errs, merger.Close())
	}
	for _, worker := range e.workers {
		errs = append(errs, worker.Close())
	}
	for _, child := range e.childShuffles {
		errs = append(errs, child.exec.Close())
	}
	errs = append(errs, e.baseExecutor.Close())

	for _, err := range errs {
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Prepare prepares channels & threads for executing.
func (e *ShuffleExec) Prepare(ctx context.Context) {
	if e.prepared {
		return
	}

	for _, child := range e.childShuffles {
		// full-merge Shuffle prepares in `Next`.
		child.exec.Prepare(ctx)
	}

	// prepare mergers before workers, to get channels ready.
	for _, merger := range e.mergers {
		merger.Prepare(ctx, e.finishCh)
	}

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(e.workers))
	for _, worker := range e.workers {
		worker.Prepare(ctx, e.finishCh)
		go worker.run(ctx, waitGroup)
	}
	go e.waitWorker(waitGroup)

	e.prepared = true
}

func (e *ShuffleExec) waitWorker(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	for _, merger := range e.mergers {
		merger.WorkersAllFinished()
	}
}

// Next implements the Executor Next interface.
// Only used for full-merge.
func (e *ShuffleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	e.Prepare(ctx)

	failpoint.Inject("shuffleError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("ShuffleExec.Next error"))
		}
	})

	return e.mergers[0].Next(ctx, req)
}

// WorkerNext is the `Next` for parent Shuffle's workers.
func (e *ShuffleExec) WorkerNext(ctx context.Context, workerIdx int, req *chunk.Chunk) error {
	failpoint.Inject("shuffleError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("ShuffleExec.WorkerNext error"))
		}
	})

	return e.mergers[workerIdx].Next(ctx, req)
}

// PushToMerger is used by worker.splitter for pushing data to merger.
func (e *ShuffleExec) PushToMerger(partitionIdx int, workerIdx int, data *shuffleData) {
	e.mergers[partitionIdx].PushToMerger(workerIdx, data)
}

// WorkerFinished notify all mergers that a worker is finished.
func (e *ShuffleExec) WorkerFinished(workerIdx int) {
	for _, merger := range e.mergers {
		merger.WorkerFinished(workerIdx)
	}
}

// shuffleWorker is the multi-thread worker executing child executors within Shuffle.
type shuffleWorker struct {
	workerIdx int
	shuffle   *ShuffleExec
	splitter  shuffleSplitter
	childExec Executor

	executed bool
}

// Open initializes worker
func (w *shuffleWorker) Open(ctx context.Context) error {
	if err := w.childExec.Open(ctx); err != nil {
		return err
	}
	if err := w.splitter.Open(ctx); err != nil {
		return err
	}
	w.executed = false
	return nil
}

func (w *shuffleWorker) Prepare(ctx context.Context, finishCh <-chan struct{}) {
	w.splitter.Prepare(ctx, finishCh)
}

// Close implements the Executor Close interface.
func (w *shuffleWorker) Close() error {
	errs := []error{
		w.splitter.Close(),
		w.childExec.Close(),
	}
	for _, err := range errs {
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (w *shuffleWorker) reportError(err error) {
	data := &shuffleData{err: err}
	w.shuffle.PushToMerger(0, 0, data) // any merger will do.
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

	chk := newFirstChunk(w.childExec)
	executed := false
	for {
		if !executed {
			if err := Next(ctx, w.childExec, chk); err != nil {
				w.reportError(err)
				return
			}
			if chk.NumRows() == 0 {
				executed = true
			}
		} else {
			chk.Reset()
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

var _ Executor = (*shuffleDataSourceStub)(nil)

// shuffleDataSourceStub is the stub for executors within Shuffle to read data source.
type shuffleDataSourceStub struct {
	baseExecutor
	workerIdx int
	childExec *ShuffleExec
}

// Open implements the Executor Open interface.
func (s *shuffleDataSourceStub) Open(ctx context.Context) error {
	return s.baseExecutor.Open(ctx)
}

// Close implements the Executor Close interface.
func (s *shuffleDataSourceStub) Close() error {
	return s.baseExecutor.Close()
}

// Next implements the Executor Next interface.
// It is called by `Tail` executor within Shuffle, to fetch data from child Shuffle.
// Initial-partition scheme will not reach here.
func (s *shuffleDataSourceStub) Next(ctx context.Context, req *chunk.Chunk) error {
	return s.childExec.WorkerNext(ctx, s.workerIdx, req)
}
