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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
//  1. It fetches chunks from M `DataSources` (value of M depends on the actual executor, e.g. M = 1 for WindowExec, M = 2 for MergeJoinExec).
//  2. It splits tuples from each `DataSource` into N partitions (Only "split by hash" is implemented so far).
//  3. It invokes N workers in parallel, each one has M `receiver` to receive partitions from `DataSources`
//  4. It assigns partitions received as input to each worker and executes child executors.
//  5. It collects outputs from each worker, then sends outputs to its parent.
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
//
type ShuffleExec struct {
	baseExecutor
	concurrency int
	workers     []*shuffleWorker

	prepared bool
	executed bool

	// each dataSource has a corresponding spliter
	splitters   []partitionSplitter
	dataSources []Executor

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
	for _, s := range e.dataSources {
		if err := s.Open(ctx); err != nil {
			return err
		}

	}
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.finishCh = make(chan struct{}, 1)
	e.outputCh = make(chan *shuffleOutput, e.concurrency)

	for _, w := range e.workers {
		w.finishCh = e.finishCh

		for _, r := range w.receivers {
			r.inputCh = make(chan *chunk.Chunk, 1)
			r.inputHolderCh = make(chan *chunk.Chunk, 1)
		}

		w.outputCh = e.outputCh
		w.outputHolderCh = make(chan *chunk.Chunk, 1)

		if err := w.childExec.Open(ctx); err != nil {
			return err
		}

		for i, r := range w.receivers {
			r.inputHolderCh <- newFirstChunk(e.dataSources[i])
		}
		w.outputHolderCh <- newFirstChunk(e)
	}

	return nil
}

// Close implements the Executor Close interface.
func (e *ShuffleExec) Close() error {
	var firstErr error
	if !e.prepared {
		for _, w := range e.workers {
			for _, r := range w.receivers {
				if r.inputHolderCh != nil {
					close(r.inputHolderCh)
				}
				if r.inputCh != nil {
					close(r.inputCh)
				}
			}
			if w.outputHolderCh != nil {
				close(w.outputHolderCh)
			}
		}
		if e.outputCh != nil {
			close(e.outputCh)
		}
	}
	if e.finishCh != nil {
		close(e.finishCh)
	}
	for _, w := range e.workers {
		for _, r := range w.receivers {
			if r.inputCh != nil {
				for range r.inputCh {
				}
			}
		}
		// close child executor of each worker
		if err := w.childExec.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if e.outputCh != nil {
		for range e.outputCh { // workers exit before `e.outputCh` is closed.
		}
	}
	e.executed = false

	if e.runtimeStats != nil {
		runtimeStats := &execdetails.RuntimeStatsWithConcurrencyInfo{}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("ShuffleConcurrency", e.concurrency))
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, runtimeStats)
	}

	// close dataSources
	for _, dataSource := range e.dataSources {
		if err := dataSource.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// close baseExecutor
	if err := e.baseExecutor.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return errors.Trace(firstErr)
}

func (e *ShuffleExec) prepare4ParallelExec(ctx context.Context) {
	// create a goroutine for each dataSource to fetch and split data
	for i := range e.dataSources {
		go e.fetchDataAndSplit(ctx, i)
	}

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

func (e *ShuffleExec) fetchDataAndSplit(ctx context.Context, dataSourceIndex int) {
	var (
		err           error
		workerIndices []int
	)
	results := make([]*chunk.Chunk, len(e.workers))
	chk := newFirstChunk(e.dataSources[dataSourceIndex])

	defer func() {
		if r := recover(); r != nil {
			recoveryShuffleExec(e.outputCh, r)
		}
		for _, w := range e.workers {
			close(w.receivers[dataSourceIndex].inputCh)
		}
	}()

	for {
		err = Next(ctx, e.dataSources[dataSourceIndex], chk)
		if err != nil {
			e.outputCh <- &shuffleOutput{err: err}
			return
		}
		if chk.NumRows() == 0 {
			break
		}

		workerIndices, err = e.splitters[dataSourceIndex].split(e.ctx, chk, workerIndices)
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
				case results[workerIdx] = <-w.receivers[dataSourceIndex].inputHolderCh:
					break
				}
			}
			results[workerIdx].AppendRow(chk.GetRow(i))
			if results[workerIdx].IsFull() {
				w.receivers[dataSourceIndex].inputCh <- results[workerIdx]
				results[workerIdx] = nil
			}
		}
	}
	for i, w := range e.workers {
		if results[i] != nil {
			w.receivers[dataSourceIndex].inputCh <- results[i]
			results[i] = nil
		}
	}
}

var _ Executor = &shuffleReceiver{}

// shuffleReceiver receives chunk from dataSource through inputCh
type shuffleReceiver struct {
	baseExecutor

	finishCh <-chan struct{}
	executed bool

	inputCh       chan *chunk.Chunk
	inputHolderCh chan *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *shuffleReceiver) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.executed = false
	return nil
}

// Close implements the Executor Close interface.
func (e *shuffleReceiver) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
// It is called by `Tail` executor within "shuffle", to fetch data from `DataSource` by `inputCh`.
func (e *shuffleReceiver) Next(ctx context.Context, req *chunk.Chunk) error {
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

// shuffleWorker is the multi-thread worker executing child executors within "partition".
type shuffleWorker struct {
	childExec Executor

	finishCh <-chan struct{}

	// each receiver corresponse to a dataSource
	receivers []*shuffleReceiver

	outputCh       chan *shuffleOutput
	outputHolderCh chan *chunk.Chunk
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
var _ partitionSplitter = &partitionRangeSplitter{}

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

func buildPartitionHashSplitter(concurrency int, byItems []expression.Expression) *partitionHashSplitter {
	return &partitionHashSplitter{
		byItems:    byItems,
		numWorkers: concurrency,
	}
}

type partitionRangeSplitter struct {
	byItems      []expression.Expression
	numWorkers   int
	groupChecker *vecGroupChecker
	idx          int
}

func buildPartitionRangeSplitter(ctx sessionctx.Context, concurrency int, byItems []expression.Expression) *partitionRangeSplitter {
	return &partitionRangeSplitter{
		byItems:      byItems,
		numWorkers:   concurrency,
		groupChecker: newVecGroupChecker(ctx, byItems),
		idx:          0,
	}
}

// This method is supposed to be used for shuffle with sorted `dataSource`
// the caller of this method should guarantee that `input` is grouped,
// which means that rows with the same byItems should be continuous, the order does not matter.
func (s *partitionRangeSplitter) split(ctx sessionctx.Context, input *chunk.Chunk, workerIndices []int) ([]int, error) {
	_, err := s.groupChecker.splitIntoGroups(input)
	if err != nil {
		return workerIndices, err
	}

	workerIndices = workerIndices[:0]
	for !s.groupChecker.isExhausted() {
		begin, end := s.groupChecker.getNextGroup()
		for i := begin; i < end; i++ {
			workerIndices = append(workerIndices, s.idx)
		}
		s.idx = (s.idx + 1) % s.numWorkers
	}

	return workerIndices, nil
}
