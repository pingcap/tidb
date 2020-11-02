package executor

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
)

// ShuffleMergeJoinExec implements the shuffle version of merge join algorithm
type ShuffleMergeJoinExec struct {
	baseExecutor
	concurrency int
	workers     []*shuffleMergeJoinWorker

	prepared bool
	executed bool

	innerSplitter   partitionSplitter
	outerSplitter   partitionSplitter
	innerDataSource Executor
	outerDataSource Executor

	finishCh chan struct{}
	outputCh chan *shuffleOutput
}

// Open implements the Executor Open interface.
func (e *ShuffleMergeJoinExec) Open(ctx context.Context) error {
	if err := e.innerDataSource.Open(ctx); err != nil {
		return err
	}
	if err := e.outerDataSource.Open(ctx); err != nil {
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

		w.innerPartition.inputCh = make(chan *chunk.Chunk, 1)
		w.innerPartition.inputHolderCh = make(chan *chunk.Chunk, 1)
		w.outerPartition.inputCh = make(chan *chunk.Chunk, 1)
		w.outerPartition.inputHolderCh = make(chan *chunk.Chunk, 1)

		w.outputCh = e.outputCh
		w.outputHolderCh = make(chan *chunk.Chunk, 1)

		if err := w.childExec.Open(ctx); err != nil {
			return err
		}

		w.innerPartition.inputHolderCh <- newFirstChunk(e.innerDataSource)
		w.outerPartition.inputHolderCh <- newFirstChunk(e.outerDataSource)
		w.outputHolderCh <- newFirstChunk(e)
	}

	return nil
}

// Close implements the Executor Close interface.
func (e *ShuffleMergeJoinExec) Close() error {
	if !e.prepared {
		for _, w := range e.workers {
			close(w.innerPartition.inputHolderCh)
			close(w.outerPartition.inputHolderCh)
			close(w.innerPartition.inputCh)
			close(w.outerPartition.inputCh)
			close(w.outputHolderCh)
		}
		close(e.outputCh)
	}
	close(e.finishCh)
	for _, w := range e.workers {
		for range w.innerPartition.inputCh {
		}
		for range w.outerPartition.inputCh {
		}
	}
	for range e.outputCh { // workers exit before `e.outputCh` is closed.
	}
	e.executed = false

	if e.runtimeStats != nil {
		runtimeStats := &execdetails.RuntimeStatsWithConcurrencyInfo{}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("ShuffleConcurrency", e.concurrency))
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, runtimeStats)
	}

	if err := e.innerDataSource.Close(); err != nil {
		return errors.Trace(err)
	}
	if err := e.outerDataSource.Close(); err != nil {
		return errors.Trace(err)
	}
	err := e.baseExecutor.Close()
	return errors.Trace(err)
}

func (e *ShuffleMergeJoinExec) prepare4ParallelExec(ctx context.Context) {
	go e.fetchInnerDataAndSplit(ctx)
	go e.fetchOuterDataAndSplit(ctx)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(e.workers))
	for _, w := range e.workers {
		go w.run(ctx, waitGroup)
	}

	go e.waitWorkerAndCloseOutput(waitGroup)
}

func (e *ShuffleMergeJoinExec) waitWorkerAndCloseOutput(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.outputCh)
}

// Next implements the Executor Next interface.
func (e *ShuffleMergeJoinExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.prepared {
		e.prepare4ParallelExec(ctx)
		e.prepared = true
	}

	failpoint.Inject("shuffleError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("ShuffleMergeJoinExec.Next error"))
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
	req.SwapColumns(result.chk) // `shuffleMergeJoinWorker` will not send an empty `result.chk` to `e.outputCh`.
	result.giveBackCh <- result.chk

	return nil
}

func (e *ShuffleMergeJoinExec) fetchInnerDataAndSplit(ctx context.Context) {
	var (
		err           error
		workerIndices []int
	)
	results := make([]*chunk.Chunk, len(e.workers))
	chk := newFirstChunk(e.innerDataSource)

	defer func() {
		if r := recover(); r != nil {
			recoveryShuffleExec(e.outputCh, r)
		}
		for _, w := range e.workers {
			close(w.innerPartition.inputCh)
		}
	}()

	for {
		err = Next(ctx, e.innerDataSource, chk)
		if err != nil {
			e.outputCh <- &shuffleOutput{err: err}
			return
		}
		if chk.NumRows() == 0 {
			break
		}

		workerIndices, err = e.innerSplitter.split(e.ctx, chk, workerIndices)
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
				case results[workerIdx] = <-w.innerPartition.inputHolderCh:
					break
				}
			}
			results[workerIdx].AppendRow(chk.GetRow(i))
			if results[workerIdx].IsFull() {
				w.innerPartition.inputCh <- results[workerIdx]
				results[workerIdx] = nil
			}
		}
	}
	for i, w := range e.workers {
		if results[i] != nil {
			w.innerPartition.inputCh <- results[i]
			results[i] = nil
		}
	}
}

func (e *ShuffleMergeJoinExec) fetchOuterDataAndSplit(ctx context.Context) {
	var (
		err           error
		workerIndices []int
	)
	results := make([]*chunk.Chunk, len(e.workers))
	chk := newFirstChunk(e.outerDataSource)

	defer func() {
		if r := recover(); r != nil {
			recoveryShuffleExec(e.outputCh, r)
		}
		for _, w := range e.workers {
			close(w.outerPartition.inputCh)
		}
	}()

	for {
		err = Next(ctx, e.outerDataSource, chk)
		if err != nil {
			e.outputCh <- &shuffleOutput{err: err}
			return
		}
		if chk.NumRows() == 0 {
			break
		}

		workerIndices, err = e.outerSplitter.split(e.ctx, chk, workerIndices)
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
				case results[workerIdx] = <-w.outerPartition.inputHolderCh:
					break
				}
			}
			results[workerIdx].AppendRow(chk.GetRow(i))
			if results[workerIdx].IsFull() {
				w.outerPartition.inputCh <- results[workerIdx]
				results[workerIdx] = nil
			}
		}
	}
	for i, w := range e.workers {
		if results[i] != nil {
			w.outerPartition.inputCh <- results[i]
			results[i] = nil
		}
	}
}

type shufflePartition struct {
	baseExecutor

	finishCh <-chan struct{}
	// executed bool

	inputCh       chan *chunk.Chunk
	inputHolderCh chan *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *shufflePartition) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	// e.executed = false
	return nil
}

// Close implements the Executor Close interface.
func (e *shufflePartition) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
// It is called by `Tail` executor within "shuffle", to fetch data from `DataSource` by `inputCh`.
func (e *shufflePartition) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	// if e.executed {
	// 	return nil
	// }
	select {
	case <-e.finishCh:
		// 	e.executed = true
		return nil
	case result, ok := <-e.inputCh:
		if !ok || result.NumRows() == 0 {
			// e.executed = true
			return nil
		}
		req.SwapColumns(result)
		e.inputHolderCh <- result
		return nil
	}
}

type shuffleMergeJoinWorker struct {
	childExec Executor

	finishCh <-chan struct{}
	// executed bool

	innerPartition shufflePartition
	outerPartition shufflePartition

	outputCh       chan *shuffleOutput
	outputHolderCh chan *chunk.Chunk
}

func (w *shuffleMergeJoinWorker) run(ctx context.Context, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			recoveryShuffleExec(w.outputCh, r)
		}
		waitGroup.Done()
	}()

	for {
		select {
		case <-w.finishCh:
			return
		case chk := <-w.outputHolderCh:
			if err := Next(ctx, w.childExec, chk); err != nil {
				w.outputCh <- &shuffleOutput{err: err}
				return
			}

			// Should not send an empty `chk` to `w.outputCh`.
			if chk.NumRows() == 0 {
				return
			}
			w.outputCh <- &shuffleOutput{chk: chk, giveBackCh: w.outputHolderCh}
		}
	}
}
