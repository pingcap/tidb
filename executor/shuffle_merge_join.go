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

	// each dataSource has a corresponding spliter
	splitters   []partitionSplitter
	dataSources []Executor

	finishCh chan struct{}
	outputCh chan *shuffleOutput
}

// Open implements the Executor Open interface.
func (e *ShuffleMergeJoinExec) Open(ctx context.Context) error {
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
func (e *ShuffleMergeJoinExec) Close() error {
	if !e.prepared {
		for _, w := range e.workers {
			for _, r := range w.receivers {
				close(r.inputHolderCh)
				close(r.inputCh)
			}
			close(w.outputHolderCh)
		}
		close(e.outputCh)
	}
	close(e.finishCh)
	for _, w := range e.workers {
		for _, r := range w.receivers {
			for range r.inputCh {
			}
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

	for _, r := range e.dataSources {
		if err := r.Close(); err != nil {
			return errors.Trace(err)
		}
	}
	err := e.baseExecutor.Close()
	return errors.Trace(err)
}

func (e *ShuffleMergeJoinExec) prepare4ParallelExec(ctx context.Context) {
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

func (e *ShuffleMergeJoinExec) fetchDataAndSplit(ctx context.Context, dataSourceIndex int) {
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

// shuffleReceiver receives chunk from dataSource through inputCh
type shuffleReceiver struct {
	baseExecutor

	finishCh <-chan struct{}
	// executed bool

	inputCh       chan *chunk.Chunk
	inputHolderCh chan *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *shuffleReceiver) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	// e.executed = false
	return nil
}

// Close implements the Executor Close interface.
func (e *shuffleReceiver) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *shuffleReceiver) Next(ctx context.Context, req *chunk.Chunk) error {
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

	// each receiver corresponse to a dataSource
	receivers []shuffleReceiver

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
