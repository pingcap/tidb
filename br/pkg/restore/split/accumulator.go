// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
)

var errClosedAccumulator = errors.New("table split accumulator is closed")

const (
	accumulatorBatchSize = 256
	// accumulatorMaxPendingBatches bounds the spans buffered between the producer
	// and the workers (and therefore the input buffers they keep alive). With the
	// limits below at most ~128K spans (~8 MiB of Valued) are queued; larger
	// limits did not improve throughput on real log-backup metadata.
	accumulatorMaxPendingBatches = 256
	// accumulatorMaxLocalSpans bounds the spans held in producer-side partial
	// batches (there is one partial batch per table).
	accumulatorMaxLocalSpans = 1 << 16
)

// TableSplitAccumulator merges valued spans into one SplitHelper per table using
// a pool of workers. Spans of the same table are merged in the order they were
// added, by at most one worker at a time, so the result is identical to merging
// them sequentially; different tables are merged concurrently and any idle
// worker picks up any table with pending spans (load is bounded only by the
// largest table).
//
// Add, Wait and Abort must all be called from the same (owner) goroutine; they
// are not safe for concurrent use with each other. Call Wait exactly once to
// collect the result, or Abort to stop on error. Add after Wait or Abort
// returns an error. Canceling the context passed to the constructor is safe
// from any goroutine: a blocked Add returns, and Wait reports the error.
type TableSplitAccumulator struct {
	ctx    context.Context
	cancel context.CancelFunc

	tables map[int64]*accTable
	local  map[int64][]Valued // producer-side batch per table
	nLocal int                // spans in `local`

	ready    chan *accTable
	tokens   chan struct{} // one per pending batch
	inflight sync.WaitGroup
	workers  sync.WaitGroup
	closed   bool
}

type accTable struct {
	mu        sync.Mutex
	pending   [][]Valued
	scheduled bool
	helper    *SplitHelper
}

// NewTableSplitAccumulator starts `workers` merge goroutines.
func NewTableSplitAccumulator(ctx context.Context, workers int) *TableSplitAccumulator {
	if workers < 1 {
		workers = 1
	}
	cctx, cancel := context.WithCancel(ctx)
	a := &TableSplitAccumulator{
		ctx:    cctx,
		cancel: cancel,
		tables: make(map[int64]*accTable),
		local:  make(map[int64][]Valued),
		ready:  make(chan *accTable, accumulatorMaxPendingBatches),
		tokens: make(chan struct{}, accumulatorMaxPendingBatches),
	}
	for range workers {
		a.workers.Add(1)
		go a.work()
	}
	return a
}

func (a *TableSplitAccumulator) work() {
	defer a.workers.Done()
	var batches [][]Valued
	for t := range a.ready {
		for {
			t.mu.Lock()
			batches, t.pending = t.pending, batches[:0]
			if len(batches) == 0 {
				t.scheduled = false
				t.mu.Unlock()
				a.inflight.Done()
				break
			}
			t.mu.Unlock()
			for i, b := range batches {
				if a.ctx.Err() == nil {
					for _, v := range b {
						t.helper.Merge(v)
					}
				}
				batches[i] = nil
				<-a.tokens
			}
		}
	}
}

// Add queues a span for the table. It blocks when too many spans are pending.
func (a *TableSplitAccumulator) Add(tableID int64, v Valued) error {
	if a.closed {
		return errClosedAccumulator
	}
	b := append(a.local[tableID], v)
	a.nLocal++
	if len(b) < accumulatorBatchSize {
		a.local[tableID] = b
		if a.nLocal >= accumulatorMaxLocalSpans {
			return a.flushLocal()
		}
		return nil
	}
	a.local[tableID] = nil
	a.nLocal -= len(b)
	return a.flush(tableID, b)
}

// flushLocal hands all partial batches to the workers.
func (a *TableSplitAccumulator) flushLocal() error {
	for id, b := range a.local {
		if len(b) == 0 {
			continue
		}
		a.local[id] = nil
		a.nLocal -= len(b)
		if err := a.flush(id, b); err != nil {
			return err
		}
	}
	return nil
}

func (a *TableSplitAccumulator) flush(tableID int64, b []Valued) error {
	if a.closed {
		return errClosedAccumulator
	}
	select {
	case a.tokens <- struct{}{}:
	case <-a.ctx.Done():
		return errors.Trace(a.ctx.Err())
	}
	t, ok := a.tables[tableID]
	if !ok {
		t = &accTable{helper: NewSplitHelper()}
		a.tables[tableID] = t
	}
	t.mu.Lock()
	t.pending = append(t.pending, b)
	wake := !t.scheduled
	t.scheduled = true
	t.mu.Unlock()
	if wake {
		// at most one entry per table is in `ready`; the token bound keeps the
		// number of scheduled tables <= cap(ready), so this send never blocks forever.
		a.inflight.Add(1)
		a.ready <- t
	}
	return nil
}

func (a *TableSplitAccumulator) stop() {
	if a.closed {
		return
	}
	a.closed = true
	a.inflight.Wait()
	close(a.ready)
	a.workers.Wait()
}

// Wait flushes the remaining spans, waits for all merges and returns the
// per-table helpers. It returns an error if the context was canceled.
func (a *TableSplitAccumulator) Wait() (map[int64]*SplitHelper, error) {
	defer a.cancel()
	if err := a.flushLocal(); err != nil {
		a.stop()
		return nil, err
	}
	a.stop()
	if err := a.ctx.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	out := make(map[int64]*SplitHelper, len(a.tables))
	for id, t := range a.tables {
		out[id] = t.helper
	}
	return out, nil
}

// Abort stops the workers and drops all pending spans.
func (a *TableSplitAccumulator) Abort() {
	a.cancel()
	a.stop()
}
