// Copyright 2018 PingCAP, Inc.
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
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

type projectionInput struct {
	chk    *chunk.Chunk
	worker *projectionWorker
}

type projectionOutput struct {
	chk  *chunk.Chunk
	done chan error
}

// ProjectionExec represents a select fields executor.
type ProjectionExec struct {
	baseExecutor

	evaluatorSuit    *expression.EvaluatorSuit // dispatched to projection workers.
	calculateNoDelay bool

	prepared   bool
	output     chan *projectionOutput
	fetcher    projectionInputFetcher
	waitGroup  sync.WaitGroup
	numWorkers int64
	workers    []*projectionWorker
}

// Open implements the Executor Open interface.
func (e *ProjectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	if e.numWorkers > 0 && !e.evaluatorSuit.Vectorizable() {
		e.numWorkers = 1
	}

	if e.numWorkers <= 0 {
		return nil
	}

	e.output = make(chan *projectionOutput, e.numWorkers)
	e.prepared = false

	e.fetcher.child = e.children[0]
	e.fetcher.input = make(chan *projectionInput, e.numWorkers)
	e.fetcher.output = make(chan *projectionOutput, e.numWorkers)

	e.workers = make([]*projectionWorker, 0, e.numWorkers)
	for i := int64(0); i < e.numWorkers; i++ {
		e.workers = append(e.workers, &projectionWorker{
			ctx:           e.ctx,
			evaluatorSuit: e.evaluatorSuit,
			input:         make(chan *projectionInput, 1),
			output:        make(chan *projectionOutput, 1),
		})

		e.fetcher.input <- &projectionInput{
			chk:    e.children[0].newChunk(),
			worker: e.workers[i],
		}
		e.fetcher.output <- &projectionOutput{
			chk:  e.newChunk(),
			done: make(chan error, 1),
		}
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *ProjectionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()

	if e.numWorkers <= 0 {
		err := e.children[0].Next(ctx, e.childrenResults[0])
		if err != nil {
			return errors.Trace(err)
		}
		err = e.evaluatorSuit.Run(e.ctx, e.childrenResults[0], chk)
		return errors.Trace(err)
	}

	if !e.prepared {
		e.prepare(ctx)
		e.prepared = true
	}

	output, ok := <-e.output
	if !ok {
		return nil
	}

	err := <-output.done
	if err != nil {
		return errors.Trace(err)
	}

	chk.SwapColumns(output.chk)
	e.fetcher.output <- output
	return nil
}

func (e *ProjectionExec) prepare(ctx context.Context) {
	go e.fetcher.run(ctx, e.output)

	e.waitGroup.Add(len(e.workers))
	for i := range e.workers {
		go e.workers[i].run(e.fetcher.input, &e.waitGroup)
	}

	go e.waitAndFinish()
}

func (e *ProjectionExec) waitAndFinish() {
	e.waitGroup.Wait()
	close(e.output)
}

// Close implements the Executor Close interface.
func (e *ProjectionExec) Close() error {
	if e.numWorkers > 0 {
		close(e.fetcher.input)
		close(e.fetcher.output)
	}
	return errors.Trace(e.baseExecutor.Close())
}

type projectionInputFetcher struct {
	child  Executor
	input  chan *projectionInput
	output chan *projectionOutput
}

func (w *projectionInputFetcher) run(ctx context.Context, dst chan<- *projectionOutput) {
	defer func() {
		for input := range w.input {
			input.worker.finish()
		}
	}()

	for {
		input, ok1 := <-w.input
		if !ok1 {
			return
		}

		output, ok2 := <-w.output
		if !ok2 {
			input.worker.finish()
			return
		}

		dst <- output

		err := w.child.Next(ctx, input.chk)
		if err != nil {
			output.done <- errors.Trace(err)
			input.worker.finish()
			return
		}

		if input.chk.NumRows() == 0 {
			output.done <- nil
			input.worker.finish()
			return
		}

		input.worker.input <- input
		input.worker.output <- output
	}
}

type projectionWorker struct {
	ctx           sessionctx.Context
	evaluatorSuit *expression.EvaluatorSuit

	input  chan *projectionInput
	output chan *projectionOutput
}

func (w *projectionWorker) run(giveBack chan<- *projectionInput, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	for {
		input, ok1 := <-w.input
		if !ok1 {
			return
		}

		output, ok2 := <-w.output
		if !ok2 {
			return
		}

		err := w.evaluatorSuit.Run(w.ctx, input.chk, output.chk)
		output.done <- errors.Trace(err)

		if err != nil {
			return
		}
		giveBack <- input
	}
}

func (w *projectionWorker) finish() {
	close(w.input)
	close(w.output)
}
