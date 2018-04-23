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

// This file contains the implementation of the physical Projection Operator:
//
// https://en.wikipedia.org/wiki/Projection_(relational_algebra)
//
// For parallel implementation, a "projectionInputFetcher" fetches input Chunks
// and dispatches them to many "projectionWorker"s, every "projectionWorker"
// picks an input Chunk and do the projection operation then output the result
// Chunk to the main thread: "ProjectionExec.Next()" function.
// NOTE:
// 1. number of "projectionWorker"s is controlled by the global session
//    variable "tidb_projection_concurrency".
// 2. if "tidb_projection_concurrency" is 0, we use the old non-parallel
//    implementation.

type projectionInput struct {
	chk          *chunk.Chunk
	targetWorker *projectionWorker
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

	if e.numWorkers <= 0 {
		return nil
	}

	// For now a Projection can not be executated vectorially only because it
	// contains "SetVar" or "GetVar" functions, in this scenario this
	// Projection can not be executed parallelly by more than 1 worker as well.
	if !e.evaluatorSuit.Vectorizable() {
		e.numWorkers = 1
	}

	e.output = make(chan *projectionOutput, e.numWorkers)
	e.prepared = false

	// initialize projectionInputFetcher.
	e.fetcher.child = e.children[0]
	e.fetcher.globalOutput = e.output
	e.fetcher.input = make(chan *projectionInput, e.numWorkers)
	e.fetcher.output = make(chan *projectionOutput, e.numWorkers)

	// initialize projectionWorker.
	e.workers = make([]*projectionWorker, 0, e.numWorkers)
	for i := int64(0); i < e.numWorkers; i++ {
		e.workers = append(e.workers, &projectionWorker{
			sctx:          e.ctx,
			evaluatorSuit: e.evaluatorSuit,
			globalWG:      &e.waitGroup,
			inputGiveBack: e.fetcher.input,
			input:         make(chan *projectionInput, 1),
			output:        make(chan *projectionOutput, 1),
		})

		e.fetcher.input <- &projectionInput{
			chk:          e.children[0].newChunk(),
			targetWorker: e.workers[i],
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
	go e.fetcher.run(ctx)

	e.waitGroup.Add(len(e.workers))
	for i := range e.workers {
		go e.workers[i].run(ctx)
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
	child        Executor
	globalOutput chan<- *projectionOutput

	input  chan *projectionInput
	output chan *projectionOutput
}

func (w *projectionInputFetcher) run(ctx context.Context) {
	defer func() {
		for input := range w.input {
			input.targetWorker.finish()
		}
	}()

	for {
		input, ok1 := <-w.input
		if !ok1 {
			return
		}
		targetWorker := input.targetWorker

		output, ok2 := <-w.output
		if !ok2 {
			targetWorker.finish()
			return
		}

		w.globalOutput <- output

		err := w.child.Next(ctx, input.chk)
		if err != nil || input.chk.NumRows() == 0 {
			output.done <- errors.Trace(err)
			targetWorker.finish()
			return
		}

		targetWorker.input <- input
		targetWorker.output <- output
	}
}

type projectionWorker struct {
	sctx          sessionctx.Context
	evaluatorSuit *expression.EvaluatorSuit
	globalWG      *sync.WaitGroup
	inputGiveBack chan<- *projectionInput

	// channel "input" and "output" is :
	// 0. inited  by "ProjectionExec.Open"
	// 1. written by "projectionInputFetcher.run"
	// 2. read    by "projectionWorker.run"
	// 3. closed  by "projectionWorker.finish"
	input  chan *projectionInput
	output chan *projectionOutput
}

func (w *projectionWorker) run(ctx context.Context) {
	defer w.globalWG.Done()

	for {
		input, ok1 := <-w.input
		if !ok1 {
			return
		}

		output, ok2 := <-w.output
		if !ok2 {
			return
		}

		err := w.evaluatorSuit.Run(w.sctx, input.chk, output.chk)
		output.done <- errors.Trace(err)

		if err != nil {
			return
		}
		w.inputGiveBack <- input
	}
}

func (w *projectionWorker) finish() {
	close(w.input)
	close(w.output)
}
