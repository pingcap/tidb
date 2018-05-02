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
	finishCh   chan struct{}
	outputCh   chan *projectionOutput
	fetcher    projectionInputFetcher
	numWorkers int64
	workers    []*projectionWorker
}

// Open implements the Executor Open interface.
func (e *ProjectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	// For now a Projection can not be executated vectorially only because it
	// contains "SetVar" or "GetVar" functions, in this scenario this
	// Projection can not be executed parallelly by more than 1 worker as well.
	if e.numWorkers > 0 && !e.evaluatorSuit.Vectorizable() {
		e.numWorkers = 1
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

	output, ok := <-e.outputCh
	if !ok {
		e.outputCh = nil
		return nil
	}

	err := <-output.done
	if err != nil {
		return errors.Trace(err)
	}

	chk.SwapColumns(output.chk)
	e.fetcher.outputCh <- output
	return nil
}

func (e *ProjectionExec) prepare(ctx context.Context) {

	e.finishCh = make(chan struct{})
	e.outputCh = make(chan *projectionOutput, e.numWorkers)
	e.prepared = false

	// initialize projectionInputFetcher.
	e.fetcher = projectionInputFetcher{
		child:          e.children[0],
		globalFinishCh: e.finishCh,
		globalOutputCh: e.outputCh,
		inputCh:        make(chan *projectionInput, e.numWorkers),
		outputCh:       make(chan *projectionOutput, e.numWorkers),
	}

	// initialize projectionWorker.
	e.workers = make([]*projectionWorker, 0, e.numWorkers)
	for i := int64(0); i < e.numWorkers; i++ {
		e.workers = append(e.workers, &projectionWorker{
			sctx:            e.ctx,
			evaluatorSuit:   e.evaluatorSuit,
			globalFinishCh:  e.finishCh,
			inputGiveBackCh: e.fetcher.inputCh,
			inputCh:         make(chan *projectionInput, 1),
			outputCh:        make(chan *projectionOutput, 1),
		})

		e.fetcher.inputCh <- &projectionInput{
			chk:          e.children[0].newChunk(),
			targetWorker: e.workers[i],
		}
		e.fetcher.outputCh <- &projectionOutput{
			chk:  e.newChunk(),
			done: make(chan error, 1),
		}
	}

	go e.fetcher.run(ctx)

	for i := range e.workers {
		go e.workers[i].run(ctx)
	}
}

// Close implements the Executor Close interface.
func (e *ProjectionExec) Close() error {
	// wait for projectionInputFetcher to be finished.
	if e.outputCh != nil {
		close(e.finishCh)
		for range e.outputCh {
		}
		e.outputCh = nil
	}
	return errors.Trace(e.baseExecutor.Close())
}

type projectionInputFetcher struct {
	child          Executor
	globalFinishCh <-chan struct{}
	globalOutputCh chan<- *projectionOutput

	inputCh  chan *projectionInput
	outputCh chan *projectionOutput
}

func (w *projectionInputFetcher) run(ctx context.Context) {
	defer func() {
		close(w.globalOutputCh)
	}()

	for {
		input := readProjectionInput(w.inputCh, w.globalFinishCh)
		if input == nil {
			return
		}
		targetWorker := input.targetWorker

		output := readProjectionOutput(w.outputCh, w.globalFinishCh)
		if output == nil {
			return
		}

		w.globalOutputCh <- output

		err := w.child.Next(ctx, input.chk)
		if err != nil || input.chk.NumRows() == 0 {
			output.done <- errors.Trace(err)
			return
		}

		targetWorker.inputCh <- input
		targetWorker.outputCh <- output
	}
}

type projectionWorker struct {
	sctx            sessionctx.Context
	evaluatorSuit   *expression.EvaluatorSuit
	globalFinishCh  <-chan struct{}
	inputGiveBackCh chan<- *projectionInput

	// channel "input" and "output" is :
	// 0. inited  by "ProjectionExec.Open"
	// 1. written by "projectionInputFetcher.run"
	// 2. read    by "projectionWorker.run"
	// 3. closed  by "projectionInputFetcher.run"
	inputCh  chan *projectionInput
	outputCh chan *projectionOutput
}

func (w *projectionWorker) run(ctx context.Context) {
	for {
		input := readProjectionInput(w.inputCh, w.globalFinishCh)
		if input == nil {
			return
		}

		output := readProjectionOutput(w.outputCh, w.globalFinishCh)
		if output == nil {
			return
		}

		err := w.evaluatorSuit.Run(w.sctx, input.chk, output.chk)
		output.done <- errors.Trace(err)

		if err != nil {
			return
		}

		w.inputGiveBackCh <- input
	}
}

func readProjectionInput(inputCh <-chan *projectionInput, finishCh <-chan struct{}) *projectionInput {
	select {
	case <-finishCh:
		return nil
	case input, ok := <-inputCh:
		if !ok {
			return nil
		}
		return input
	}
}

func readProjectionOutput(outputCh <-chan *projectionOutput, finishCh <-chan struct{}) *projectionOutput {
	select {
	case <-finishCh:
		return nil
	case output, ok := <-outputCh:
		if !ok {
			return nil
		}
		return output
	}
}
