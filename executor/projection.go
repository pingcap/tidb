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
// https://en.wikipedia.org/wiki/Projection_(relational_algebra)
//
// NOTE:
// 1. The number of "projectionWorker" is controlled by the global session
//    variable "tidb_projection_concurrency".
// 2. Unparallel version is used when one of the following situations occurs:
//    a. "tidb_projection_concurrency" is set to 0.
//    b. The estimated input size is smaller than "tidb_max_chunk_size".
//    c. This projection can not be executed vectorially.

type projectionInput struct {
	chk          *chunk.Chunk
	targetWorker *projectionWorker
}

type projectionOutput struct {
	chk  *chunk.Chunk
	done chan error
}

// ProjectionExec implements the physical Projection Operator:
// https://en.wikipedia.org/wiki/Projection_(relational_algebra)
type ProjectionExec struct {
	baseExecutor

	evaluatorSuit    *expression.EvaluatorSuit
	calculateNoDelay bool

	prepared    bool
	finishCh    chan struct{}
	outputCh    chan *projectionOutput
	fetcher     projectionInputFetcher
	numWorkers  int64
	workers     []*projectionWorker
	childResult *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *ProjectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	e.prepared = false

	// For now a Projection can not be executed vectorially only because it
	// contains "SetVar" or "GetVar" functions, in this scenario this
	// Projection can not be executed parallelly.
	if e.numWorkers > 0 && !e.evaluatorSuit.Vectorizable() {
		e.numWorkers = 0
	}

	if e.isUnparallelExec() {
		e.childResult = e.children[0].newChunk()
	}

	return nil
}

// Next implements the Executor Next interface.
//
// Here we explain the execution flow of the parallel projection implementation.
// There are 3 main components:
//   1. "projectionInputFetcher": Fetch input "Chunk" from child.
//   2. "projectionWorker":       Do the projection work.
//   3. "ProjectionExec.Next":    Return result to parent.
//
// 1. "projectionInputFetcher" gets its input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it fetches child's result into "input.chk" and:
//   a. Dispatches this input to the worker specified in "input.targetWorker"
//   b. Dispatches this output to the main thread: "ProjectionExec.Next"
//   c. Dispatches this output to the worker specified in "input.targetWorker"
// It is finished and exited once:
//   a. There is no more input from child.
//   b. "ProjectionExec" close the "globalFinishCh"
//
// 2. "projectionWorker" gets its input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it calculates the projection result use "input.chk" as the input
// and "output.chk" as the output, once the calculation is done, it:
//   a. Sends "nil" or error to "output.done" to mark this input is finished.
//   b. Returns the "input" resource to "projectionInputFetcher.inputCh"
// They are finished and exited once:
//   a. "ProjectionExec" closes the "globalFinishCh"
//
// 3. "ProjectionExec.Next" gets its output resources from its "outputCh" channel.
// After receiving an output from "outputCh", it should wait to receive a "nil"
// or error from "output.done" channel. Once a "nil" or error is received:
//   a. Returns this output to its parent
//   b. Returns the "output" resource to "projectionInputFetcher.outputCh"
//
//  +-----------+----------------------+--------------------------+
//  |           |                      |                          |
//  |  +--------+---------+   +--------+---------+       +--------+---------+
//  |  | projectionWorker |   + projectionWorker |  ...  + projectionWorker |
//  |  +------------------+   +------------------+       +------------------+
//  |       ^       ^              ^       ^                  ^       ^
//  |       |       |              |       |                  |       |
//  |    inputCh outputCh       inputCh outputCh           inputCh outputCh
//  |       ^       ^              ^       ^                  ^       ^
//  |       |       |              |       |                  |       |
//  |                              |       |
//  |                              |       +----------------->outputCh
//  |                              |       |                      |
//  |                              |       |                      v
//  |                      +-------+-------+--------+   +---------------------+
//  |                      | projectionInputFetcher |   | ProjectionExec.Next |
//  |                      +------------------------+   +---------+-----------+
//  |                              ^       ^                      |
//  |                              |       |                      |
//  |                           inputCh outputCh                  |
//  |                              ^       ^                      |
//  |                              |       |                      |
//  +------------------------------+       +----------------------+
//
func (e *ProjectionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.isUnparallelExec() {
		return errors.Trace(e.unParallelExecute(ctx, chk))
	}
	return errors.Trace(e.parallelExecute(ctx, chk))

}

func (e *ProjectionExec) isUnparallelExec() bool {
	return e.numWorkers <= 0
}

func (e *ProjectionExec) unParallelExecute(ctx context.Context, chk *chunk.Chunk) error {
	err := e.children[0].Next(ctx, e.childResult)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.evaluatorSuit.Run(e.ctx, e.childResult, chk)
	return errors.Trace(err)
}

func (e *ProjectionExec) parallelExecute(ctx context.Context, chk *chunk.Chunk) error {
	if !e.prepared {
		e.prepare(ctx)
		e.prepared = true
	}

	output, ok := <-e.outputCh
	if !ok {
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

	// Initialize projectionInputFetcher.
	e.fetcher = projectionInputFetcher{
		child:          e.children[0],
		globalFinishCh: e.finishCh,
		globalOutputCh: e.outputCh,
		inputCh:        make(chan *projectionInput, e.numWorkers),
		outputCh:       make(chan *projectionOutput, e.numWorkers),
	}

	// Initialize projectionWorker.
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
	if e.isUnparallelExec() {
		e.childResult = nil
	}
	if e.outputCh != nil {
		close(e.finishCh)
		// Wait for "projectionInputFetcher" to finish and exit.
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

// run gets projectionInputFetcher's input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it fetches child's result into "input.chk" and:
//   a. Dispatches this input to the worker specified in "input.targetWorker"
//   b. Dispatches this output to the main thread: "ProjectionExec.Next"
//   c. Dispatches this output to the worker specified in "input.targetWorker"
//
// It is finished and exited once:
//   a. There is no more input from child.
//   b. "ProjectionExec" close the "globalFinishCh"
func (f *projectionInputFetcher) run(ctx context.Context) {
	defer func() {
		close(f.globalOutputCh)
	}()

	for {
		input := readProjectionInput(f.inputCh, f.globalFinishCh)
		if input == nil {
			return
		}
		targetWorker := input.targetWorker

		output := readProjectionOutput(f.outputCh, f.globalFinishCh)
		if output == nil {
			return
		}

		f.globalOutputCh <- output

		err := f.child.Next(ctx, input.chk)
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
	// a. initialized by "ProjectionExec.prepare"
	// b. written	  by "projectionInputFetcher.run"
	// c. read    	  by "projectionWorker.run"
	inputCh  chan *projectionInput
	outputCh chan *projectionOutput
}

// run gets projectionWorker's input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it calculate the projection result use "input.chk" as the input
// and "output.chk" as the output, once the calculation is done, it:
//   a. Sends "nil" or error to "output.done" to mark this input is finished.
//   b. Returns the "input" resource to "projectionInputFetcher.inputCh".
//
// It is finished and exited once:
//   a. "ProjectionExec" closes the "globalFinishCh".
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
