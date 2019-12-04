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
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
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

	evaluatorSuit    *expression.EvaluatorSuite
	calculateNoDelay bool

	prepared    bool
	finishCh    chan struct{}
	outputCh    chan *projectionOutput
	fetcher     projectionInputFetcher
	numWorkers  int64
	workers     []*projectionWorker
	childResult *chunk.Chunk

	// parentReqRows indicates how many rows the parent executor is
	// requiring. It is set when parallelExecute() is called and used by the
	// concurrent projectionInputFetcher.
	//
	// NOTE: It should be protected by atomic operations.
	parentReqRows int64
}

// Open implements the Executor Open interface.
func (e *ProjectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	return e.open(ctx)
}

func (e *ProjectionExec) open(ctx context.Context) error {
	e.prepared = false
	e.parentReqRows = int64(e.maxChunkSize)

	// For now a Projection can not be executed vectorially only because it
	// contains "SetVar" or "GetVar" functions, in this scenario this
	// Projection can not be executed parallelly.
	if e.numWorkers > 0 && !e.evaluatorSuit.Vectorizable() {
		e.numWorkers = 0
	}

	if e.isUnparallelExec() {
		e.childResult = newFirstChunk(e.children[0])
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
func (e *ProjectionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.isUnparallelExec() {
		return e.unParallelExecute(ctx, req)
	}
	return e.parallelExecute(ctx, req)

}

func (e *ProjectionExec) isUnparallelExec() bool {
	return e.numWorkers <= 0
}

func (e *ProjectionExec) unParallelExecute(ctx context.Context, chk *chunk.Chunk) error {
	// transmit the requiredRows
	e.childResult.SetRequiredRows(chk.RequiredRows(), e.maxChunkSize)
	err := Next(ctx, e.children[0], e.childResult)
	if err != nil {
		return err
	}
	if e.childResult.NumRows() == 0 {
		return nil
	}
	err = e.evaluatorSuit.Run(e.ctx, e.childResult, chk)
	return err
}

func (e *ProjectionExec) parallelExecute(ctx context.Context, chk *chunk.Chunk) error {
	atomic.StoreInt64(&e.parentReqRows, int64(chk.RequiredRows()))
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
		return err
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
		proj:           e,
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
			chk:          newFirstChunk(e.children[0]),
			targetWorker: e.workers[i],
		}
		e.fetcher.outputCh <- &projectionOutput{
			chk:  newFirstChunk(e),
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
	if e.runtimeStats != nil {
		if e.isUnparallelExec() {
			e.runtimeStats.SetConcurrencyInfo("Concurrency", 0)
		} else {
			e.runtimeStats.SetConcurrencyInfo("Concurrency", int(e.numWorkers))
		}
	}
	return e.baseExecutor.Close()
}

type projectionInputFetcher struct {
	proj           *ProjectionExec
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
	var output *projectionOutput
	defer func() {
		if r := recover(); r != nil {
			recoveryProjection(output, r)
		}
		close(f.globalOutputCh)
	}()

	for {
		input := readProjectionInput(f.inputCh, f.globalFinishCh)
		if input == nil {
			return
		}
		targetWorker := input.targetWorker

		output = readProjectionOutput(f.outputCh, f.globalFinishCh)
		if output == nil {
			return
		}

		f.globalOutputCh <- output

		requiredRows := atomic.LoadInt64(&f.proj.parentReqRows)
		input.chk.SetRequiredRows(int(requiredRows), f.proj.maxChunkSize)
		err := Next(ctx, f.child, input.chk)
		if err != nil || input.chk.NumRows() == 0 {
			output.done <- err
			return
		}

		targetWorker.inputCh <- input
		targetWorker.outputCh <- output
	}
}

type projectionWorker struct {
	sctx            sessionctx.Context
	evaluatorSuit   *expression.EvaluatorSuite
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
	var output *projectionOutput
	defer func() {
		if r := recover(); r != nil {
			recoveryProjection(output, r)
		}
	}()
	for {
		input := readProjectionInput(w.inputCh, w.globalFinishCh)
		if input == nil {
			return
		}

		output = readProjectionOutput(w.outputCh, w.globalFinishCh)
		if output == nil {
			return
		}

		err := w.evaluatorSuit.Run(w.sctx, input.chk, output.chk)
		output.done <- err

		if err != nil {
			return
		}

		w.inputGiveBackCh <- input
	}
}

func recoveryProjection(output *projectionOutput, r interface{}) {
	if output != nil {
		output.done <- errors.Errorf("%v", r)
	}
	buf := util.GetStack()
	logutil.BgLogger().Error("projection executor panicked", zap.String("error", fmt.Sprintf("%v", r)), zap.String("stack", string(buf)))
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
