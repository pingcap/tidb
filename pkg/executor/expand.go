// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// ExpandExec is used to execute expand logical plan.
type ExpandExec struct {
	exec.BaseExecutor

	numWorkers  int64
	childResult *chunk.Chunk
	memTracker  *memory.Tracker

	// levelIterOffset is responsible for the level iteration offset.
	levelIterOffset int64

	// levelEvaluatorSuits is responsible for the level projections.
	// each level is an implicit projection helped by a evaluatorSuit.
	levelEvaluatorSuits []*expression.EvaluatorSuite
}

// Open implements the Executor Open interface.
func (e *ExpandExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	return e.open(ctx)
}

func (e *ExpandExec) open(_ context.Context) error {
	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	// todo: implement the parallel execution logic
	e.numWorkers = 0

	if e.isUnparalleled() {
		// levelIterOffset = -1 means we should cache a child chunk first.
		e.levelIterOffset = -1
		e.childResult = exec.TryNewCacheChunk(e.Children(0))
		e.memTracker.Consume(e.childResult.MemoryUsage())
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *ExpandExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.isUnparalleled() {
		return e.unParallelExecute(ctx, req)
	}
	return e.parallelExecute(ctx, req)
}

func (e *ExpandExec) isUnparalleled() bool {
	return e.numWorkers <= 0
}

func (e *ExpandExec) unParallelExecute(ctx context.Context, chk *chunk.Chunk) error {
	// for one cache input chunk, if it has already been processed N times, we need to fetch a new one.
	if e.levelIterOffset == -1 || e.levelIterOffset > int64(len(e.levelEvaluatorSuits)-1) {
		e.childResult.SetRequiredRows(chk.RequiredRows(), e.MaxChunkSize())
		mSize := e.childResult.MemoryUsage()
		err := exec.Next(ctx, e.Children(0), e.childResult)
		if err != nil {
			return err
		}
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if e.childResult.NumRows() == 0 {
			return nil
		}
		// when cache a new child chunk, rewind the levelIterOffset.
		e.levelIterOffset = 0
	}
	evalCtx := e.Ctx().GetExprCtx().GetEvalCtx()
	enableVectorized := e.Ctx().GetSessionVars().EnableVectorizedExpression
	err := e.levelEvaluatorSuits[e.levelIterOffset].Run(evalCtx, enableVectorized, e.childResult, chk)
	if err != nil {
		return err
	}
	e.levelIterOffset++
	return nil
}

func (*ExpandExec) parallelExecute(_ context.Context, _ *chunk.Chunk) error {
	return errors.New("parallel expand eval logic not implemented")
}

// Close implements the Executor Close interface.
func (e *ExpandExec) Close() error {
	// if e.BaseExecutor.Open returns error, e.childResult will be nil, see https://github.com/pingcap/tidb/issues/24210
	// for more information
	if e.isUnparalleled() && e.childResult != nil {
		e.memTracker.Consume(-e.childResult.MemoryUsage())
		e.childResult = nil
	}
	if e.BaseExecutor.RuntimeStats() != nil {
		runtimeStats := &execdetails.RuntimeStatsWithConcurrencyInfo{}
		if e.isUnparalleled() {
			runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", 0))
		} else {
			runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", int(e.numWorkers)))
		}
		e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), runtimeStats)
	}
	return e.BaseExecutor.Close()
}
