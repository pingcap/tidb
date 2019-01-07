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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor/windowfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
)

// WindowExec is the executor for window functions.
type WindowExec struct {
	baseExecutor

	groupChecker  *groupChecker
	inputIter     *chunk.Iterator4Chunk
	inputRow      chunk.Row
	groupRows     []chunk.Row
	childResults  []*chunk.Chunk
	windowFunc    windowfuncs.WindowFunc
	partialResult windowfuncs.PartialResult
	executed      bool
	childCols     []*expression.Column
}

// Close implements the Executor Close interface.
func (e *WindowExec) Close() error {
	e.childResults = nil
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *WindowExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("windowExec.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.Reset()
	if e.windowFunc.HasRemainingResults() {
		err := e.appendResult2Chunk(chk)
		if err != nil {
			return err
		}
	}
	for !e.executed && (chk.NumRows() == 0 || chk.RemainedRows(chk.NumCols()-1) > 0) {
		err := e.consumeOneGroup(ctx, chk)
		if err != nil {
			e.executed = true
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *WindowExec) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) error {
	if err := e.fetchChildIfNecessary(ctx, chk); err != nil {
		return errors.Trace(err)
	}
	for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
		meetNewGroup, err := e.groupChecker.meetNewGroup(e.inputRow)
		if err != nil {
			return errors.Trace(err)
		}
		if meetNewGroup {
			err := e.consumeGroupRows(chk)
			if err != nil {
				return errors.Trace(err)
			}
			err = e.appendResult2Chunk(chk)
			if err != nil {
				return errors.Trace(err)
			}
		}
		e.groupRows = append(e.groupRows, e.inputRow)
		if meetNewGroup {
			e.inputRow = e.inputIter.Next()
			return nil
		}
	}
	return nil
}

func (e *WindowExec) consumeGroupRows(chk *chunk.Chunk) error {
	if len(e.groupRows) == 0 {
		return nil
	}
	e.copyChk(chk)
	var err error
	e.groupRows, err = e.windowFunc.ProcessOneChunk(e.ctx, e.groupRows, chk, e.partialResult)
	return err
}

func (e *WindowExec) fetchChildIfNecessary(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.inputIter != nil && e.inputRow != e.inputIter.End() {
		return nil
	}

	// Before fetching a new batch of input, we should consume the last groupChecker.
	err = e.consumeGroupRows(chk)
	if err != nil {
		return errors.Trace(err)
	}

	childResult := e.children[0].newFirstChunk()
	err = e.children[0].Next(ctx, childResult)
	if err != nil {
		return errors.Trace(err)
	}
	e.childResults = append(e.childResults, childResult)
	// No more data.
	if childResult.NumRows() == 0 {
		e.executed = true
		err = e.appendResult2Chunk(chk)
		return errors.Trace(err)
	}

	e.inputIter = chunk.NewIterator4Chunk(childResult)
	e.inputRow = e.inputIter.Begin()
	return nil
}

// appendResult2Chunk appends result of the window function to the result chunk.
func (e *WindowExec) appendResult2Chunk(chk *chunk.Chunk) error {
	e.copyChk(chk)
	var err error
	e.groupRows, err = e.windowFunc.ExhaustResult(e.ctx, e.groupRows, chk, e.partialResult)
	return err
}

func (e *WindowExec) copyChk(chk *chunk.Chunk) {
	if len(e.childResults) == 0 || chk.NumRows() > 0 {
		return
	}
	childResult := e.childResults[0]
	e.childResults = e.childResults[1:]
	for i, col := range e.childCols {
		chk.CopyColumns(childResult, i, col.Index)
	}
}
