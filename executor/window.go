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

	"github.com/cznic/mathutil"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/executor/windowfunc"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// WindowExec is the executor for window functions.
type WindowExec struct {
	baseExecutor

	groupChecker         *groupChecker
	inputIter            *chunk.Iterator4Chunk
	inputRow             chunk.Row
	groupRows            []chunk.Row
	childResults         []*chunk.Chunk
	executed             bool
	meetNewGroup         bool
	remainingRowsInGroup int
	remainingRowsInChunk int
	processor            windowProcessor
}

// Close implements the Executor Close interface.
func (e *WindowExec) Close() error {
	e.childResults = nil
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *WindowExec) Next(ctx context.Context, chk *chunk.RecordBatch) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("windowExec.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.Reset()
	if e.meetNewGroup && e.remainingRowsInGroup > 0 {
		err := e.appendResult2Chunk(chk.Chunk)
		if err != nil {
			return err
		}
	}
	for !e.executed && (chk.NumRows() == 0 || e.remainingRowsInChunk > 0) {
		err := e.consumeOneGroup(ctx, chk.Chunk)
		if err != nil {
			e.executed = true
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *WindowExec) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) error {
	var err error
	if err = e.fetchChildIfNecessary(ctx, chk); err != nil {
		return errors.Trace(err)
	}
	for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
		e.meetNewGroup, err = e.groupChecker.meetNewGroup(e.inputRow)
		if err != nil {
			return errors.Trace(err)
		}
		if e.meetNewGroup {
			err := e.consumeGroupRows(chk)
			if err != nil {
				return errors.Trace(err)
			}
			err = e.appendResult2Chunk(chk)
			if err != nil {
				return errors.Trace(err)
			}
		}
		e.remainingRowsInGroup++
		e.groupRows = append(e.groupRows, e.inputRow)
		if e.meetNewGroup {
			e.inputRow = e.inputIter.Next()
			return nil
		}
	}
	return nil
}

func (e *WindowExec) consumeGroupRows(chk *chunk.Chunk) (err error) {
	if len(e.groupRows) == 0 {
		return nil
	}
	e.copyChk(chk)
	remained := mathutil.Min(e.remainingRowsInChunk, e.remainingRowsInGroup)
	oldRemained := remained
	e.groupRows, remained, err = e.processor.consumeGroupRows(e.ctx, e.groupRows, chk, remained)
	if err != nil {
		return errors.Trace(err)
	}
	e.remainingRowsInGroup -= oldRemained - remained
	e.remainingRowsInChunk -= oldRemained - remained
	return nil
}

func (e *WindowExec) fetchChildIfNecessary(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.inputIter != nil && e.inputRow != e.inputIter.End() {
		return nil
	}

	// Before fetching a new batch of input, we should consume the last group rows.
	err = e.consumeGroupRows(chk)
	if err != nil {
		return errors.Trace(err)
	}

	childResult := e.children[0].newFirstChunk()
	err = e.children[0].Next(ctx, &chunk.RecordBatch{Chunk: childResult})
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
func (e *WindowExec) appendResult2Chunk(chk *chunk.Chunk) (err error) {
	e.copyChk(chk)
	remained := mathutil.Min(e.remainingRowsInChunk, e.remainingRowsInGroup)
	oldRemained := remained
	e.groupRows, remained, err = e.processor.appendResult2Chunk(e.ctx, e.groupRows, chk, remained)
	if err != nil {
		return err
	}
	e.remainingRowsInGroup -= oldRemained - remained
	e.remainingRowsInChunk -= oldRemained - remained
	if e.remainingRowsInGroup == 0 {
		e.processor.resetPartialResult()
	}
	return nil
}

func (e *WindowExec) copyChk(chk *chunk.Chunk) {
	if len(e.childResults) == 0 || chk.NumRows() > 0 {
		return
	}
	childResult := e.childResults[0]
	e.childResults = e.childResults[1:]
	e.remainingRowsInChunk = childResult.NumRows()
	columns := e.Schema().Columns[:len(e.Schema().Columns)-1]
	for i, col := range columns {
		chk.MakeRefTo(i, childResult, col.Index)
	}
}

// windowProcessor is the interface for processing different kinds of window functions.
type windowProcessor interface {
	consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, int, error)
	appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, int, error)
	resetPartialResult()
}

type aggWindowProcessor struct {
	windowFunc    aggfuncs.AggFunc
	partialResult aggfuncs.PartialResult
}

func (p *aggWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, int, error) {
	err := p.windowFunc.UpdatePartialResult(ctx, rows, p.partialResult)
	rows = rows[:0]
	return rows, remained, err
}

func (p *aggWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, int, error) {
	for remained > 0 {
		// TODO: We can extend the agg func interface to avoid the `for` loop  here.
		err := p.windowFunc.AppendFinalResult2Chunk(ctx, p.partialResult, chk)
		if err != nil {
			return rows, remained, err
		}
		remained--
	}
	return rows, remained, nil
}

func (p *aggWindowProcessor) resetPartialResult() {
	p.windowFunc.ResetPartialResult(p.partialResult)
}

type noFrameWindowProcessor struct {
	windowFunc    windowfuncs.WindowFunc
	partialResult windowfuncs.PartialResult
}

func (p *noFrameWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, int, error) {
	var err error
	rows, remained, err = p.windowFunc.ProcessOneChunk(ctx, rows, p.partialResult, chk, remained)
	return rows, remained, err
}

func (p *noFrameWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, int, error) {
	var err error
	for remained > 0 {
		rows, remained, err = p.windowFunc.ExhaustResult(ctx, rows, p.partialResult, chk, remained)
		if err != nil {
			return rows, remained, err
		}
	}
	return rows, remained, err
}

func (p *noFrameWindowProcessor) resetPartialResult() {
	p.windowFunc.ResetPartialResult(p.partialResult)
}
