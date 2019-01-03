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

package windowfuncs

import (
	"errors"

	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// WindowFunc is the interface for processing window functions.
type WindowFunc interface {
	// ProcessOneChunk processes one chunk.
	ProcessOneChunk(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk) ([]chunk.Row, error)
	// ExhaustResult exhausts result to the result chunk.
	ExhaustResult(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk) ([]chunk.Row, error)
	// HasRemainingResults checks if there are some remained results to be exhausted.
	HasRemainingResults() bool
}

// aggWithoutFrame deals with agg functions with no frame specification.
type aggWithoutFrame struct {
	result   aggfuncs.PartialResult
	agg      aggfuncs.AggFunc
	remained int64
}

// ProcessOneChunk implements the WindowFunc interface.
func (wf *aggWithoutFrame) ProcessOneChunk(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk) ([]chunk.Row, error) {
	err := wf.agg.UpdatePartialResult(sctx, rows, wf.result)
	if err != nil {
		return nil, err
	}
	wf.remained += int64(len(rows))
	rows = rows[:0]
	return rows, nil
}

// ExhaustResult implements the WindowFunc interface.
func (wf *aggWithoutFrame) ExhaustResult(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk) ([]chunk.Row, error) {
	rows = rows[:0]
	for wf.remained > 0 && dest.RemainedRows(dest.NumCols()-1) > 0 {
		err := wf.agg.AppendFinalResult2Chunk(sctx, wf.result, dest)
		if err != nil {
			return rows, err
		}
		wf.remained--
	}
	if wf.remained == 0 {
		wf.agg.ResetPartialResult(wf.result)
	}
	return rows, nil
}

// HasRemainingResults implements the WindowFunc interface.
func (wf *aggWithoutFrame) HasRemainingResults() bool {
	return wf.remained > 0
}

// BuildWindowFunc builds window functions according to the window functions description.
func BuildWindowFunc(ctx sessionctx.Context, window *aggregation.WindowFuncDesc, ordinal int) (WindowFunc, error) {
	aggDesc := aggregation.NewAggFuncDesc(ctx, window.Name, window.Args, false)
	agg := aggfuncs.Build(ctx, aggDesc, ordinal)
	if agg == nil {
		return nil, errors.New("window evaluator only support aggregation functions without frame now")
	}
	return &aggWithoutFrame{
		agg:    agg,
		result: agg.AllocPartialResult(),
	}, nil
}
