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
	"unsafe"

	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// PartialResult represents data structure to store the partial result for the
// aggregate functions. Here we use unsafe.Pointer to allow the partial result
// to be any type.
type PartialResult unsafe.Pointer

// WindowFunc is the interface for processing window functions.
type WindowFunc interface {
	// ProcessOneChunk processes one chunk.
	ProcessOneChunk(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk, pr PartialResult) ([]chunk.Row, error)
	// ExhaustResult exhausts result to the result chunk.
	ExhaustResult(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk, pr PartialResult) ([]chunk.Row, error)
	// HasRemainingResults checks if there are some remained results to be exhausted.
	HasRemainingResults() bool
	// AllocPartialResult allocates a specific data structure to store the partial result.
	AllocPartialResult() PartialResult
}

// aggWithoutFrame deals with agg functions with no frame specification.
type aggWithoutFrame struct {
	agg aggfuncs.AggFunc
	remained int64
}

type partialResult4AggWithoutFrame struct {
	result   aggfuncs.PartialResult
}

// ProcessOneChunk implements the WindowFunc interface.
func (wf *aggWithoutFrame) ProcessOneChunk(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk, pr PartialResult) ([]chunk.Row, error) {
	p := (*partialResult4AggWithoutFrame)(pr)
	err := wf.agg.UpdatePartialResult(sctx, rows, p.result)
	if err != nil {
		return nil, err
	}
	wf.remained += int64(len(rows))
	rows = rows[:0]
	return rows, nil
}

// ExhaustResult implements the WindowFunc interface.
func (wf *aggWithoutFrame) ExhaustResult(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk, pr PartialResult) ([]chunk.Row, error) {
	rows = rows[:0]
	p := (*partialResult4AggWithoutFrame)(pr)
	for wf.remained > 0 && dest.RemainedRows(dest.NumCols()-1) > 0 {
		err := wf.agg.AppendFinalResult2Chunk(sctx, p.result, dest)
		if err != nil {
			return rows, err
		}
		wf.remained--
	}
	if wf.remained == 0 {
		wf.agg.ResetPartialResult(p.result)
	}
	return rows, nil
}

// AllocPartialResult implements the WindowFunc interface.
func (wf *aggWithoutFrame) AllocPartialResult() PartialResult{
	return PartialResult(&partialResult4AggWithoutFrame{wf.agg.AllocPartialResult()})
}

// HasRemainingResults implements the WindowFunc interface.
func (wf *aggWithoutFrame) HasRemainingResults() bool {
	return wf.remained > 0
}
