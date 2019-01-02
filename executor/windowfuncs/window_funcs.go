package windowfuncs

import (
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
	// RemainResult checks if there are some remained results to be exhausted.
	RemainResult() bool
}

// aggNoFrame deals with agg functions with no frame specification.
type aggNoFrame struct {
	result   aggfuncs.PartialResult
	agg      aggfuncs.AggFunc
	remained int64
}

// ProcessOneChunk implements the WindowFunc interface.
func (wf *aggNoFrame) ProcessOneChunk(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk) ([]chunk.Row, error) {
	err := wf.agg.UpdatePartialResult(sctx, rows, wf.result)
	if err != nil {
		return nil, err
	}
	wf.remained += int64(len(rows))
	rows = rows[:0]
	return rows, nil
}

// ExhaustResult implements the WindowFunc interface.
func (wf *aggNoFrame) ExhaustResult(sctx sessionctx.Context, rows []chunk.Row, dest *chunk.Chunk) ([]chunk.Row, error) {
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

// RemainResult implements the WindowFunc interface.
func (wf *aggNoFrame) RemainResult() bool {
	return wf.remained > 0
}

// BuildWindowFunc builds window functions according to the window functions description.
func BuildWindowFunc(ctx sessionctx.Context, window *aggregation.WindowFuncDesc, ordinal int) WindowFunc {
	aggDesc := aggregation.NewAggFuncDesc(ctx, window.Name, window.Args, false)
	agg := aggfuncs.Build(ctx, aggDesc, ordinal)
	return &aggNoFrame{
		agg:    agg,
		result: agg.AllocPartialResult(),
	}
}
