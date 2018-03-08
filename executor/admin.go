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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

var _ Executor = new(CheckIndexRangeExec)

// CheckIndexRangeExec outputs the index values which has handle between begin and end.
type CheckIndexRangeExec struct {
	baseExecutor

	table    *model.TableInfo
	index    *model.IndexInfo
	is       infoschema.InfoSchema
	startKey []types.Datum

	handleRanges []ast.HandleRange
	srcChunk     *chunk.Chunk

	result distsql.SelectResult
	cols   []*model.ColumnInfo
}

// NextChunk implements the Executor NextChunk interface.
func (e *CheckIndexRangeExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	handleIdx := e.schema.Len() - 1
	for {
		err := e.result.NextChunk(ctx, e.srcChunk)
		if err != nil {
			return errors.Trace(err)
		}
		if e.srcChunk.NumRows() == 0 {
			return nil
		}
		iter := chunk.NewIterator4Chunk(e.srcChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle := row.GetInt64(handleIdx)
			for _, hr := range e.handleRanges {
				if handle >= hr.Begin && handle < hr.End {
					chk.AppendRow(row)
					break
				}
			}
		}
		if chk.NumRows() > 0 {
			return nil
		}
	}
}

// Next implements the Executor Next interface.
func (e *CheckIndexRangeExec) Next(ctx context.Context) (Row, error) {
	// No need to support to-be-removed method.
	return nil, nil
}

// Open implements the Executor Open interface.
func (e *CheckIndexRangeExec) Open(ctx context.Context) error {
	tCols := e.table.Cols()
	for _, ic := range e.index.Columns {
		col := tCols[ic.Offset]
		e.cols = append(e.cols, col)
	}
	e.cols = append(e.cols, &model.ColumnInfo{
		ID:   model.ExtraHandleID,
		Name: model.ExtraHandleName,
	})
	e.srcChunk = e.newChunk()
	dagPB, err := e.buildDAGPB()
	if err != nil {
		return errors.Trace(err)
	}
	sc := e.ctx.GetSessionVars().StmtCtx
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetIndexRanges(sc, e.table.ID, e.index.ID, ranger.FullNewRange()).
		SetDAGRequest(dagPB).
		SetKeepOrder(true).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()

	e.result, err = distsql.Select(ctx, e.ctx, kvReq, e.retFieldTypes)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(ctx)
	return nil
}

func (e *CheckIndexRangeExec) buildDAGPB() (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = e.ctx.Txn().StartTS()
	dagReq.TimeZoneOffset = timeZoneOffset(e.ctx)
	sc := e.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	for i := range e.schema.Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	execPB := e.constructIndexScanPB()
	dagReq.Executors = append(dagReq.Executors, execPB)

	err := setPBColumnsDefaultValue(e.ctx, dagReq.Executors[0].IdxScan.Columns, e.cols)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return dagReq, nil
}

func (e *CheckIndexRangeExec) constructIndexScanPB() *tipb.Executor {
	idxExec := &tipb.IndexScan{
		TableId: e.table.ID,
		IndexId: e.index.ID,
		Columns: plan.ColumnsToProto(e.cols, e.table.PKIsHandle),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}
}

// Close implements the Executor Close interface.
func (e *CheckIndexRangeExec) Close() error {
	return nil
}
