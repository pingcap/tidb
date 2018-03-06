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
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

var _ Executor = new(CheckIndexRangeExec)

// CheckIndexRangeExec output the index values which has handle between begin and end.
type CheckIndexRangeExec struct {
	baseExecutor

	table    table.Table
	index    table.Index
	is       infoschema.InfoSchema
	startKey []types.Datum

	handleRanges []ast.HandleRange
	srcChunk     *chunk.Chunk

	result   distsql.SelectResult
	partial  distsql.PartialResult
	typesMap map[int64]*types.FieldType
	cols     []*model.ColumnInfo
}

// NextChunk implements the Executor Next interface.
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
	e.typesMap = make(map[int64]*types.FieldType)
	tCols := e.table.Meta().Cols()
	for _, ic := range e.index.Meta().Columns {
		col := tCols[ic.Offset]
		e.typesMap[col.ID] = &col.FieldType
		e.cols = append(e.cols, col)
	}
	e.srcChunk = e.newChunk()
	dagPB, err := e.buildDagPB()
	if err != nil {
		return errors.Trace(err)
	}
	sc := e.ctx.GetSessionVars().StmtCtx
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetIndexRanges(sc, e.table.Meta().ID, e.index.Meta().ID, ranger.FullNewRange()).
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

func (e *CheckIndexRangeExec) buildDagPB() (*tipb.DAGRequest, error) {
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
	tableColumns := e.table.Meta().Columns
	var columns []*model.ColumnInfo
	for _, col := range e.index.Meta().Columns {
		columns = append(columns, tableColumns[col.Offset])
	}
	columns = append(columns, &model.ColumnInfo{
		ID:   model.ExtraHandleID,
		Name: model.NewCIStr("_rowid"),
	})
	idxExec := &tipb.IndexScan{
		TableId: e.table.Meta().ID,
		IndexId: e.index.Meta().ID,
		Columns: plan.ColumnsToProto(columns, e.table.Meta().PKIsHandle),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}
}

// Close implements plan.Plan Close interface.
func (e *CheckIndexRangeExec) Close() error {
	return nil
}
