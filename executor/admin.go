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
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

type idxResult struct {
	IndexVals [][]types.Datum
	Handles   []int64
	LastVal   []types.Datum
	finished  bool
}

// CheckIndexExec output the index values which has handle between begin and end.
type CheckIndexExec struct {
	baseExecutor

	table    table.Table
	index    table.Index
	is       infoschema.InfoSchema
	startKey []types.Datum

	handleRanges []ast.HandleRange

	result   distsql.NewSelectResult
	partial  distsql.NewPartialResult
	typesMap map[int64]*types.FieldType
	cols     []*model.ColumnInfo
}

// Next implements the Executor Next interface.
func (e *CheckIndexExec) Next() (Row, error) {
	rowData, err := e.getNextInRangeIndex()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if rowData == nil {
		return nil, nil
	}
	err = decodeRawValues(rowData, e.Schema(), e.ctx.GetSessionVars().StmtCtx.TimeZone)
	return rowData, errors.Trace(err)
}

func (e *CheckIndexExec) getNextInRangeIndex() ([]types.Datum, error) {
	for {
		if e.partial == nil {
			var err error
			e.partial, err = e.result.Next()
			if err != nil {
				return nil, nil
			}
			if e.partial == nil {
				return nil, nil
			}
		}
		rowData, err := e.partial.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			e.partial = nil
			continue
		}
		err = decodeRawValues(rowData, e.schema, e.ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return nil, errors.Trace(err)
		}
		handle := rowData[len(rowData)-1].GetInt64()
		for _, hr := range e.handleRanges {
			if handle >= hr.Begin && handle < hr.End {
				return rowData, nil
			}
		}
	}
}

// Open implements the Executor interface.
func (e *CheckIndexExec) Open() error {
	e.typesMap = make(map[int64]*types.FieldType)
	tCols := e.table.Meta().Cols()
	for _, ic := range e.index.Meta().Columns {
		col := tCols[ic.Offset]
		e.typesMap[col.ID] = &col.FieldType
		e.cols = append(e.cols, col)
	}

	fieldTypes := make([]*types.FieldType, len(e.index.Meta().Columns))
	for i, v := range e.index.Meta().Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	dagPB, err := e.buildDagPB()
	if err != nil {
		return errors.Trace(err)
	}
	sc := e.ctx.GetSessionVars().StmtCtx
	var builder requestBuilder
	kvReq, err := builder.SetIndexRanges(sc, e.table.Meta().ID, e.index.Meta().ID, ranger.FullIndexRange(), fieldTypes).
		SetDAGRequest(dagPB).
		SetKeepOrder(true).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()

	e.result, err = distsql.NewSelectDAG(e.ctx.GoCtx(), e.ctx.GetClient(), kvReq, e.Schema().Len())
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

func (e *CheckIndexExec) buildDagPB() (*tipb.DAGRequest, error) {
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

func (e *CheckIndexExec) constructIndexScanPB() *tipb.Executor {
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
		Columns: distsql.ColumnsToProto(columns, e.table.Meta().PKIsHandle),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}
}

// Close implements plan.Plan Close interface.
func (e *CheckIndexExec) Close() error {
	return nil
}
