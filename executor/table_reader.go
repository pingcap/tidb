// Copyright 2017 PingCAP, Inc.
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
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

// TableReaderExecutor sends dag request and reads table data from kv layer.
type TableReaderExecutor struct {
	table     table.Table
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []types.IntColumnRange
	dagPB     *tipb.DAGRequest
	ctx       context.Context
	schema    *expression.Schema
	// columns are only required by union scan.
	columns []*model.ColumnInfo

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result        distsql.SelectResult
	partialResult distsql.PartialResult
	priority      int
}

// Schema implements the Executor Schema interface.
func (e *TableReaderExecutor) Schema() *expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	err := closeAll(e.result, e.partialResult)
	e.result = nil
	e.partialResult = nil
	return errors.Trace(err)
}

// Next implements the Executor Next interface.
func (e *TableReaderExecutor) Next() (Row, error) {
	for {
		// Get partial result.
		if e.partialResult == nil {
			var err error
			e.partialResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				// Finished.
				return nil, nil
			}
		}
		// Get a row from partial result.
		rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish the current partial result and get the next one.
			err = e.partialResult.Close()
			terror.Log(errors.Trace(err))
			e.partialResult = nil
			continue
		}
		err = decodeRawValues(rowData, e.schema, e.ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return nil, errors.Trace(err)
		}
		return rowData, nil
	}
}

// Open implements the Executor Open interface.
func (e *TableReaderExecutor) Open() error {
	var builder requestBuilder
	kvReq, err := builder.SetTableRanges(e.tableID, e.ranges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.SelectDAG(goctx.Background(), e.ctx.GetClient(), kvReq, e.schema.Len())
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

// doRequestForHandles constructs kv ranges by handles. It is used by index look up executor.
func (e *TableReaderExecutor) doRequestForHandles(goCtx goctx.Context, handles []int64) error {
	sort.Sort(int64Slice(handles))
	var builder requestBuilder
	kvReq, err := builder.SetTableHandles(e.tableID, handles).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.SelectDAG(goCtx, e.ctx.GetClient(), kvReq, e.schema.Len())
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(goCtx)
	return nil
}

// doRequestForDatums constructs kv ranges by Datums. It is used by index look up join.
// Every lens for `datums` will always be one and must be type of int64.
func (e *TableReaderExecutor) doRequestForDatums(goCtx goctx.Context, datums [][]types.Datum) error {
	handles := make([]int64, 0, len(datums))
	for _, datum := range datums {
		handles = append(handles, datum[0].GetInt64())
	}
	return errors.Trace(e.doRequestForHandles(goCtx, handles))
}
