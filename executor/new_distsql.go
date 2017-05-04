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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

var (
	_ = &TableReaderExecutor{}
)

// TableReaderExecutor sends dag request and reads table data from kv layer.
type TableReaderExecutor struct {
	asName    *model.CIStr
	table     table.Table
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []types.IntColumnRange
	dagPB     *tipb.DAGRequest
	ctx       context.Context
	schema    *expression.Schema

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result        distsql.SelectResult
	partialResult distsql.PartialResult
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
func (e *TableReaderExecutor) Next() (*Row, error) {
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
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish the current partial result and get the next one.
			e.partialResult.Close()
			e.partialResult = nil
			continue
		}
		values := make([]types.Datum, e.schema.Len())
		err = codec.SetRawValues(rowData, values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = decodeRawValues(values, e.schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return resultRowToRow(e.table, h, values, e.asName), nil
	}
}

func (e *TableReaderExecutor) doRequest() error {
	kvRanges := tableRangesToKVRanges(e.tableID, e.ranges)
	var err error
	e.result, err = distsql.SelectDAG(e.ctx.GetClient(), goctx.Background(), e.dagPB, kvRanges, e.ctx.GetSessionVars().DistSQLScanConcurrency, e.keepOrder, e.desc)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

// IndexReaderExecutor sends dag request and reads index data from kv layer.
type IndexReaderExecutor struct {
	asName    *model.CIStr
	table     table.Table
	index     *model.IndexInfo
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []*types.IndexRange
	dagPB     *tipb.DAGRequest
	ctx       context.Context
	schema    *expression.Schema

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result        distsql.SelectResult
	partialResult distsql.PartialResult
}

// Schema implements the Executor Schema interface.
func (e *IndexReaderExecutor) Schema() *expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *IndexReaderExecutor) Close() error {
	err := closeAll(e.result, e.partialResult)
	e.result = nil
	e.partialResult = nil
	return errors.Trace(err)
}

// Next implements the Executor Next interface.
func (e *IndexReaderExecutor) Next() (*Row, error) {
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
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish the current partial result and get the next one.
			e.partialResult.Close()
			e.partialResult = nil
			continue
		}
		values := make([]types.Datum, e.schema.Len())
		err = codec.SetRawValues(rowData, values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = decodeRawValues(values, e.schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return resultRowToRow(e.table, h, values, e.asName), nil
	}
}

func (e *IndexReaderExecutor) doRequest() error {
	fieldTypes := make([]*types.FieldType, len(e.index.Columns))
	for i, v := range e.index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	kvRanges, err := indexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.tableID, e.index.ID, e.ranges, fieldTypes)
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.SelectDAG(e.ctx.GetClient(), goctx.Background(), e.dagPB, kvRanges, e.ctx.GetSessionVars().DistSQLScanConcurrency, e.keepOrder, e.desc)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}
