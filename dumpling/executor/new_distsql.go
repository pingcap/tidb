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

	// result returns one or more distsql.PartialResult and each PartialResult is return by one region.
	result        distsql.SelectResult
	partialResult distsql.PartialResult
}

// Schema implements the Executor Schema interface.
func (t *TableReaderExecutor) Schema() *expression.Schema {
	return t.schema
}

// Close implements the Executor Close interface.
func (t *TableReaderExecutor) Close() error {
	err := closeAll(t.result, t.partialResult)
	t.result = nil
	t.partialResult = nil
	return errors.Trace(err)
}

// Next implements the Executor Next interface.
func (t *TableReaderExecutor) Next() (*Row, error) {
	for {
		// Get partial result.
		if t.partialResult == nil {
			var err error
			t.partialResult, err = t.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if t.partialResult == nil {
				// Finished.
				return nil, nil
			}
		}
		// Get a row from partial result.
		h, rowData, err := t.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish the current partial result and get the next one.
			t.partialResult.Close()
			t.partialResult = nil
			continue
		}
		values := make([]types.Datum, t.schema.Len())
		err = codec.SetRawValues(rowData, values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = decodeRawValues(values, t.schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return resultRowToRow(t.table, h, values, t.asName), nil
	}
}

func (t *TableReaderExecutor) doRequest() error {
	kvRanges := tableRangesToKVRanges(t.tableID, t.ranges)
	var err error
	t.result, err = distsql.SelectDAG(t.ctx.GetClient(), goctx.Background(), t.dagPB, kvRanges, t.ctx.GetSessionVars().DistSQLScanConcurrency, t.keepOrder, t.desc)
	if err != nil {
		return errors.Trace(err)
	}
	t.result.Fetch(t.ctx.GoCtx())
	return nil
}
