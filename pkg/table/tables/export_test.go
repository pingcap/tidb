// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

var SetAssertionForTest = setAssertion

// LocateRangeColumnsByDatumsForTest locates a row of a RANGE COLUMNS partitioned table by
// comparing datums. ok is false if the table or the row needs the bound expressions.
func LocateRangeColumnsByDatumsForTest(ctx expression.EvalContext, pt table.PartitionedTable, r []types.Datum) (idx int, ok bool) {
	pe := pt.GetPartitionedTable().(*partitionedTable).partitionExpr
	if pe.ForRangeColumnsPruning == nil || pe.lessThanDatums == nil {
		return 0, false
	}
	return pe.locateByDatums(ctx.TypeCtx(), pe.ColumnOffset, r)
}

// LocateRangeColumnsByExprForTest locates a row of a RANGE COLUMNS partitioned table by
// evaluating the bound expressions.
func LocateRangeColumnsByExprForTest(ctx expression.EvalContext, pt table.PartitionedTable, r []types.Datum) (int, error) {
	t := pt.GetPartitionedTable().(*partitionedTable)
	pe := *t.partitionExpr
	rc := *pe.ForRangeColumnsPruning
	rc.lessThanDatums = nil
	pe.ForRangeColumnsPruning = &rc
	return t.locateRangeColumnPartition(ctx, &pe, r)
}
