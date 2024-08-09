// Copyright 2023 PingCAP, Inc.
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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// DeriveTopNFromWindow pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type DeriveTopNFromWindow struct {
}

// checkPartitionBy mainly checks if partition by of window function is a prefix of
// data order (clustered index) of the data source. TiFlash is allowed only for empty partition by.
func checkPartitionBy(p *logicalop.LogicalWindow, d *DataSource) bool {
	// No window partition by. We are OK.
	if len(p.PartitionBy) == 0 {
		return true
	}

	// Table not clustered and window has partition by. Can not do the TopN push down.
	if d.HandleCols == nil {
		return false
	}

	if len(p.PartitionBy) > d.HandleCols.NumCols() {
		return false
	}

	for i, col := range p.PartitionBy {
		if !(col.Col.EqualColumn(d.HandleCols.GetCol(i))) {
			return false
		}
	}
	return true
}

/*
		Check the following pattern of filter over row number window function:
	  - Filter is simple condition of row_number < value or row_number <= value
	  - The window function is a simple row number
	  - With default frame: rows between current row and current row. Check is not necessary since
	    current row is only frame applicable to row number
	  - Child is a data source with no tiflash option.
*/
func windowIsTopN(lp base.LogicalPlan) (bool, uint64) {
	p := lp.(*logicalop.LogicalSelection)
	// Check if child is window function.
	child, isLogicalWindow := p.Children()[0].(*logicalop.LogicalWindow)
	if !isLogicalWindow {
		return false, 0
	}

	if len(p.Conditions) != 1 {
		return false, 0
	}

	// Check if filter is column < constant or column <= constant. If it is in this form find column and constant.
	column, limitValue := expression.FindUpperBound(p.Conditions[0])
	if column == nil || limitValue <= 0 {
		return false, 0
	}

	// Check if filter on window function
	windowColumns := child.GetWindowResultColumns()
	if len(windowColumns) != 1 || !(column.Equal(p.SCtx().GetExprCtx().GetEvalCtx(), windowColumns[0])) {
		return false, 0
	}

	grandChild := child.Children()[0]
	dataSource, isDataSource := grandChild.(*DataSource)
	if !isDataSource {
		return false, 0
	}

	// Give up if TiFlash is one possible access path. Pushing down window aggregation is good enough in this case.
	for _, path := range dataSource.PossibleAccessPaths {
		if path.StoreType == kv.TiFlash {
			return false, 0
		}
	}

	if len(child.WindowFuncDescs) == 1 && child.WindowFuncDescs[0].Name == "row_number" &&
		child.Frame.Type == ast.Rows && child.Frame.Start.Type == ast.CurrentRow && child.Frame.End.Type == ast.CurrentRow &&
		checkPartitionBy(child, dataSource) {
		return true, uint64(limitValue)
	}
	return false, 0
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*DeriveTopNFromWindow) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.DeriveTopN(opt), planChanged, nil
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*DeriveTopNFromWindow) Name() string {
	return "derive_topn_from_window"
}
