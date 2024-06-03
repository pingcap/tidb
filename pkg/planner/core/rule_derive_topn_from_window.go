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
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// deriveTopNFromWindow pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type deriveTopNFromWindow struct {
}

func appendDerivedTopNTrace(topN base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	child := topN.Children()[0]
	action := func() string {
		return fmt.Sprintf("%v_%v top N added below  %v_%v ", topN.TP(), topN.ID(), child.TP(), child.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v filter on row number", topN.TP())
	}
	opt.AppendStepToCurrent(topN.ID(), topN.TP(), reason, action)
}

// checkPartitionBy mainly checks if partition by of window function is a prefix of
// data order (clustered index) of the data source. TiFlash is allowed only for empty partition by.
func checkPartitionBy(p *LogicalWindow, d *DataSource) bool {
	// No window partition by. We are OK.
	if len(p.PartitionBy) == 0 {
		return true
	}

	// Table not clustered and window has partition by. Can not do the TopN push down.
	if d.handleCols == nil {
		return false
	}

	if len(p.PartitionBy) > d.handleCols.NumCols() {
		return false
	}

	for i, col := range p.PartitionBy {
		if !(col.Col.EqualColumn(d.handleCols.GetCol(i))) {
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
func windowIsTopN(p *LogicalSelection) (bool, uint64) {
	// Check if child is window function.
	child, isLogicalWindow := p.Children()[0].(*LogicalWindow)
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
	for _, path := range dataSource.possibleAccessPaths {
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

func (*deriveTopNFromWindow) optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.DeriveTopN(opt), planChanged, nil
}

// DeriveTopN implements the LogicalPlan interface.
func (s *LogicalSelection) DeriveTopN(opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	p := s.Self().(*LogicalSelection)
	windowIsTopN, limitValue := windowIsTopN(p)
	if windowIsTopN {
		child := p.Children()[0].(*LogicalWindow)
		grandChild := child.Children()[0].(*DataSource)
		// Build order by for derived Limit
		byItems := make([]*util.ByItems, 0, len(child.OrderBy))
		for _, col := range child.OrderBy {
			byItems = append(byItems, &util.ByItems{Expr: col.Col, Desc: col.Desc})
		}
		// Build derived Limit
		derivedTopN := LogicalTopN{Count: limitValue, ByItems: byItems, PartitionBy: child.GetPartitionBy()}.Init(grandChild.SCtx(), grandChild.QueryBlockOffset())
		derivedTopN.SetChildren(grandChild)
		/* return select->datasource->topN->window */
		child.SetChildren(derivedTopN)
		p.SetChildren(child)
		appendDerivedTopNTrace(p, opt)
		return p
	}
	return p
}

func (*deriveTopNFromWindow) name() string {
	return "derive_topn_from_window"
}
