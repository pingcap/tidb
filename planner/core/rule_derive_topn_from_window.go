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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner/util"
)

// deriveTopNFromWindow pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type deriveTopNFromWindow struct {
}

func appendDerivedTopNTrace(topN LogicalPlan, opt *logicalOptimizeOp) {
	child := topN.Children()[0]
	action := func() string {
		return fmt.Sprintf("%v_%v top N added below  %v_%v ", topN.TP(), topN.ID(), child.TP(), child.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v filter on row number", topN.TP())
	}
	opt.appendStepToCurrent(topN.ID(), topN.TP(), reason, action)
}

/*
		Check the following pattern of filter over row number window function:
	  - Filter is simple condition of row_number < value or row_number <= value
	  - The window function is a simple row number
	  - With default frame: rows between current row and current row. Check is not necessary since
	    current row is only frame applicable to row number
	  - No partition
	  - Child is a data source.
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
	if len(windowColumns) != 1 || !(column.Equal(p.ctx, windowColumns[0])) {
		return false, 0
	}

	grandChild := child.Children()[0]
	_, isDataSource := grandChild.(*DataSource)
	if !isDataSource {
		return false, 0
	}
	if len(child.WindowFuncDescs) == 1 && child.WindowFuncDescs[0].Name == "row_number" && len(child.PartitionBy) == 0 &&
		child.Frame.Type == ast.Rows && child.Frame.Start.Type == ast.CurrentRow && child.Frame.End.Type == ast.CurrentRow {
		return true, uint64(limitValue)
	}
	return false, 0
}

func (s *deriveTopNFromWindow) optimize(_ context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	return p.deriveTopN(opt), nil
}

func (s *baseLogicalPlan) deriveTopN(opt *logicalOptimizeOp) LogicalPlan {
	p := s.self
	for i, child := range p.Children() {
		newChild := child.deriveTopN(opt)
		p.SetChild(i, newChild)
	}
	return p
}

func (s *LogicalSelection) deriveTopN(opt *logicalOptimizeOp) LogicalPlan {
	p := s.self.(*LogicalSelection)
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
		derivedTopN := LogicalTopN{Count: limitValue, ByItems: byItems}.Init(grandChild.ctx, grandChild.blockOffset)
		derivedTopN.SetChildren(grandChild)
		/* return datasource->topN->window */
		child.SetChildren(derivedTopN)
		appendDerivedTopNTrace(child, opt)
		return child
	}
	return p
}

func (*deriveTopNFromWindow) name() string {
	return "derive_topn_from_window"
}
