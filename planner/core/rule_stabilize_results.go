package core

import (
	"context"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/util"
)

type resultsStabilizer struct {
}

func (rs *resultsStabilizer) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	stable := rs.stabilizeSort(lp)
	if !stable {
		lp = rs.injectSort(lp)
	}
	return lp, nil
}

func (rs *resultsStabilizer) stabilizeSort(lp LogicalPlan) bool {
	switch x := lp.(type) {
	case *LogicalSort:
		cols := rs.extractHandleCols(x.Children()[0])
		for _, col := range cols {
			exist := false
			for _, byItem := range x.ByItems {
				if col.Equal(nil, byItem.Expr) {
					exist = true
					break
				}
			}
			if !exist {
				x.ByItems = append(x.ByItems, &util.ByItems{Expr: col})
			}
		}
		return true
	case *LogicalSelection, *LogicalProjection, *LogicalLimit:
		return rs.stabilizeSort(lp.Children()[0])
	}
	return false
}

func (rs *resultsStabilizer) injectSort(lp LogicalPlan) LogicalPlan {
	switch lp.(type) {
	case *LogicalSelection, *LogicalProjection, *LogicalLimit:
		newChild := rs.injectSort(lp.Children()[0])
		lp.SetChildren(newChild)
		return lp
	default:
		byItems := make([]*util.ByItems, 0, len(lp.Schema().Columns))
		cols := rs.extractHandleCols(lp.Children()[0])
		for _, col := range cols {
			byItems = append(byItems, &util.ByItems{Expr: col})
		}
		sort := LogicalSort{
			ByItems: byItems,
		}.Init(lp.SCtx(), lp.SelectBlockOffset())
		sort.SetChildren(lp)
		return sort
	}
}

// extractHandleCols does the best effort to get handle columns from this plan.
func (rs *resultsStabilizer) extractHandleCols(lp LogicalPlan) []*expression.Column {
	switch x := lp.(type) {
	case *LogicalSelection, *LogicalLimit:
		return rs.extractHandleCols(lp.Children()[0])
	case *DataSource:
		if x.handleCol != nil {
			return []*expression.Column{x.handleCol}
		}
	}
	return lp.Schema().Columns
}

func (rs *resultsStabilizer) name() string {
	return "stabilize_results"
}
