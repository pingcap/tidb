package core

import (
	"context"

	"github.com/pingcap/tidb/planner/util"
)

type resultsStabilizer struct {
}

func (rs *resultsStabilizer) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	ok, err := rs.stabilizeSort(lp)
	if err != nil {
		return nil, err
	}
	if !ok {
		byItems := make([]*util.ByItems, 0, len(lp.Schema().Columns))
		for _, col := range lp.Schema().Columns {
			byItems = append(byItems, &util.ByItems{Expr: col})
		}
		sort := LogicalSort{
			ByItems: byItems,
		}.Init(lp.SCtx(), lp.SelectBlockOffset())
		sort.SetChildren(lp)
		lp = sort
	}
	return lp, nil
}

func (rs *resultsStabilizer) stabilizeSort(lp LogicalPlan) (bool, error) {
	switch x := lp.(type) {
	case *LogicalSort:
		for _, col := range x.Schema().Columns {
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
		return true, nil
	case *LogicalSelection, *LogicalProjection:
		return rs.stabilizeSort(lp.Children()[0])
	}
	return false, nil
}

func (rs *resultsStabilizer) name() string {
	return "stabilize_results"
}
