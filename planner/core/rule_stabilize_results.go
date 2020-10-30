package core

import (
	"context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/util"
)

type resultsStabilizer struct {
}

/*
	1. find the first Sort in the plan, return the original plan if there is no Sort;
	2. extract handleCols from DataSources and inject them into the Sort;
	3. create an new Projection upon the Sort;
*/
func (rs *resultsStabilizer) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	return rs.injectHandleColsIntoSort(lp)
}

func (rs *resultsStabilizer) injectHandleColsIntoSort(lp LogicalPlan) (LogicalPlan, error) {
	switch x := lp.(type) {
	case *LogicalSort:
		proj := LogicalProjection{Exprs: expression.Column2Exprs(lp.Schema().Columns)}.Init(lp.SCtx(), 0)
		handleCols, err := rs.extraHandleCols(lp)
		if err != nil {
			return nil, err
		}
		for _, hc := range handleCols {
			x.ByItems = append(x.ByItems, &util.ByItems{Expr: hc})
		}
		proj.SetChildren(x)
		return proj, nil
	case *LogicalSelection, *LogicalProjection:
		newChild, err := rs.injectHandleColsIntoSort(lp.Children()[0])
		if err != nil {
			return nil, err
		}
		lp.SetChild(0, newChild)
	}
	return lp, nil
}

func (rs *resultsStabilizer) extraHandleCols(lp LogicalPlan) ([]*expression.Column, error) {
	handleCols := make([]*expression.Column, 0, 2)
	switch x := lp.(type) {
	case *DataSource:
		handleCols = append(handleCols, x.handleCol)
	default:
		for _, child := range lp.Children() {
			cols, err := rs.extraHandleCols(child)
			if err != nil {
				return nil, err
			}
			handleCols = append(handleCols, cols...)
		}
		for _, hc := range handleCols {
			notExist := true
			for _, col := range lp.Schema().Columns {
				if col.Equal(nil, hc) {
					notExist = false
					break
				}
			}
			if notExist {
				lp.Schema().Columns = append(lp.Schema().Columns, hc)
			}
		}
	}
	return handleCols, nil
}

func (rs *resultsStabilizer) name() string {
	return "stabilize_results"
}
