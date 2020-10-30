package core

import (
	"context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/util"
)

type resultsStabilizer struct {
}

func (rs *resultsStabilizer) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	pks, err := rs.extractPKs(lp)
	if err != nil {
		return nil, err
	}
	if _, err := rs.injectPKsToSort(lp, pks); err != nil {
		return nil, err
	}
	return lp, nil
}

func (rs *resultsStabilizer) injectPKsToSort(lp LogicalPlan, pks []*expression.Column) (bool, error) {
	switch x := lp.(type) {
	case *LogicalSort:
		for _, pk := range pks {
			redundant := false
			for _, col := range x.ByItems {
				if pk.Equal(nil, col.Expr) {
					redundant = true
					break
				}
			}
			if !redundant {
				x.ByItems = append(x.ByItems, &util.ByItems{Expr: pk})
			}
		}
		return true, nil
	case *LogicalSelection, *LogicalProjection:
		return rs.injectPKsToSort(lp.Children()[0], pks)
	}
	return false, nil
}

func (rs *resultsStabilizer) extractPKs(lp LogicalPlan) ([]*expression.Column, error) {
	pks := make([]*expression.Column, 0, 4)
	switch x := lp.(type) {
	case *DataSource:
		for _, col := range x.Schema().Columns {
			if x.tableInfo.PKIsHandle {
				pkCol := x.tableInfo.GetPkColInfo()
				if col.ID == pkCol.ID {
					pks = append(pks, col)
				}
			} else {
				// TODO
			}
		}
	default:
		for _, child := range lp.Children() {
			cols, err := rs.extractPKs(child)
			if err != nil {
				return nil, err
			}
			pks = append(pks, cols...)
		}
	}
	return pks, nil
}

func (rs *resultsStabilizer) name() string {
	return "stabilize_results"
}
