package stats

import (
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func deriveStats4LogicalTableScan(ts *logicalop.LogicalTableScan) (_ *property.StatsInfo, _ bool, err error) {
	InitStats(ts.Source)
	ts.SetStats(DeriveStatsByFilter(ts.Source, ts.AccessConds, nil))
	// ts.Handle could be nil if PK is Handle, and PK column has been pruned.
	// TODO: support clustered index.
	if ts.HandleCols != nil {
		// TODO: restrict mem usage of table ranges.
		ts.Ranges, _, _, err = ranger.BuildTableRange(ts.AccessConds, ts.SCtx().GetRangerCtx(), ts.HandleCols.GetCol(0).RetType, 0)
	} else {
		isUnsigned := false
		if ts.Source.TableInfo.PKIsHandle {
			if pkColInfo := ts.Source.TableInfo.GetPkColInfo(); pkColInfo != nil {
				isUnsigned = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			}
		}
		ts.Ranges = ranger.FullIntRange(isUnsigned)
	}
	if err != nil {
		return nil, false, err
	}
	return ts.StatsInfo(), true, nil
}
