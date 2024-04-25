package core

import (
	"encoding/json"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/statistics"
)

func col2tbl(fullColName string) string {
	tmp := strings.Split(strings.ToLower(fullColName), ".")
	return tmp[0] + "." + tmp[1]
}

type UnityColumnInfo struct {
	NDV int
}

type UnityIndexInfo struct {
	NDV int
}

type UnityTableInfo struct {
	AsName       string
	Columns      map[string]*UnityColumnInfo // db.table.col
	Indexes      map[string]*UnityIndexInfo  // db.table.index
	RealtimeRows int64
	ModifiedRows int64

	stats  *statistics.Table `json:"-"`
	col2id map[string]int64  `json:"-"`
}

func collectColumn(c *expression.Column, result map[string]*UnityTableInfo) {
	colName := strings.ToLower(c.OrigName)
	result[col2tbl(colName)].Columns[colName] = &UnityColumnInfo{}
}

func collectColumnFromExpr(expr expression.Expression, result map[string]*UnityTableInfo) {
	switch x := expr.(type) {
	case *expression.Column:
		collectColumn(x, result)
	case *expression.CorrelatedColumn:
		collectColumn(&x.Column, result)
	case *expression.ScalarFunction:
		for _, arg := range x.GetArgs() {
			collectColumnFromExpr(arg, result)
		}
	}
}

func collectUnityInfo(p base.LogicalPlan, result map[string]*UnityTableInfo) {
	for _, child := range p.Children() {
		collectUnityInfo(child, result)
	}
	switch x := p.(type) {
	case *DataSource:
		tableName := x.DBName.L + "." + x.tableInfo.Name.L
		if _, ok := result[tableName]; !ok {
			result[tableName] = &UnityTableInfo{
				AsName:  x.TableAsName.L,
				Columns: map[string]*UnityColumnInfo{},
				Indexes: map[string]*UnityIndexInfo{},
				stats:   x.statisticTable,
			}
			for _, col := range x.tableInfo.Columns {
				colName := tableName + "." + col.Name.L
				result[tableName].col2id[colName] = col.ID
			}
		}
		for _, expr := range x.allConds {
			collectColumnFromExpr(expr, result)
		}
		for _, idx := range x.tableInfo.Indices {
			idxName := tableName + "." + idx.Name.L
			result[tableName].Indexes[idxName] = &UnityIndexInfo{}
		}
		if x.tableInfo.PKIsHandle || x.tableInfo.IsCommonHandle {
			result[tableName].Indexes[tableName+".primary"] = &UnityIndexInfo{}
		}
	case *LogicalSelection:
		for _, expr := range x.Conditions {
			collectColumnFromExpr(expr, result)
		}
	case *LogicalJoin:
		for _, expr := range x.EqualConditions {
			collectColumnFromExpr(expr, result)
		}
		for _, expr := range x.NAEQConditions {
			collectColumnFromExpr(expr, result)
		}
		for _, expr := range x.LeftConditions {
			collectColumnFromExpr(expr, result)
		}
		for _, expr := range x.RightConditions {
			collectColumnFromExpr(expr, result)
		}
		for _, expr := range x.OtherConditions {
			collectColumnFromExpr(expr, result)
		}
	case *LogicalAggregation:
		for _, expr := range x.GroupByItems {
			collectColumnFromExpr(expr, result)
		}
	case *LogicalSort:
		for _, item := range x.ByItems {
			collectColumnFromExpr(item.Expr, result)
		}
	default:
	}
}

func fillUpStats(result map[string]*UnityTableInfo) {
	for _, tblInfo := range result {
		tblStats := tblInfo.stats
		tblInfo.ModifiedRows = tblStats.ModifyCount
		tblInfo.RealtimeRows = tblStats.RealtimeCount
		for colName, col := range tblInfo.Columns {
			col.NDV = int(tblStats.Columns[tblInfo.col2id[colName]].NDV)
		}
	}
}

func prepareForUnity(p base.LogicalPlan) string {
	result := make(map[string]*UnityTableInfo)
	collectUnityInfo(p, result)
	fillUpStats(result)

	v, err := json.Marshal(result)
	must(err)
	return string(v)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
