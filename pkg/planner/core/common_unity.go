package core

import (
	"encoding/json"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

func col2tbl(fullColName string) string {
	tmp := strings.Split(strings.ToLower(fullColName), ".")
	return tmp[0] + "." + tmp[1]
}

type UnityTableInfo struct {
	AsName  string
	Columns map[string]bool
}

func collectColumn(c *expression.Column, result map[string]UnityTableInfo) {
	colName := strings.ToLower(c.OrigName)
	result[col2tbl(colName)].Columns[colName] = true
}

func collectColumnFromExpr(expr expression.Expression, result map[string]UnityTableInfo) {
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

func prepareUnityInfo(p base.PhysicalPlan, result map[string]UnityTableInfo) {
	switch x := p.(type) {
	case *PhysicalTableReader:
		prepareUnityInfo(x.tablePlan, result)
	case *PhysicalIndexReader:
		prepareUnityInfo(x.indexPlan, result)
	case *PhysicalIndexLookUpReader:
		prepareUnityInfo(x.tablePlan, result)
		prepareUnityInfo(x.indexPlan, result)
	case *PhysicalIndexMergeReader:
		prepareUnityInfo(x.tablePlan, result)
		for _, indexPlan := range x.partialPlans {
			prepareUnityInfo(indexPlan, result)
		}
	}

	for _, child := range p.Children() {
		prepareUnityInfo(child, result)
	}
	switch x := p.(type) {
	case *PhysicalTableScan:
		tableName := x.DBName.L + "." + x.Table.Name.L
		if _, ok := result[tableName]; !ok {
			result[tableName] = UnityTableInfo{AsName: x.TableAsName.L, Columns: map[string]bool{}}
		}

		for _, expr := range x.filterCondition {
			collectColumnFromExpr(expr, result)
		}
		for _, expr := range x.AccessCondition {
			collectColumnFromExpr(expr, result)
		}
	case *PhysicalIndexScan:
		tableName := x.DBName.L + "." + x.Table.Name.L
		if _, ok := result[tableName]; !ok {
			result[tableName] = UnityTableInfo{AsName: x.TableAsName.L, Columns: map[string]bool{}}
		}

		for _, expr := range x.AccessCondition {
			collectColumnFromExpr(expr, result)
		}
	case *PhysicalSelection:
		for _, expr := range x.Conditions {
			collectColumnFromExpr(expr, result)
		}
	case *PhysicalHashJoin:
		for _, expr := range x.EqualConditions {
			collectColumnFromExpr(expr, result)
		}
	case *PhysicalMergeJoin:
	case *PhysicalIndexJoin:
	default:
	}
}

func prepareForUnity(p base.PhysicalPlan) string {
	result := make(map[string]UnityTableInfo)
	prepareUnityInfo(p, result)

	v, err := json.Marshal(result)
	must(err)
	return string(v)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
