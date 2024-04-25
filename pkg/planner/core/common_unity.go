package core

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/planner/core/base"
)

type UnityTableName struct {
	SchemaName string `json:"schemaName"`
	TableName  string `json:"tableName"`
}

type UnityColumnName struct {
	UnityTableName
	ColumnName string `json:"columnName"`
}

type UnityColumnInfo struct {
}

type UnityTableInfo struct {
	AsName  string                              `json:"asName"`
	Columns map[UnityColumnName]UnityColumnInfo `json:"columns"`
}

func prepareUnityInfo(p base.PhysicalPlan, result map[UnityTableName]UnityTableInfo) {
	switch x := p.(type) {
	case *PhysicalTableScan:
		tableName := UnityTableName{SchemaName: x.DBName.O, TableName: x.Table.Name.O}
		tableInfo := UnityTableInfo{
			AsName: x.TableAsName.O,
		}
		result[tableName] = tableInfo
	default:
	}

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
}

func prepareForUnity(p base.PhysicalPlan) string {
	result := make(map[UnityTableName]UnityTableInfo)
	prepareUnityInfo(p, result)
	return fmt.Sprintf("%v", result)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
