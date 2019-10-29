// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	"strconv"
	"strings"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/stmtsummary"
)

const (
	clusterTableSuffix                            = "_CLUSTER"
	TableNameEventsStatementsSummaryByDigestUpper = "EVENTS_STATEMENTS_SUMMARY_BY_DIGEST"
)

// Cluster table list.
const (
	clusterTableSlowLog                             = tableSlowLog + clusterTableSuffix
	clusterTableProcesslist                         = tableProcesslist + clusterTableSuffix
	clusterTableNameEventsStatementsSummaryByDigest = TableNameEventsStatementsSummaryByDigestUpper + clusterTableSuffix
)

// register for cluster memory tables;
var clusterTableMap = map[string]struct{}{
	clusterTableSlowLog:                             {},
	clusterTableProcesslist:                         {},
	clusterTableNameEventsStatementsSummaryByDigest: {},
}

var clusterTableCols = columnInfo{"NODE_ID", mysql.TypeVarchar, 64, mysql.UnsignedFlag, nil, nil}

func init() {
	for tableName := range clusterTableMap {
		memTableName := tableName[:len(tableName)-len(clusterTableSuffix)]
		memTableCols := tableNameToColumns[memTableName]
		if len(memTableCols) == 0 {
			continue
		}
		cols := make([]columnInfo, 0, len(memTableCols)+1)
		cols = append(cols, memTableCols...)
		cols = append(cols, clusterTableCols)
		tableNameToColumns[tableName] = cols
	}
	// Use for memTableReader in mpp.
	// This is used for avoid circle import.
	mocktikv.GetClusterMemTableRows = GetClusterMemTableRows
	mocktikv.IsClusterTable = IsClusterTable
}

func IsClusterTable(tableName string) bool {
	tableName = strings.ToUpper(tableName)
	_, ok := clusterTableMap[tableName]
	return ok
}

func GetClusterMemTableRows(ctx sessionctx.Context, tableName string) (rows [][]types.Datum, err error) {
	tableName = strings.ToUpper(tableName)
	switch tableName {
	case clusterTableSlowLog:
		rows, err = dataForClusterSlowLog(ctx)
	case clusterTableProcesslist:
		rows, err = dataForClusterProcesslist(ctx)
	case clusterTableNameEventsStatementsSummaryByDigest:
		rows = dataForClusterTableNameEventsStatementsSummaryByDigest()
	}
	return rows, err
}

func dataForClusterSlowLog(ctx sessionctx.Context) ([][]types.Datum, error) {
	rows, err := dataForSlowLog(ctx)
	if err != nil {
		return nil, err
	}
	return appendClusterColumnsToRows(rows), nil
}

func dataForClusterProcesslist(ctx sessionctx.Context) ([][]types.Datum, error) {
	rows := dataForProcesslist(ctx)
	return appendClusterColumnsToRows(rows), nil
}

func dataForClusterTableNameEventsStatementsSummaryByDigest() [][]types.Datum {
	rows := stmtsummary.StmtSummaryByDigestMap.ToDatum()
	return appendClusterColumnsToRows(rows)
}

func appendClusterColumnsToRows(rows [][]types.Datum) [][]types.Datum {
	nodeID := infosync.GetGlobalServerID()
	for i := range rows {
		rows[i] = append(rows[i], types.NewStringDatum("tidb"+strconv.FormatInt(nodeID, 10)))
	}
	return rows
}
