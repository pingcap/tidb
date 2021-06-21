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
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sem"
)

// Cluster table list, attention:
// 1. the table name should be upper case.
// 2. clusterTableName should equal to "CLUSTER_" + memTableTableName.
const (
	// ClusterTableSlowLog is the string constant of cluster slow query memory table.
	ClusterTableSlowLog     = "CLUSTER_SLOW_QUERY"
	ClusterTableProcesslist = "CLUSTER_PROCESSLIST"
	// ClusterTableStatementsSummary is the string constant of cluster statement summary table.
	ClusterTableStatementsSummary = "CLUSTER_STATEMENTS_SUMMARY"
	// ClusterTableStatementsSummaryHistory is the string constant of cluster statement summary history table.
	ClusterTableStatementsSummaryHistory = "CLUSTER_STATEMENTS_SUMMARY_HISTORY"
	// ClusterTableStatementsSummaryEvicted is the string constant of cluster statement summary evict table.
	ClusterTableStatementsSummaryEvicted = "CLUSTER_STATEMENTS_SUMMARY_EVICTED"
	// ClusterTableTiDBTrx is the string constant of cluster transaction running table.
	ClusterTableTiDBTrx = "CLUSTER_TIDB_TRX"
	// ClusterTableDeadlocks is the string constant of cluster dead lock table.
	ClusterTableDeadlocks = "CLUSTER_DEADLOCKS"
)

// memTableToClusterTables means add memory table to cluster table.
var memTableToClusterTables = map[string]string{
	TableSlowQuery:                ClusterTableSlowLog,
	TableProcesslist:              ClusterTableProcesslist,
	TableStatementsSummary:        ClusterTableStatementsSummary,
	TableStatementsSummaryHistory: ClusterTableStatementsSummaryHistory,
	TableStatementsSummaryEvicted: ClusterTableStatementsSummaryEvicted,
	TableTiDBTrx:                  ClusterTableTiDBTrx,
	TableDeadlocks:                ClusterTableDeadlocks,
}

func init() {
	var addrCol = columnInfo{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64}
	for memTableName, clusterMemTableName := range memTableToClusterTables {
		memTableCols := tableNameToColumns[memTableName]
		if len(memTableCols) == 0 {
			continue
		}
		cols := make([]columnInfo, 0, len(memTableCols)+1)
		cols = append(cols, addrCol)
		cols = append(cols, memTableCols...)
		tableNameToColumns[clusterMemTableName] = cols
	}
}

// isClusterTableByName used to check whether the table is a cluster memory table.
func isClusterTableByName(dbName, tableName string) bool {
	dbName = strings.ToUpper(dbName)
	switch dbName {
	case util.InformationSchemaName.O, util.PerformanceSchemaName.O:
		break
	default:
		return false
	}
	tableName = strings.ToUpper(tableName)
	for _, name := range memTableToClusterTables {
		name = strings.ToUpper(name)
		if name == tableName {
			return true
		}
	}
	return false
}

// AppendHostInfoToRows appends host info to the rows.
func AppendHostInfoToRows(ctx sessionctx.Context, rows [][]types.Datum) ([][]types.Datum, error) {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return nil, err
	}
	addr := serverInfo.IP + ":" + strconv.FormatUint(uint64(serverInfo.StatusPort), 10)
	if sem.IsEnabled() {
		checker := privilege.GetPrivilegeManager(ctx)
		if checker == nil || !checker.RequestDynamicVerification(ctx.GetSessionVars().ActiveRoles, "RESTRICTED_TABLES_ADMIN", false) {
			addr = serverInfo.ID
		}
	}
	for i := range rows {
		row := make([]types.Datum, 0, len(rows[i])+1)
		row = append(row, types.NewStringDatum(addr))
		row = append(row, rows[i]...)
		rows[i] = row
	}
	return rows, nil
}
