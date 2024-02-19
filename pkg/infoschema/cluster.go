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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	"net"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sem"
)

// Cluster table indicates that these tables need to get data from other tidb nodes, which may get from all other nodes, or may get from the ddl owner.
// Cluster table list, attention:
// 1. the table name should be upper case.
// 2. For tables that need to get data from all other TiDB nodes, clusterTableName should equal to "CLUSTER_" + memTableTableName.
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
	// ClusterTableDeadlocks is the string constant of cluster transaction summary table.
	ClusterTableTrxSummary = "CLUSTER_TRX_SUMMARY"
	// ClusterTableMemoryUsage is the memory usage status of tidb cluster.
	ClusterTableMemoryUsage = "CLUSTER_MEMORY_USAGE"
	// ClusterTableMemoryUsageOpsHistory is the memory control operators history of tidb cluster.
	ClusterTableMemoryUsageOpsHistory = "CLUSTER_MEMORY_USAGE_OPS_HISTORY"
	// ClusterTableTiDBIndexUsage is a table to show the usage stats of indexes across the whole cluster.
	ClusterTableTiDBIndexUsage = "CLUSTER_TIDB_INDEX_USAGE"
)

// memTableToAllTiDBClusterTables means add memory table to cluster table that will send cop request to all TiDB nodes.
var memTableToAllTiDBClusterTables = map[string]string{
	TableSlowQuery:                ClusterTableSlowLog,
	TableProcesslist:              ClusterTableProcesslist,
	TableStatementsSummary:        ClusterTableStatementsSummary,
	TableStatementsSummaryHistory: ClusterTableStatementsSummaryHistory,
	TableStatementsSummaryEvicted: ClusterTableStatementsSummaryEvicted,
	TableTiDBTrx:                  ClusterTableTiDBTrx,
	TableDeadlocks:                ClusterTableDeadlocks,
	TableTrxSummary:               ClusterTableTrxSummary,
	TableMemoryUsage:              ClusterTableMemoryUsage,
	TableMemoryUsageOpsHistory:    ClusterTableMemoryUsageOpsHistory,
	TableTiDBIndexUsage:           ClusterTableTiDBIndexUsage,
}

// memTableToDDLOwnerClusterTables means add memory table to cluster table that will send cop request to DDL owner node.
var memTableToDDLOwnerClusterTables = map[string]string{
	TableTiFlashReplica: TableTiFlashReplica,
}

// ClusterTableCopDestination means the destination that cluster tables will send cop requests to.
type ClusterTableCopDestination int

const (
	// AllTiDB is uese by CLUSTER_* table, means that these tables will send cop request to all TiDB nodes.
	AllTiDB ClusterTableCopDestination = iota
	// DDLOwner is uese by tiflash_replica currently, means that this table will send cop request to DDL owner node.
	DDLOwner
)

// GetClusterTableCopDestination gets cluster table cop request destination.
func GetClusterTableCopDestination(tableName string) ClusterTableCopDestination {
	if _, exist := memTableToDDLOwnerClusterTables[strings.ToUpper(tableName)]; exist {
		return DDLOwner
	}
	return AllTiDB
}

func init() {
	var addrCol = columnInfo{name: util.ClusterTableInstanceColumnName, tp: mysql.TypeVarchar, size: 64}
	for memTableName, clusterMemTableName := range memTableToAllTiDBClusterTables {
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
		tableName = strings.ToUpper(tableName)
		for _, name := range memTableToAllTiDBClusterTables {
			name = strings.ToUpper(name)
			if name == tableName {
				return true
			}
		}
		for _, name := range memTableToDDLOwnerClusterTables {
			name = strings.ToUpper(name)
			if name == tableName {
				return true
			}
		}
	default:
	}
	return false
}

// AppendHostInfoToRows appends host info to the rows.
func AppendHostInfoToRows(ctx sessionctx.Context, rows [][]types.Datum) ([][]types.Datum, error) {
	addr, err := GetInstanceAddr(ctx)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		row := make([]types.Datum, 0, len(rows[i])+1)
		row = append(row, types.NewStringDatum(addr))
		row = append(row, rows[i]...)
		rows[i] = row
	}
	return rows, nil
}

// GetInstanceAddr gets the instance address.
func GetInstanceAddr(ctx sessionctx.Context) (string, error) {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return "", err
	}
	addr := net.JoinHostPort(serverInfo.IP, strconv.FormatUint(uint64(serverInfo.StatusPort), 10))
	if sem.IsEnabled() {
		checker := privilege.GetPrivilegeManager(ctx)
		if checker == nil || !checker.RequestDynamicVerification(ctx.GetSessionVars().ActiveRoles, "RESTRICTED_TABLES_ADMIN", false) {
			addr = serverInfo.ID
		}
	}
	return addr, nil
}
