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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
)

// Cluster table list, attention:
// 1. the table name should be upper case.
// 2. clusterTableName should equal to "CLUSTER_" + memTableTableName.
const (
	// ClusterTableSlowLog is the string constant of cluster slow query memory table.
	ClusterTableSlowLog     = "CLUSTER_SLOW_QUERY"
	ClusterTableProcesslist = "CLUSTER_PROCESSLIST"
)

// memTableToClusterTables means add memory table to cluster table.
var memTableToClusterTables = map[string]string{
	TableSlowQuery:   ClusterTableSlowLog,
	TableProcesslist: ClusterTableProcesslist,
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
func AppendHostInfoToRows(rows [][]types.Datum) ([][]types.Datum, error) {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return nil, err
	}
	addr := serverInfo.IP + ":" + strconv.FormatUint(uint64(serverInfo.StatusPort), 10)
	for i := range rows {
		row := make([]types.Datum, 0, len(rows[i])+1)
		row = append(row, types.NewStringDatum(addr))
		row = append(row, rows[i]...)
		rows[i] = row
	}
	return rows, nil
}
