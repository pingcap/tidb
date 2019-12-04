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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
)

// Cluster table list, attention:
// 1. the table name should be upper case.
// 2. clusterTableName should equal to "CLUSTER_" + memTableTableName.
const (
	clusterTableSlowLog     = "CLUSTER_SLOW_QUERY"
	clusterTableProcesslist = "CLUSTER_PROCESSLIST"
)

// memTableToClusterTables means add memory table to cluster table.
var memTableToClusterTables = map[string]string{
	tableSlowLog:     clusterTableSlowLog,
	tableProcesslist: clusterTableProcesslist,
}

func init() {
	var addrCol = columnInfo{"ADDRESS", mysql.TypeVarchar, 64, 0, nil, nil}
	for memTableName, clusterMemTableName := range memTableToClusterTables {
		memTableCols := tableNameToColumns[memTableName]
		if len(memTableCols) == 0 {
			continue
		}
		cols := make([]columnInfo, 0, len(memTableCols)+1)
		cols = append(cols, memTableCols...)
		cols = append(cols, addrCol)
		tableNameToColumns[clusterMemTableName] = cols
	}
}

// isClusterTableByName used to check whether the table is a cluster memory table.
func isClusterTableByName(dbName, tableName string) bool {
	dbName = strings.ToUpper(dbName)
	switch dbName {
	case util.InformationSchemaName, util.PerformanceSchemaName:
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

func getClusterMemTableRows(ctx sessionctx.Context, tableName string) (rows [][]types.Datum, err error) {
	tableName = strings.ToUpper(tableName)
	switch tableName {
	case clusterTableSlowLog:
		rows, err = dataForSlowLog(ctx)
	case clusterTableProcesslist:
		rows = dataForProcesslist(ctx)
	default:
		err = errors.Errorf("unknown cluster table: %v", tableName)
	}
	if err != nil {
		return nil, err
	}
	return appendHostInfoToRows(rows)
}

func appendHostInfoToRows(rows [][]types.Datum) ([][]types.Datum, error) {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return nil, err
	}
	addr := serverInfo.IP + ":" + strconv.FormatUint(uint64(serverInfo.StatusPort), 10)
	for i := range rows {
		rows[i] = append(rows[i], types.NewStringDatum(addr))
	}
	return rows, nil
}
