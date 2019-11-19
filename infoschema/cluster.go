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
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/types"
)

const (
	clusterTablePrefix = "TIDB_CLUSTER_"
)

// Cluster table list.
const (
	clusterTableSlowLog     = clusterTablePrefix + tableSlowLog
	clusterTableProcesslist = clusterTablePrefix + tableProcesslist
)

// memTableToClusterTableMap means add memory table to cluster table.
var memTableToClusterTableMap = map[string]struct{}{
	tableSlowLog:     {},
	tableProcesslist: {},
}

// clusterTableMap is the cluster table map.
// This will initialize in init function.
var clusterTableMap = map[string]struct{}{}

func init() {
	var clusterTableCols = columnInfo{"NODE_ID", mysql.TypeVarchar, 64, mysql.UnsignedFlag, nil, nil}
	for memTableName := range memTableToClusterTableMap {
		memTableCols := tableNameToColumns[memTableName]
		if len(memTableCols) == 0 {
			continue
		}
		cols := make([]columnInfo, 0, len(memTableCols)+1)
		cols = append(cols, memTableCols...)
		cols = append(cols, clusterTableCols)
		clusterTableName := strings.ToUpper(clusterTablePrefix + memTableName)
		tableNameToColumns[clusterTableName] = cols
		clusterTableMap[clusterTableName] = struct{}{}
	}
	// This is used for avoid circle import, use for memTableReader in TiDB RPC server.
	mocktikv.GetClusterMemTableRows = getClusterMemTableRows
	mocktikv.IsClusterTable = IsClusterTable
}

// IsClusterTable used to check whether the table is a cluster memory table.
func IsClusterTable(tableName string) bool {
	tableName = strings.ToUpper(tableName)
	_, ok := clusterTableMap[tableName]
	return ok
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
	return appendClusterColumnsToRows(rows), nil
}

func appendClusterColumnsToRows(rows [][]types.Datum) [][]types.Datum {
	nodeID := infosync.GetGlobalServerID()
	for i := range rows {
		rows[i] = append(rows[i], types.NewStringDatum("tidb"+strconv.FormatInt(nodeID, 10)))
	}
	return rows
}
