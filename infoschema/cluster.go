package infoschema

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/stmtsummary"
	"strings"
)

const clusterTableSuffix = "_CLUSTER"

const TableNameEventsStatementsSummaryByDigestUpper = "EVENTS_STATEMENTS_SUMMARY_BY_DIGEST"

// Cluster table list.
const (
	clusterTableSlowLog                             = tableSlowLog + clusterTableSuffix
	clusterTableProcesslist                         = tableProcesslist + clusterTableSuffix
	clusterTableNameEventsStatementsSummaryByDigest = TableNameEventsStatementsSummaryByDigestUpper + clusterTableSuffix
	clusterTableServerVariable                      = "SERVER_VARIABLES" + clusterTableSuffix
	clusterTableTiDBConfig                          = tableTiDBConfig + clusterTableSuffix
)

// cluster table columns
var (
	clusterSlowQueryCols       []columnInfo
	clusterProcesslistCols     []columnInfo
	clusterServerVarCols       []columnInfo
	clusterTableTiDBConfigCols []columnInfo
)

// register for cluster memory tables;
var clusterTableMap = map[string]struct{}{
	clusterTableSlowLog:                             {},
	clusterTableProcesslist:                         {},
	clusterTableNameEventsStatementsSummaryByDigest: {},
	clusterTableServerVariable:                      {},
	clusterTableTiDBConfig:                          {},
}

var clusterTableCols = []columnInfo{
	{"TiDB_ID", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
}

func init() {
	// Slow query
	clusterSlowQueryCols = append(clusterSlowQueryCols, slowQueryCols...)
	clusterSlowQueryCols = append(clusterSlowQueryCols, clusterTableCols...)
	// ProcessList
	clusterProcesslistCols = append(clusterProcesslistCols, tableProcesslistCols...)
	clusterProcesslistCols = append(clusterProcesslistCols, clusterTableCols...)

	// Register Server_variables
	clusterServerVarCols = append(clusterServerVarCols, sessionVarCols...)
	clusterServerVarCols = append(clusterServerVarCols, clusterTableCols...)

	// TiDB_CONFIG
	clusterTableTiDBConfigCols = append(clusterTableTiDBConfigCols, tableTiDBConfigCols...)
	clusterTableTiDBConfigCols = append(clusterTableTiDBConfigCols, clusterTableCols...)

	// Register tidb_mem_cluster information_schema tables.
	tableNameToColumns[clusterTableSlowLog] = clusterSlowQueryCols
	tableNameToColumns[clusterTableProcesslist] = clusterProcesslistCols

	// Register tikv mem_table to information_schema tables.
	tableNameToColumns[tableTiKVInfo] = tikvInfoCols

	// Register Server_variables
	tableNameToColumns[clusterTableServerVariable] = clusterServerVarCols

	// Register TiDB_CONFIG
	tableNameToColumns[clusterTableTiDBConfig] = clusterTableTiDBConfigCols
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
	case clusterTableServerVariable:
		rows, err = dataForServerVar(ctx)
	case clusterTableTiDBConfig:
		rows, err = dataForClusterConfigInfo()
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

func dataForClusterConfigInfo() ([][]types.Datum, error) {
	rows, err := dataForConfigInfo()
	if err != nil {
		return nil, err
	}
	return appendClusterColumnsToRows(rows), nil
}

func dataForServerVar(ctx sessionctx.Context) (rows [][]types.Datum, err error) {
	sessionVars := ctx.GetSessionVars()
	for name := range variable.ServerVariableMap {
		var value string
		value, err = variable.GetSessionSystemVar(sessionVars, name)
		if err != nil {
			return nil, err
		}
		row := types.MakeDatums(name, value)
		rows = append(rows, row)
	}
	return appendClusterColumnsToRows(rows), nil
}

func appendClusterColumnsToRows(rows [][]types.Datum) [][]types.Datum {
	for i := range rows {
		rows[i] = append(rows[i], types.NewUintDatum(uint64(infosync.GetGlobalServerID())))
	}
	return rows
}
