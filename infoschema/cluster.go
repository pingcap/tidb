package infoschema

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/stmtsummary"
	"strconv"
	"strings"
)

const clusterTableSuffix = "_CLUSTER"

const TableNameEventsStatementsSummaryByDigestUpper = "EVENTS_STATEMENTS_SUMMARY_BY_DIGEST"

// Cluster table list.
const (
	clusterTableSlowLog                             = tableSlowLog + clusterTableSuffix
	clusterTableProcesslist                         = tableProcesslist + clusterTableSuffix
	clusterTableNameEventsStatementsSummaryByDigest = TableNameEventsStatementsSummaryByDigestUpper + clusterTableSuffix
)

// cluster table columns
var (
	clusterSlowQueryCols   []columnInfo
	clusterProcesslistCols []columnInfo
)

// register for cluster memory tables;
var clusterTableMap = map[string]struct{}{
	clusterTableSlowLog:                             {},
	clusterTableProcesslist:                         {},
	clusterTableNameEventsStatementsSummaryByDigest: {},
}

var clusterTableCols = []columnInfo{
	{"NODE_ID", mysql.TypeVarchar, 64, mysql.UnsignedFlag, nil, nil},
}

func init() {
	// Slow query
	clusterSlowQueryCols = append(clusterSlowQueryCols, slowQueryCols...)
	clusterSlowQueryCols = append(clusterSlowQueryCols, clusterTableCols...)
	// ProcessList
	clusterProcesslistCols = append(clusterProcesslistCols, tableProcesslistCols...)
	clusterProcesslistCols = append(clusterProcesslistCols, clusterTableCols...)

	registerTables()

	mocktikv.GetClusterMemTableRows = GetClusterMemTableRows
	mocktikv.IsClusterTable = IsClusterTable
}

func registerTables() {
	// Register tidb_mem_cluster information_schema tables.
	tableNameToColumns[clusterTableSlowLog] = clusterSlowQueryCols
	tableNameToColumns[clusterTableProcesslist] = clusterProcesslistCols
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
	for i := range rows {
		rows[i] = append(rows[i], types.NewStringDatum("tidb"+strconv.FormatInt(infosync.GetGlobalServerID(), 10)))
	}
	return rows
}
