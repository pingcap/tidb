package infoschema

import (
	"github.com/pingcap/parser/mysql"
	"strings"
)

const (
	tableTiKVInfo         = "TIKV_SERVER_STATS_INFO_CLUSTER"
	tableTiKVNetStatsInfo = "TIKV_SERVER_NET_STATS_INFO_CLUSTER"
)

// register for tikv memory tables;
var tikvMemTableMap = map[string]struct{}{
	tableTiKVInfo:         {},
	tableTiKVNetStatsInfo: {},
}

func IsTiKVMemTable(tableName string) bool {
	tableName = strings.ToUpper(tableName)
	_, ok := tikvMemTableMap[tableName]
	return ok
}

var tikvInfoCols = []columnInfo{
	{"IP", mysql.TypeVarchar, 64, 0, nil, nil},
	{"CPU_USAGE", mysql.TypeDouble, 22, 0, nil, nil},
	{"MEM_USAGE", mysql.TypeDouble, 22, 0, nil, nil},
	{"NODE_ID", mysql.TypeVarchar, 64, 0, nil, nil},
}

var tikvNetStatsInfoCols = []columnInfo{
	{"IP", mysql.TypeVarchar, 64, 0, nil, nil},
	{"NETCARD_NAME", mysql.TypeVarchar, 64, 0, nil, nil},
	{"BYTES_SENT", mysql.TypeLonglong, 21, 0, nil, nil},
	{"BYTES_RECV", mysql.TypeLonglong, 21, 0, nil, nil},
	{"NODE_ID", mysql.TypeVarchar, 64, 0, nil, nil},
}
