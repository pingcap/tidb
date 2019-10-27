package infoschema

import (
	"github.com/pingcap/parser/mysql"
	"strings"
)

const (
	tableServerTiKVInfo   = "TIKV_SERVER_STATS_INFO_CLUSTER"
	tableTiKVNetStatsInfo = "TIKV_SERVER_NET_STATS_INFO_CLUSTER"
	tableTiKVInfo         = "TIKV_INFOS"
)

// register for tikv memory tables;
var tikvMemTableMap = map[string]struct{}{
	tableServerTiKVInfo:   {},
	tableTiKVNetStatsInfo: {},
	tableTiKVInfo:         {},
}

func IsTiKVMemTable(tableName string) bool {
	tableName = strings.ToUpper(tableName)
	_, ok := tikvMemTableMap[tableName]
	return ok
}

var tikvServerInfoCols = []columnInfo{
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

var tikvInfoCols = []columnInfo{
	{"CPU_USAGE", mysql.TypeDouble, 22, 0, nil, nil},
	{"NETWORK_IN", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{"NETWORK_OUT", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{"TOTAL_MEMORY", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{"USED_MEMORY", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
}
