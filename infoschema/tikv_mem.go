package infoschema

import (
	"github.com/pingcap/parser/mysql"
	"strings"
)

const (
	tableTiKVInfo = "TIKV_INFOS"
)

// register for tikv memory tables;
var tikvMemTableMap = map[string]struct{}{
	tableTiKVInfo: {},
}

func IsTiKVMemTable(tableName string) bool {
	tableName = strings.ToUpper(tableName)
	_, ok := tikvMemTableMap[tableName]
	return ok
}

var tikvInfoCols = []columnInfo{
	{"CPU_USAGE", mysql.TypeDouble, 22, 0, nil, nil},
	{"NETWORK_IN", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{"NETWORK_OUT", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{"TOTAL_MEMORY", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{"USED_MEMORY", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
}
