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
	{"CPU", mysql.TypeDouble, 22, 0, nil, nil},
	{"MEM", mysql.TypeDouble, 22, 0, nil, nil},
	{"NET", mysql.TypeDouble, 22, 0, nil, nil},
	{"DISK", mysql.TypeDouble, 22, 0, nil, nil},
	{"OTHER", mysql.TypeVarchar, 64, 0, nil, nil},
}
