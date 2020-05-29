package export

import (
	"github.com/pingcap/dumpling/v4/log"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"
)

func filterDirtySchemaTables(conf *Config) {
	switch conf.ServerInfo.ServerType {
	case ServerTypeTiDB:
		if conf.Sql == "" {
			for dbName := range conf.Tables {
				if filter.IsSystemSchema(dbName) {
					log.Warn("unsupported dump schema in TiDB now", zap.String("schema", dbName))
					delete(conf.Tables, dbName)
				}
			}
		}
	}
}

func filterTables(conf *Config) {
	log.Debug("filter tables")
	// filter dirty schema tables because of non-impedance implementation reasons
	filterDirtySchemaTables(conf)
	dbTables := DatabaseTables{}
	ignoredDBTable := DatabaseTables{}

	for dbName, tables := range conf.Tables {
		for _, table := range tables {
			if conf.TableFilter.MatchTable(dbName, table.Name) {
				dbTables.AppendTable(dbName, table)
			} else {
				ignoredDBTable.AppendTable(dbName, table)
			}
		}
		// 1. this dbName doesn't match black white list, don't add
		// 2. this dbName matches black white list, but there is no table in this database, add
		if _, ok := dbTables[dbName]; !ok && conf.TableFilter.MatchSchema(dbName) {
			dbTables[dbName] = make([]*TableInfo, 0)
		}
	}

	if len(ignoredDBTable) > 0 {
		log.Debug("ignore table", zap.String("", ignoredDBTable.Literal()))
	}

	conf.Tables = dbTables
}
