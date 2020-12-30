// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

func filterTables(conf *Config) {
	log.Debug("filter tables")
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
		// 1. this dbName doesn't match block allow list, don't add
		// 2. this dbName matches block allow list, but there is no table in this database, add
		if conf.DumpEmptyDatabase {
			if _, ok := dbTables[dbName]; !ok && conf.TableFilter.MatchSchema(dbName) {
				dbTables[dbName] = make([]*TableInfo, 0)
			}
		}
	}

	if len(ignoredDBTable) > 0 {
		log.Debug("ignore table", zap.String("tables", ignoredDBTable.Literal()))
	}

	conf.Tables = dbTables
}
