// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"go.uber.org/zap"

	tcontext "github.com/pingcap/tidb/dumpling/context"
)

func filterDatabases(tctx *tcontext.Context, conf *Config, databases []string) []string {
	tctx.L().Debug("start to filter databases")
	newDatabases := make([]string, 0, len(databases))
	ignoreDatabases := make([]string, 0, len(databases))
	for _, database := range databases {
		if conf.TableFilter.MatchSchema(database) {
			newDatabases = append(newDatabases, database)
		} else {
			ignoreDatabases = append(ignoreDatabases, database)
		}
	}
	if len(ignoreDatabases) > 0 {
		tctx.L().Debug("ignore database", zap.Strings("databases", ignoreDatabases))
	}
	return newDatabases
}

func filterTables(tctx *tcontext.Context, conf *Config) {
	filterTablesFunc(tctx, conf, conf.TableFilter.MatchTable)
}

func filterTablesFunc(tctx *tcontext.Context, conf *Config, matchTable func(string, string) bool) {
	tctx.L().Debug("start to filter tables")
	dbTables := DatabaseTables{}
	ignoredDBTable := DatabaseTables{}

	for dbName, tables := range conf.Tables {
		for _, table := range tables {
			if matchTable(dbName, table.Name) {
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
		tctx.L().Debug("ignore table", zap.String("tables", ignoredDBTable.Literal()))
	}

	conf.Tables = dbTables
}
