package export

import (
	"database/sql"
	"strings"

	"github.com/pingcap/dumpling/v4/log"
)

func detectServerInfo(db *sql.DB) (ServerInfo, error) {
	versionStr, err := SelectVersion(db)
	if err != nil {
		return ServerInfoUnknown, err
	}
	return ParseServerInfo(versionStr), nil
}

func prepareDumpingDatabases(conf *Config, db *sql.DB) ([]string, error) {
	if conf.Database == "" {
		return ShowDatabases(db)
	} else {
		return strings.Split(conf.Database, ","), nil
	}
}

func listAllTables(db *sql.DB, databaseNames []string) (DatabaseTables, error) {
	log.Zap().Debug("list all the tables")
	dbTables := DatabaseTables{}
	for _, dbName := range databaseNames {
		tables, err := ListAllTables(db, dbName)
		if err != nil {
			return nil, err
		}
		dbTables = dbTables.AppendTables(dbName, tables...)
	}
	return dbTables, nil
}

func listAllViews(db *sql.DB, databaseNames []string) (DatabaseTables, error) {
	log.Zap().Debug("list all the views")
	dbTables := DatabaseTables{}
	for _, dbName := range databaseNames {
		views, err := ListAllViews(db, dbName)
		if err != nil {
			return nil, err
		}
		dbTables = dbTables.AppendViews(dbName, views...)
	}
	return dbTables, nil
}

type databaseName = string

type TableType int8

const (
	TableTypeBase TableType = iota
	TableTypeView
)

type TableInfo struct {
	Name string
	Type TableType
}

func (t *TableInfo) Equals(other *TableInfo) bool {
	return t.Name == other.Name && t.Type == other.Type
}

type DatabaseTables map[databaseName][]*TableInfo

func NewDatabaseTables() DatabaseTables {
	return DatabaseTables{}
}

func (d DatabaseTables) AppendTables(dbName string, tableNames ...string) DatabaseTables {
	for _, t := range tableNames {
		d[dbName] = append(d[dbName], &TableInfo{t, TableTypeBase})
	}
	return d
}

func (d DatabaseTables) AppendViews(dbName string, viewNames ...string) DatabaseTables {
	for _, v := range viewNames {
		d[dbName] = append(d[dbName], &TableInfo{v, TableTypeView})
	}
	return d
}

func (d DatabaseTables) Merge(other DatabaseTables) {
	for name, infos := range other {
		d[name] = append(d[name], infos...)
	}
}
