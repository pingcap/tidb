package export

import (
	"database/sql"
	"strings"

	"github.com/pingcap/dumpling/v4/log"
)

func adjustConfig(conf *Config) error {
	// Init logger
	if conf.Logger != nil {
		log.SetAppLogger(conf.Logger)
	} else {
		err := log.InitAppLogger(&log.Config{Level: conf.LogLevel})
		if err != nil {
			return err
		}
	}

	if conf.Rows != UnspecifiedSize {
		// Disable filesize if rows was set
		conf.FileSize = UnspecifiedSize
	}

	return nil
}

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
	log.Debug("list all the tables")
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
	log.Debug("list all the views")
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

func (d DatabaseTables) AppendTable(dbName string, table *TableInfo) DatabaseTables {
	d[dbName] = append(d[dbName], table)
	return d
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

func (d DatabaseTables) Literal() string {
	var b strings.Builder
	b.WriteString("tables list\n")
	b.WriteString("\n")

	for dbName, tables := range d {
		b.WriteString("schema ")
		b.WriteString(dbName)
		b.WriteString(" :[")
		for _, tbl := range tables {
			b.WriteString(tbl.Name)
			b.WriteString(", ")
		}
		b.WriteString("]")
	}

	return b.String()
}
