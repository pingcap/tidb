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
		err := UseDatabase(db, dbName)
		if err != nil {
			return nil, err
		}
		tables, err := ShowTables(db)
		if err != nil {
			return nil, err
		}
		dbTables[dbName] = tables
	}
	return dbTables, nil
}
