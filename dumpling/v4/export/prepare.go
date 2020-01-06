package export

import (
	"database/sql"
	"errors"
	"strings"
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

type ConsistencyController interface {
	Setup() error
	TearDown() error
}

type ConsistencyNone struct{}

func (c *ConsistencyNone) Setup() error {
	return nil
}

func (c *ConsistencyNone) TearDown() error {
	return nil
}

type ConsistencyFlushTableWithReadLock struct {
	serverType ServerType
	db         *sql.DB
}

func (c *ConsistencyFlushTableWithReadLock) Setup() error {
	if c.serverType == ServerTypeTiDB {
		return withStack(errors.New("'flush table with read lock' cannot be used to ensure the consistency in TiDB"))
	}
	return FlushTableWithReadLock(c.db)
}

func (c *ConsistencyFlushTableWithReadLock) TearDown() error {
	err := c.db.Ping()
	if err != nil {
		return withStack(errors.New("ConsistencyFlushTableWithReadLock lost database connection"))
	}
	return UnlockTables(c.db)
}

type ConsistencyLockDumpingTables struct {
	db        *sql.DB
	allTables DatabaseTables
}

func (c *ConsistencyLockDumpingTables) Setup() error {
	for dbName, tables := range c.allTables {
		for _, table := range tables {
			err := LockTables(c.db, dbName, table)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *ConsistencyLockDumpingTables) TearDown() error {
	err := c.db.Ping()
	if err != nil {
		return withStack(errors.New("ConsistencyLockDumpingTables lost database connection"))
	}
	return UnlockTables(c.db)
}

type ConsistencySnapshot struct {
	serverType ServerType
	snapshot   string
	db         *sql.DB
}

const showMasterStatusFieldNum = 5
const snapshotFieldIndex = 1

func (c *ConsistencySnapshot) Setup() error {
	if c.serverType != ServerTypeTiDB {
		return withStack(errors.New("snapshot consistency is not supported for this server"))
	}
	if c.snapshot == "" {
		str, err := ShowMasterStatus(c.db, showMasterStatusFieldNum)
		if err != nil {
			return err
		}
		c.snapshot = str[snapshotFieldIndex]
	}
	return SetTiDBSnapshot(c.db, c.snapshot)
}

func (c *ConsistencySnapshot) TearDown() error {
	return nil
}
