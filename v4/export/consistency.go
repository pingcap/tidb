package export

import (
	"database/sql"
	"errors"
	"fmt"
)

func NewConsistencyController(conf *Config, session *sql.DB) (ConsistencyController, error) {
	resolveAutoConsistency(conf)
	switch conf.Consistency {
	case "flush":
		return &ConsistencyFlushTableWithReadLock{
			serverType: conf.ServerInfo.ServerType,
			db:         session,
		}, nil
	case "lock":
		return &ConsistencyLockDumpingTables{
			db:        session,
			allTables: conf.Tables,
		}, nil
	case "snapshot":
		if conf.ServerInfo.ServerType != ServerTypeTiDB {
			return nil, withStack(errors.New("snapshot consistency is not supported for this server"))
		}
		return &ConsistencyNone{}, nil
	case "none":
		return &ConsistencyNone{}, nil
	default:
		return nil, withStack(fmt.Errorf("invalid consistency option %s", conf.Consistency))
	}
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
			err := LockTables(c.db, dbName, table.Name)
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

const showMasterStatusFieldNum = 5
const snapshotFieldIndex = 1

func resolveAutoConsistency(conf *Config) {
	if conf.Consistency != "auto" {
		return
	}
	switch conf.ServerInfo.ServerType {
	case ServerTypeTiDB:
		conf.Consistency = "snapshot"
	case ServerTypeMySQL, ServerTypeMariaDB:
		conf.Consistency = "flush"
	default:
		conf.Consistency = "none"
	}
}
