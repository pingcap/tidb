package export

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func NewConsistencyController(ctx context.Context, conf *Config, session *sql.DB) (ConsistencyController, error) {
	resolveAutoConsistency(conf)
	conn, err := session.Conn(ctx)
	if err != nil {
		return nil, err
	}
	switch conf.Consistency {
	case "flush":
		return &ConsistencyFlushTableWithReadLock{
			serverType: conf.ServerInfo.ServerType,
			conn:       conn,
		}, nil
	case "lock":
		return &ConsistencyLockDumpingTables{
			conn:      conn,
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
	Setup(context.Context) error
	TearDown(context.Context) error
}

type ConsistencyNone struct{}

func (c *ConsistencyNone) Setup(_ context.Context) error {
	return nil
}

func (c *ConsistencyNone) TearDown(_ context.Context) error {
	return nil
}

type ConsistencyFlushTableWithReadLock struct {
	serverType ServerType
	conn       *sql.Conn
}

func (c *ConsistencyFlushTableWithReadLock) Setup(ctx context.Context) error {
	if c.serverType == ServerTypeTiDB {
		return withStack(errors.New("'flush table with read lock' cannot be used to ensure the consistency in TiDB"))
	}
	return FlushTableWithReadLock(ctx, c.conn)
}

func (c *ConsistencyFlushTableWithReadLock) TearDown(ctx context.Context) error {
	if c.conn == nil {
		return nil
	}
	defer func() {
		c.conn.Close()
		c.conn = nil
	}()
	return UnlockTables(ctx, c.conn)
}

type ConsistencyLockDumpingTables struct {
	conn      *sql.Conn
	allTables DatabaseTables
}

func (c *ConsistencyLockDumpingTables) Setup(ctx context.Context) error {
	for dbName, tables := range c.allTables {
		for _, table := range tables {
			err := LockTables(ctx, c.conn, dbName, table.Name)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *ConsistencyLockDumpingTables) TearDown(ctx context.Context) error {
	if c.conn == nil {
		return nil
	}
	defer func() {
		c.conn.Close()
		c.conn = nil
	}()
	return UnlockTables(ctx, c.conn)
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
