package export

import (
	"context"
	"database/sql"

	"github.com/pingcap/errors"
)

const (
	consistencyTypeAuto     = "auto"
	consistencyTypeFlush    = "flush"
	consistencyTypeLock     = "lock"
	consistencyTypeSnapshot = "snapshot"
	consistencyTypeNone     = "none"
)

func NewConsistencyController(ctx context.Context, conf *Config, session *sql.DB) (ConsistencyController, error) {
	resolveAutoConsistency(conf)
	conn, err := session.Conn(ctx)
	if err != nil {
		return nil, err
	}
	switch conf.Consistency {
	case consistencyTypeFlush:
		return &ConsistencyFlushTableWithReadLock{
			serverType: conf.ServerInfo.ServerType,
			conn:       conn,
		}, nil
	case consistencyTypeLock:
		return &ConsistencyLockDumpingTables{
			conn:      conn,
			allTables: conf.Tables,
		}, nil
	case consistencyTypeSnapshot:
		if conf.ServerInfo.ServerType != ServerTypeTiDB {
			return nil, errors.New("snapshot consistency is not supported for this server")
		}
		return &ConsistencyNone{}, nil
	case consistencyTypeNone:
		return &ConsistencyNone{}, nil
	default:
		return nil, errors.Errorf("invalid consistency option %s", conf.Consistency)
	}
}

type ConsistencyController interface {
	Setup(context.Context) error
	TearDown(context.Context) error
	PingContext(context.Context) error
}

type ConsistencyNone struct{}

func (c *ConsistencyNone) Setup(_ context.Context) error {
	return nil
}

func (c *ConsistencyNone) TearDown(_ context.Context) error {
	return nil
}

func (c *ConsistencyNone) PingContext(_ context.Context) error {
	return nil
}

type ConsistencyFlushTableWithReadLock struct {
	serverType ServerType
	conn       *sql.Conn
}

func (c *ConsistencyFlushTableWithReadLock) Setup(ctx context.Context) error {
	if c.serverType == ServerTypeTiDB {
		return errors.New("'flush table with read lock' cannot be used to ensure the consistency in TiDB")
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

func (c *ConsistencyFlushTableWithReadLock) PingContext(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("consistency connection has already been closed!")
	}
	return c.conn.PingContext(ctx)
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

func (c *ConsistencyLockDumpingTables) PingContext(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("consistency connection has already been closed!")
	}
	return c.conn.PingContext(ctx)
}

const snapshotFieldIndex = 1

func resolveAutoConsistency(conf *Config) {
	if conf.Consistency != consistencyTypeAuto {
		return
	}
	switch conf.ServerInfo.ServerType {
	case ServerTypeTiDB:
		conf.Consistency = consistencyTypeSnapshot
	case ServerTypeMySQL, ServerTypeMariaDB:
		conf.Consistency = consistencyTypeFlush
	default:
		conf.Consistency = consistencyTypeNone
	}
}
