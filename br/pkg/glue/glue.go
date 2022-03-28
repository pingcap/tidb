// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import (
	"context"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	pd "github.com/tikv/pd/client"
)

// Glue is an abstraction of TiDB function calls used in BR.
type Glue interface {
	GetDomain(store kv.Storage) (*domain.Domain, error)
	CreateSession(store kv.Storage) (Session, error)
	Open(path string, option pd.SecurityOption) (kv.Storage, error)

	// OwnsStorage returns whether the storage returned by Open() is owned
	// If this method returns false, the connection manager will never close the storage.
	OwnsStorage() bool

	StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) Progress

	// Record records some information useful for log-less summary.
	Record(name string, value uint64)

	// GetVersion gets BR package version to run backup/restore job
	GetVersion() string
}

// Session is an abstraction of the session.Session interface.
type Session interface {
	Execute(ctx context.Context, sql string) error
	ExecuteInternal(ctx context.Context, sql string, args ...interface{}) error
	CreateDatabase(ctx context.Context, schema *model.DBInfo) error
	CreateTable(ctx context.Context, dbName model.CIStr, table *model.TableInfo) error
	CreatePlacementPolicy(ctx context.Context, policy *model.PolicyInfo) error
	Close()
	GetGlobalVariable(name string) (string, error)
}

// BatchCreateTableSession is an interface to batch create table parallelly
type BatchCreateTableSession interface {
	CreateTables(ctx context.Context, tables map[string][]*model.TableInfo) error
}

// Progress is an interface recording the current execution progress.
type Progress interface {
	// Inc increases the progress. This method must be goroutine-safe, and can
	// be called from any goroutine.
	Inc()
	// Close marks the progress as 100% complete and that Inc() can no longer be
	// called.
	Close()
}
