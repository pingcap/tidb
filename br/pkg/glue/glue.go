// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import (
	"context"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
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

	// UseOneShotSession temporary creates session from store when run backup job.
	// because we don't have to own domain/session during the whole backup.
	// we can close domain as soon as possible.
	// and we must reuse the exists session and don't close it in SQL backup job.
	UseOneShotSession(store kv.Storage, closeDomain bool, fn func(se Session) error) error
}

// Session is an abstraction of the session.Session interface.
type Session interface {
	Execute(ctx context.Context, sql string) error
	ExecuteInternal(ctx context.Context, sql string, args ...any) error
	CreateDatabase(ctx context.Context, schema *model.DBInfo) error
	CreateTable(ctx context.Context, dbName model.CIStr, table *model.TableInfo,
		cs ...ddl.CreateTableWithInfoConfigurier) error
	CreatePlacementPolicy(ctx context.Context, policy *model.PolicyInfo) error
	Close()
	GetGlobalVariable(name string) (string, error)
	GetSessionCtx() sessionctx.Context
}

// BatchCreateTableSession is an interface to batch create table parallelly
type BatchCreateTableSession interface {
	CreateTables(ctx context.Context, tables map[string][]*model.TableInfo,
		cs ...ddl.CreateTableWithInfoConfigurier) error
}

// Progress is an interface recording the current execution progress.
type Progress interface {
	// Inc increases the progress. This method must be goroutine-safe, and can
	// be called from any goroutine.
	Inc()
	// IncBy increases the progress by cnt. This method must be goroutine-safe, and can
	// be called from any goroutine.
	IncBy(cnt int64)
	// GetCurrent reports the progress.
	GetCurrent() int64
	// Close marks the progress as 100% complete and that Inc() can no longer be
	// called.
	Close()
}
