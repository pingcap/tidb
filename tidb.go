// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tidb

import (
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
)

// Engine prefix name
const (
	EngineGoLevelDBMemory        = "memory://"
	defaultMaxRetries            = 30
	retryInterval         uint64 = 500
)

type domainMap struct {
	domains map[string]*domain.Domain
	mu      sync.Mutex
}

func (dm *domainMap) Get(store kv.Storage) (d *domain.Domain, err error) {
	key := store.UUID()
	dm.mu.Lock()
	defer dm.mu.Unlock()
	d = dm.domains[key]
	if d != nil {
		return
	}

	lease := time.Duration(0)
	if !localstore.IsLocalStore(store) {
		lease = schemaLease
	}
	err = util.RunWithRetry(defaultMaxRetries, retryInterval, func() (retry bool, err1 error) {
		log.Infof("store %v new domain, lease %v", store.UUID(), lease)
		d, err1 = domain.NewDomain(store, lease)
		return true, errors.Trace(err1)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	dm.domains[key] = d

	return
}

func (dm *domainMap) Delete(store kv.Storage) {
	dm.mu.Lock()
	delete(dm.domains, store.UUID())
	dm.mu.Unlock()
}

var (
	domap = &domainMap{
		domains: map[string]*domain.Domain{},
	}
	stores = make(map[string]kv.Driver)
	// store.UUID()-> IfBootstrapped
	storeBootstrapped = make(map[string]bool)

	// schemaLease is the time for re-updating remote schema.
	// In online DDL, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schema.
	// Default schema lease time is 1 second, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	// For production, you should set a big schema lease, like 300s+.
	schemaLease = 1 * time.Second

	// The maximum number of retries to recover from retryable errors.
	commitRetryLimit = 10
)

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	schemaLease = lease
}

// SetCommitRetryLimit setups the maximum number of retries when trying to recover
// from retryable errors.
// Retryable errors are generally refer to temporary errors that are expected to be
// reinstated by retry, including network interruption, transaction conflicts, and
// so on.
func SetCommitRetryLimit(limit int) {
	commitRetryLimit = limit
}

// Parse parses a query string to raw ast.StmtNode.
func Parse(ctx context.Context, src string) ([]ast.StmtNode, error) {
	log.Debug("compiling", src)
	charset, collation := ctx.GetSessionVars().GetCharsetInfo()
	p := parser.New()
	p.SetSQLMode(ctx.GetSessionVars().SQLMode)
	stmts, err := p.Parse(src, charset, collation)
	if err != nil {
		log.Warnf("compiling %s, error: %v", src, err)
		return nil, errors.Trace(err)
	}
	return stmts, nil
}

// Before every execution, we must clear statement context.
func resetStmtCtx(ctx context.Context, s ast.StmtNode) {
	sessVars := ctx.GetSessionVars()
	sc := new(variable.StatementContext)
	switch s.(type) {
	case *ast.UpdateStmt, *ast.InsertStmt, *ast.DeleteStmt:
		sc.IgnoreTruncate = false
		sc.TruncateAsWarning = !sessVars.StrictSQLMode
		if _, ok := s.(*ast.InsertStmt); !ok {
			sc.InUpdateOrDeleteStmt = true
		}
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		// Make sure the sql_mode is strict when checking column default value.
		sc.IgnoreTruncate = false
		sc.TruncateAsWarning = false
	default:
		sc.IgnoreTruncate = true
		if show, ok := s.(*ast.ShowStmt); ok {
			if show.Tp == ast.ShowWarnings {
				sc.InShowWarning = true
				sc.SetWarnings(sessVars.StmtCtx.GetWarnings())
			}
		}
	}
	if sessVars.LastInsertID > 0 {
		sessVars.PrevLastInsertID = sessVars.LastInsertID
		sessVars.LastInsertID = 0
	}
	sessVars.InsertID = 0
	sessVars.StmtCtx = sc
}

// Compile is safe for concurrent use by multiple goroutines.
func Compile(ctx context.Context, rawStmt ast.StmtNode) (ast.Statement, error) {
	compiler := executor.Compiler{}
	st, err := compiler.Compile(ctx, rawStmt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return st, nil
}

// runStmt executes the ast.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, s ast.Statement) (ast.RecordSet, error) {
	var err error
	var rs ast.RecordSet
	se := ctx.(*session)
	rs, err = s.Exec(ctx)
	// All the history should be added here.
	getHistory(ctx).add(0, s, se.sessionVars.StmtCtx)
	if !se.sessionVars.InTxn() {
		if err != nil {
			log.Info("RollbackTxn for ddl/autocommit error.")
			se.RollbackTxn()
		} else {
			err = se.CommitTxn()
		}
	}
	return rs, errors.Trace(err)
}

func getHistory(ctx context.Context) *stmtHistory {
	hist, ok := ctx.GetSessionVars().TxnCtx.Histroy.(*stmtHistory)
	if ok {
		return hist
	}
	hist = new(stmtHistory)
	ctx.GetSessionVars().TxnCtx.Histroy = hist
	return hist
}

// GetRows gets all the rows from a RecordSet.
func GetRows(rs ast.RecordSet) ([][]types.Datum, error) {
	if rs == nil {
		return nil, nil
	}
	var rows [][]types.Datum
	defer rs.Close()
	// Negative limit means no limit.
	for {
		row, err := rs.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		rows = append(rows, row.Data)
	}
	return rows, nil
}

// RegisterStore registers a kv storage with unique name and its associated Driver.
func RegisterStore(name string, driver kv.Driver) error {
	name = strings.ToLower(name)

	if _, ok := stores[name]; ok {
		return errors.Errorf("%s is already registered", name)
	}

	stores[name] = driver
	return nil
}

// RegisterLocalStore registers a local kv storage with unique name and its associated engine Driver.
func RegisterLocalStore(name string, driver engine.Driver) error {
	d := localstore.Driver{Driver: driver}
	return RegisterStore(name, d)
}

// NewStore creates a kv Storage with path.
//
// The path must be a URL format 'engine://path?params' like the one for
// tidb.Open() but with the dbname cut off.
// Examples:
//    goleveldb://relative/path
//    boltdb:///absolute/path
//
// The engine should be registered before creating storage.
func NewStore(path string) (kv.Storage, error) {
	return newStoreWithRetry(path, defaultMaxRetries)
}

func newStoreWithRetry(path string, maxRetries int) (kv.Storage, error) {
	url, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	name := strings.ToLower(url.Scheme)
	d, ok := stores[name]
	if !ok {
		return nil, errors.Errorf("invalid uri format, storage %s is not registered", name)
	}

	var s kv.Storage
	util.RunWithRetry(maxRetries, retryInterval, func() (bool, error) {
		s, err = d.Open(path)
		return kv.IsRetryableError(err), err
	})
	return s, errors.Trace(err)
}

var queryStmtTable = []string{"explain", "select", "show", "execute", "describe", "desc", "admin"}

func trimSQL(sql string) string {
	// Trim space.
	sql = strings.TrimSpace(sql)
	// Trim leading /*comment*/
	// There may be multiple comments
	for strings.HasPrefix(sql, "/*") {
		i := strings.Index(sql, "*/")
		if i != -1 && i < len(sql)+1 {
			sql = sql[i+2:]
			sql = strings.TrimSpace(sql)
			continue
		}
		break
	}
	// Trim leading '('. For `(select 1);` is also a query.
	return strings.TrimLeft(sql, "( ")
}

// IsQuery checks if a sql statement is a query statement.
func IsQuery(sql string) bool {
	sqlText := strings.ToLower(trimSQL(sql))
	for _, key := range queryStmtTable {
		if strings.HasPrefix(sqlText, key) {
			return true
		}
	}

	return false
}

func init() {
	// Register default memory and goleveldb storage
	RegisterLocalStore("memory", goleveldb.MemoryDriver{})
	RegisterLocalStore("goleveldb", goleveldb.Driver{})
}
