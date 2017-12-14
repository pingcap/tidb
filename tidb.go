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
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

	ddlLease := time.Duration(0)
	statisticLease := time.Duration(0)
	ddlLease = schemaLease
	statisticLease = statsLease
	err = util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (retry bool, err1 error) {
		log.Infof("store %v new domain, ddl lease %v, stats lease %d", store.UUID(), ddlLease, statisticLease)
		factory := createSessionFunc(store)
		sysFactory := createSessionWithDomainFunc(store)
		d = domain.NewDomain(store, ddlLease, statisticLease, factory)
		err1 = d.Init(ddlLease, sysFactory)
		if err1 != nil {
			// If we don't clean it, there are some dirty data when retrying the function of Init.
			d.Close()
			log.Errorf("[ddl] init domain failed %v", errors.ErrorStack(errors.Trace(err1)))
		}
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

	// statsLease is the time for reload stats table.
	statsLease = 3 * time.Second

	// The maximum number of retries to recover from retryable errors.
	commitRetryLimit = 10
)

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	schemaLease = lease
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	statsLease = lease
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

// Compile is safe for concurrent use by multiple goroutines.
func Compile(goCtx goctx.Context, ctx context.Context, stmtNode ast.StmtNode) (ast.Statement, error) {
	compiler := executor.Compiler{Ctx: ctx}
	stmt, err := compiler.Compile(goCtx, stmtNode)
	return stmt, errors.Trace(err)
}

// runStmt executes the ast.Statement and commit or rollback the current transaction.
func runStmt(goCtx goctx.Context, ctx context.Context, s ast.Statement) (ast.RecordSet, error) {
	span, ctx1 := opentracing.StartSpanFromContext(goCtx, "runStmt")
	span.LogKV("sql", s.OriginText())
	defer span.Finish()

	var err error
	var rs ast.RecordSet
	se := ctx.(*session)
	rs, err = s.Exec(goCtx)
	span.SetTag("txn.id", se.sessionVars.TxnCtx.StartTS)
	// All the history should be added here.
	GetHistory(ctx).Add(0, s, se.sessionVars.StmtCtx)
	if !se.sessionVars.InTxn() {
		if err != nil {
			log.Info("RollbackTxn for ddl/autocommit error.")
			err1 := se.RollbackTxn(ctx1)
			terror.Log(errors.Trace(err1))
		} else {
			err = se.CommitTxn(ctx1)
		}
	}
	return rs, errors.Trace(err)
}

// GetHistory get all stmtHistory in current txn. Exported only for test.
func GetHistory(ctx context.Context) *StmtHistory {
	hist, ok := ctx.GetSessionVars().TxnCtx.Histroy.(*StmtHistory)
	if ok {
		return hist
	}
	hist = new(StmtHistory)
	ctx.GetSessionVars().TxnCtx.Histroy = hist
	return hist
}

// GetRows4Test gets all the rows from a RecordSet, only used for test.
func GetRows4Test(goCtx goctx.Context, rs ast.RecordSet) ([]types.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []types.Row
	if rs.SupportChunk() {
		for {
			// Since we collect all the rows, we can not reuse the chunk.
			chk := rs.NewChunk()
			err := rs.NextChunk(goCtx, chk)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if chk.NumRows() == 0 {
				break
			}
			for row := chk.Begin(); row != chk.End(); row = row.Next() {
				rows = append(rows, row)
			}
		}
	} else {
		for {
			row, err := rs.Next(goCtx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if row == nil {
				break
			}
			rows = append(rows, row)
		}
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
	return newStoreWithRetry(path, util.DefaultMaxRetries)
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
	err1 := util.RunWithRetry(maxRetries, util.RetryInterval, func() (bool, error) {
		log.Infof("new store")
		s, err = d.Open(path)
		return kv.IsRetryableError(err), err
	})
	return s, errors.Trace(err1)
}

// DialPumpClientWithRetry tries to dial to binlogSocket,
// if any error happens, it will try to re-dial,
// or return this error when timeout.
func DialPumpClientWithRetry(binlogSocket string, maxRetries int, dialerOpt grpc.DialOption) (*grpc.ClientConn, error) {
	var clientCon *grpc.ClientConn
	err := util.RunWithRetry(maxRetries, util.RetryInterval, func() (bool, error) {
		log.Infof("setup binlog client")
		var err error
		tlsConfig, err := config.GetGlobalConfig().Security.ToTLSConfig()
		if err != nil {
			log.Infof("error happen when setting binlog client: %s", errors.ErrorStack(err))
		}

		if tlsConfig != nil {
			clientCon, err = grpc.Dial(binlogSocket, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), dialerOpt)
		} else {
			clientCon, err = grpc.Dial(binlogSocket, grpc.WithInsecure(), dialerOpt)
		}

		if err != nil {
			log.Infof("error happen when setting binlog client: %s", errors.ErrorStack(err))
		}
		return true, errors.Trace(err)
	})
	return clientCon, errors.Trace(err)
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
}
