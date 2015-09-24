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
	"net/http"
	// For pprof
	_ "net/http/pprof"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/stmt/stmts"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/boltdb"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

// Engine prefix name
const (
	EngineGoLevelDBMemory     = "memory://"
	EngineGoLevelDBPersistent = "goleveldb://"
	EngineHBase               = "zk://"
	EngineBoltDB              = "boltdb://"
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
	d, err = domain.NewDomain(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dm.domains[key] = d
	return
}

var (
	domap = &domainMap{
		domains: map[string]*domain.Domain{},
	}
	stores = make(map[string]kv.Driver)
	// Debug is the switch for pprof. If Debug is true, tidb will start with pprof on.
	Debug = true
	// PprofAddr is the pprof url.
	PprofAddr = "localhost:8888"
	// store.UUID()-> IfBootstrapped
	storeBootstrapped = make(map[string]bool)
)

// Compile is safe for concurrent use by multiple goroutines.
func Compile(src string) ([]stmt.Statement, error) {
	log.Debug("compiling", src)
	l := parser.NewLexer(src)
	if parser.YYParse(l) != 0 {
		log.Warnf("compiling %s, error: %v", src, l.Errors()[0])
		return nil, errors.Trace(l.Errors()[0])
	}

	return l.Stmts(), nil
}

// CompilePrepare compiles prepared statement, allows placeholder as expr.
// The return values are compiled statement, parameter list and error.
func CompilePrepare(src string) (stmt.Statement, []*expression.ParamMarker, error) {
	log.Debug("compiling prepared", src)
	l := parser.NewLexer(src)
	l.SetPrepare()
	if parser.YYParse(l) != 0 {
		log.Errorf("compiling %s\n, error: %v", src, l.Errors()[0])
		return nil, nil, errors.Trace(l.Errors()[0])
	}
	sms := l.Stmts()
	if len(sms) != 1 {
		log.Warnf("compiling %s, error: prepared statement should have only one statement.", src)
		return nil, nil, nil
	}
	sm := sms[0]
	return sm, l.ParamList, nil
}

func prepareStmt(ctx context.Context, sqlText string) (stmtID uint32, paramCount int, fields []*field.ResultField, err error) {
	s := &stmts.PreparedStmt{
		InPrepare: true,
		SQLText:   sqlText,
	}
	_, err = runStmt(ctx, s, nil)
	return s.ID, len(s.Params), s.Fields, errors.Trace(err)
}

func executePreparedStmt(ctx context.Context, stmtID uint32, args ...interface{}) (rset.Recordset, error) {
	s := &stmts.ExecuteStmt{ID: stmtID}
	return runStmt(ctx, s, args...)
}

func dropPreparedStmt(ctx context.Context, stmtID uint32) error {
	s := &stmts.DeallocateStmt{ID: stmtID}
	_, err := runStmt(ctx, s, nil)
	return err
}

func runStmt(ctx context.Context, s stmt.Statement, args ...interface{}) (rset.Recordset, error) {
	var err error
	var rs rset.Recordset
	// before every execution, we must clear affectedrows.
	variable.GetSessionVars(ctx).SetAffectedRows(0)
	switch s.(type) {
	case *stmts.PreparedStmt:
		ps := s.(*stmts.PreparedStmt)
		return runPreparedStmt(ctx, ps)
	case *stmts.ExecuteStmt:
		es := s.(*stmts.ExecuteStmt)
		rs, err = runExecute(ctx, es, args...)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		if s.IsDDL() {
			err = ctx.FinishTxn(false)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		stmt.BindExecArgs(ctx, args)
		rs, err = s.Exec(ctx)
		stmt.ClearExecArgs(ctx)
	}
	// MySQL DDL should be auto-commit
	if err == nil && (s.IsDDL() || variable.ShouldAutocommit(ctx)) {
		err = ctx.FinishTxn(false)
	}
	return rs, errors.Trace(err)
}

func runExecute(ctx context.Context, es *stmts.ExecuteStmt, args ...interface{}) (rset.Recordset, error) {
	// TODO: if the args are passed by binary protocol, we should set execute args from arg parameters into es.
	// Then call es.Exec(ctx)
	if len(args) > 0 {
		es.UsingVars = make([]expression.Expression, 0, len(args))
		for _, v := range args {
			var exp expression.Expression = &expression.Value{Val: v}
			es.UsingVars = append(es.UsingVars, exp)
		}
	}
	return es.Exec(ctx)
}

func runPreparedStmt(ctx context.Context, ps *stmts.PreparedStmt) (rset.Recordset, error) {
	SQLText, err := ps.GetSQL(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(SQLText) == 0 {
		// TODO: Empty sql should return error?
		return nil, nil
	}
	// compile SQLText
	stmt, params, err := CompilePrepare(SQLText)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ps.Params = params
	ps.SQLStmt = stmt
	rs, err := ps.Exec(ctx)
	return rs, errors.Trace(err)
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

// NewStore creates a kv Storage with specific uri.
// The uri format must be engine://schema, like goleveldb://testpath
// Engine is the storage name registered with RegisterStore.
// Schema is the storage specific format.
func NewStore(uri string) (kv.Storage, error) {
	pos := strings.Index(uri, "://")
	if pos == -1 {
		return nil, errors.Errorf("invalid uri format, must engine://schema")
	}

	name := strings.ToLower(uri[0:pos])
	schema := uri[pos+3:]

	d, ok := stores[name]
	if !ok {
		return nil, errors.Errorf("invalid uri foramt, storage %s is not registered", name)
	}

	s, err := d.Open(schema)
	return s, errors.Trace(err)
}

var queryStmtTable = []string{"explain", "select", "show", "execute", "describe", "desc"}

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
	return sql
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
	RegisterLocalStore("boltdb", boltdb.Driver{})

	// start pprof handlers
	if Debug {
		go http.ListenAndServe(PprofAddr, nil)
	}
}
