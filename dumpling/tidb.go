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
	"time"
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
	"github.com/pingcap/tidb/optimizer"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/stmt/stmts"
	"github.com/pingcap/tidb/store/hbase"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/boltdb"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

// Engine prefix name
const (
	EngineGoLevelDBMemory     = "memory://"
	EngineGoLevelDBPersistent = "goleveldb://"
	EngineBoltDB              = "boltdb://"
	EngineHBase               = "hbase://"
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
	d, err = domain.NewDomain(store, lease)
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

	// SchemaLease is the time(seconds) for re-updating remote schema.
	// In online DDL, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schema.
	// Default schema lease time is 300 seconds, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	schemaLease = 300 * time.Second
)

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	schemaLease = lease
}

// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and collation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// collation_connection is important for comparisons of literal strings.
// For comparisons of strings with column values, collation_connection does not matter because columns
// have their own collation, which has a higher collation precedence.
// See: https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
func getCtxCharsetInfo(ctx context.Context) (string, string) {
	sessionVars := variable.GetSessionVars(ctx)
	charset := sessionVars.Systems["character_set_connection"]
	collation := sessionVars.Systems["collation_connection"]
	return charset, collation
}

// Compile is safe for concurrent use by multiple goroutines.
func Compile(ctx context.Context, src string) ([]stmt.Statement, error) {
	log.Debug("compiling", src)
	l := parser.NewLexer(src)
	l.SetCharsetInfo(getCtxCharsetInfo(ctx))
	if parser.YYParse(l) != 0 {
		log.Warnf("compiling %s, error: %v", src, l.Errors()[0])
		return nil, errors.Trace(l.Errors()[0])
	}
	rawStmt := l.Stmts()
	stmts := make([]stmt.Statement, len(rawStmt))
	for i, v := range rawStmt {
		compiler := &optimizer.Compiler{}
		stm, err := compiler.Compile(v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		stmts[i] = stm
	}
	return stmts, nil
}

// CompilePrepare compiles prepared statement, allows placeholder as expr.
// The return values are compiled statement, parameter list and error.
func CompilePrepare(ctx context.Context, src string) (stmt.Statement, []*expression.ParamMarker, error) {
	log.Debug("compiling prepared", src)
	l := parser.NewLexer(src)
	l.SetCharsetInfo(getCtxCharsetInfo(ctx))
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
	compiler := &optimizer.Compiler{}
	statement, err := compiler.Compile(sm)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return statement, compiler.ParamMarkers(), nil
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
	se := ctx.(*session)
	if isPreparedStmt(s) {
		ps := s.(*stmts.PreparedStmt)
		se.history.add(ps.ID, s)
	} else {
		se.history.add(0, s)
	}
	// MySQL DDL should be auto-commit
	if err == nil && (s.IsDDL() || autocommit.ShouldAutocommit(ctx)) {
		err = ctx.FinishTxn(false)
	}
	return rs, errors.Trace(err)
}

func runExecute(ctx context.Context, es *stmts.ExecuteStmt, args ...interface{}) (rset.Recordset, error) {
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
	stmt, params, err := CompilePrepare(ctx, SQLText)
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
	RegisterLocalStore("boltdb", boltdb.Driver{})
	RegisterStore("hbase", hbasekv.Driver{})

	// start pprof handlers
	if Debug {
		go http.ListenAndServe(PprofAddr, nil)
	}
}
