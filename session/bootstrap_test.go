// Copyright 2016 PingCAP, Inc.
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

package session

import (
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/testleak"
)

type testBootstrapSuite struct {
	dbName          string
	dbNameBootstrap string
}

func (s *testBootstrapSuite) SetUpSuite(c *C) {
	s.dbName = "test_bootstrap"
	s.dbNameBootstrap = "test_main_db_bootstrap"
}

func (s *testBootstrapSuite) TestBootstrap(c *C) {
	defer testleak.AfterTest(c)()
	store, dom := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()
	defer dom.Close()
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "USE mysql;")
	r := mustExecSQL(c, se, `select * from user;`)
	c.Assert(r, NotNil)
	ctx := context.Background()
	req := r.NewChunk()
	err := r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	datums := statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(c, datums, `%`, "root", []byte(""), "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "anyhost"}, []byte(""), []byte("")), IsTrue)
	mustExecSQL(c, se, "USE test;")
	// Check privilege tables.
	mustExecSQL(c, se, "SELECT * from mysql.global_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.db;")
	mustExecSQL(c, se, "SELECT * from mysql.tables_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.columns_priv;")
	// Check privilege tables.
	r = mustExecSQL(c, se, "SELECT COUNT(*) from mysql.global_variables;")
	c.Assert(r, NotNil)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.GetRow(0).GetInt64(0), Equals, globalVarsCount())

	// Check a storage operations are default autocommit after the second start.
	mustExecSQL(c, se, "USE test;")
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id int)")
	unsetStoreBootstrapped(store.UUID())
	se.Close()
	se, err = CreateSession4Test(store)
	c.Assert(err, IsNil)
	mustExecSQL(c, se, "USE test;")
	mustExecSQL(c, se, "insert t values (?)", 3)
	se, err = CreateSession4Test(store)
	c.Assert(err, IsNil)
	mustExecSQL(c, se, "USE test;")
	r = mustExecSQL(c, se, "select * from t")
	c.Assert(r, NotNil)

	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	datums = statistics.RowToDatums(req.GetRow(0), r.Fields())
	match(c, datums, 3)
	mustExecSQL(c, se, "drop table if exists t")
	se.Close()

	// Try to do bootstrap dml jobs on an already bootstraped TiDB system will not cause fatal.
	// For https://github.com/pingcap/tidb/issues/1096
	se, err = CreateSession4Test(store)
	c.Assert(err, IsNil)
	doDMLWorks(se)
}

func globalVarsCount() int64 {
	var count int64
	for _, v := range variable.SysVars {
		if v.Scope != variable.ScopeSession {
			count++
		}
	}
	return count
}

// bootstrapWithOnlyDDLWork creates a new session on store but only do ddl works.
func (s *testBootstrapSuite) bootstrapWithOnlyDDLWork(store kv.Storage, c *C) {
	ss := &session{
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	ss.txn.init()
	ss.mu.values = make(map[fmt.Stringer]interface{})
	ss.SetValue(sessionctx.Initing, true)
	dom, err := domap.Get(store)
	c.Assert(err, IsNil)
	domain.BindDomain(ss, dom)
	b, err := checkBootstrapped(ss)
	c.Assert(b, IsFalse)
	c.Assert(err, IsNil)
	doDDLWorks(ss)
	// Leave dml unfinished.
}

// testBootstrapWithError :
// When a session failed in bootstrap process (for example, the session is killed after doDDLWorks()).
// We should make sure that the following session could finish the bootstrap process.
func (s *testBootstrapSuite) TestBootstrapWithError(c *C) {
	ctx := context.Background()
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbNameBootstrap)
	defer store.Close()
	s.bootstrapWithOnlyDDLWork(store, c)
	dom, err := domap.Get(store)
	c.Assert(err, IsNil)
	domap.Delete(store)
	dom.Close()

	dom1, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom1.Close()

	se := newSession(c, store, s.dbNameBootstrap)
	mustExecSQL(c, se, "USE mysql;")
	r := mustExecSQL(c, se, `select * from user;`)
	req := r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	row := req.GetRow(0)
	datums := statistics.RowToDatums(row, r.Fields())
	match(c, datums, `%`, "root", []byte(""), "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y")
	c.Assert(r.Close(), IsNil)

	mustExecSQL(c, se, "USE test;")
	// Check privilege tables.
	mustExecSQL(c, se, "SELECT * from mysql.global_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.db;")
	mustExecSQL(c, se, "SELECT * from mysql.tables_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.columns_priv;")
	// Check role tables.
	mustExecSQL(c, se, "SELECT * from mysql.role_edges;")
	mustExecSQL(c, se, "SELECT * from mysql.default_roles;")
	// Check global variables.
	r = mustExecSQL(c, se, "SELECT COUNT(*) from mysql.global_variables;")
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	v := req.GetRow(0)
	c.Assert(v.GetInt64(0), Equals, globalVarsCount())
	c.Assert(r.Close(), IsNil)

	r = mustExecSQL(c, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="bootstrapped";`)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(row.GetBytes(0), BytesEquals, []byte("True"))
	c.Assert(r.Close(), IsNil)
}

// TestUpgrade tests upgrading
func (s *testBootstrapSuite) TestUpgrade(c *C) {
	ctx := context.Background()
	defer testleak.AfterTest(c)()
	store, _ := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "USE mysql;")

	// bootstrap with currentBootstrapVersion
	r := mustExecSQL(c, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version";`)
	req := r.NewChunk()
	err := r.Next(ctx, req)
	row := req.GetRow(0)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(row.GetBytes(0), BytesEquals, []byte(fmt.Sprintf("%d", currentBootstrapVersion)))
	c.Assert(r.Close(), IsNil)

	se1 := newSession(c, store, s.dbName)
	ver, err := getBootstrapVersion(se1)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(currentBootstrapVersion))

	// Do something to downgrade the store.
	// downgrade meta bootstrap version
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(1))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mustExecSQL(c, se1, `delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version";`)
	mustExecSQL(c, se1, fmt.Sprintf(`delete from mysql.global_variables where VARIABLE_NAME="%s";`,
		variable.TiDBDistSQLScanConcurrency))
	mustExecSQL(c, se1, `commit;`)
	unsetStoreBootstrapped(store.UUID())
	// Make sure the version is downgraded.
	r = mustExecSQL(c, se1, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version";`)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsTrue)
	c.Assert(r.Close(), IsNil)

	ver, err = getBootstrapVersion(se1)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(0))

	// Create a new session then upgrade() will run automatically.
	dom1, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom1.Close()
	se2 := newSession(c, store, s.dbName)
	r = mustExecSQL(c, se2, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version";`)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(row.GetBytes(0), BytesEquals, []byte(fmt.Sprintf("%d", currentBootstrapVersion)))
	c.Assert(r.Close(), IsNil)

	ver, err = getBootstrapVersion(se2)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(currentBootstrapVersion))

	// Verify that 'new_collation_enabled' is false.
	r = mustExecSQL(c, se2, fmt.Sprintf(`SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME='%s';`, tidbNewCollationEnabled))
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows(), Equals, 1)
	c.Assert(req.GetRow(0).GetString(0), Equals, "False")
	c.Assert(r.Close(), IsNil)
}

func (s *testBootstrapSuite) TestANSISQLMode(c *C) {
	defer testleak.AfterTest(c)()
	store, dom := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "USE mysql;")
	mustExecSQL(c, se, `set @@global.sql_mode="NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI"`)
	mustExecSQL(c, se, `delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version";`)
	unsetStoreBootstrapped(store.UUID())
	se.Close()

	// Do some clean up, BootstrapSession will not create a new domain otherwise.
	dom.Close()
	domap.Delete(store)

	// Set ANSI sql_mode and bootstrap again, to cover a bugfix.
	// Once we have a SQL like that:
	// select variable_value from mysql.tidb where variable_name = "system_tz"
	// it fails to execute in the ANSI sql_mode, and makes TiDB cluster fail to bootstrap.
	dom1, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom1.Close()
	se = newSession(c, store, s.dbName)
	mustExecSQL(c, se, "select @@global.sql_mode")
	se.Close()
}

func (s *testBootstrapSuite) TestOldPasswordUpgrade(c *C) {
	pwd := "abc"
	oldpwd := fmt.Sprintf("%X", auth.Sha1Hash([]byte(pwd)))
	newpwd, err := oldPasswordUpgrade(oldpwd)
	c.Assert(err, IsNil)
	c.Assert(newpwd, Equals, "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E")
}

func (s *testBootstrapSuite) TestBootstrapInitExpensiveQueryHandle(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	defer store.Close()
	se, err := createSession(store)
	c.Assert(err, IsNil)
	dom := domain.GetDomain(se)
	c.Assert(dom, NotNil)
	defer dom.Close()
	dom.InitExpensiveQueryHandle()
	c.Assert(dom.ExpensiveQueryHandle(), NotNil)
}

func (s *testBootstrapSuite) TestStmtSummary(c *C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	store, dom := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()
	defer dom.Close()
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, `update mysql.global_variables set variable_value='' where variable_name='tidb_enable_stmt_summary'`)
	writeStmtSummaryVars(se)

	r := mustExecSQL(c, se, "select variable_value from mysql.global_variables where variable_name='tidb_enable_stmt_summary'")
	req := r.NewChunk()
	c.Assert(r.Next(ctx, req), IsNil)
	row := req.GetRow(0)
	c.Assert(row.GetBytes(0), BytesEquals, []byte("1"))
	c.Assert(r.Close(), IsNil)
}
