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
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/mockstore"
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
	match(c, datums, `%`, "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", "Y", "Y")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "anyhost"}, []byte(""), []byte("")), IsTrue)
	mustExecSQL(c, se, "USE test;")
	// Check privilege tables.
	rs := mustExecSQL(c, se, "SELECT * from mysql.global_priv;")
	c.Assert(rs.Close(), IsNil)
	rs = mustExecSQL(c, se, "SELECT * from mysql.db;")
	c.Assert(rs.Close(), IsNil)
	rs = mustExecSQL(c, se, "SELECT * from mysql.tables_priv;")
	c.Assert(rs.Close(), IsNil)
	rs = mustExecSQL(c, se, "SELECT * from mysql.columns_priv;")
	c.Assert(rs.Close(), IsNil)
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
	for _, v := range variable.GetSysVars() {
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
	match(c, datums, `%`, "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", "Y", "Y")
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
	c.Assert(ver, Equals, currentBootstrapVersion)

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
	c.Assert(ver, Equals, currentBootstrapVersion)

	// Verify that 'new_collation_enabled' is false.
	r = mustExecSQL(c, se2, fmt.Sprintf(`SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME='%s';`, tidbNewCollationEnabled))
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows(), Equals, 1)
	c.Assert(req.GetRow(0).GetString(0), Equals, "False")
	c.Assert(r.Close(), IsNil)
}

func (s *testBootstrapSuite) TestIssue17979_1(c *C) {
	oomAction := config.GetGlobalConfig().OOMAction
	defer func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.OOMAction = oomAction
		})
	}()
	ctx := context.Background()
	defer testleak.AfterTest(c)()
	store, _ := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()

	// test issue 20900, upgrade from v3.0 to v4.0.11+
	seV3 := newSession(c, store, s.dbName)
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(58))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mustExecSQL(c, seV3, "update mysql.tidb set variable_value='58' where variable_name='tidb_server_version'")
	mustExecSQL(c, seV3, "delete from mysql.tidb where variable_name='default_oom_action'")
	mustExecSQL(c, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(58))

	domV4, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer domV4.Close()
	seV4 := newSession(c, store, s.dbName)
	ver, err = getBootstrapVersion(seV4)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, currentBootstrapVersion)
	r := mustExecSQL(c, seV4, "select variable_value from mysql.tidb where variable_name='default_oom_action'")
	req := r.NewChunk()
	r.Next(ctx, req)
	c.Assert(req.GetRow(0).GetString(0), Equals, "log")
	c.Assert(config.GetGlobalConfig().OOMAction, Equals, config.OOMActionLog)
}

func (s *testBootstrapSuite) TestIssue17979_2(c *C) {
	oomAction := config.GetGlobalConfig().OOMAction
	defer func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.OOMAction = oomAction
		})
	}()
	ctx := context.Background()
	defer testleak.AfterTest(c)()
	store, _ := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()

	// test issue 20900, upgrade from v4.0.11 to v4.0.11
	seV3 := newSession(c, store, s.dbName)
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(59))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mustExecSQL(c, seV3, "update mysql.tidb set variable_value=59 where variable_name='tidb_server_version'")
	mustExecSQL(c, seV3, "delete from mysql.tidb where variable_name='default_iim_action'")
	mustExecSQL(c, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(59))

	domV4, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer domV4.Close()
	seV4 := newSession(c, store, s.dbName)
	ver, err = getBootstrapVersion(seV4)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, currentBootstrapVersion)
	r := mustExecSQL(c, seV4, "select variable_value from mysql.tidb where variable_name='default_oom_action'")
	req := r.NewChunk()
	r.Next(ctx, req)
	c.Assert(req.NumRows(), Equals, 0)
	c.Assert(config.GetGlobalConfig().OOMAction, Equals, config.OOMActionCancel)
}

func (s *testBootstrapSuite) TestIssue20900_1(c *C) {
	ctx := context.Background()
	defer testleak.AfterTest(c)()
	store, _ := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()

	// test issue 20900, upgrade from v3.0 to v4.0.9+
	seV3 := newSession(c, store, s.dbName)
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(38))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mustExecSQL(c, seV3, "update mysql.tidb set variable_value=38 where variable_name='tidb_server_version'")
	mustExecSQL(c, seV3, "delete from mysql.tidb where variable_name='default_memory_quota_query'")
	mustExecSQL(c, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(38))

	domV4, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer domV4.Close()
	seV4 := newSession(c, store, s.dbName)
	ver, err = getBootstrapVersion(seV4)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, currentBootstrapVersion)
	r := mustExecSQL(c, seV4, "select @@tidb_mem_quota_query")
	req := r.NewChunk()
	r.Next(ctx, req)
	c.Assert(req.GetRow(0).GetString(0), Equals, "34359738368")
	r = mustExecSQL(c, seV4, "select variable_value from mysql.tidb where variable_name='default_memory_quota_query'")
	req = r.NewChunk()
	r.Next(ctx, req)
	c.Assert(req.GetRow(0).GetString(0), Equals, "34359738368")
	c.Assert(seV4.GetSessionVars().MemQuotaQuery, Equals, int64(34359738368))
}

func (s *testBootstrapSuite) TestIssue20900_2(c *C) {
	ctx := context.Background()
	defer testleak.AfterTest(c)()
	store, _ := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()

	// test issue 20900, upgrade from v4.0.8 to v4.0.9+
	seV3 := newSession(c, store, s.dbName)
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(52))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mustExecSQL(c, seV3, "update mysql.tidb set variable_value=52 where variable_name='tidb_server_version'")
	mustExecSQL(c, seV3, "delete from mysql.tidb where variable_name='default_memory_quota_query'")
	mustExecSQL(c, seV3, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV3)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(52))

	domV4, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer domV4.Close()
	seV4 := newSession(c, store, s.dbName)
	ver, err = getBootstrapVersion(seV4)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, currentBootstrapVersion)
	r := mustExecSQL(c, seV4, "select @@tidb_mem_quota_query")
	req := r.NewChunk()
	r.Next(ctx, req)
	c.Assert(req.GetRow(0).GetString(0), Equals, "1073741824")
	c.Assert(seV4.GetSessionVars().MemQuotaQuery, Equals, int64(1073741824))
	r = mustExecSQL(c, seV4, "select variable_value from mysql.tidb where variable_name='default_memory_quota_query'")
	req = r.NewChunk()
	r.Next(ctx, req)
	c.Assert(req.NumRows(), Equals, 0)
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
	c.Assert(row.GetBytes(0), BytesEquals, []byte("ON"))
	c.Assert(r.Close(), IsNil)
}

type bindTestStruct struct {
	originText   string
	bindText     string
	db           string
	originWithDB string
	bindWithDB   string
	deleteText   string
}

func (s *testBootstrapSuite) TestUpdateBindInfo(c *C) {
	bindCases := []bindTestStruct{
		{
			originText:   "select * from t where a > ?",
			bindText:     "select /*+ use_index(t, idxb) */ * from t where a > 1",
			db:           "test",
			originWithDB: "select * from `test` . `t` where `a` > ?",
			bindWithDB:   "SELECT /*+ use_index(`t` `idxb`)*/ * FROM `test`.`t` WHERE `a` > 1",
			deleteText:   "select * from test.t where a > 1",
		},
		{
			originText:   "select count ( ? ), max ( a ) from t group by b",
			bindText:     "select /*+ use_index(t, idx) */ count(1), max(a) from t group by b",
			db:           "test",
			originWithDB: "select count ( ? ) , max ( `a` ) from `test` . `t` group by `b`",
			bindWithDB:   "SELECT /*+ use_index(`t` `idx`)*/ count(1),max(`a`) FROM `test`.`t` GROUP BY `b`",
			deleteText:   "select count(1), max(a) from test.t group by b",
		},
		{
			originText:   "select * from `test` . `t` where `a` = (_charset) ?",
			bindText:     "SELECT * FROM test.t WHERE a = _utf8\\'ab\\'",
			db:           "test",
			originWithDB: "select * from `test` . `t` where `a` = ?",
			bindWithDB:   "SELECT * FROM `test`.`t` WHERE `a` = 'ab'",
			deleteText:   "select * from test.t where a = 'c'",
		},
	}
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	store, dom := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()
	defer dom.Close()
	se := newSession(c, store, s.dbName)
	for _, bindCase := range bindCases {
		sql := fmt.Sprintf("insert into mysql.bind_info values('%s', '%s', '%s', 'using', '2021-01-04 14:50:58.257', '2021-01-04 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')",
			bindCase.originText,
			bindCase.bindText,
			bindCase.db,
		)
		mustExecSQL(c, se, sql)

		upgradeToVer67(se, version66)
		r := mustExecSQL(c, se, `select original_sql, bind_sql, default_db, status from mysql.bind_info where source != 'builtin'`)
		req := r.NewChunk()
		c.Assert(r.Next(ctx, req), IsNil)
		row := req.GetRow(0)
		c.Assert(row.GetString(0), Equals, bindCase.originWithDB)
		c.Assert(row.GetString(1), Equals, bindCase.bindWithDB)
		c.Assert(row.GetString(2), Equals, "")
		c.Assert(row.GetString(3), Equals, "using")
		c.Assert(r.Close(), IsNil)
		sql = fmt.Sprintf("drop global binding for %s", bindCase.deleteText)
		mustExecSQL(c, se, sql)
		r = mustExecSQL(c, se, `select original_sql, bind_sql, status from mysql.bind_info where source != 'builtin'`)
		c.Assert(r.Next(ctx, req), IsNil)
		row = req.GetRow(0)
		c.Assert(row.GetString(0), Equals, bindCase.originWithDB)
		c.Assert(row.GetString(1), Equals, bindCase.bindWithDB)
		c.Assert(row.GetString(2), Equals, "deleted")
		c.Assert(r.Close(), IsNil)
		sql = fmt.Sprintf("delete from mysql.bind_info where original_sql = '%s'", bindCase.originWithDB)
		mustExecSQL(c, se, sql)
	}
}

func (s *testBootstrapSuite) TestUpdateDuplicateBindInfo(c *C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	store, dom := newStoreWithBootstrap(c, s.dbName)
	defer store.Close()
	defer dom.Close()
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, `insert into mysql.bind_info values('select * from t', 'select /*+ use_index(t, idx_a)*/ * from t', 'test', 'using', '2021-01-04 14:50:58.257', '2021-01-04 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)
	// The latest one.
	mustExecSQL(c, se, `insert into mysql.bind_info values('select * from test . t', 'select /*+ use_index(t, idx_b)*/ * from test.t', 'test', 'using', '2021-01-04 14:50:58.257', '2021-01-09 14:50:58.257', 'utf8', 'utf8_general_ci', 'manual')`)

	mustExecSQL(c, se, `insert into mysql.bind_info values('select * from t where a < ?', 'select * from t use index(idx) where a < 1', 'test', 'deleted', '2021-06-04 17:04:43.333', '2021-06-04 17:04:43.335', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExecSQL(c, se, `insert into mysql.bind_info values('select * from t where a < ?', 'select * from t ignore index(idx) where a < 1', 'test', 'using', '2021-06-04 17:04:43.335', '2021-06-04 17:04:43.335', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExecSQL(c, se, `insert into mysql.bind_info values('select * from test . t where a <= ?', 'select * from test.t use index(idx) where a <= 1', '', 'deleted', '2021-06-04 17:04:43.345', '2021-06-04 17:04:45.334', 'utf8', 'utf8_general_ci', 'manual')`)
	mustExecSQL(c, se, `insert into mysql.bind_info values('select * from test . t where a <= ?', 'select * from test.t ignore index(idx) where a <= 1', '', 'using', '2021-06-04 17:04:45.334', '2021-06-04 17:04:45.334', 'utf8', 'utf8_general_ci', 'manual')`)

	upgradeToVer67(se, version66)

	r := mustExecSQL(c, se, `select original_sql, bind_sql, default_db, status, create_time from mysql.bind_info where source != 'builtin' order by create_time`)
	req := r.NewChunk()
	c.Assert(r.Next(ctx, req), IsNil)
	c.Assert(req.NumRows(), Equals, 3)
	row := req.GetRow(0)
	c.Assert(row.GetString(0), Equals, "select * from `test` . `t`")
	c.Assert(row.GetString(1), Equals, "SELECT /*+ use_index(`t` `idx_b`)*/ * FROM `test`.`t`")
	c.Assert(row.GetString(2), Equals, "")
	c.Assert(row.GetString(3), Equals, "using")
	c.Assert(row.GetTime(4).String(), Equals, "2021-01-04 14:50:58.257")
	row = req.GetRow(1)
	c.Assert(row.GetString(0), Equals, "select * from `test` . `t` where `a` < ?")
	c.Assert(row.GetString(1), Equals, "SELECT * FROM `test`.`t` IGNORE INDEX (`idx`) WHERE `a` < 1")
	c.Assert(row.GetString(2), Equals, "")
	c.Assert(row.GetString(3), Equals, "using")
	c.Assert(row.GetTime(4).String(), Equals, "2021-06-04 17:04:43.335")
	row = req.GetRow(2)
	c.Assert(row.GetString(0), Equals, "select * from `test` . `t` where `a` <= ?")
	c.Assert(row.GetString(1), Equals, "SELECT * FROM `test`.`t` IGNORE INDEX (`idx`) WHERE `a` <= 1")
	c.Assert(row.GetString(2), Equals, "")
	c.Assert(row.GetString(3), Equals, "using")
	c.Assert(row.GetTime(4).String(), Equals, "2021-06-04 17:04:45.334")

	c.Assert(r.Close(), IsNil)
	mustExecSQL(c, se, "delete from mysql.bind_info where original_sql = 'select * from test . t'")
}

func (s *testBootstrapSuite) TestUpgradeClusteredIndexDefaultValue(c *C) {
	var err error
	defer testleak.AfterTest(c)()
	store, _ := newStoreWithBootstrap(c, s.dbName)
	defer func() {
		c.Assert(store.Close(), IsNil)
	}()

	seV67 := newSession(c, store, s.dbName)
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(67))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mustExecSQL(c, seV67, "update mysql.tidb set variable_value='67' where variable_name='tidb_server_version'")
	mustExecSQL(c, seV67, "UPDATE mysql.global_variables SET VARIABLE_VALUE = 'OFF' where VARIABLE_NAME = 'tidb_enable_clustered_index'")
	c.Assert(seV67.GetSessionVars().StmtCtx.AffectedRows(), Equals, uint64(1))
	mustExecSQL(c, seV67, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV67)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(67))

	domV68, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer domV68.Close()
	seV68 := newSession(c, store, s.dbName)
	ver, err = getBootstrapVersion(seV68)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, currentBootstrapVersion)

	r := mustExecSQL(c, seV68, `select @@global.tidb_enable_clustered_index, @@session.tidb_enable_clustered_index`)
	req := r.NewChunk()
	c.Assert(r.Next(context.Background(), req), IsNil)
	c.Assert(req.NumRows(), Equals, 1)
	row := req.GetRow(0)
	c.Assert(row.GetString(0), Equals, "INT_ONLY")
	c.Assert(row.GetString(1), Equals, "INT_ONLY")
}

func (s *testBootstrapSuite) TestUpgradeVersion66(c *C) {
	var err error
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	store, _ := newStoreWithBootstrap(c, s.dbName)
	defer func() {
		c.Assert(store.Close(), IsNil)
	}()

	seV65 := newSession(c, store, s.dbName)
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(65))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mustExecSQL(c, seV65, "update mysql.tidb set variable_value='65' where variable_name='tidb_server_version'")
	mustExecSQL(c, seV65, "set @@global.tidb_track_aggregate_memory_usage = 0")
	mustExecSQL(c, seV65, "commit")
	unsetStoreBootstrapped(store.UUID())
	ver, err := getBootstrapVersion(seV65)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(65))

	domV66, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer domV66.Close()
	seV66 := newSession(c, store, s.dbName)
	ver, err = getBootstrapVersion(seV66)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, currentBootstrapVersion)
	r := mustExecSQL(c, seV66, `select @@global.tidb_track_aggregate_memory_usage, @@session.tidb_track_aggregate_memory_usage`)
	req := r.NewChunk()
	c.Assert(r.Next(ctx, req), IsNil)
	c.Assert(req.NumRows(), Equals, 1)
	row := req.GetRow(0)
	c.Assert(row.GetInt64(0), Equals, int64(1))
	c.Assert(row.GetInt64(1), Equals, int64(1))
}

func (s *testBootstrapSuite) TestForIssue23387(c *C) {
	// For issue https://github.com/pingcap/tidb/issues/23387
	saveCurrentBootstrapVersion := currentBootstrapVersion
	currentBootstrapVersion = version57

	// Bootstrap to an old version, create a user.
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer store.Close()
	_, err = BootstrapSession(store)
	// domain leaked here, Close() is not called. For testing, it's OK.
	// If we close it and BootstrapSession again, we'll get an error "session pool is closed".
	// The problem is caused by some the global level variable, domain map is not intended for multiple instances.
	c.Assert(err, IsNil)

	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "create user quatest")

	// Upgrade to a newer version, check the user's privilege.
	currentBootstrapVersion = saveCurrentBootstrapVersion
	dom, err := BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom.Close()

	se = newSession(c, store, s.dbName)
	rs, err := exec(se, "show grants for quatest")
	c.Assert(err, IsNil)
	rows, err := ResultSetToStringSlice(context.Background(), se, rs)
	c.Assert(err, IsNil)
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "GRANT USAGE ON *.* TO 'quatest'@'%'")
}
