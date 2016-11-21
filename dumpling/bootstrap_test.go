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

package tidb

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSessionSuite) TestBootstrap(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "USE mysql;")
	r := mustExecSQL(c, se, `select * from user;`)
	c.Assert(r, NotNil)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	match(c, row.Data, []byte("%"), []byte("root"), []byte(""), "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")

	c.Assert(se.Auth("root@anyhost", []byte(""), []byte("")), IsTrue)
	mustExecSQL(c, se, "USE test;")
	// Check privilege tables.
	mustExecSQL(c, se, "SELECT * from mysql.db;")
	mustExecSQL(c, se, "SELECT * from mysql.tables_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.columns_priv;")
	// Check privilege tables.
	r = mustExecSQL(c, se, "SELECT COUNT(*) from mysql.global_variables;")
	c.Assert(r, NotNil)
	v, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(v.Data[0].GetInt64(), Equals, int64(len(variable.SysVars)))

	// Check a storage operations are default autocommit after the second start.
	mustExecSQL(c, se, "USE test;")
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id int)")
	delete(storeBootstrapped, store.UUID())
	se.Close()
	se, err = CreateSession(store)
	c.Assert(err, IsNil)
	mustExecSQL(c, se, "USE test;")
	mustExecSQL(c, se, "insert t values (?)", 3)
	se, err = CreateSession(store)
	c.Assert(err, IsNil)
	mustExecSQL(c, se, "USE test;")
	r = mustExecSQL(c, se, "select * from t")
	c.Assert(r, NotNil)
	v, err = r.Next()
	c.Assert(err, IsNil)
	match(c, v.Data, 3)
	mustExecSQL(c, se, "drop table if exists t")
	se.Close()

	// Try to do bootstrap dml jobs on an already bootstraped TiDB system will not cause fatal.
	// For https://github.com/pingcap/tidb/issues/1096
	store = newStore(c, s.dbName)
	se, err = CreateSession(store)
	c.Assert(err, IsNil)
	doDMLWorks(se)

	err = store.Close()
	c.Assert(err, IsNil)
}

// Create a new session on store but only do ddl works.
func (s *testSessionSuite) bootstrapWithOnlyDDLWork(store kv.Storage, c *C) {
	ss := &session{
		values: make(map[fmt.Stringer]interface{}),
		store:  store,
		parser: parser.New(),
	}
	ss.SetValue(context.Initing, true)
	domain, err := domap.Get(store)
	c.Assert(err, IsNil)
	sessionctx.BindDomain(ss, domain)
	variable.BindSessionVars(ss)
	variable.GetSessionVars(ss).SetStatusFlag(mysql.ServerStatusAutocommit, true)
	// session implements autocommit.Checker. Bind it to ctx
	autocommit.BindAutocommitChecker(ss, ss)
	sessionMu.Lock()
	defer sessionMu.Unlock()
	b, err := checkBootstrapped(ss)
	c.Assert(b, IsFalse)
	c.Assert(err, IsNil)
	doDDLWorks(ss)
	// Leave dml unfinished.
}

// When a session failed in bootstrap process (for example, the session is killed after doDDLWorks()).
// We should make sure that the following session could finish the bootstrap process.
func (s *testSessionSuite) TestBootstrapWithError(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbNameBootstrap)
	s.bootstrapWithOnlyDDLWork(store, c)

	se := newSession(c, store, s.dbNameBootstrap)
	mustExecSQL(c, se, "USE mysql;")
	r := mustExecSQL(c, se, `select * from user;`)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	match(c, row.Data, []byte("%"), []byte("root"), []byte(""), "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")
	mustExecSQL(c, se, "USE test;")
	// Check privilege tables.
	mustExecSQL(c, se, "SELECT * from mysql.db;")
	mustExecSQL(c, se, "SELECT * from mysql.tables_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.columns_priv;")
	// Check global variables.
	r = mustExecSQL(c, se, "SELECT COUNT(*) from mysql.global_variables;")
	v, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(v.Data[0].GetInt64(), Equals, int64(len(variable.SysVars)))

	r = mustExecSQL(c, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="bootstrapped";`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	c.Assert(row.Data, HasLen, 1)
	c.Assert(row.Data[0].GetBytes(), BytesEquals, []byte("True"))

	err = store.Close()
	c.Assert(err, IsNil)
}

// Test case for upgrade
func (s *testSessionSuite) TestUpgrade(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "USE mysql;")

	// bootstrap with currentBootstrapVersion
	r := mustExecSQL(c, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version";`)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	c.Assert(row.Data, HasLen, 1)
	c.Assert(row.Data[0].GetBytes(), BytesEquals, []byte(fmt.Sprintf("%d", currentBootstrapVersion)))

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
	err = txn.Commit()
	c.Assert(err, IsNil)
	mustExecSQL(c, se1, `delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version";`)
	mustExecSQL(c, se1, fmt.Sprintf(`delete from mysql.global_variables where VARIABLE_NAME="%s" or VARIABLE_NAME="%s";`,
		variable.DistSQLScanConcurrencyVar, variable.DistSQLJoinConcurrencyVar))
	mustExecSQL(c, se1, `commit;`)
	delete(storeBootstrapped, store.UUID())
	// Make sure the version is downgraded.
	r = mustExecSQL(c, se1, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version";`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)

	ver, err = getBootstrapVersion(se1)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(0))

	// Create a new session then upgrade() will run automatically.
	se2 := newSession(c, store, s.dbName)
	r = mustExecSQL(c, se2, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version";`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	c.Assert(row.Data, HasLen, 1)
	c.Assert(row.Data[0].GetBytes(), BytesEquals, []byte(fmt.Sprintf("%d", currentBootstrapVersion)))

	ver, err = getBootstrapVersion(se2)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(currentBootstrapVersion))
}
