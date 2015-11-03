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

package privileges_test

import (
	"fmt"
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testutil"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPrivilegeSuite{})

type testPrivilegeSuite struct {
	store  kv.Storage
	dbName string

	createDBSQL              string
	createDB1SQL             string
	dropDBSQL                string
	useDBSQL                 string
	createTableSQL           string
	createSystemDBSQL        string
	createUserTableSQL       string
	createDBPrivTableSQL     string
	createTablePrivTableSQL  string
	createColumnPrivTableSQL string
}

func (t *testPrivilegeSuite) SetUpTest(c *C) {
	log.SetLevelByString("error")
	t.dbName = "test"
	t.store = newStore(c, t.dbName)
	se := newSession(c, t.store, t.dbName)
	t.createDBSQL = fmt.Sprintf("create database if not exists %s;", t.dbName)
	t.createDB1SQL = fmt.Sprintf("create database if not exists %s1;", t.dbName)
	t.dropDBSQL = fmt.Sprintf("drop database if exists %s;", t.dbName)
	t.useDBSQL = fmt.Sprintf("use %s;", t.dbName)
	t.createTableSQL = `CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));`

	mustExec(c, se, t.createDBSQL)
	mustExec(c, se, t.createDB1SQL) // create database test1
	mustExec(c, se, t.useDBSQL)
	mustExec(c, se, t.createTableSQL)

	t.createSystemDBSQL = fmt.Sprintf("create database if not exists %s;", mysql.SystemDB)
	t.createUserTableSQL = tidb.CreateUserTable
	t.createDBPrivTableSQL = tidb.CreateDBPrivTable
	t.createTablePrivTableSQL = tidb.CreateTablePrivTable
	t.createColumnPrivTableSQL = tidb.CreateColumnPrivTable

	mustExec(c, se, t.createSystemDBSQL)
	mustExec(c, se, t.createUserTableSQL)
	mustExec(c, se, t.createDBPrivTableSQL)
	mustExec(c, se, t.createTablePrivTableSQL)
	mustExec(c, se, t.createColumnPrivTableSQL)
}

func (t *testPrivilegeSuite) TearDownTest(c *C) {
	// drop db
	se := newSession(c, t.store, t.dbName)
	mustExec(c, se, t.dropDBSQL)
}

func (t *testPrivilegeSuite) TestCheckDBPrivilege(c *C) {
	se := newSession(c, t.store, t.dbName)
	mustExec(c, se, `CREATE USER 'test'@'localhost' identified by '123';`)
	pc := &privileges.UserPrivileges{}
	db := &model.DBInfo{
		Name: model.NewCIStr("test"),
	}
	ctx, _ := se.(context.Context)
	variable.GetSessionVars(ctx).User = "test@localhost"
	r, err := pc.Check(ctx, db, nil, mysql.SelectPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT SELECT ON *.* TO  'test'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	r, err = pc.Check(ctx, db, nil, mysql.SelectPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
	r, err = pc.Check(ctx, db, nil, mysql.UpdatePriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT Update ON test.* TO  'test'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	r, err = pc.Check(ctx, db, nil, mysql.UpdatePriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
}

func (t *testPrivilegeSuite) TestCheckTablePrivilege(c *C) {
	se := newSession(c, t.store, t.dbName)
	mustExec(c, se, `CREATE USER 'test1'@'localhost' identified by '123';`)
	pc := &privileges.UserPrivileges{}
	db := &model.DBInfo{
		Name: model.NewCIStr("test"),
	}
	tbl := &model.TableInfo{
		Name: model.NewCIStr("test"),
	}
	ctx, _ := se.(context.Context)
	variable.GetSessionVars(ctx).User = "test1@localhost"
	r, err := pc.Check(ctx, db, tbl, mysql.SelectPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT SELECT ON *.* TO  'test1'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	r, err = pc.Check(ctx, db, tbl, mysql.SelectPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
	r, err = pc.Check(ctx, db, tbl, mysql.UpdatePriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT Update ON test.* TO  'test1'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	r, err = pc.Check(ctx, db, tbl, mysql.UpdatePriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
	r, err = pc.Check(ctx, db, tbl, mysql.IndexPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT Index ON test.test TO  'test1'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	r, err = pc.Check(ctx, db, tbl, mysql.IndexPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
}

func (t *testPrivilegeSuite) TestShowGrants(c *C) {
	se := newSession(c, t.store, t.dbName)
	ctx, _ := se.(context.Context)
	mustExec(c, se, `CREATE USER 'show'@'localhost' identified by '123';`)
	mustExec(c, se, `GRANT Index ON *.* TO  'show'@'localhost';`)
	pc := &privileges.UserPrivileges{}
	gs, err := pc.ShowGrants(ctx, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Index ON *.* TO 'show'@'localhost'`)

	mustExec(c, se, `GRANT Select ON *.* TO  'show'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	gs, err = pc.ShowGrants(ctx, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Select,Index ON *.* TO 'show'@'localhost'`)

	// The order of privs is the same with AllGlobalPrivs
	mustExec(c, se, `GRANT Update ON *.* TO  'show'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	gs, err = pc.ShowGrants(ctx, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Select,Update,Index ON *.* TO 'show'@'localhost'`)

	// All privileges
	mustExec(c, se, `GRANT ALL ON *.* TO  'show'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	gs, err = pc.ShowGrants(ctx, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`)

	// Add db scope privileges
	mustExec(c, se, `GRANT Select ON test.* TO  'show'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	gs, err = pc.ShowGrants(ctx, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 2)
	expected := []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustExec(c, se, `GRANT Index ON test1.* TO  'show'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	gs, err = pc.ShowGrants(ctx, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT Index ON test1.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustExec(c, se, `GRANT ALL ON test1.* TO  'show'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	gs, err = pc.ShowGrants(ctx, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	// Add table scope privileges
	mustExec(c, se, `GRANT Update ON test.test TO  'show'@'localhost';`)
	pc = &privileges.UserPrivileges{}
	gs, err = pc.ShowGrants(ctx, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 4)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`,
		`GRANT Update ON test.test TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)
}

func (t *testPrivilegeSuite) TestDropTablePriv(c *C) {
	se := newSession(c, t.store, t.dbName)
	ctx, _ := se.(context.Context)
	mustExec(c, se, `CREATE TABLE todrop(c int);`)
	variable.GetSessionVars(ctx).User = "root@localhost"
	mustExec(c, se, `CREATE USER 'drop'@'localhost' identified by '123';`)
	mustExec(c, se, `GRANT Select ON test.todrop TO  'drop'@'localhost';`)

	variable.GetSessionVars(ctx).User = "drop@localhost"
	mustExec(c, se, `SELECT * FROM todrop;`)

	_, err := se.Execute("DROP TABLE todrop;")
	c.Assert(err, NotNil)

	variable.GetSessionVars(ctx).User = "root@localhost"
	mustExec(c, se, `GRANT Drop ON test.todrop TO  'drop'@'localhost';`)

	se1 := newSession(c, t.store, t.dbName)
	ctx1, _ := se1.(context.Context)
	variable.GetSessionVars(ctx1).User = "drop@localhost"
	mustExec(c, se1, `DROP TABLE todrop;`)
}

func mustExec(c *C, se tidb.Session, sql string) {
	_, err := se.Execute(sql)
	c.Assert(err, IsNil)
}

func newStore(c *C, dbPath string) kv.Storage {
	store, err := tidb.NewStore("memory" + "://" + dbPath)
	c.Assert(err, IsNil)
	return store
}

func newSession(c *C, store kv.Storage, dbName string) tidb.Session {
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	mustExec(c, se, "create database if not exists "+dbName)
	mustExec(c, se, "use "+dbName)
	return se
}
