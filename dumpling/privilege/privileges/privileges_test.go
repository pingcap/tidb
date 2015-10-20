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
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPrivilegeSuite{})

type testPrivilegeSuite struct {
	store  kv.Storage
	dbName string

	createDBSQL              string
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
	t.dropDBSQL = fmt.Sprintf("drop database if exists %s;", t.dbName)
	t.useDBSQL = fmt.Sprintf("use %s;", t.dbName)
	t.createTableSQL = `CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));`

	mustExec(c, se, t.createDBSQL)
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
	pc := &privileges.PrivilegeCheck{}
	db := &model.DBInfo{
		Name: model.NewCIStr("test"),
	}
	ctx, _ := se.(context.Context)
	variable.GetSessionVars(ctx).User = "test@localhost"
	r, err := pc.CheckDBPrivilege(ctx, db, mysql.SelectPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT SELECT ON *.* TO  'test'@'localhost';`)
	pc = &privileges.PrivilegeCheck{}
	r, err = pc.CheckDBPrivilege(ctx, db, mysql.SelectPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
	r, err = pc.CheckDBPrivilege(ctx, db, mysql.UpdatePriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT Update ON test.* TO  'test'@'localhost';`)
	pc = &privileges.PrivilegeCheck{}
	r, err = pc.CheckDBPrivilege(ctx, db, mysql.UpdatePriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
}

func (t *testPrivilegeSuite) TestCheckTablePrivilege(c *C) {
	se := newSession(c, t.store, t.dbName)
	mustExec(c, se, `CREATE USER 'test1'@'localhost' identified by '123';`)
	pc := &privileges.PrivilegeCheck{}
	db := &model.DBInfo{
		Name: model.NewCIStr("test"),
	}
	tbl := &model.TableInfo{
		Name: model.NewCIStr("test"),
	}
	ctx, _ := se.(context.Context)
	variable.GetSessionVars(ctx).User = "test1@localhost"
	r, err := pc.CheckTablePrivilege(ctx, db, tbl, mysql.SelectPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT SELECT ON *.* TO  'test1'@'localhost';`)
	pc = &privileges.PrivilegeCheck{}
	r, err = pc.CheckTablePrivilege(ctx, db, tbl, mysql.SelectPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
	r, err = pc.CheckTablePrivilege(ctx, db, tbl, mysql.UpdatePriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT Update ON test.* TO  'test1'@'localhost';`)
	pc = &privileges.PrivilegeCheck{}
	r, err = pc.CheckTablePrivilege(ctx, db, tbl, mysql.UpdatePriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
	r, err = pc.CheckTablePrivilege(ctx, db, tbl, mysql.IndexPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsFalse)

	mustExec(c, se, `GRANT Index ON test.test TO  'test1'@'localhost';`)
	pc = &privileges.PrivilegeCheck{}
	r, err = pc.CheckTablePrivilege(ctx, db, tbl, mysql.IndexPriv)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
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
