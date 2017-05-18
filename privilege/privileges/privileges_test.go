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
	"os"
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
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

func (s *testPrivilegeSuite) SetUpSuit(c *C) {
	logLevel := os.Getenv("log_level")
	log.SetLevelByString(logLevel)
}

func (s *testPrivilegeSuite) SetUpTest(c *C) {
	privileges.Enable = true
	s.dbName = "test"
	s.store = newStore(c, s.dbName)
	se := newSession(c, s.store, s.dbName)
	_, err := tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.createDBSQL = fmt.Sprintf("create database if not exists %s;", s.dbName)
	s.createDB1SQL = fmt.Sprintf("create database if not exists %s1;", s.dbName)
	s.dropDBSQL = fmt.Sprintf("drop database if exists %s;", s.dbName)
	s.useDBSQL = fmt.Sprintf("use %s;", s.dbName)
	s.createTableSQL = `CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));`

	mustExec(c, se, s.createDBSQL)
	mustExec(c, se, s.createDB1SQL) // create database test1
	mustExec(c, se, s.useDBSQL)
	mustExec(c, se, s.createTableSQL)

	s.createSystemDBSQL = fmt.Sprintf("create database if not exists %s;", mysql.SystemDB)
	s.createUserTableSQL = tidb.CreateUserTable
	s.createDBPrivTableSQL = tidb.CreateDBPrivTable
	s.createTablePrivTableSQL = tidb.CreateTablePrivTable
	s.createColumnPrivTableSQL = tidb.CreateColumnPrivTable

	mustExec(c, se, s.createSystemDBSQL)
	mustExec(c, se, s.createUserTableSQL)
	mustExec(c, se, s.createDBPrivTableSQL)
	mustExec(c, se, s.createTablePrivTableSQL)
	mustExec(c, se, s.createColumnPrivTableSQL)
}

func (s *testPrivilegeSuite) TearDownTest(c *C) {
	// drop db
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, s.dropDBSQL)
}

func (s *testPrivilegeSuite) TestCheckDBPrivilege(c *C) {
	defer testleak.AfterTest(c)()
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'testcheck'@'localhost';`)
	mustExec(c, rootSe, `FLUSH PRIVILEGES;`)

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth("testcheck@localhost", nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification("test", "", "", mysql.SelectPriv), IsFalse)

	mustExec(c, rootSe, `GRANT SELECT ON *.* TO  'testcheck'@'localhost';`)
	mustExec(c, rootSe, `FLUSH PRIVILEGES;`)
	c.Assert(pc.RequestVerification("test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification("test", "", "", mysql.UpdatePriv), IsFalse)

	mustExec(c, rootSe, `GRANT Update ON test.* TO  'testcheck'@'localhost';`)
	mustExec(c, rootSe, `FLUSH PRIVILEGES;`)
	c.Assert(pc.RequestVerification("test", "", "", mysql.UpdatePriv), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckTablePrivilege(c *C) {
	defer testleak.AfterTest(c)()
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'test1'@'localhost';`)
	mustExec(c, rootSe, `FLUSH PRIVILEGES;`)

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth("test1@localhost", nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.SelectPriv), IsFalse)

	mustExec(c, rootSe, `GRANT SELECT ON *.* TO  'test1'@'localhost';`)
	mustExec(c, rootSe, `FLUSH PRIVILEGES;`)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.UpdatePriv), IsFalse)

	mustExec(c, rootSe, `GRANT Update ON test.* TO  'test1'@'localhost';`)
	mustExec(c, rootSe, `FLUSH PRIVILEGES;`)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.UpdatePriv), IsTrue)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.IndexPriv), IsFalse)

	mustExec(c, rootSe, `GRANT Index ON test.test TO  'test1'@'localhost';`)
	mustExec(c, rootSe, `FLUSH PRIVILEGES;`)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.IndexPriv), IsTrue)
}

func (s *testPrivilegeSuite) TestShowGrants(c *C) {
	defer testleak.AfterTest(c)()
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'show'@'localhost' identified by '123';`)
	mustExec(c, se, `GRANT Index ON *.* TO  'show'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)
	pc := privilege.GetPrivilegeManager(se)

	gs, err := pc.ShowGrants(se, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Index ON *.* TO 'show'@'localhost'`)

	mustExec(c, se, `GRANT Select ON *.* TO  'show'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)
	gs, err = pc.ShowGrants(se, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Select,Index ON *.* TO 'show'@'localhost'`)

	// The order of privs is the same with AllGlobalPrivs
	mustExec(c, se, `GRANT Update ON *.* TO  'show'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)
	gs, err = pc.ShowGrants(se, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Select,Update,Index ON *.* TO 'show'@'localhost'`)

	// All privileges
	mustExec(c, se, `GRANT ALL ON *.* TO  'show'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)
	gs, err = pc.ShowGrants(se, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`)

	// Add db scope privileges
	mustExec(c, se, `GRANT Select ON test.* TO  'show'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)
	gs, err = pc.ShowGrants(se, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 2)
	expected := []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustExec(c, se, `GRANT Index ON test1.* TO  'show'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)
	gs, err = pc.ShowGrants(se, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT Index ON test1.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustExec(c, se, `GRANT ALL ON test1.* TO  'show'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)
	gs, err = pc.ShowGrants(se, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	// Add table scope privileges
	mustExec(c, se, `GRANT Update ON test.test TO  'show'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)
	gs, err = pc.ShowGrants(se, `show@localhost`)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 4)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`,
		`GRANT Update ON test.test TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)
}

func (s *testPrivilegeSuite) TestDropTablePriv(c *C) {
	defer testleak.AfterTest(c)()
	se := newSession(c, s.store, s.dbName)
	ctx, _ := se.(context.Context)
	mustExec(c, se, `CREATE TABLE todrop(c int);`)
	// ctx.GetSessionVars().User = "root@localhost"
	c.Assert(se.Auth("root@localhost", nil, nil), IsTrue)
	mustExec(c, se, `CREATE USER 'drop'@'localhost';`)
	mustExec(c, se, `GRANT Select ON test.todrop TO  'drop'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)

	// ctx.GetSessionVars().User = "drop@localhost"
	c.Assert(se.Auth("drop@localhost", nil, nil), IsTrue)
	mustExec(c, se, `SELECT * FROM todrop;`)
	_, err := se.Execute("DROP TABLE todrop;")
	c.Assert(err, NotNil)

	se = newSession(c, s.store, s.dbName)
	ctx.GetSessionVars().User = "root@localhost"
	mustExec(c, se, `GRANT Drop ON test.todrop TO  'drop'@'localhost';`)
	mustExec(c, se, `FLUSH PRIVILEGES;`)

	se = newSession(c, s.store, s.dbName)
	ctx.GetSessionVars().User = "drop@localhost"
	mustExec(c, se, `DROP TABLE todrop;`)
}

func mustExec(c *C, se tidb.Session, sql string) {
	_, err := se.Execute(sql)
	c.Assert(err, IsNil)
}

func newStore(c *C, dbPath string) kv.Storage {
	store, err := tidb.NewStore("memory" + "://" + dbPath)
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
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
