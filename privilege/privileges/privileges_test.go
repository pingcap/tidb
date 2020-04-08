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
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"golang.org/x/net/context"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testPrivilegeSuite{})

type testPrivilegeSuite struct {
	store  kv.Storage
	dom    *domain.Domain
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

func (s *testPrivilegeSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.dbName = "test"
	s.dom, s.store = newStore(c, s.dbName)
}

func (s *testPrivilegeSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testPrivilegeSuite) SetUpTest(c *C) {
	se := newSession(c, s.store, s.dbName)
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
	s.createUserTableSQL = session.CreateUserTable
	s.createDBPrivTableSQL = session.CreateDBPrivTable
	s.createTablePrivTableSQL = session.CreateTablePrivTable
	s.createColumnPrivTableSQL = session.CreateColumnPrivTable

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
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'testcheck'@'localhost';`)

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "testcheck", Hostname: "localhost"}, nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification("test", "", "", mysql.SelectPriv), IsFalse)

	mustExec(c, rootSe, `GRANT SELECT ON *.* TO  'testcheck'@'localhost';`)
	c.Assert(pc.RequestVerification("test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification("test", "", "", mysql.UpdatePriv), IsFalse)

	mustExec(c, rootSe, `GRANT Update ON test.* TO  'testcheck'@'localhost';`)
	c.Assert(pc.RequestVerification("test", "", "", mysql.UpdatePriv), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckPointGetDBPrivilege(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'tester'@'localhost';`)
	mustExec(c, rootSe, `GRANT SELECT,UPDATE ON test.* TO  'tester'@'localhost';`)
	mustExec(c, rootSe, `flush privileges;`)
	mustExec(c, rootSe, `create database test2`)
	mustExec(c, rootSe, `create table test2.t(id int, v int, primary key(id))`)
	mustExec(c, rootSe, `insert into test2.t(id, v) values(1, 1)`)

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tester", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `use test;`)
	_, err := se.Execute(context.Background(), `select * from test2.t where id = 1`)
	fmt.Println(err.Error())
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)
	_, err = se.Execute(context.Background(), "update test2.t set v = 2 where id = 1")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckTablePrivilege(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'test1'@'localhost';`)

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test1", Hostname: "localhost"}, nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.SelectPriv), IsFalse)

	mustExec(c, rootSe, `GRANT SELECT ON *.* TO  'test1'@'localhost';`)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.UpdatePriv), IsFalse)

	mustExec(c, rootSe, `GRANT Update ON test.* TO  'test1'@'localhost';`)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.UpdatePriv), IsTrue)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.IndexPriv), IsFalse)

	mustExec(c, rootSe, `GRANT Index ON test.test TO  'test1'@'localhost';`)
	c.Assert(pc.RequestVerification("test", "test", "", mysql.IndexPriv), IsTrue)
}

func (s *testPrivilegeSuite) TestShowGrants(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'show'@'localhost' identified by '123';`)
	mustExec(c, se, `GRANT Index ON *.* TO  'show'@'localhost';`)
	pc := privilege.GetPrivilegeManager(se)

	gs, err := pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"})
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Index ON *.* TO 'show'@'localhost'`)

	mustExec(c, se, `GRANT Select ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"})
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Select,Index ON *.* TO 'show'@'localhost'`)

	// The order of privs is the same with AllGlobalPrivs
	mustExec(c, se, `GRANT Update ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"})
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Select,Update,Index ON *.* TO 'show'@'localhost'`)

	// All privileges
	mustExec(c, se, `GRANT ALL ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"})
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`)

	// Add db scope privileges
	mustExec(c, se, `GRANT Select ON test.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"})
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 2)
	expected := []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustExec(c, se, `GRANT Index ON test1.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"})
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT Index ON test1.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustExec(c, se, `GRANT ALL ON test1.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"})
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	// Add table scope privileges
	mustExec(c, se, `GRANT Update ON test.test TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"})
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 4)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`,
		`GRANT Update ON test.test TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	// Fix a issue that empty privileges is displayed when revoke after grant.
	mustExec(c, se, "TRUNCATE TABLE mysql.db")
	mustExec(c, se, "TRUNCATE TABLE mysql.user")
	mustExec(c, se, "TRUNCATE TABLE mysql.tables_priv")
	mustExec(c, se, `GRANT ALL PRIVILEGES ON `+"`"+`te%`+"`"+`.* TO 'show'@'localhost'`)
	mustExec(c, se, `REVOKE ALL PRIVILEGES ON `+"`"+`te%`+"`"+`.* FROM 'show'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"})
	c.Assert(err, IsNil)
	// It should not be "GRANT ON `te%`.* to 'show'@'localhost'"
	c.Assert(gs, HasLen, 0)
}

func (s *testPrivilegeSuite) TestDropTablePriv(c *C) {
	se := newSession(c, s.store, s.dbName)
	ctx, _ := se.(sessionctx.Context)
	mustExec(c, se, `CREATE TABLE todrop(c int);`)
	// ctx.GetSessionVars().User = "root@localhost"
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `CREATE USER 'drop'@'localhost';`)
	mustExec(c, se, `GRANT Select ON test.todrop TO  'drop'@'localhost';`)

	// ctx.GetSessionVars().User = "drop@localhost"
	c.Assert(se.Auth(&auth.UserIdentity{Username: "drop", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `SELECT * FROM todrop;`)
	_, err := se.Execute(context.Background(), "DROP TABLE todrop;")
	c.Assert(err, NotNil)

	se = newSession(c, s.store, s.dbName)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	mustExec(c, se, `GRANT Drop ON test.todrop TO  'drop'@'localhost';`)

	se = newSession(c, s.store, s.dbName)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "drop", Hostname: "localhost"}
	mustExec(c, se, `DROP TABLE todrop;`)
}

func (s *testPrivilegeSuite) TestSetPasswdStmt(c *C) {

	se := newSession(c, s.store, s.dbName)

	// high privileged user setting password for other user (passes)
	mustExec(c, se, "CREATE USER 'superuser'")
	mustExec(c, se, "CREATE USER 'nobodyuser'")
	mustExec(c, se, "GRANT ALL ON *.* TO 'superuser'")
	mustExec(c, se, "FLUSH PRIVILEGES")

	// low privileged user trying to set password for other user (fails)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "nobodyuser", Hostname: "localhost", AuthUsername: "nobodyuser", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.Execute(context.Background(), "SET PASSWORD for 'superuser' = 'newpassword'")
	c.Assert(err, NotNil)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "superuser", Hostname: "localhost", AuthUsername: "superuser", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "SET PASSWORD for 'nobodyuser' = 'newpassword'")

}

func (s *testPrivilegeSuite) TestCheckAuthenticate(c *C) {

	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'u1'@'localhost';`)
	mustExec(c, se, `CREATE USER 'u2'@'localhost' identified by 'abc';`)
	mustExec(c, se, `CREATE USER 'u3@example.com'@'localhost';`)
	mustExec(c, se, `CREATE USER u4@localhost;`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil), IsFalse)
	salt := []byte{85, 92, 45, 22, 58, 79, 107, 6, 122, 125, 58, 80, 12, 90, 103, 32, 90, 10, 74, 82}
	authentication := []byte{24, 180, 183, 225, 166, 6, 81, 102, 70, 248, 199, 143, 91, 204, 169, 9, 161, 171, 203, 33}
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, authentication, salt), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u3@example.com", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil), IsTrue)

	se1 := newSession(c, s.store, s.dbName)
	mustExec(c, se1, "drop user 'u1'@'localhost'")
	mustExec(c, se1, "drop user 'u2'@'localhost'")
	mustExec(c, se1, "drop user 'u3@example.com'@'localhost'")
	mustExec(c, se1, "drop user u4@localhost")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u3@example.com", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil), IsFalse)
}

func (s *testPrivilegeSuite) TestInformationSchema(c *C) {

	// This test tests no privilege check for INFORMATION_SCHEMA database.
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'u1'@'localhost';`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `select * from information_schema.tables`)
	mustExec(c, se, `select * from information_schema.key_column_usage`)
}

func (s *testPrivilegeSuite) TestAdminCommand(c *C) {
	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `CREATE USER 'test_admin'@'localhost';`)
	mustExec(c, se, `CREATE TABLE t(a int)`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_admin", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.Execute(context.Background(), "ADMIN SHOW DDL JOBS")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)
	_, err = se.Execute(context.Background(), "ADMIN CHECK TABLE t")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.Execute(context.Background(), "ADMIN SHOW DDL JOBS")
	c.Assert(err, IsNil)
}

func (s *testPrivilegeSuite) TestAuthHost(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	se := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'test_auth_host'@'%';`)
	mustExec(c, rootSe, `GRANT ALL ON *.* TO 'test_auth_host'@'%' WITH GRANT OPTION;`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_auth_host", Hostname: "192.168.0.10"}, nil, nil), IsTrue)
	mustExec(c, se, "CREATE USER 'test_auth_host'@'192.168.%';")
	mustExec(c, se, "GRANT SELECT ON *.* TO 'test_auth_host'@'192.168.%';")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_auth_host", Hostname: "192.168.0.10"}, nil, nil), IsTrue)
	_, err := se.Execute(context.Background(), "create user test_auth_host_a")
	c.Assert(err, NotNil)

	mustExec(c, rootSe, "DROP USER 'test_auth_host'@'192.168.%';")
	mustExec(c, rootSe, "DROP USER 'test_auth_host'@'%';")
}

func mustExec(c *C, se session.Session, sql string) {
	_, err := se.Execute(context.Background(), sql)
	c.Assert(err, IsNil)
}

func newStore(c *C, dbPath string) (*domain.Domain, kv.Storage) {
	store, err := mockstore.NewMockTikvStore()
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	return dom, store
}

func newSession(c *C, store kv.Storage, dbName string) session.Session {
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	mustExec(c, se, "create database if not exists "+dbName)
	mustExec(c, se, "use "+dbName)
	return se
}
