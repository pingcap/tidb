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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testkit"
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
	mustExec(c, rootSe, `CREATE USER 'testcheck_tmp'@'localhost';`)

	se := newSession(c, s.store, s.dbName)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "testcheck", Hostname: "localhost"}, nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv), IsFalse)

	mustExec(c, rootSe, `GRANT SELECT ON *.* TO  'testcheck'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv), IsFalse)

	mustExec(c, rootSe, `GRANT Update ON test.* TO  'testcheck'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv), IsTrue)

	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "testcheck", Hostname: "localhost"})
	mustExec(c, rootSe, `GRANT 'testcheck'@'localhost' TO 'testcheck_tmp'@'localhost';`)
	se2 := newSession(c, s.store, s.dbName)
	c.Assert(se2.Auth(&auth.UserIdentity{Username: "testcheck_tmp", Hostname: "localhost"}, nil, nil), IsTrue)
	pc = privilege.GetPrivilegeManager(se2)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv), IsTrue)
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
	_, err := se.ExecuteInternal(context.Background(), `select * from test2.t where id = 1`)
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "update test2.t set v = 2 where id = 1")
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)
}

func (s *testPrivilegeSuite) TestIssue22946(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, "create database db1;")
	mustExec(c, rootSe, "create database db2;")
	mustExec(c, rootSe, "use test;")
	mustExec(c, rootSe, "create table a(id int);")
	mustExec(c, rootSe, "use db1;")
	mustExec(c, rootSe, "create table a(id int primary key,name varchar(20));")
	mustExec(c, rootSe, "use db2;")
	mustExec(c, rootSe, "create table b(id int primary key,address varchar(50));")
	mustExec(c, rootSe, "CREATE USER 'delTest'@'localhost';")
	mustExec(c, rootSe, "grant all on db1.* to delTest@'localhost';")
	mustExec(c, rootSe, "grant all on db2.* to delTest@'localhost';")
	mustExec(c, rootSe, "grant select on test.* to delTest@'localhost';")
	mustExec(c, rootSe, "flush privileges;")

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "delTest", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), `delete from db1.a as A where exists(select 1 from db2.b as B where A.id = B.id);`)
	c.Assert(err, IsNil)
	mustExec(c, rootSe, "use db1;")
	_, err = se.ExecuteInternal(context.Background(), "delete from test.a as A;")
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckTablePrivilege(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'test1'@'localhost';`)
	mustExec(c, rootSe, `CREATE USER 'test1_tmp'@'localhost';`)

	se := newSession(c, s.store, s.dbName)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test1", Hostname: "localhost"}, nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", mysql.SelectPriv), IsFalse)

	mustExec(c, rootSe, `GRANT SELECT ON *.* TO  'test1'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", mysql.UpdatePriv), IsFalse)

	mustExec(c, rootSe, `GRANT Update ON test.* TO  'test1'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", mysql.UpdatePriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv), IsFalse)

	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "test1", Hostname: "localhost"})
	se2 := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `GRANT 'test1'@'localhost' TO 'test1_tmp'@'localhost';`)
	c.Assert(se2.Auth(&auth.UserIdentity{Username: "test1_tmp", Hostname: "localhost"}, nil, nil), IsTrue)
	pc2 := privilege.GetPrivilegeManager(se2)
	c.Assert(pc2.RequestVerification(activeRoles, "test", "test", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc2.RequestVerification(activeRoles, "test", "test", "", mysql.UpdatePriv), IsTrue)
	c.Assert(pc2.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv), IsFalse)

	mustExec(c, rootSe, `GRANT Index ON test.test TO  'test1'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv), IsTrue)
	c.Assert(pc2.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckViewPrivilege(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'vuser'@'localhost';`)
	mustExec(c, rootSe, `CREATE VIEW v AS SELECT * FROM test;`)

	se := newSession(c, s.store, s.dbName)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "vuser", Hostname: "localhost"}, nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", mysql.SelectPriv), IsFalse)

	mustExec(c, rootSe, `GRANT SELECT ON test.v TO 'vuser'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", mysql.ShowViewPriv), IsFalse)

	mustExec(c, rootSe, `GRANT SHOW VIEW ON test.v TO 'vuser'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", mysql.ShowViewPriv), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckPrivilegeWithRoles(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'test_role'@'localhost';`)
	mustExec(c, rootSe, `CREATE ROLE r_1, r_2, r_3;`)
	mustExec(c, rootSe, `GRANT r_1, r_2, r_3 TO 'test_role'@'localhost';`)

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_role", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `SET ROLE r_1, r_2;`)
	mustExec(c, rootSe, `SET DEFAULT ROLE r_1 TO 'test_role'@'localhost';`)

	mustExec(c, rootSe, `GRANT SELECT ON test.* TO r_1;`)
	pc := privilege.GetPrivilegeManager(se)
	activeRoles := se.GetSessionVars().ActiveRoles
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv), IsFalse)
	mustExec(c, rootSe, `GRANT UPDATE ON test.* TO r_2;`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv), IsTrue)

	mustExec(c, se, `SET ROLE NONE;`)
	c.Assert(len(se.GetSessionVars().ActiveRoles), Equals, 0)
	mustExec(c, se, `SET ROLE DEFAULT;`)
	c.Assert(len(se.GetSessionVars().ActiveRoles), Equals, 1)
	mustExec(c, se, `SET ROLE ALL;`)
	c.Assert(len(se.GetSessionVars().ActiveRoles), Equals, 3)
	mustExec(c, se, `SET ROLE ALL EXCEPT r_1, r_2;`)
	c.Assert(len(se.GetSessionVars().ActiveRoles), Equals, 1)
}

func (s *testPrivilegeSuite) TestShowGrants(c *C) {
	se := newSession(c, s.store, s.dbName)
	ctx, _ := se.(sessionctx.Context)
	mustExec(c, se, `CREATE USER 'show'@'localhost' identified by '123';`)
	mustExec(c, se, `GRANT Index ON *.* TO  'show'@'localhost';`)
	pc := privilege.GetPrivilegeManager(se)

	gs, err := pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT INDEX ON *.* TO 'show'@'localhost'`)

	mustExec(c, se, `GRANT Select ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT SELECT,INDEX ON *.* TO 'show'@'localhost'`)

	// The order of privs is the same with AllGlobalPrivs
	mustExec(c, se, `GRANT Update ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT SELECT,UPDATE,INDEX ON *.* TO 'show'@'localhost'`)

	// All privileges
	mustExec(c, se, `GRANT ALL ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`)

	// All privileges with grant option
	mustExec(c, se, `GRANT ALL ON *.* TO 'show'@'localhost' WITH GRANT OPTION;`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost' WITH GRANT OPTION`)

	// Revoke grant option
	mustExec(c, se, `REVOKE GRANT OPTION ON *.* FROM 'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`)

	// Add db scope privileges
	mustExec(c, se, `GRANT Select ON test.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 2)
	expected := []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT SELECT ON test.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustExec(c, se, `GRANT Index ON test1.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT SELECT ON test.* TO 'show'@'localhost'`,
		`GRANT INDEX ON test1.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustExec(c, se, `GRANT ALL ON test1.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT SELECT ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	// Add table scope privileges
	mustExec(c, se, `GRANT Update ON test.test TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 4)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT SELECT ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`,
		`GRANT UPDATE ON test.test TO 'show'@'localhost'`}
	c.Assert(testutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	// Expected behavior: Usage still exists after revoking all privileges
	mustExec(c, se, `REVOKE ALL PRIVILEGES ON *.* FROM 'show'@'localhost'`)
	mustExec(c, se, `REVOKE Select on test.* FROM 'show'@'localhost'`)
	mustExec(c, se, `REVOKE ALL ON test1.* FROM 'show'@'localhost'`)
	mustExec(c, se, `REVOKE UPDATE on test.test FROM 'show'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT USAGE ON *.* TO 'show'@'localhost'`)

	// Usage should not exist after dropping the user
	// Which we need privileges to do so!
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	mustExec(c, se, `DROP USER 'show'@'localhost'`)

	// This should now return an error
	_, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, NotNil)
	// cant show grants for non-existent
	c.Assert(terror.ErrorEqual(err, privileges.ErrNonexistingGrant), IsTrue)

	// Test SHOW GRANTS with USING roles.
	mustExec(c, se, `CREATE ROLE 'r1', 'r2'`)
	mustExec(c, se, `GRANT SELECT ON test.* TO 'r1'`)
	mustExec(c, se, `GRANT INSERT, UPDATE ON test.* TO 'r2'`)
	mustExec(c, se, `CREATE USER 'testrole'@'localhost' IDENTIFIED BY 'u1pass'`)
	mustExec(c, se, `GRANT 'r1', 'r2' TO 'testrole'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 2)
	roles := make([]*auth.RoleIdentity, 0)
	roles = append(roles, &auth.RoleIdentity{Username: "r2", Hostname: "%"})
	mustExec(c, se, `GRANT DELETE ON test.* TO 'testrole'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	roles = append(roles, &auth.RoleIdentity{Username: "r1", Hostname: "%"})
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	mustExec(c, se, `GRANT INSERT, DELETE ON test.test TO 'r2'`)
	mustExec(c, se, `create table test.b (id int)`)
	mustExec(c, se, `GRANT UPDATE ON test.b TO 'testrole'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 5)
	mustExec(c, se, `DROP ROLE 'r1', 'r2'`)
	mustExec(c, se, `DROP USER 'testrole'@'localhost'`)
	mustExec(c, se, `CREATE ROLE 'r1', 'r2'`)
	mustExec(c, se, `GRANT SELECT ON test.* TO 'r2'`)
	mustExec(c, se, `CREATE USER 'testrole'@'localhost' IDENTIFIED BY 'u1pass'`)
	mustExec(c, se, `GRANT 'r1' TO 'testrole'@'localhost'`)
	mustExec(c, se, `GRANT 'r2' TO 'r1'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 2)
	roles = make([]*auth.RoleIdentity, 0)
	roles = append(roles, &auth.RoleIdentity{Username: "r1", Hostname: "%"})
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
}

func (s *testPrivilegeSuite) TestShowColumnGrants(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `USE test`)
	mustExec(c, se, `CREATE USER 'column'@'%'`)
	mustExec(c, se, `CREATE TABLE column_table (a int, b int, c int)`)
	mustExec(c, se, `GRANT Select(a),Update(a,b),Insert(c) ON test.column_table TO  'column'@'%'`)

	pc := privilege.GetPrivilegeManager(se)
	gs, err := pc.ShowGrants(se, &auth.UserIdentity{Username: "column", Hostname: "%"}, nil)
	c.Assert(err, IsNil)
	c.Assert(strings.Join(gs, " "), Equals, "GRANT USAGE ON *.* TO 'column'@'%' GRANT SELECT(a), INSERT(c), UPDATE(a, b) ON test.column_table TO 'column'@'%'")
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
	_, err := se.ExecuteInternal(context.Background(), "DROP TABLE todrop;")
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

	c.Assert(se.Auth(&auth.UserIdentity{Username: "superuser", Hostname: "localhost", AuthUsername: "superuser", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "SET PASSWORD for 'nobodyuser' = 'newpassword'")
	mustExec(c, se, "SET PASSWORD for 'nobodyuser' = ''")

	// low privileged user trying to set password for other user (fails)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "nobodyuser", Hostname: "localhost", AuthUsername: "nobodyuser", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), "SET PASSWORD for 'superuser' = 'newpassword'")
	c.Assert(err, NotNil)
}

func (s *testPrivilegeSuite) TestAlterUserStmt(c *C) {
	se := newSession(c, s.store, s.dbName)

	// high privileged user setting password for other user (passes)
	mustExec(c, se, "CREATE USER superuser2, nobodyuser2, nobodyuser3, nobodyuser4, nobodyuser5, semuser1, semuser2, semuser3, semuser4")
	mustExec(c, se, "GRANT ALL ON *.* TO superuser2")
	mustExec(c, se, "GRANT CREATE USER ON *.* TO nobodyuser2")
	mustExec(c, se, "GRANT SYSTEM_USER ON *.* TO nobodyuser4")
	mustExec(c, se, "GRANT UPDATE ON mysql.user TO nobodyuser5, semuser1")
	mustExec(c, se, "GRANT RESTRICTED_TABLES_ADMIN ON *.* TO semuser1")
	mustExec(c, se, "GRANT RESTRICTED_USER_ADMIN ON *.* TO semuser1, semuser2, semuser3")
	mustExec(c, se, "GRANT SYSTEM_USER ON *.* to semuser3") // user is both restricted + has SYSTEM_USER (or super)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "superuser2", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY 'newpassword'")
	mustExec(c, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")

	// low privileged user trying to set password for others
	// nobodyuser3 = SUCCESS (not a SYSTEM_USER)
	// nobodyuser4 = FAIL (has SYSTEM_USER)
	// superuser2  = FAIL (has SYSTEM_USER privilege implied by SUPER)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "nobodyuser2", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY 'newpassword'")
	mustExec(c, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	mustExec(c, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")
	_, err := se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser4' IDENTIFIED BY 'newpassword'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SYSTEM_USER or SUPER privilege(s) for this operation")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'superuser2' IDENTIFIED BY 'newpassword'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SYSTEM_USER or SUPER privilege(s) for this operation")

	// Nobody3 has no privileges at all, but they can still alter their own password.
	// Any other user fails.
	c.Assert(se.Auth(&auth.UserIdentity{Username: "nobodyuser3", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser4' IDENTIFIED BY 'newpassword'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'superuser2' IDENTIFIED BY 'newpassword'") // it checks create user before SYSTEM_USER
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")

	// Nobody5 doesn't explicitly have CREATE USER, but mysql also accepts UDPATE on mysql.user
	// as a substitute so it can modify nobody2 and nobody3 but not nobody4

	c.Assert(se.Auth(&auth.UserIdentity{Username: "nobodyuser5", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	mustExec(c, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser4' IDENTIFIED BY 'newpassword'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SYSTEM_USER or SUPER privilege(s) for this operation")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "semuser1", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, "ALTER USER 'semuser1' IDENTIFIED BY ''")
	mustExec(c, se, "ALTER USER 'semuser2' IDENTIFIED BY ''")
	mustExec(c, se, "ALTER USER 'semuser3' IDENTIFIED BY ''")

	sem.Enable()
	defer sem.Disable()

	// When SEM is enabled, even though we have UPDATE privilege on mysql.user, it explicitly
	// denies writeable privileges to system schemas unless RESTRICTED_TABLES_ADMIN is granted.
	// so the previous method of granting to the table instead of CREATE USER will fail now.
	// This is intentional because SEM plugs directly into the privilege manager to DENY
	// any request for UpdatePriv on mysql.user even if the privilege exists in the internal mysql.user table.

	// UpdatePriv on mysql.user
	c.Assert(se.Auth(&auth.UserIdentity{Username: "nobodyuser5", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser2' IDENTIFIED BY 'newpassword'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")

	// actual CreateUserPriv
	c.Assert(se.Auth(&auth.UserIdentity{Username: "nobodyuser2", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	mustExec(c, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")

	// UpdatePriv on mysql.user but also has RESTRICTED_TABLES_ADMIN
	c.Assert(se.Auth(&auth.UserIdentity{Username: "semuser1", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	mustExec(c, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")

	// As it has (RESTRICTED_TABLES_ADMIN + UpdatePriv on mysql.user) + RESTRICTED_USER_ADMIN it can modify other restricted_user_admins like semuser2
	// and it can modify semuser3 because RESTRICTED_USER_ADMIN does not also need SYSTEM_USER
	mustExec(c, se, "ALTER USER 'semuser1' IDENTIFIED BY ''")
	mustExec(c, se, "ALTER USER 'semuser2' IDENTIFIED BY ''")
	mustExec(c, se, "ALTER USER 'semuser3' IDENTIFIED BY ''")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "superuser2", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'semuser1' IDENTIFIED BY 'newpassword'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_USER_ADMIN privilege(s) for this operation")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "semuser4", Hostname: "localhost"}, nil, nil), IsTrue)
	// has restricted_user_admin but not CREATE USER or (update on mysql.user + RESTRICTED_TABLES_ADMIN)
	mustExec(c, se, "ALTER USER 'semuser4' IDENTIFIED BY ''") // can modify self
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser3' IDENTIFIED BY 'newpassword'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'semuser1' IDENTIFIED BY 'newpassword'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'semuser3' IDENTIFIED BY 'newpassword'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
}

func (s *testPrivilegeSuite) TestSelectViewSecurity(c *C) {
	se := newSession(c, s.store, s.dbName)
	ctx, _ := se.(sessionctx.Context)
	mustExec(c, se, `CREATE TABLE viewsecurity(c int);`)
	// ctx.GetSessionVars().User = "root@localhost"
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `CREATE USER 'selectusr'@'localhost';`)
	mustExec(c, se, `GRANT CREATE VIEW ON test.* TO  'selectusr'@'localhost';`)
	mustExec(c, se, `GRANT SELECT ON test.viewsecurity TO  'selectusr'@'localhost';`)

	// ctx.GetSessionVars().User = "selectusr@localhost"
	c.Assert(se.Auth(&auth.UserIdentity{Username: "selectusr", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `SELECT * FROM test.viewsecurity;`)
	mustExec(c, se, `CREATE ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW test.selectviewsecurity as select * FROM test.viewsecurity;`)

	se = newSession(c, s.store, s.dbName)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	mustExec(c, se, "SELECT * FROM test.selectviewsecurity")
	mustExec(c, se, `REVOKE Select ON test.viewsecurity FROM  'selectusr'@'localhost';`)
	_, err := se.ExecuteInternal(context.Background(), "select * from test.selectviewsecurity")
	c.Assert(err.Error(), Equals, core.ErrViewInvalid.GenWithStackByArgs("test", "selectviewsecurity").Error())
}

func (s *testPrivilegeSuite) TestRoleAdminSecurity(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'ar1'@'localhost';`)
	mustExec(c, se, `CREATE USER 'ar2'@'localhost';`)
	mustExec(c, se, `GRANT ALL ON *.* to ar1@localhost`)
	defer func() {
		c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
		mustExec(c, se, "drop user 'ar1'@'localhost'")
		mustExec(c, se, "drop user 'ar2'@'localhost'")
	}()

	c.Assert(se.Auth(&auth.UserIdentity{Username: "ar1", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `create role r_test1@localhost`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "ar2", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), `create role r_test2@localhost`)
	c.Assert(terror.ErrorEqual(err, core.ErrSpecificAccessDenied), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckCertBasedAuth(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'r1'@'localhost';`)
	mustExec(c, se, `CREATE USER 'r2'@'localhost' require none;`)
	mustExec(c, se, `CREATE USER 'r3'@'localhost' require ssl;`)
	mustExec(c, se, `CREATE USER 'r4'@'localhost' require x509;`)
	mustExec(c, se, `CREATE USER 'r5'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1' cipher 'TLS_AES_128_GCM_SHA256'`)
	mustExec(c, se, `CREATE USER 'r6'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	mustExec(c, se, `CREATE USER 'r7_issuer_only'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'`)
	mustExec(c, se, `CREATE USER 'r8_subject_only'@'localhost' require subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	mustExec(c, se, `CREATE USER 'r9_subject_disorder'@'localhost' require subject '/ST=Beijing/C=ZH/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	mustExec(c, se, `CREATE USER 'r10_issuer_disorder'@'localhost' require issuer '/ST=California/C=US/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'`)
	mustExec(c, se, `CREATE USER 'r11_cipher_only'@'localhost' require cipher 'TLS_AES_256_GCM_SHA384'`)
	mustExec(c, se, `CREATE USER 'r12_old_tidb_user'@'localhost'`)
	mustExec(c, se, "DELETE FROM mysql.global_priv WHERE `user` = 'r12_old_tidb_user' and `host` = 'localhost'")
	mustExec(c, se, `CREATE USER 'r13_broken_user'@'localhost'require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	mustExec(c, se, "UPDATE mysql.global_priv set priv = 'abc' where `user` = 'r13_broken_user' and `host` = 'localhost'")
	mustExec(c, se, `CREATE USER 'r14_san_only_pass'@'localhost' require san 'URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me1'`)
	mustExec(c, se, `CREATE USER 'r15_san_only_fail'@'localhost' require san 'URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me2'`)
	mustExec(c, se, "flush privileges")

	defer func() {
		c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
		mustExec(c, se, "drop user 'r1'@'localhost'")
		mustExec(c, se, "drop user 'r2'@'localhost'")
		mustExec(c, se, "drop user 'r3'@'localhost'")
		mustExec(c, se, "drop user 'r4'@'localhost'")
		mustExec(c, se, "drop user 'r5'@'localhost'")
		mustExec(c, se, "drop user 'r6'@'localhost'")
		mustExec(c, se, "drop user 'r7_issuer_only'@'localhost'")
		mustExec(c, se, "drop user 'r8_subject_only'@'localhost'")
		mustExec(c, se, "drop user 'r9_subject_disorder'@'localhost'")
		mustExec(c, se, "drop user 'r10_issuer_disorder'@'localhost'")
		mustExec(c, se, "drop user 'r11_cipher_only'@'localhost'")
		mustExec(c, se, "drop user 'r12_old_tidb_user'@'localhost'")
		mustExec(c, se, "drop user 'r13_broken_user'@'localhost'")
		mustExec(c, se, "drop user 'r14_san_only_pass'@'localhost'")
		mustExec(c, se, "drop user 'r15_san_only_fail'@'localhost'")
	}()

	// test without ssl or ca
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)

	// test use ssl without ca
	se.GetSessionVars().TLSConnectionState = &tls.ConnectionState{VerifiedChains: nil}
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)

	// test use ssl with signed but info wrong ca.
	se.GetSessionVars().TLSConnectionState = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)

	// test a all pass case
	se.GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "US"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "ZH"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Haidian"),
				util.MockPkixAttribute(util.Organization, "PingCAP.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256, func(cert *x509.Certificate) {
			var url url.URL
			err := url.UnmarshalBinary([]byte("spiffe://mesh.pingcap.com/ns/timesh/sa/me1"))
			c.Assert(err, IsNil)
			cert.URIs = append(cert.URIs, &url)
		})
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r14_san_only_pass", Hostname: "localhost"}, nil, nil), IsTrue)

	// test require but give nothing
	se.GetSessionVars().TLSConnectionState = nil
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)

	// test mismatch cipher
	se.GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "US"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "ZH"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Haidian"),
				util.MockPkixAttribute(util.Organization, "PingCAP.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_256_GCM_SHA384)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r6", Hostname: "localhost"}, nil, nil), IsTrue) // not require cipher
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r11_cipher_only", Hostname: "localhost"}, nil, nil), IsTrue)

	// test only subject or only issuer
	se.GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "US"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "AZ"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Shijingshang"),
				util.MockPkixAttribute(util.Organization, "CAPPing.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester2"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r7_issuer_only", Hostname: "localhost"}, nil, nil), IsTrue)
	se.GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "AU"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin2"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "ZH"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Haidian"),
				util.MockPkixAttribute(util.Organization, "PingCAP.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r8_subject_only", Hostname: "localhost"}, nil, nil), IsTrue)

	// test disorder issuer or subject
	se.GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "ZH"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Haidian"),
				util.MockPkixAttribute(util.Organization, "PingCAP.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r9_subject_disorder", Hostname: "localhost"}, nil, nil), IsFalse)
	se.GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "US"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{},
		},
		tls.TLS_AES_128_GCM_SHA256)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r10_issuer_disorder", Hostname: "localhost"}, nil, nil), IsFalse)

	// test mismatch san
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r15_san_only_fail", Hostname: "localhost"}, nil, nil), IsFalse)

	// test old data and broken data
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r12_old_tidb_user", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r13_broken_user", Hostname: "localhost"}, nil, nil), IsFalse)

}

func connectionState(issuer, subject pkix.Name, cipher uint16, opt ...func(c *x509.Certificate)) *tls.ConnectionState {
	cert := &x509.Certificate{Issuer: issuer, Subject: subject}
	for _, o := range opt {
		o(cert)
	}
	return &tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{cert}},
		CipherSuite:    cipher,
	}
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

	se2 := newSession(c, s.store, s.dbName)
	mustExec(c, se2, "create role 'r1'@'localhost'")
	mustExec(c, se2, "create role 'r2'@'localhost'")
	mustExec(c, se2, "create role 'r3@example.com'@'localhost'")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3@example.com", Hostname: "localhost"}, nil, nil), IsFalse)

	mustExec(c, se1, "drop user 'r1'@'localhost'")
	mustExec(c, se1, "drop user 'r2'@'localhost'")
	mustExec(c, se1, "drop user 'r3@example.com'@'localhost'")
}

func (s *testPrivilegeSuite) TestUseDB(c *C) {

	se := newSession(c, s.store, s.dbName)
	// high privileged user
	mustExec(c, se, "CREATE USER 'usesuper'")
	mustExec(c, se, "CREATE USER 'usenobody'")
	mustExec(c, se, "GRANT ALL ON *.* TO 'usesuper'")
	// without grant option
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil), IsTrue)
	_, e := se.ExecuteInternal(context.Background(), "GRANT SELECT ON mysql.* TO 'usenobody'")
	c.Assert(e, NotNil)
	// with grant option
	se = newSession(c, s.store, s.dbName)
	// high privileged user
	mustExec(c, se, "GRANT ALL ON *.* TO 'usesuper' WITH GRANT OPTION")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "use mysql")
	// low privileged user
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usenobody", Hostname: "localhost", AuthUsername: "usenobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), "use mysql")
	c.Assert(err, NotNil)

	// try again after privilege granted
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "GRANT SELECT ON mysql.* TO 'usenobody'")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usenobody", Hostname: "localhost", AuthUsername: "usenobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "use mysql")
	c.Assert(err, IsNil)

	// test `use db` for role.
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, `CREATE DATABASE app_db`)
	mustExec(c, se, `CREATE ROLE 'app_developer'`)
	mustExec(c, se, `GRANT ALL ON app_db.* TO 'app_developer'`)
	mustExec(c, se, `CREATE USER 'dev'@'localhost'`)
	mustExec(c, se, `GRANT 'app_developer' TO 'dev'@'localhost'`)
	mustExec(c, se, `SET DEFAULT ROLE 'app_developer' TO 'dev'@'localhost'`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "dev", Hostname: "localhost", AuthUsername: "dev", AuthHostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "use app_db")
	c.Assert(err, IsNil)
	_, err = se.ExecuteInternal(context.Background(), "use mysql")
	c.Assert(err, NotNil)
}

func (s *testPrivilegeSuite) TestRevokePrivileges(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, "CREATE USER 'hasgrant'")
	mustExec(c, se, "CREATE USER 'withoutgrant'")
	mustExec(c, se, "GRANT ALL ON *.* TO 'hasgrant'")
	mustExec(c, se, "GRANT ALL ON mysql.* TO 'withoutgrant'")
	// Without grant option
	c.Assert(se.Auth(&auth.UserIdentity{Username: "hasgrant", Hostname: "localhost", AuthUsername: "hasgrant", AuthHostname: "%"}, nil, nil), IsTrue)
	_, e := se.ExecuteInternal(context.Background(), "REVOKE SELECT ON mysql.* FROM 'withoutgrant'")
	c.Assert(e, NotNil)
	// With grant option
	se = newSession(c, s.store, s.dbName)
	mustExec(c, se, "GRANT ALL ON *.* TO 'hasgrant' WITH GRANT OPTION")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "hasgrant", Hostname: "localhost", AuthUsername: "hasgrant", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "REVOKE SELECT ON mysql.* FROM 'withoutgrant'")
	mustExec(c, se, "REVOKE ALL ON mysql.* FROM withoutgrant")

	// For issue https://github.com/pingcap/tidb/issues/23850
	mustExec(c, se, "CREATE USER u4")
	mustExec(c, se, "GRANT ALL ON *.* TO u4 WITH GRANT OPTION")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost", AuthUsername: "u4", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "REVOKE ALL ON *.* FROM CURRENT_USER()")
}

func (s *testPrivilegeSuite) TestSetGlobal(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER setglobal_a@localhost`)
	mustExec(c, se, `CREATE USER setglobal_b@localhost`)
	mustExec(c, se, `GRANT SUPER ON *.* to setglobal_a@localhost`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "setglobal_a", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `set global innodb_commit_concurrency=16`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "setglobal_b", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), `set global innodb_commit_concurrency=16`)
	c.Assert(terror.ErrorEqual(err, core.ErrSpecificAccessDenied), IsTrue)
}

func (s *testPrivilegeSuite) TestCreateDropUser(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER tcd1, tcd2`)
	mustExec(c, se, `GRANT ALL ON *.* to tcd2 WITH GRANT OPTION`)

	// should fail
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthUsername: "tcd1", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), `CREATE USER acdc`)
	c.Assert(terror.ErrorEqual(err, core.ErrSpecificAccessDenied), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), `DROP USER tcd2`)
	c.Assert(terror.ErrorEqual(err, core.ErrSpecificAccessDenied), IsTrue)

	// should pass
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd2", Hostname: "localhost", AuthUsername: "tcd2", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, `DROP USER tcd1`)
	mustExec(c, se, `CREATE USER tcd1`)

	// should pass
	mustExec(c, se, `GRANT tcd2 TO tcd1`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthUsername: "tcd1", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, `SET ROLE tcd2;`)
	mustExec(c, se, `CREATE USER tcd3`)
	mustExec(c, se, `DROP USER tcd3`)
}

func (s *testPrivilegeSuite) TestConfigPrivilege(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `DROP USER IF EXISTS tcd1`)
	mustExec(c, se, `CREATE USER tcd1`)
	mustExec(c, se, `GRANT ALL ON *.* to tcd1`)
	mustExec(c, se, `DROP USER IF EXISTS tcd2`)
	mustExec(c, se, `CREATE USER tcd2`)
	mustExec(c, se, `GRANT ALL ON *.* to tcd2`)
	mustExec(c, se, `REVOKE CONFIG ON *.* FROM tcd2`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthHostname: "tcd1", AuthUsername: "%"}, nil, nil), IsTrue)
	mustExec(c, se, `SHOW CONFIG`)
	mustExec(c, se, `SET CONFIG TIKV testkey="testval"`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd2", Hostname: "localhost", AuthHostname: "tcd2", AuthUsername: "%"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), `SHOW CONFIG`)
	c.Assert(err, ErrorMatches, ".*you need \\(at least one of\\) the CONFIG privilege\\(s\\) for this operation")
	_, err = se.ExecuteInternal(context.Background(), `SET CONFIG TIKV testkey="testval"`)
	c.Assert(err, ErrorMatches, ".*you need \\(at least one of\\) the CONFIG privilege\\(s\\) for this operation")
	mustExec(c, se, `DROP USER tcd1, tcd2`)
}

func (s *testPrivilegeSuite) TestShowCreateTable(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER tsct1, tsct2`)
	mustExec(c, se, `GRANT select ON mysql.* to tsct2`)

	// should fail
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tsct1", Hostname: "localhost", AuthUsername: "tsct1", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), `SHOW CREATE TABLE mysql.user`)
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)

	// should pass
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tsct2", Hostname: "localhost", AuthUsername: "tsct2", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, `SHOW CREATE TABLE mysql.user`)
}

func (s *testPrivilegeSuite) TestReplaceAndInsertOnDuplicate(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER tr_insert`)
	mustExec(c, se, `CREATE USER tr_update`)
	mustExec(c, se, `CREATE USER tr_delete`)
	mustExec(c, se, `CREATE TABLE t1 (a int primary key, b int)`)
	mustExec(c, se, `GRANT INSERT ON t1 TO tr_insert`)
	mustExec(c, se, `GRANT UPDATE ON t1 TO tr_update`)
	mustExec(c, se, `GRANT DELETE ON t1 TO tr_delete`)

	// Restrict the permission to INSERT only.
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tr_insert", Hostname: "localhost", AuthUsername: "tr_insert", AuthHostname: "%"}, nil, nil), IsTrue)

	// REPLACE requires INSERT + DELETE privileges, having INSERT alone is insufficient.
	_, err := se.ExecuteInternal(context.Background(), `REPLACE INTO t1 VALUES (1, 2)`)
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)
	c.Assert(err.Error(), Equals, "[planner:1142]DELETE command denied to user 'tr_insert'@'%' for table 't1'")

	// INSERT ON DUPLICATE requires INSERT + UPDATE privileges, having INSERT alone is insufficient.
	_, err = se.ExecuteInternal(context.Background(), `INSERT INTO t1 VALUES (3, 4) ON DUPLICATE KEY UPDATE b = 5`)
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)
	c.Assert(err.Error(), Equals, "[planner:1142]UPDATE command denied to user 'tr_insert'@'%' for table 't1'")

	// Plain INSERT should work.
	mustExec(c, se, `INSERT INTO t1 VALUES (6, 7)`)

	// Also check that having DELETE alone is insufficient for REPLACE.
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tr_delete", Hostname: "localhost", AuthUsername: "tr_delete", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), `REPLACE INTO t1 VALUES (8, 9)`)
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)
	c.Assert(err.Error(), Equals, "[planner:1142]INSERT command denied to user 'tr_delete'@'%' for table 't1'")

	// Also check that having UPDATE alone is insufficient for INSERT ON DUPLICATE.
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tr_update", Hostname: "localhost", AuthUsername: "tr_update", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), `INSERT INTO t1 VALUES (10, 11) ON DUPLICATE KEY UPDATE b = 12`)
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)
	c.Assert(err.Error(), Equals, "[planner:1142]INSERT command denied to user 'tr_update'@'%' for table 't1'")
}

func (s *testPrivilegeSuite) TestAnalyzeTable(c *C) {

	se := newSession(c, s.store, s.dbName)
	// high privileged user
	mustExec(c, se, "CREATE USER 'asuper'")
	mustExec(c, se, "CREATE USER 'anobody'")
	mustExec(c, se, "GRANT ALL ON *.* TO 'asuper' WITH GRANT OPTION")
	mustExec(c, se, "CREATE DATABASE atest")
	mustExec(c, se, "use atest")
	mustExec(c, se, "CREATE TABLE t1 (a int)")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "analyze table mysql.user")
	// low privileged user
	c.Assert(se.Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), "analyze table t1")
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)
	c.Assert(err.Error(), Equals, "[planner:1142]INSERT command denied to user 'anobody'@'%' for table 't1'")

	_, err = se.ExecuteInternal(context.Background(), "select * from t1")
	c.Assert(err.Error(), Equals, "[planner:1142]SELECT command denied to user 'anobody'@'%' for table 't1'")

	// try again after SELECT privilege granted
	c.Assert(se.Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "GRANT SELECT ON atest.* TO 'anobody'")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "analyze table t1")
	c.Assert(terror.ErrorEqual(err, core.ErrTableaccessDenied), IsTrue)
	c.Assert(err.Error(), Equals, "[planner:1142]INSERT command denied to user 'anobody'@'%' for table 't1'")
	// Add INSERT privilege and it should work.
	c.Assert(se.Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se, "GRANT INSERT ON atest.* TO 'anobody'")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "analyze table t1")
	c.Assert(err, IsNil)

}

func (s *testPrivilegeSuite) TestSystemSchema(c *C) {
	// This test tests no privilege check for INFORMATION_SCHEMA database.
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'u1'@'localhost';`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `select * from information_schema.tables`)
	mustExec(c, se, `select * from information_schema.key_column_usage`)
	_, err := se.ExecuteInternal(context.Background(), "create table information_schema.t(a int)")
	c.Assert(strings.Contains(err.Error(), "denied to user"), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "drop table information_schema.tables")
	c.Assert(strings.Contains(err.Error(), "denied to user"), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "update information_schema.tables set table_name = 'tst' where table_name = 'mysql'")
	c.Assert(strings.Contains(err.Error(), "privilege check"), IsTrue)

	// Test performance_schema.
	mustExec(c, se, `select * from performance_schema.events_statements_summary_by_digest`)
	_, err = se.ExecuteInternal(context.Background(), "drop table performance_schema.events_statements_summary_by_digest")
	c.Assert(strings.Contains(err.Error(), "denied to user"), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "update performance_schema.events_statements_summary_by_digest set schema_name = 'tst'")
	c.Assert(strings.Contains(err.Error(), "privilege check"), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "delete from performance_schema.events_statements_summary_by_digest")
	c.Assert(strings.Contains(err.Error(), "DELETE command denied to user"), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "create table performance_schema.t(a int)")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "CREATE command denied"), IsTrue, Commentf(err.Error()))

	// Test metric_schema.
	mustExec(c, se, `select * from metrics_schema.tidb_query_duration`)
	_, err = se.ExecuteInternal(context.Background(), "drop table metrics_schema.tidb_query_duration")
	c.Assert(strings.Contains(err.Error(), "denied to user"), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "update metrics_schema.tidb_query_duration set instance = 'tst'")
	c.Assert(strings.Contains(err.Error(), "privilege check"), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "delete from metrics_schema.tidb_query_duration")
	c.Assert(strings.Contains(err.Error(), "DELETE command denied to user"), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "create table metric_schema.t(a int)")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "CREATE command denied"), IsTrue, Commentf(err.Error()))
}

func (s *testPrivilegeSuite) TestAdminCommand(c *C) {
	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `CREATE USER 'test_admin'@'localhost';`)
	mustExec(c, se, `CREATE TABLE t(a int)`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_admin", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), "ADMIN SHOW DDL JOBS")
	c.Assert(strings.Contains(err.Error(), "privilege check"), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "ADMIN CHECK TABLE t")
	c.Assert(strings.Contains(err.Error(), "privilege check"), IsTrue)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "ADMIN SHOW DDL JOBS")
	c.Assert(err, IsNil)
}

func (s *testPrivilegeSuite) TestTableNotExistNoPermissions(c *C) {
	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `CREATE USER 'testnotexist'@'localhost';`)
	mustExec(c, se, `CREATE DATABASE dbexists`)
	mustExec(c, se, `CREATE TABLE dbexists.t1 (a int)`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "testnotexist", Hostname: "localhost"}, nil, nil), IsTrue)

	tests := []struct {
		stmt     string
		stmtType string
	}{
		{
			"SELECT * FROM %s.%s",
			"SELECT",
		},
		{
			"SHOW CREATE TABLE %s.%s",
			"SHOW",
		},
		{
			"DELETE FROM %s.%s WHERE a=0",
			"DELETE",
		},
		{
			"DELETE FROM %s.%s",
			"DELETE",
		},
	}

	for _, t := range tests {

		_, err1 := se.ExecuteInternal(context.Background(), fmt.Sprintf(t.stmt, "dbexists", "t1"))
		_, err2 := se.ExecuteInternal(context.Background(), fmt.Sprintf(t.stmt, "dbnotexists", "t1"))

		// Check the error is the same whether table exists or not.
		c.Assert(terror.ErrorEqual(err1, err2), IsTrue)

		// Check it is permission denied, not not found.
		c.Assert(err2.Error(), Equals, fmt.Sprintf("[planner:1142]%s command denied to user 'testnotexist'@'localhost' for table 't1'", t.stmtType))

	}

}

func (s *testPrivilegeSuite) TestLoadDataPrivilege(c *C) {
	// Create file.
	path := "/tmp/load_data_priv.csv"
	fp, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)
	defer func() {
		err = fp.Close()
		c.Assert(err, IsNil)
		err = os.Remove(path)
		c.Assert(err, IsNil)
	}()
	_, err = fp.WriteString("1\n")
	c.Assert(err, IsNil)

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `CREATE USER 'test_load'@'localhost';`)
	mustExec(c, se, `CREATE TABLE t_load(a int)`)
	mustExec(c, se, `GRANT SELECT on *.* to 'test_load'@'localhost'`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' INTO TABLE t_load")
	c.Assert(strings.Contains(err.Error(), "INSERT command denied to user 'test_load'@'localhost' for table 't_load'"), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, se, `GRANT INSERT on *.* to 'test_load'@'localhost'`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.ExecuteInternal(context.Background(), "LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' INTO TABLE t_load")
	c.Assert(err, IsNil)
}

func (s *testPrivilegeSuite) TestSelectIntoNoPremissions(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'nofile'@'localhost';`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "nofile", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.ExecuteInternal(context.Background(), `select 1 into outfile '/tmp/doesntmatter-no-permissions'`)
	message := "Access denied; you need (at least one of) the FILE privilege(s) for this operation"
	c.Assert(strings.Contains(err.Error(), message), IsTrue)
}

func (s *testPrivilegeSuite) TestGetEncodedPassword(c *C) {
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'test_encode_u'@'localhost' identified by 'root';`)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.GetEncodedPassword("test_encode_u", "localhost"), Equals, "*81F5E21E35407D884A6CD4A731AEBFB6AF209E1B")
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
	_, err := se.ExecuteInternal(context.Background(), "create user test_auth_host_a")
	c.Assert(err, NotNil)

	mustExec(c, rootSe, "DROP USER 'test_auth_host'@'192.168.%';")
	mustExec(c, rootSe, "DROP USER 'test_auth_host'@'%';")
}

func (s *testPrivilegeSuite) TestDefaultRoles(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, `CREATE USER 'testdefault'@'localhost';`)
	mustExec(c, rootSe, `CREATE ROLE 'testdefault_r1'@'localhost', 'testdefault_r2'@'localhost';`)
	mustExec(c, rootSe, `GRANT 'testdefault_r1'@'localhost', 'testdefault_r2'@'localhost' TO 'testdefault'@'localhost';`)

	se := newSession(c, s.store, s.dbName)
	pc := privilege.GetPrivilegeManager(se)

	ret := pc.GetDefaultRoles("testdefault", "localhost")
	c.Assert(len(ret), Equals, 0)

	mustExec(c, rootSe, `SET DEFAULT ROLE ALL TO 'testdefault'@'localhost';`)
	mustExec(c, rootSe, `flush privileges;`)
	ret = pc.GetDefaultRoles("testdefault", "localhost")
	c.Assert(len(ret), Equals, 2)

	mustExec(c, rootSe, `SET DEFAULT ROLE NONE TO 'testdefault'@'localhost';`)
	mustExec(c, rootSe, `flush privileges;`)
	ret = pc.GetDefaultRoles("testdefault", "localhost")
	c.Assert(len(ret), Equals, 0)
}

func (s *testPrivilegeSuite) TestUserTableConsistency(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create user superadmin")
	tk.MustExec("grant all privileges on *.* to 'superadmin'")

	// GrantPriv is not in AllGlobalPrivs any more, see pingcap/parser#581
	c.Assert(len(mysql.Priv2UserCol), Equals, len(mysql.AllGlobalPrivs)+1)

	var buf bytes.Buffer
	var res bytes.Buffer
	buf.WriteString("select ")
	i := 0
	for _, priv := range mysql.AllGlobalPrivs {
		if i != 0 {
			buf.WriteString(", ")
			res.WriteString(" ")
		}
		buf.WriteString(mysql.Priv2UserCol[priv])
		res.WriteString("Y")
		i++
	}
	buf.WriteString(" from mysql.user where user = 'superadmin'")
	tk.MustQuery(buf.String()).Check(testkit.Rows(res.String()))
}

func (s *testPrivilegeSuite) TestFieldList(c *C) { // Issue #14237 List fields RPC
	se := newSession(c, s.store, s.dbName)
	mustExec(c, se, `CREATE USER 'tableaccess'@'localhost'`)
	mustExec(c, se, `CREATE TABLE fieldlistt1 (a int)`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tableaccess", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.FieldList("fieldlistt1")
	message := "SELECT command denied to user 'tableaccess'@'localhost' for table 'fieldlistt1'"
	c.Assert(strings.Contains(err.Error(), message), IsTrue)
}

func mustExec(c *C, se session.Session, sql string) {
	_, err := se.ExecuteInternal(context.Background(), sql)
	c.Assert(err, IsNil)
}

func newStore(c *C, dbPath string) (*domain.Domain, kv.Storage) {
	store, err := mockstore.NewMockStore()
	session.SetSchemaLease(0)
	session.DisableStats4Test()
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

func (s *testPrivilegeSuite) TestDynamicPrivs(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, "CREATE USER notsuper")
	mustExec(c, rootSe, "CREATE USER otheruser")
	mustExec(c, rootSe, "CREATE ROLE anyrolename")

	se := newSession(c, s.store, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "notsuper", Hostname: "%"}, nil, nil), IsTrue)

	// test SYSTEM_VARIABLES_ADMIN
	_, err := se.ExecuteInternal(context.Background(), "SET GLOBAL wait_timeout = 86400")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	mustExec(c, rootSe, "GRANT SYSTEM_VARIABLES_admin ON *.* TO notsuper")
	mustExec(c, se, "SET GLOBAL wait_timeout = 86400")

	// test ROLE_ADMIN
	_, err = se.ExecuteInternal(context.Background(), "GRANT anyrolename TO otheruser")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SUPER or ROLE_ADMIN privilege(s) for this operation")
	mustExec(c, rootSe, "GRANT ROLE_ADMIN ON *.* TO notsuper")
	mustExec(c, se, "GRANT anyrolename TO otheruser")

	// revoke SYSTEM_VARIABLES_ADMIN, confirm it is dropped
	mustExec(c, rootSe, "REVOKE SYSTEM_VARIABLES_AdmIn ON *.* FROM notsuper")
	_, err = se.ExecuteInternal(context.Background(), "SET GLOBAL wait_timeout = 86000")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")

	// grant super, confirm that it is also a substitute for SYSTEM_VARIABLES_ADMIN
	mustExec(c, rootSe, "GRANT SUPER ON *.* TO notsuper")
	mustExec(c, se, "SET GLOBAL wait_timeout = 86400")

	// revoke SUPER, assign SYSTEM_VARIABLES_ADMIN to anyrolename.
	// confirm that a dynamic privilege can be inherited from a role.
	mustExec(c, rootSe, "REVOKE SUPER ON *.* FROM notsuper")
	mustExec(c, rootSe, "GRANT SYSTEM_VARIABLES_AdmIn ON *.* TO anyrolename")
	mustExec(c, rootSe, "GRANT anyrolename TO notsuper")

	// It's not a default role, this should initially fail:
	_, err = se.ExecuteInternal(context.Background(), "SET GLOBAL wait_timeout = 86400")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	mustExec(c, se, "SET ROLE anyrolename")
	mustExec(c, se, "SET GLOBAL wait_timeout = 87000")
}

func (s *testPrivilegeSuite) TestDynamicGrantOption(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, "CREATE USER varuser1")
	mustExec(c, rootSe, "CREATE USER varuser2")
	mustExec(c, rootSe, "CREATE USER varuser3")

	mustExec(c, rootSe, "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser1")
	mustExec(c, rootSe, "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser2 WITH GRANT OPTION")

	se1 := newSession(c, s.store, s.dbName)

	c.Assert(se1.Auth(&auth.UserIdentity{Username: "varuser1", Hostname: "%"}, nil, nil), IsTrue)
	_, err := se1.ExecuteInternal(context.Background(), "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser3")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the GRANT OPTION privilege(s) for this operation")

	se2 := newSession(c, s.store, s.dbName)

	c.Assert(se2.Auth(&auth.UserIdentity{Username: "varuser2", Hostname: "%"}, nil, nil), IsTrue)
	mustExec(c, se2, "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser3")
}

func (s *testPrivilegeSuite) TestSecurityEnhancedModeRestrictedTables(c *C) {
	// This provides an integration test of the tests in util/security/security_test.go
	cloudAdminSe := newSession(c, s.store, s.dbName)
	mustExec(c, cloudAdminSe, "CREATE USER cloudadmin")
	mustExec(c, cloudAdminSe, "GRANT RESTRICTED_TABLES_ADMIN, SELECT ON *.* to cloudadmin")
	mustExec(c, cloudAdminSe, "GRANT CREATE ON mysql.* to cloudadmin")
	mustExec(c, cloudAdminSe, "CREATE USER uroot")
	mustExec(c, cloudAdminSe, "GRANT ALL ON *.* to uroot WITH GRANT OPTION") // A "MySQL" all powerful user.
	c.Assert(cloudAdminSe.Auth(&auth.UserIdentity{Username: "cloudadmin", Hostname: "%"}, nil, nil), IsTrue)
	urootSe := newSession(c, s.store, s.dbName)
	c.Assert(urootSe.Auth(&auth.UserIdentity{Username: "uroot", Hostname: "%"}, nil, nil), IsTrue)

	sem.Enable()
	defer sem.Disable()

	_, err := urootSe.ExecuteInternal(context.Background(), "use metrics_schema")
	c.Assert(err.Error(), Equals, "[executor:1044]Access denied for user 'uroot'@'%' to database 'metrics_schema'")

	_, err = urootSe.ExecuteInternal(context.Background(), "SELECT * FROM metrics_schema.uptime")
	c.Assert(err.Error(), Equals, "[planner:1142]SELECT command denied to user 'uroot'@'%' for table 'uptime'")

	_, err = urootSe.ExecuteInternal(context.Background(), "CREATE TABLE mysql.abcd (a int)")
	c.Assert(err.Error(), Equals, "[planner:1142]CREATE command denied to user 'uroot'@'%' for table 'abcd'")

	mustExec(c, cloudAdminSe, "USE metrics_schema")
	mustExec(c, cloudAdminSe, "SELECT * FROM metrics_schema.uptime")
	mustExec(c, cloudAdminSe, "CREATE TABLE mysql.abcd (a int)")
}

func (s *testPrivilegeSuite) TestSecurityEnhancedModeInfoschema(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER uroot1, uroot2, uroot3")
	tk.MustExec("GRANT SUPER ON *.* to uroot1 WITH GRANT OPTION") // super not process
	tk.MustExec("GRANT SUPER, PROCESS, RESTRICTED_TABLES_ADMIN ON *.* to uroot2 WITH GRANT OPTION")
	tk.Se.Auth(&auth.UserIdentity{
		Username: "uroot1",
		Hostname: "localhost",
	}, nil, nil)

	sem.Enable()
	defer sem.Disable()

	// Even though we have super, we still can't read protected information from tidb_servers_info, cluster_* tables
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.tidb_servers_info WHERE ip IS NOT NULL`).Check(testkit.Rows("0"))
	err := tk.QueryToErr(`SELECT COUNT(*) FROM information_schema.cluster_info WHERE status_address IS NOT NULL`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	// That is unless we have the RESTRICTED_TABLES_ADMIN privilege
	tk.Se.Auth(&auth.UserIdentity{
		Username: "uroot2",
		Hostname: "localhost",
	}, nil, nil)

	// flip from is NOT NULL etc
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.tidb_servers_info WHERE ip IS NULL`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.cluster_info WHERE status_address IS NULL`).Check(testkit.Rows("0"))
}

func (s *testPrivilegeSuite) TestClusterConfigInfoschema(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER ccnobody, ccconfig, ccprocess")
	tk.MustExec("GRANT CONFIG ON *.* TO ccconfig")
	tk.MustExec("GRANT Process ON *.* TO ccprocess")

	// incorrect/no permissions
	tk.Se.Auth(&auth.UserIdentity{
		Username: "ccnobody",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows("GRANT USAGE ON *.* TO 'ccnobody'@'%'"))

	err := tk.QueryToErr("SELECT * FROM information_schema.cluster_config")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CONFIG privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_hardware")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CONFIG privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_info")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_load")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_systeminfo")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_log WHERE time BETWEEN '2021-07-13 00:00:00' AND '2021-07-13 02:00:00' AND message like '%'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	// With correct/CONFIG permissions
	tk.Se.Auth(&auth.UserIdentity{
		Username: "ccconfig",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows("GRANT CONFIG ON *.* TO 'ccconfig'@'%'"))
	// Needs CONFIG privilege
	tk.MustQuery("SELECT * FROM information_schema.cluster_config")
	tk.MustQuery("SELECT * FROM information_schema.cluster_HARDWARE")
	// Missing Process privilege
	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_INFO")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")
	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_LOAD")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")
	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_SYSTEMINFO")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")
	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_LOG WHERE time BETWEEN '2021-07-13 00:00:00' AND '2021-07-13 02:00:00' AND message like '%'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	// With correct/Process permissions
	tk.Se.Auth(&auth.UserIdentity{
		Username: "ccprocess",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows("GRANT PROCESS ON *.* TO 'ccprocess'@'%'"))
	// Needs Process privilege
	tk.MustQuery("SELECT * FROM information_schema.CLUSTER_info")
	tk.MustQuery("SELECT * FROM information_schema.CLUSTER_load")
	tk.MustQuery("SELECT * FROM information_schema.CLUSTER_systeminfo")
	tk.MustQuery("SELECT * FROM information_schema.CLUSTER_log WHERE time BETWEEN '1970-07-13 00:00:00' AND '1970-07-13 02:00:00' AND message like '%'")
	// Missing CONFIG privilege
	err = tk.QueryToErr("SELECT * FROM information_schema.CLUSTER_config")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CONFIG privilege(s) for this operation")
	err = tk.QueryToErr("SELECT * FROM information_schema.CLUSTER_hardware")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the CONFIG privilege(s) for this operation")
}

func (s *testPrivilegeSuite) TestSecurityEnhancedModeStatusVars(c *C) {
	// Without TiKV the status var list does not include tidb_gc_leader_desc
	// So we can only test that the dynamic privilege is grantable.
	// We will have to use an integration test to run SHOW STATUS LIKE 'tidb_gc_leader_desc'
	// and verify if it appears.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER unostatus, ustatus")
	tk.MustExec("GRANT RESTRICTED_STATUS_ADMIN ON *.* to ustatus")
	tk.Se.Auth(&auth.UserIdentity{
		Username: "unostatus",
		Hostname: "localhost",
	}, nil, nil)

}

func (s *testPrivilegeSuite) TestSecurityEnhancedLocalBackupRestore(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER backuprestore")
	tk.MustExec("GRANT BACKUP_ADMIN,RESTORE_ADMIN ON *.* to backuprestore")
	tk.Se.Auth(&auth.UserIdentity{
		Username: "backuprestore",
		Hostname: "localhost",
	}, nil, nil)

	// Prior to SEM nolocal has permission, the error should be because backup requires tikv
	_, err := tk.Se.ExecuteInternal(context.Background(), "BACKUP DATABASE * TO 'Local:///tmp/test';")
	c.Assert(err.Error(), Equals, "BACKUP requires tikv store, not unistore")

	_, err = tk.Se.ExecuteInternal(context.Background(), "RESTORE DATABASE * FROM 'LOCAl:///tmp/test';")
	c.Assert(err.Error(), Equals, "RESTORE requires tikv store, not unistore")

	sem.Enable()
	defer sem.Disable()

	// With SEM enabled nolocal does not have permission, but yeslocal does.
	_, err = tk.Se.ExecuteInternal(context.Background(), "BACKUP DATABASE * TO 'Local:///tmp/test';")
	c.Assert(err.Error(), Equals, "[planner:8132]Feature 'local://' is not supported when security enhanced mode is enabled")

	_, err = tk.Se.ExecuteInternal(context.Background(), "RESTORE DATABASE * FROM 'LOCAl:///tmp/test';")
	c.Assert(err.Error(), Equals, "[planner:8132]Feature 'local://' is not supported when security enhanced mode is enabled")

}

func (s *testPrivilegeSuite) TestRenameUser(c *C) {
	rootSe := newSession(c, s.store, s.dbName)
	mustExec(c, rootSe, "DROP USER IF EXISTS 'ru1'@'localhost'")
	mustExec(c, rootSe, "DROP USER IF EXISTS ru3")
	mustExec(c, rootSe, "DROP USER IF EXISTS ru6@localhost")
	mustExec(c, rootSe, "CREATE USER 'ru1'@'localhost'")
	mustExec(c, rootSe, "CREATE USER ru3")
	mustExec(c, rootSe, "CREATE USER ru6@localhost")
	se1 := newSession(c, s.store, s.dbName)
	c.Assert(se1.Auth(&auth.UserIdentity{Username: "ru1", Hostname: "localhost"}, nil, nil), IsTrue)

	// Check privileges (need CREATE USER)
	_, err := se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru4")
	c.Assert(err, ErrorMatches, ".*Access denied; you need .at least one of. the CREATE USER privilege.s. for this operation")
	mustExec(c, rootSe, "GRANT UPDATE ON mysql.user TO 'ru1'@'localhost'")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru4")
	c.Assert(err, ErrorMatches, ".*Access denied; you need .at least one of. the CREATE USER privilege.s. for this operation")
	mustExec(c, rootSe, "GRANT CREATE USER ON *.* TO 'ru1'@'localhost'")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru4")
	c.Assert(err, IsNil)

	// Test a few single rename (both Username and Hostname)
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER 'ru4'@'%' TO 'ru3'@'localhost'")
	c.Assert(err, IsNil)
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER 'ru3'@'localhost' TO 'ru3'@'%'")
	c.Assert(err, IsNil)
	// Including negative tests, i.e. non existing from user and existing to user
	_, err = rootSe.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru1@localhost")
	c.Assert(err, ErrorMatches, ".*Operation RENAME USER failed for ru3@%.*")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru4 TO ru5@localhost")
	c.Assert(err, ErrorMatches, ".*Operation RENAME USER failed for ru4@%.*")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru3")
	c.Assert(err, ErrorMatches, ".*Operation RENAME USER failed for ru3@%.*")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru5@localhost, ru4 TO ru7")
	c.Assert(err, ErrorMatches, ".*Operation RENAME USER failed for ru4@%.*")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru5@localhost, ru6@localhost TO ru1@localhost")
	c.Assert(err, ErrorMatches, ".*Operation RENAME USER failed for ru6@localhost.*")

	// Test multi rename, this is a full swap of ru3 and ru6, i.e. need to read its previous state in the same transaction.
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER 'ru3' TO 'ru3_tmp', ru6@localhost TO ru3, 'ru3_tmp' to ru6@localhost")
	c.Assert(err, IsNil)

	// Cleanup
	mustExec(c, rootSe, "DROP USER ru6@localhost")
	mustExec(c, rootSe, "DROP USER ru3")
	mustExec(c, rootSe, "DROP USER 'ru1'@'localhost'")
}

func (s *testPrivilegeSuite) TestSecurityEnhancedModeSysVars(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER svroot1, svroot2")
	tk.MustExec("GRANT SUPER ON *.* to svroot1 WITH GRANT OPTION")
	tk.MustExec("GRANT SUPER, RESTRICTED_VARIABLES_ADMIN ON *.* to svroot2")

	sem.Enable()
	defer sem.Disable()

	// svroot1 has SUPER but in SEM will be restricted
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "svroot1",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil)

	tk.MustQuery(`SHOW VARIABLES LIKE 'tidb_force_priority'`).Check(testkit.Rows())
	tk.MustQuery(`SHOW GLOBAL VARIABLES LIKE 'tidb_enable_telemetry'`).Check(testkit.Rows())

	_, err := tk.Exec("SET tidb_force_priority = 'NO_PRIORITY'")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")
	_, err = tk.Exec("SET GLOBAL tidb_enable_telemetry = OFF")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")

	_, err = tk.Exec("SELECT @@session.tidb_force_priority")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")
	_, err = tk.Exec("SELECT @@global.tidb_enable_telemetry")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")

	tk.Se.Auth(&auth.UserIdentity{
		Username:     "svroot2",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil)

	tk.MustQuery(`SHOW VARIABLES LIKE 'tidb_force_priority'`).Check(testkit.Rows("tidb_force_priority NO_PRIORITY"))
	tk.MustQuery(`SHOW GLOBAL VARIABLES LIKE 'tidb_enable_telemetry'`).Check(testkit.Rows("tidb_enable_telemetry ON"))

	// should not actually make any change.
	tk.MustExec("SET tidb_force_priority = 'NO_PRIORITY'")
	tk.MustExec("SET GLOBAL tidb_enable_telemetry = ON")

	tk.MustQuery(`SELECT @@session.tidb_force_priority`).Check(testkit.Rows("NO_PRIORITY"))
	tk.MustQuery(`SELECT @@global.tidb_enable_telemetry`).Check(testkit.Rows("1"))
}

// TestViewDefiner tests that default roles are correctly applied in the algorithm definer
// See: https://github.com/pingcap/tidb/issues/24414
func (s *testPrivilegeSuite) TestViewDefiner(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE DATABASE issue24414")
	tk.MustExec("USE issue24414")
	tk.MustExec(`create table table1(
		col1 int,
		col2 int,
		col3 int
		)`)
	tk.MustExec(`insert into table1 values (1,1,1),(2,2,2)`)
	tk.MustExec(`CREATE ROLE 'ACL-mobius-admin'`)
	tk.MustExec(`CREATE USER 'mobius-admin'`)
	tk.MustExec(`CREATE USER 'mobius-admin-no-role'`)
	tk.MustExec(`GRANT Select,Insert,Update,Delete,Create,Drop,Alter,Index,Create View,Show View ON issue24414.* TO 'ACL-mobius-admin'@'%'`)
	tk.MustExec(`GRANT Select,Insert,Update,Delete,Create,Drop,Alter,Index,Create View,Show View ON issue24414.* TO 'mobius-admin-no-role'@'%'`)
	tk.MustExec(`GRANT 'ACL-mobius-admin'@'%' to 'mobius-admin'@'%'`)
	tk.MustExec(`SET DEFAULT ROLE ALL TO 'mobius-admin'`)
	// create tables
	tk.MustExec(`CREATE ALGORITHM = UNDEFINED DEFINER = 'mobius-admin'@'127.0.0.1' SQL SECURITY DEFINER VIEW test_view (col1 , col2 , col3) AS SELECT * from table1`)
	tk.MustExec(`CREATE ALGORITHM = UNDEFINED DEFINER = 'mobius-admin-no-role'@'127.0.0.1' SQL SECURITY DEFINER VIEW test_view2 (col1 , col2 , col3) AS SELECT * from table1`)

	// all examples should work
	tk.MustExec("select * from test_view")
	tk.MustExec("select * from test_view2")
}

func (s *testPrivilegeSuite) TestSecurityEnhancedModeRestrictedUsers(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER ruroot1, ruroot2, ruroot3")
	tk.MustExec("CREATE ROLE notimportant")
	tk.MustExec("GRANT SUPER, CREATE USER ON *.* to ruroot1 WITH GRANT OPTION")
	tk.MustExec("GRANT SUPER, RESTRICTED_USER_ADMIN,  CREATE USER  ON *.* to ruroot2 WITH GRANT OPTION")
	tk.MustExec("GRANT RESTRICTED_USER_ADMIN ON *.* to ruroot3")
	tk.MustExec("GRANT notimportant TO ruroot2, ruroot3")

	sem.Enable()
	defer sem.Disable()

	stmts := []string{
		"SET PASSWORD for ruroot3 = 'newpassword'",
		"REVOKE notimportant FROM ruroot3",
		"REVOKE SUPER ON *.* FROM ruroot3",
		"DROP USER ruroot3",
	}

	// ruroot1 has SUPER but in SEM will be restricted
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "ruroot1",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil)

	for _, stmt := range stmts {
		err := tk.ExecToErr(stmt)
		c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_USER_ADMIN privilege(s) for this operation")
	}

	// Switch to ruroot2, it should be permitted
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "ruroot2",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil)

	for _, stmt := range stmts {
		err := tk.ExecToErr(stmt)
		c.Assert(err, IsNil)
	}
}

func (s *testPrivilegeSuite) TestDynamicPrivsRegistration(c *C) {
	se := newSession(c, s.store, s.dbName)
	pm := privilege.GetPrivilegeManager(se)

	count := len(privileges.GetDynamicPrivileges())

	c.Assert(pm.IsDynamicPrivilege("ACDC_ADMIN"), IsFalse)
	c.Assert(privileges.RegisterDynamicPrivilege("ACDC_ADMIN"), IsNil)
	c.Assert(pm.IsDynamicPrivilege("ACDC_ADMIN"), IsTrue)
	c.Assert(len(privileges.GetDynamicPrivileges()), Equals, count+1)

	c.Assert(pm.IsDynamicPrivilege("iAmdynamIC"), IsFalse)
	c.Assert(privileges.RegisterDynamicPrivilege("IAMdynamic"), IsNil)
	c.Assert(pm.IsDynamicPrivilege("IAMdyNAMIC"), IsTrue)
	c.Assert(len(privileges.GetDynamicPrivileges()), Equals, count+2)

	c.Assert(privileges.RegisterDynamicPrivilege("THIS_PRIVILEGE_NAME_IS_TOO_LONG_THE_MAX_IS_32_CHARS").Error(), Equals, "privilege name is longer than 32 characters")
	c.Assert(pm.IsDynamicPrivilege("THIS_PRIVILEGE_NAME_IS_TOO_LONG_THE_MAX_IS_32_CHARS"), IsFalse)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER privassigntest")

	// Check that all privileges registered are assignable to users,
	// including the recently registered ACDC_ADMIN
	for _, priv := range privileges.GetDynamicPrivileges() {
		sqlGrant, err := sqlexec.EscapeSQL("GRANT %n ON *.* TO privassigntest", priv)
		c.Assert(err, IsNil)
		tk.MustExec(sqlGrant)
	}
	// Check that all privileges registered are revokable
	for _, priv := range privileges.GetDynamicPrivileges() {
		sqlGrant, err := sqlexec.EscapeSQL("REVOKE %n ON *.* FROM privassigntest", priv)
		c.Assert(err, IsNil)
		tk.MustExec(sqlGrant)
	}
}

func (s *testPrivilegeSuite) TestInfoschemaUserPrivileges(c *C) {
	// Being able to read all privileges from information_schema.user_privileges requires a very specific set of permissions.
	// SUPER user is not sufficient. It was observed in MySQL to require SELECT on mysql.*
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER isnobody, isroot, isselectonmysqluser, isselectonmysql")
	tk.MustExec("GRANT SUPER ON *.* TO isroot")
	tk.MustExec("GRANT SELECT ON mysql.user TO isselectonmysqluser")
	tk.MustExec("GRANT SELECT ON mysql.* TO isselectonmysql")

	// First as Nobody
	tk.Se.Auth(&auth.UserIdentity{
		Username: "isnobody",
		Hostname: "localhost",
	}, nil, nil)

	// I can see myself, but I can not see other users
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows("'isnobody'@'%' def USAGE NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows())

	// Basically the same result as as isselectonmysqluser
	tk.Se.Auth(&auth.UserIdentity{
		Username: "isselectonmysqluser",
		Hostname: "localhost",
	}, nil, nil)

	// Now as isselectonmysqluser
	// Tests discovered issue that SELECT on mysql.user is not sufficient. It must be on mysql.*
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows("'isselectonmysqluser'@'%' def USAGE NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysql'@'%'"`).Check(testkit.Rows())

	// Now as root
	tk.Se.Auth(&auth.UserIdentity{
		Username: "isroot",
		Hostname: "localhost",
	}, nil, nil)

	// I can see myself, but I can not see other users
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows("'isroot'@'%' def SUPER NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows())

	// Now as isselectonmysqluser
	tk.Se.Auth(&auth.UserIdentity{
		Username: "isselectonmysql",
		Hostname: "localhost",
	}, nil, nil)

	// Now as isselectonmysqluser
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows("'isnobody'@'%' def USAGE NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows("'isroot'@'%' def SUPER NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows("'isselectonmysqluser'@'%' def USAGE NO"))
}

// Issues https://github.com/pingcap/tidb/issues/25972 and https://github.com/pingcap/tidb/issues/26451
func (s *testPrivilegeSuite) TestGrantOptionAndRevoke(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("DROP USER IF EXISTS u1, u2, u3, ruser")
	tk.MustExec("CREATE USER u1, u2, u3, ruser")
	tk.MustExec("GRANT ALL ON *.* TO ruser WITH GRANT OPTION")
	tk.MustExec("GRANT SELECT ON *.* TO u1 WITH GRANT OPTION")
	tk.MustExec("GRANT UPDATE, DELETE on db.* TO u1")

	tk.Se.Auth(&auth.UserIdentity{
		Username: "ruser",
		Hostname: "localhost",
	}, nil, nil)

	tk.MustQuery(`SHOW GRANTS FOR u1`).Check(testkit.Rows("GRANT SELECT ON *.* TO 'u1'@'%' WITH GRANT OPTION", "GRANT UPDATE,DELETE ON db.* TO 'u1'@'%'"))

	tk.MustExec("GRANT SELECT ON d1.* to u2")
	tk.MustExec("GRANT SELECT ON d2.* to u2 WITH GRANT OPTION")
	tk.MustExec("GRANT SELECT ON d3.* to u2")
	tk.MustExec("GRANT SELECT ON d4.* to u2")
	tk.MustExec("GRANT SELECT ON d5.* to u2")
	tk.MustQuery(`SHOW GRANTS FOR u2;`).Sort().Check(testkit.Rows(
		"GRANT SELECT ON d1.* TO 'u2'@'%'",
		"GRANT SELECT ON d2.* TO 'u2'@'%' WITH GRANT OPTION",
		"GRANT SELECT ON d3.* TO 'u2'@'%'",
		"GRANT SELECT ON d4.* TO 'u2'@'%'",
		"GRANT SELECT ON d5.* TO 'u2'@'%'",
		"GRANT USAGE ON *.* TO 'u2'@'%'",
	))

	tk.MustExec("grant all on hchwang.* to u3 with grant option")
	tk.MustQuery(`SHOW GRANTS FOR u3;`).Check(testkit.Rows("GRANT USAGE ON *.* TO 'u3'@'%'", "GRANT ALL PRIVILEGES ON hchwang.* TO 'u3'@'%' WITH GRANT OPTION"))
	tk.MustExec("revoke all on hchwang.* from u3")
	tk.MustQuery(`SHOW GRANTS FOR u3;`).Check(testkit.Rows("GRANT USAGE ON *.* TO 'u3'@'%'", "GRANT USAGE ON hchwang.* TO 'u3'@'%' WITH GRANT OPTION"))

	// Same again but with column privileges.

	tk.MustExec("DROP TABLE IF EXISTS test.testgrant")
	tk.MustExec("CREATE TABLE test.testgrant (a int)")
	tk.MustExec("grant all on test.testgrant to u3 with grant option")
	tk.MustExec("revoke all on test.testgrant from u3")
	tk.MustQuery(`SHOW GRANTS FOR u3`).Sort().Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'u3'@'%'",
		"GRANT USAGE ON hchwang.* TO 'u3'@'%' WITH GRANT OPTION",
		"GRANT USAGE ON test.testgrant TO 'u3'@'%' WITH GRANT OPTION",
	))
}
