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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/stretchr/testify/require"
)

const dbName = "test"

func TestCheckDBPrivilege(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, `CREATE USER 'testcheck'@'localhost';`)
	mustExec(t, rootSe, `CREATE USER 'testcheck_tmp'@'localhost';`)

	se := newSession(t, store, dbName)
	activeRoles := make([]*auth.RoleIdentity, 0)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "testcheck", Hostname: "localhost"}, nil, nil))
	pc := privilege.GetPrivilegeManager(se)
	require.False(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv))

	mustExec(t, rootSe, `GRANT SELECT ON *.* TO  'testcheck'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))

	mustExec(t, rootSe, `GRANT Update ON test.* TO  'testcheck'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))

	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "testcheck", Hostname: "localhost"})
	mustExec(t, rootSe, `GRANT 'testcheck'@'localhost' TO 'testcheck_tmp'@'localhost';`)
	se2 := newSession(t, store, dbName)
	require.True(t, se2.Auth(&auth.UserIdentity{Username: "testcheck_tmp", Hostname: "localhost"}, nil, nil))
	pc = privilege.GetPrivilegeManager(se2)
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv))
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))
}

func TestCheckPointGetDBPrivilege(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, `CREATE USER 'tester'@'localhost';`)
	mustExec(t, rootSe, `GRANT SELECT,UPDATE ON test.* TO  'tester'@'localhost';`)
	mustExec(t, rootSe, `create database test2`)
	mustExec(t, rootSe, `create table test2.t(id int, v int, primary key(id))`)
	mustExec(t, rootSe, `insert into test2.t(id, v) values(1, 1)`)

	se := newSession(t, store, dbName)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tester", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `use test;`)
	_, err := se.ExecuteInternal(context.Background(), `select * from test2.t where id = 1`)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	_, err = se.ExecuteInternal(context.Background(), "update test2.t set v = 2 where id = 1")
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
}

func TestIssue22946(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, "create database db1;")
	mustExec(t, rootSe, "create database db2;")
	mustExec(t, rootSe, "use test;")
	mustExec(t, rootSe, "create table a(id int);")
	mustExec(t, rootSe, "use db1;")
	mustExec(t, rootSe, "create table a(id int primary key,name varchar(20));")
	mustExec(t, rootSe, "use db2;")
	mustExec(t, rootSe, "create table b(id int primary key,address varchar(50));")
	mustExec(t, rootSe, "CREATE USER 'delTest'@'localhost';")
	mustExec(t, rootSe, "grant all on db1.* to delTest@'localhost';")
	mustExec(t, rootSe, "grant all on db2.* to delTest@'localhost';")
	mustExec(t, rootSe, "grant select on test.* to delTest@'localhost';")

	se := newSession(t, store, dbName)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "delTest", Hostname: "localhost"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), `delete from db1.a as A where exists(select 1 from db2.b as B where A.id = B.id);`)
	require.NoError(t, err)
	mustExec(t, rootSe, "use db1;")
	_, err = se.ExecuteInternal(context.Background(), "delete from test.a as A;")
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
}

func TestCheckTablePrivilege(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, `CREATE USER 'test1'@'localhost';`)
	mustExec(t, rootSe, `CREATE USER 'test1_tmp'@'localhost';`)

	se := newSession(t, store, dbName)
	activeRoles := make([]*auth.RoleIdentity, 0)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "test1", Hostname: "localhost"}, nil, nil))
	pc := privilege.GetPrivilegeManager(se)
	require.False(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.SelectPriv))

	mustExec(t, rootSe, `GRANT SELECT ON *.* TO  'test1'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.SelectPriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.UpdatePriv))

	mustExec(t, rootSe, `GRANT Update ON test.* TO  'test1'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.UpdatePriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv))

	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "test1", Hostname: "localhost"})
	se2 := newSession(t, store, dbName)
	mustExec(t, rootSe, `GRANT 'test1'@'localhost' TO 'test1_tmp'@'localhost';`)
	require.True(t, se2.Auth(&auth.UserIdentity{Username: "test1_tmp", Hostname: "localhost"}, nil, nil))
	pc2 := privilege.GetPrivilegeManager(se2)
	require.True(t, pc2.RequestVerification(activeRoles, "test", "test", "", mysql.SelectPriv))
	require.True(t, pc2.RequestVerification(activeRoles, "test", "test", "", mysql.UpdatePriv))
	require.False(t, pc2.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv))

	mustExec(t, rootSe, `GRANT Index ON test.test TO  'test1'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv))
	require.True(t, pc2.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv))
}

func TestCheckViewPrivilege(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, `CREATE USER 'vuser'@'localhost';`)
	mustExec(t, rootSe, `CREATE VIEW v AS SELECT * FROM test;`)

	se := newSession(t, store, dbName)
	activeRoles := make([]*auth.RoleIdentity, 0)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "vuser", Hostname: "localhost"}, nil, nil))
	pc := privilege.GetPrivilegeManager(se)
	require.False(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.SelectPriv))

	mustExec(t, rootSe, `GRANT SELECT ON test.v TO 'vuser'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.SelectPriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.ShowViewPriv))

	mustExec(t, rootSe, `GRANT SHOW VIEW ON test.v TO 'vuser'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.SelectPriv))
	require.True(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.ShowViewPriv))
}

func TestCheckPrivilegeWithRoles(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, `CREATE USER 'test_role'@'localhost';`)
	mustExec(t, rootSe, `CREATE ROLE r_1, r_2, r_3;`)
	mustExec(t, rootSe, `GRANT r_1, r_2, r_3 TO 'test_role'@'localhost';`)

	se := newSession(t, store, dbName)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "test_role", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `SET ROLE r_1, r_2;`)
	mustExec(t, rootSe, `SET DEFAULT ROLE r_1 TO 'test_role'@'localhost';`)
	// test bogus role for current user.
	_, err := se.ExecuteInternal(context.Background(), `SET DEFAULT ROLE roledoesnotexist TO 'test_role'@'localhost';`)
	require.True(t, terror.ErrorEqual(err, executor.ErrRoleNotGranted))

	mustExec(t, rootSe, `GRANT SELECT ON test.* TO r_1;`)
	pc := privilege.GetPrivilegeManager(se)
	activeRoles := se.GetSessionVars().ActiveRoles
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))
	mustExec(t, rootSe, `GRANT UPDATE ON test.* TO r_2;`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))

	mustExec(t, se, `SET ROLE NONE;`)
	require.Equal(t, 0, len(se.GetSessionVars().ActiveRoles))
	mustExec(t, se, `SET ROLE DEFAULT;`)
	require.Equal(t, 1, len(se.GetSessionVars().ActiveRoles))
	mustExec(t, se, `SET ROLE ALL;`)
	require.Equal(t, 3, len(se.GetSessionVars().ActiveRoles))
	mustExec(t, se, `SET ROLE ALL EXCEPT r_1, r_2;`)
	require.Equal(t, 1, len(se.GetSessionVars().ActiveRoles))
}

func TestShowGrants(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	ctx, _ := se.(sessionctx.Context)
	mustExec(t, se, `CREATE USER 'show'@'localhost' identified by '123';`)
	mustExec(t, se, `GRANT Index ON *.* TO  'show'@'localhost';`)
	pc := privilege.GetPrivilegeManager(se)

	gs, err := pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 1)
	require.Equal(t, `GRANT INDEX ON *.* TO 'show'@'localhost'`, gs[0])

	mustExec(t, se, `GRANT Select ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 1)
	require.Equal(t, `GRANT SELECT,INDEX ON *.* TO 'show'@'localhost'`, gs[0])

	// The order of privs is the same with AllGlobalPrivs
	mustExec(t, se, `GRANT Update ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 1)
	require.Equal(t, `GRANT SELECT,UPDATE,INDEX ON *.* TO 'show'@'localhost'`, gs[0])

	// All privileges
	mustExec(t, se, `GRANT ALL ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 1)
	require.Equal(t, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`, gs[0])

	// All privileges with grant option
	mustExec(t, se, `GRANT ALL ON *.* TO 'show'@'localhost' WITH GRANT OPTION;`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 1)
	require.Equal(t, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost' WITH GRANT OPTION`, gs[0])

	// Revoke grant option
	mustExec(t, se, `REVOKE GRANT OPTION ON *.* FROM 'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 1)
	require.Equal(t, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`, gs[0])

	// Add db scope privileges
	mustExec(t, se, `GRANT Select ON test.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 2)
	expected := []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT SELECT ON test.* TO 'show'@'localhost'`}
	require.True(t, testutil.CompareUnorderedStringSlice(gs, expected), fmt.Sprintf("gs: %v, expected: %v", gs, expected))

	mustExec(t, se, `GRANT Index ON test1.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT SELECT ON test.* TO 'show'@'localhost'`,
		`GRANT INDEX ON test1.* TO 'show'@'localhost'`}
	require.True(t, testutil.CompareUnorderedStringSlice(gs, expected), fmt.Sprintf("gs: %v, expected: %v", gs, expected))

	// Add another db privilege to the same db and test again.
	mustExec(t, se, `GRANT Delete ON test1.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT SELECT ON test.* TO 'show'@'localhost'`,
		`GRANT DELETE,INDEX ON test1.* TO 'show'@'localhost'`}
	require.True(t, testutil.CompareUnorderedStringSlice(gs, expected), fmt.Sprintf("gs: %v, expected: %v", gs, expected))

	mustExec(t, se, `GRANT ALL ON test1.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT SELECT ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`}
	require.True(t, testutil.CompareUnorderedStringSlice(gs, expected), fmt.Sprintf("gs: %v, expected: %v", gs, expected))

	// Add table scope privileges
	mustExec(t, se, `GRANT Update ON test.test TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 4)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT SELECT ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`,
		`GRANT UPDATE ON test.test TO 'show'@'localhost'`}
	require.True(t, testutil.CompareUnorderedStringSlice(gs, expected), fmt.Sprintf("gs: %v, expected: %v", gs, expected))

	// Revoke the db privilege of `test` and test again. See issue #30855.
	mustExec(t, se, `REVOKE SELECT ON test.* FROM 'show'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Len(t, gs, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`,
		`GRANT UPDATE ON test.test TO 'show'@'localhost'`}
	require.True(t, testutil.CompareUnorderedStringSlice(gs, expected), fmt.Sprintf("gs: %v, expected: %v", gs, expected))

	// Add another table privilege and test again.
	mustExec(t, se, `GRANT Select ON test.test TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`,
		`GRANT SELECT,UPDATE ON test.test TO 'show'@'localhost'`}
	require.True(t, testutil.CompareUnorderedStringSlice(gs, expected), fmt.Sprintf("gs: %v, expected: %v", gs, expected))

	// Expected behavior: Usage still exists after revoking all privileges
	mustExec(t, se, `REVOKE ALL PRIVILEGES ON *.* FROM 'show'@'localhost'`)
	mustExec(t, se, `REVOKE ALL ON test1.* FROM 'show'@'localhost'`)
	mustExec(t, se, `REVOKE UPDATE, SELECT on test.test FROM 'show'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 1)
	require.Equal(t, `GRANT USAGE ON *.* TO 'show'@'localhost'`, gs[0])

	// Usage should not exist after dropping the user
	// Which we need privileges to do so!
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	mustExec(t, se, `DROP USER 'show'@'localhost'`)

	// This should now return an error
	_, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	require.Error(t, err)
	// cant show grants for non-existent
	require.True(t, terror.ErrorEqual(err, privileges.ErrNonexistingGrant))

	// Test SHOW GRANTS with USING roles.
	mustExec(t, se, `CREATE ROLE 'r1', 'r2'`)
	mustExec(t, se, `GRANT SELECT ON test.* TO 'r1'`)
	mustExec(t, se, `GRANT INSERT, UPDATE ON test.* TO 'r2'`)
	mustExec(t, se, `CREATE USER 'testrole'@'localhost' IDENTIFIED BY 'u1pass'`)
	mustExec(t, se, `GRANT 'r1', 'r2' TO 'testrole'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 2)
	roles := make([]*auth.RoleIdentity, 0)
	roles = append(roles, &auth.RoleIdentity{Username: "r2", Hostname: "%"})
	mustExec(t, se, `GRANT DELETE ON test.* TO 'testrole'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	require.NoError(t, err)
	require.Len(t, gs, 3)
	roles = append(roles, &auth.RoleIdentity{Username: "r1", Hostname: "%"})
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	require.NoError(t, err)
	require.Len(t, gs, 3)
	mustExec(t, se, `GRANT INSERT, DELETE ON test.test TO 'r2'`)
	mustExec(t, se, `create table test.b (id int)`)
	mustExec(t, se, `GRANT UPDATE ON test.b TO 'testrole'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	require.NoError(t, err)
	require.Len(t, gs, 5)
	mustExec(t, se, `DROP ROLE 'r1', 'r2'`)
	mustExec(t, se, `DROP USER 'testrole'@'localhost'`)
	mustExec(t, se, `CREATE ROLE 'r1', 'r2'`)
	mustExec(t, se, `GRANT SELECT ON test.* TO 'r2'`)
	mustExec(t, se, `CREATE USER 'testrole'@'localhost' IDENTIFIED BY 'u1pass'`)
	mustExec(t, se, `GRANT 'r1' TO 'testrole'@'localhost'`)
	mustExec(t, se, `GRANT 'r2' TO 'r1'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 2)
	roles = make([]*auth.RoleIdentity, 0)
	roles = append(roles, &auth.RoleIdentity{Username: "r1", Hostname: "%"})
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	require.NoError(t, err)
	require.Len(t, gs, 3)
}

// TestErrorMessage checks that the identity in error messages matches the mysql.user table one.
// MySQL is inconsistent in its error messages, as some match the loginHost and others the
// identity from mysql.user. In TiDB we now use the identity from mysql.user in error messages
// for consistency.
func TestErrorMessage(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, `CREATE USER wildcard`)
	mustExec(t, rootSe, `CREATE USER specifichost@192.168.1.1`)
	mustExec(t, rootSe, `GRANT SELECT on test.* TO wildcard`)
	mustExec(t, rootSe, `GRANT SELECT on test.* TO specifichost@192.168.1.1`)

	wildSe := newSession(t, store, dbName)

	// The session.Auth() func will populate the AuthUsername and AuthHostname fields.
	// We don't have to explicitly specify them.
	require.True(t, wildSe.Auth(&auth.UserIdentity{Username: "wildcard", Hostname: "192.168.1.1"}, nil, nil))
	_, err := wildSe.ExecuteInternal(context.Background(), "use mysql;")
	require.Equal(t, "[executor:1044]Access denied for user 'wildcard'@'%' to database 'mysql'", err.Error())

	specificSe := newSession(t, store, dbName)
	require.True(t, specificSe.Auth(&auth.UserIdentity{Username: "specifichost", Hostname: "192.168.1.1"}, nil, nil))
	_, err = specificSe.ExecuteInternal(context.Background(), "use mysql;")
	require.Equal(t, "[executor:1044]Access denied for user 'specifichost'@'192.168.1.1' to database 'mysql'", err.Error())
}

func TestShowColumnGrants(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	se := newSession(t, store, dbName)
	mustExec(t, se, `USE test`)
	mustExec(t, se, `CREATE USER 'column'@'%'`)
	mustExec(t, se, `CREATE TABLE column_table (a int, b int, c int)`)
	mustExec(t, se, `GRANT Select(a),Update(a,b),Insert(c) ON test.column_table TO  'column'@'%'`)

	pc := privilege.GetPrivilegeManager(se)
	gs, err := pc.ShowGrants(se, &auth.UserIdentity{Username: "column", Hostname: "%"}, nil)
	require.NoError(t, err)
	require.Equal(t, "GRANT USAGE ON *.* TO 'column'@'%' GRANT SELECT(a), INSERT(c), UPDATE(a, b) ON test.column_table TO 'column'@'%'", strings.Join(gs, " "))
}

func TestDropTablePrivileges(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	ctx, _ := se.(sessionctx.Context)
	mustExec(t, se, `CREATE TABLE todrop(c int);`)
	// ctx.GetSessionVars().User = "root@localhost"
	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `CREATE USER 'drop'@'localhost';`)
	mustExec(t, se, `GRANT Select ON test.todrop TO  'drop'@'localhost';`)

	// ctx.GetSessionVars().User = "drop@localhost"
	require.True(t, se.Auth(&auth.UserIdentity{Username: "drop", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `SELECT * FROM todrop;`)
	_, err := se.ExecuteInternal(context.Background(), "DROP TABLE todrop;")
	require.Error(t, err)

	se = newSession(t, store, dbName)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	mustExec(t, se, `GRANT Drop ON test.todrop TO  'drop'@'localhost';`)

	se = newSession(t, store, dbName)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "drop", Hostname: "localhost"}
	mustExec(t, se, `DROP TABLE todrop;`)
}

func TestSetPasswdStmt(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	se := newSession(t, store, dbName)

	// high privileged user setting password for other user (passes)
	mustExec(t, se, "CREATE USER 'superuser'")
	mustExec(t, se, "CREATE USER 'nobodyuser'")
	mustExec(t, se, "GRANT ALL ON *.* TO 'superuser'")

	require.True(t, se.Auth(&auth.UserIdentity{Username: "superuser", Hostname: "localhost", AuthUsername: "superuser", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, "SET PASSWORD for 'nobodyuser' = 'newpassword'")
	mustExec(t, se, "SET PASSWORD for 'nobodyuser' = ''")

	// low privileged user trying to set password for other user (fails)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "nobodyuser", Hostname: "localhost", AuthUsername: "nobodyuser", AuthHostname: "%"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), "SET PASSWORD for 'superuser' = 'newpassword'")
	require.Error(t, err)
}

func TestAlterUserStmt(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	se := newSession(t, store, dbName)

	// high privileged user setting password for other user (passes)
	mustExec(t, se, "CREATE USER superuser2, nobodyuser2, nobodyuser3, nobodyuser4, nobodyuser5, semuser1, semuser2, semuser3, semuser4")
	mustExec(t, se, "GRANT ALL ON *.* TO superuser2")
	mustExec(t, se, "GRANT CREATE USER ON *.* TO nobodyuser2")
	mustExec(t, se, "GRANT SYSTEM_USER ON *.* TO nobodyuser4")
	mustExec(t, se, "GRANT UPDATE ON mysql.user TO nobodyuser5, semuser1")
	mustExec(t, se, "GRANT RESTRICTED_TABLES_ADMIN ON *.* TO semuser1")
	mustExec(t, se, "GRANT RESTRICTED_USER_ADMIN ON *.* TO semuser1, semuser2, semuser3")
	mustExec(t, se, "GRANT SYSTEM_USER ON *.* to semuser3") // user is both restricted + has SYSTEM_USER (or super)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "superuser2", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY 'newpassword'")
	mustExec(t, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")

	// low privileged user trying to set password for others
	// nobodyuser3 = SUCCESS (not a SYSTEM_USER)
	// nobodyuser4 = FAIL (has SYSTEM_USER)
	// superuser2  = FAIL (has SYSTEM_USER privilege implied by SUPER)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "nobodyuser2", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY 'newpassword'")
	mustExec(t, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	mustExec(t, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")
	_, err := se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser4' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SYSTEM_USER or SUPER privilege(s) for this operation")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'superuser2' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SYSTEM_USER or SUPER privilege(s) for this operation")

	// Nobody3 has no privileges at all, but they can still alter their own password.
	// Any other user fails.
	require.True(t, se.Auth(&auth.UserIdentity{Username: "nobodyuser3", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser4' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'superuser2' IDENTIFIED BY 'newpassword'") // it checks create user before SYSTEM_USER
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")

	// Nobody5 doesn't explicitly have CREATE USER, but mysql also accepts UDPATE on mysql.user
	// as a substitute so it can modify nobody2 and nobody3 but not nobody4

	require.True(t, se.Auth(&auth.UserIdentity{Username: "nobodyuser5", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	mustExec(t, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser4' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SYSTEM_USER or SUPER privilege(s) for this operation")

	require.True(t, se.Auth(&auth.UserIdentity{Username: "semuser1", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, "ALTER USER 'semuser1' IDENTIFIED BY ''")
	mustExec(t, se, "ALTER USER 'semuser2' IDENTIFIED BY ''")
	mustExec(t, se, "ALTER USER 'semuser3' IDENTIFIED BY ''")

	sem.Enable()
	defer sem.Disable()

	// When SEM is enabled, even though we have UPDATE privilege on mysql.user, it explicitly
	// denies writeable privileges to system schemas unless RESTRICTED_TABLES_ADMIN is granted.
	// so the previous method of granting to the table instead of CREATE USER will fail now.
	// This is intentional because SEM plugs directly into the privilege manager to DENY
	// any request for UpdatePriv on mysql.user even if the privilege exists in the internal mysql.user table.

	// UpdatePriv on mysql.user
	require.True(t, se.Auth(&auth.UserIdentity{Username: "nobodyuser5", Hostname: "localhost"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser2' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")

	// actual CreateUserPriv
	require.True(t, se.Auth(&auth.UserIdentity{Username: "nobodyuser2", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	mustExec(t, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")

	// UpdatePriv on mysql.user but also has RESTRICTED_TABLES_ADMIN
	require.True(t, se.Auth(&auth.UserIdentity{Username: "semuser1", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, "ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	mustExec(t, se, "ALTER USER 'nobodyuser3' IDENTIFIED BY ''")

	// As it has (RESTRICTED_TABLES_ADMIN + UpdatePriv on mysql.user) + RESTRICTED_USER_ADMIN it can modify other restricted_user_admins like semuser2
	// and it can modify semuser3 because RESTRICTED_USER_ADMIN does not also need SYSTEM_USER
	mustExec(t, se, "ALTER USER 'semuser1' IDENTIFIED BY ''")
	mustExec(t, se, "ALTER USER 'semuser2' IDENTIFIED BY ''")
	mustExec(t, se, "ALTER USER 'semuser3' IDENTIFIED BY ''")

	require.True(t, se.Auth(&auth.UserIdentity{Username: "superuser2", Hostname: "localhost"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'semuser1' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_USER_ADMIN privilege(s) for this operation")
	require.True(t, se.Auth(&auth.UserIdentity{Username: "semuser4", Hostname: "localhost"}, nil, nil))
	// has restricted_user_admin but not CREATE USER or (update on mysql.user + RESTRICTED_TABLES_ADMIN)
	mustExec(t, se, "ALTER USER 'semuser4' IDENTIFIED BY ''") // can modify self
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'nobodyuser3' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'semuser1' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	_, err = se.ExecuteInternal(context.Background(), "ALTER USER 'semuser3' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
}

func TestSelectViewSecurity(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	ctx, _ := se.(sessionctx.Context)
	mustExec(t, se, `CREATE TABLE viewsecurity(c int);`)
	// ctx.GetSessionVars().User = "root@localhost"
	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `CREATE USER 'selectusr'@'localhost';`)
	mustExec(t, se, `GRANT CREATE VIEW ON test.* TO  'selectusr'@'localhost';`)
	mustExec(t, se, `GRANT SELECT ON test.viewsecurity TO  'selectusr'@'localhost';`)

	// ctx.GetSessionVars().User = "selectusr@localhost"
	require.True(t, se.Auth(&auth.UserIdentity{Username: "selectusr", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `SELECT * FROM test.viewsecurity;`)
	mustExec(t, se, `CREATE ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW test.selectviewsecurity as select * FROM test.viewsecurity;`)

	se = newSession(t, store, dbName)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	mustExec(t, se, "SELECT * FROM test.selectviewsecurity")
	mustExec(t, se, `REVOKE Select ON test.viewsecurity FROM  'selectusr'@'localhost';`)
	_, err := se.ExecuteInternal(context.Background(), "select * from test.selectviewsecurity")
	require.EqualError(t, err, core.ErrViewInvalid.GenWithStackByArgs("test", "selectviewsecurity").Error())
}

func TestShowViewPriv(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`DROP VIEW IF EXISTS test.v`)
	tk.MustExec(`CREATE VIEW test.v AS SELECT 1`)
	tk.MustExec("CREATE USER vnobody, vshowview, vselect, vshowandselect")
	tk.MustExec("GRANT SHOW VIEW ON test.v TO vshowview")
	tk.MustExec("GRANT SELECT ON test.v TO vselect")
	tk.MustExec("GRANT SHOW VIEW, SELECT ON test.v TO vshowandselect")

	tests := []struct {
		userName     string
		showViewErr  string
		showTableErr string
		explainErr   string
		explainRes   string
		descErr      string
		descRes      string
		tablesNum    string
		columnsNum   string
	}{
		{"vnobody",
			"[planner:1142]SELECT command denied to user 'vnobody'@'%' for table 'v'",
			"[planner:1142]SHOW VIEW command denied to user 'vnobody'@'%' for table 'v'",
			"[executor:1142]SELECT command denied to user 'vnobody'@'%' for table 'v'",
			"",
			"[executor:1142]SELECT command denied to user 'vnobody'@'%' for table 'v'",
			"",
			"0",
			"0",
		},
		{"vshowview",
			"[planner:1142]SELECT command denied to user 'vshowview'@'%' for table 'v'",
			"",
			"[executor:1142]SELECT command denied to user 'vshowview'@'%' for table 'v'",
			"",
			"[executor:1142]SELECT command denied to user 'vshowview'@'%' for table 'v'",
			"",
			"1",
			"0",
		},
		{"vselect",
			"[planner:1142]SHOW VIEW command denied to user 'vselect'@'%' for table 'v'",
			"[planner:1142]SHOW VIEW command denied to user 'vselect'@'%' for table 'v'",
			"",
			"1 bigint(1) NO  <nil> ",
			"",
			"1 bigint(1) NO  <nil> ",
			"1",
			"1",
		},
		{"vshowandselect",
			"",
			"",
			"",
			"1 bigint(1) NO  <nil> ",
			"",
			"1 bigint(1) NO  <nil> ",
			"1",
			"1",
		},
	}

	for _, test := range tests {
		tk.Session().Auth(&auth.UserIdentity{Username: test.userName, Hostname: "localhost"}, nil, nil)
		err := tk.ExecToErr("SHOW CREATE VIEW test.v")
		if test.showViewErr != "" {
			require.EqualError(t, err, test.showViewErr, test)
		} else {
			require.NoError(t, err, test)
		}
		err = tk.ExecToErr("SHOW CREATE TABLE test.v")
		if test.showTableErr != "" {
			require.EqualError(t, err, test.showTableErr, test)
		} else {
			require.NoError(t, err, test)
		}
		if test.explainErr != "" {
			err = tk.QueryToErr("explain test.v")
			require.EqualError(t, err, test.explainErr, test)
		} else {
			tk.MustQuery("explain test.v").Check(testkit.Rows(test.explainRes))
		}
		if test.descErr != "" {
			err = tk.QueryToErr("explain test.v")
			require.EqualError(t, err, test.descErr, test)
		} else {
			tk.MustQuery("desc test.v").Check(testkit.Rows(test.descRes))
		}
		tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='v'").Check(testkit.Rows(test.tablesNum))
		tk.MustQuery("select count(*) from information_schema.columns where table_schema='test' and table_name='v'").Check(testkit.Rows(test.columnsNum))
	}
}

func TestRoleAdminSecurity(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER 'ar1'@'localhost';`)
	mustExec(t, se, `CREATE USER 'ar2'@'localhost';`)
	mustExec(t, se, `GRANT ALL ON *.* to ar1@localhost`)
	defer func() {
		require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
		mustExec(t, se, "drop user 'ar1'@'localhost'")
		mustExec(t, se, "drop user 'ar2'@'localhost'")
	}()

	require.True(t, se.Auth(&auth.UserIdentity{Username: "ar1", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `create role r_test1@localhost`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "ar2", Hostname: "localhost"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), `create role r_test2@localhost`)
	require.True(t, terror.ErrorEqual(err, core.ErrSpecificAccessDenied))
}

func TestCheckCertBasedAuth(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER 'r1'@'localhost';`)
	mustExec(t, se, `CREATE USER 'r2'@'localhost' require none;`)
	mustExec(t, se, `CREATE USER 'r3'@'localhost' require ssl;`)
	mustExec(t, se, `CREATE USER 'r4'@'localhost' require x509;`)
	mustExec(t, se, `CREATE USER 'r5'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1' cipher 'TLS_AES_128_GCM_SHA256'`)
	mustExec(t, se, `CREATE USER 'r6'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	mustExec(t, se, `CREATE USER 'r7_issuer_only'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'`)
	mustExec(t, se, `CREATE USER 'r8_subject_only'@'localhost' require subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	mustExec(t, se, `CREATE USER 'r9_subject_disorder'@'localhost' require subject '/ST=Beijing/C=ZH/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	mustExec(t, se, `CREATE USER 'r10_issuer_disorder'@'localhost' require issuer '/ST=California/C=US/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'`)
	mustExec(t, se, `CREATE USER 'r11_cipher_only'@'localhost' require cipher 'TLS_AES_256_GCM_SHA384'`)
	mustExec(t, se, `CREATE USER 'r12_old_tidb_user'@'localhost'`)
	mustExec(t, se, "DELETE FROM mysql.global_priv WHERE `user` = 'r12_old_tidb_user' and `host` = 'localhost'")
	mustExec(t, se, `CREATE USER 'r13_broken_user'@'localhost'require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	mustExec(t, se, "UPDATE mysql.global_priv set priv = 'abc' where `user` = 'r13_broken_user' and `host` = 'localhost'")
	mustExec(t, se, `CREATE USER 'r14_san_only_pass'@'localhost' require san 'URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me1'`)
	mustExec(t, se, `CREATE USER 'r15_san_only_fail'@'localhost' require san 'URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me2'`)

	defer func() {
		require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
		mustExec(t, se, "drop user 'r1'@'localhost'")
		mustExec(t, se, "drop user 'r2'@'localhost'")
		mustExec(t, se, "drop user 'r3'@'localhost'")
		mustExec(t, se, "drop user 'r4'@'localhost'")
		mustExec(t, se, "drop user 'r5'@'localhost'")
		mustExec(t, se, "drop user 'r6'@'localhost'")
		mustExec(t, se, "drop user 'r7_issuer_only'@'localhost'")
		mustExec(t, se, "drop user 'r8_subject_only'@'localhost'")
		mustExec(t, se, "drop user 'r9_subject_disorder'@'localhost'")
		mustExec(t, se, "drop user 'r10_issuer_disorder'@'localhost'")
		mustExec(t, se, "drop user 'r11_cipher_only'@'localhost'")
		mustExec(t, se, "drop user 'r12_old_tidb_user'@'localhost'")
		mustExec(t, se, "drop user 'r13_broken_user'@'localhost'")
		mustExec(t, se, "drop user 'r14_san_only_pass'@'localhost'")
		mustExec(t, se, "drop user 'r15_san_only_fail'@'localhost'")
	}()

	// test without ssl or ca
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil))

	// test use ssl without ca
	se.GetSessionVars().TLSConnectionState = &tls.ConnectionState{VerifiedChains: nil}
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil))

	// test use ssl with signed but info wrong ca.
	se.GetSessionVars().TLSConnectionState = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil))

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
			require.NoError(t, err)
			cert.URIs = append(cert.URIs, &url)
		})
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r14_san_only_pass", Hostname: "localhost"}, nil, nil))

	// test require but give nothing
	se.GetSessionVars().TLSConnectionState = nil
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil))

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
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r6", Hostname: "localhost"}, nil, nil)) // not require cipher
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r11_cipher_only", Hostname: "localhost"}, nil, nil))

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
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r7_issuer_only", Hostname: "localhost"}, nil, nil))
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
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r8_subject_only", Hostname: "localhost"}, nil, nil))

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
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r9_subject_disorder", Hostname: "localhost"}, nil, nil))
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
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r10_issuer_disorder", Hostname: "localhost"}, nil, nil))

	// test mismatch san
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r15_san_only_fail", Hostname: "localhost"}, nil, nil))

	// test old data and broken data
	require.True(t, se.Auth(&auth.UserIdentity{Username: "r12_old_tidb_user", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r13_broken_user", Hostname: "localhost"}, nil, nil))

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

func TestCheckAuthenticate(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER 'u1'@'localhost';`)
	mustExec(t, se, `CREATE USER 'u2'@'localhost' identified by 'abc';`)
	mustExec(t, se, `CREATE USER 'u3@example.com'@'localhost';`)
	mustExec(t, se, `CREATE USER u4@localhost;`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil))
	salt := []byte{85, 92, 45, 22, 58, 79, 107, 6, 122, 125, 58, 80, 12, 90, 103, 32, 90, 10, 74, 82}
	authentication := []byte{24, 180, 183, 225, 166, 6, 81, 102, 70, 248, 199, 143, 91, 204, 169, 9, 161, 171, 203, 33}
	require.True(t, se.Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, authentication, salt))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "u3@example.com", Hostname: "localhost"}, nil, nil))
	require.True(t, se.Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil))

	se1 := newSession(t, store, dbName)
	mustExec(t, se1, "drop user 'u1'@'localhost'")
	mustExec(t, se1, "drop user 'u2'@'localhost'")
	mustExec(t, se1, "drop user 'u3@example.com'@'localhost'")
	mustExec(t, se1, "drop user u4@localhost")

	require.False(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "u3@example.com", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil))

	se2 := newSession(t, store, dbName)
	mustExec(t, se2, "create role 'r1'@'localhost'")
	mustExec(t, se2, "create role 'r2'@'localhost'")
	mustExec(t, se2, "create role 'r3@example.com'@'localhost'")
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil))
	require.False(t, se.Auth(&auth.UserIdentity{Username: "r3@example.com", Hostname: "localhost"}, nil, nil))

	mustExec(t, se1, "drop user 'r1'@'localhost'")
	mustExec(t, se1, "drop user 'r2'@'localhost'")
	mustExec(t, se1, "drop user 'r3@example.com'@'localhost'")
}

func TestUseDB(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	// high privileged user
	mustExec(t, se, "CREATE USER 'usesuper'")
	mustExec(t, se, "CREATE USER 'usenobody'")
	mustExec(t, se, "GRANT ALL ON *.* TO 'usesuper'")
	// without grant option
	require.True(t, se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil))
	_, e := se.ExecuteInternal(context.Background(), "GRANT SELECT ON mysql.* TO 'usenobody'")
	require.Error(t, e)
	// with grant option
	se = newSession(t, store, dbName)
	// high privileged user
	mustExec(t, se, "GRANT ALL ON *.* TO 'usesuper' WITH GRANT OPTION")
	require.True(t, se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, "use mysql")
	// low privileged user
	require.True(t, se.Auth(&auth.UserIdentity{Username: "usenobody", Hostname: "localhost", AuthUsername: "usenobody", AuthHostname: "%"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), "use mysql")
	require.Error(t, err)

	// try again after privilege granted
	require.True(t, se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, "GRANT SELECT ON mysql.* TO 'usenobody'")
	require.True(t, se.Auth(&auth.UserIdentity{Username: "usenobody", Hostname: "localhost", AuthUsername: "usenobody", AuthHostname: "%"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "use mysql")
	require.NoError(t, err)

	// test `use db` for role.
	require.True(t, se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, `CREATE DATABASE app_db`)
	mustExec(t, se, `CREATE ROLE 'app_developer'`)
	mustExec(t, se, `GRANT ALL ON app_db.* TO 'app_developer'`)
	mustExec(t, se, `CREATE USER 'dev'@'localhost'`)
	mustExec(t, se, `GRANT 'app_developer' TO 'dev'@'localhost'`)
	mustExec(t, se, `SET DEFAULT ROLE 'app_developer' TO 'dev'@'localhost'`)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "dev", Hostname: "localhost", AuthUsername: "dev", AuthHostname: "localhost"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "use app_db")
	require.NoError(t, err)
	_, err = se.ExecuteInternal(context.Background(), "use mysql")
	require.Error(t, err)
}

func TestRevokePrivileges(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, "CREATE USER 'hasgrant'")
	mustExec(t, se, "CREATE USER 'withoutgrant'")
	mustExec(t, se, "GRANT ALL ON *.* TO 'hasgrant'")
	mustExec(t, se, "GRANT ALL ON mysql.* TO 'withoutgrant'")
	// Without grant option
	require.True(t, se.Auth(&auth.UserIdentity{Username: "hasgrant", Hostname: "localhost", AuthUsername: "hasgrant", AuthHostname: "%"}, nil, nil))
	_, e := se.ExecuteInternal(context.Background(), "REVOKE SELECT ON mysql.* FROM 'withoutgrant'")
	require.Error(t, e)
	// With grant option
	se = newSession(t, store, dbName)
	mustExec(t, se, "GRANT ALL ON *.* TO 'hasgrant' WITH GRANT OPTION")
	require.True(t, se.Auth(&auth.UserIdentity{Username: "hasgrant", Hostname: "localhost", AuthUsername: "hasgrant", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, "REVOKE SELECT ON mysql.* FROM 'withoutgrant'")
	mustExec(t, se, "REVOKE ALL ON mysql.* FROM withoutgrant")

	// For issue https://github.com/pingcap/tidb/issues/23850
	mustExec(t, se, "CREATE USER u4")
	mustExec(t, se, "GRANT ALL ON *.* TO u4 WITH GRANT OPTION")
	require.True(t, se.Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost", AuthUsername: "u4", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, "REVOKE ALL ON *.* FROM CURRENT_USER()")
}

func TestSetGlobal(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER setglobal_a@localhost`)
	mustExec(t, se, `CREATE USER setglobal_b@localhost`)
	mustExec(t, se, `GRANT SUPER ON *.* to setglobal_a@localhost`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "setglobal_a", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `set global innodb_commit_concurrency=16`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "setglobal_b", Hostname: "localhost"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), `set global innodb_commit_concurrency=16`)
	require.True(t, terror.ErrorEqual(err, core.ErrSpecificAccessDenied))
}

func TestCreateDropUser(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER tcd1, tcd2`)
	mustExec(t, se, `GRANT ALL ON *.* to tcd2 WITH GRANT OPTION`)

	// should fail
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthUsername: "tcd1", AuthHostname: "%"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), `CREATE USER acdc`)
	require.True(t, terror.ErrorEqual(err, core.ErrSpecificAccessDenied))
	_, err = se.ExecuteInternal(context.Background(), `DROP USER tcd2`)
	require.True(t, terror.ErrorEqual(err, core.ErrSpecificAccessDenied))

	// should pass
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tcd2", Hostname: "localhost", AuthUsername: "tcd2", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, `DROP USER tcd1`)
	mustExec(t, se, `CREATE USER tcd1`)

	// should pass
	mustExec(t, se, `GRANT tcd2 TO tcd1`)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthUsername: "tcd1", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, `SET ROLE tcd2;`)
	mustExec(t, se, `CREATE USER tcd3`)
	mustExec(t, se, `DROP USER tcd3`)
}

func TestConfigPrivilege(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `DROP USER IF EXISTS tcd1`)
	mustExec(t, se, `CREATE USER tcd1`)
	mustExec(t, se, `GRANT ALL ON *.* to tcd1`)
	mustExec(t, se, `DROP USER IF EXISTS tcd2`)
	mustExec(t, se, `CREATE USER tcd2`)
	mustExec(t, se, `GRANT ALL ON *.* to tcd2`)
	mustExec(t, se, `REVOKE CONFIG ON *.* FROM tcd2`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthHostname: "tcd1", AuthUsername: "%"}, nil, nil))
	mustExec(t, se, `SHOW CONFIG`)
	mustExec(t, se, `SET CONFIG TIKV testkey="testval"`)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tcd2", Hostname: "localhost", AuthHostname: "tcd2", AuthUsername: "%"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), `SHOW CONFIG`)
	require.Error(t, err)
	require.Regexp(t, "you need \\(at least one of\\) the CONFIG privilege\\(s\\) for this operation$", err.Error())
	_, err = se.ExecuteInternal(context.Background(), `SET CONFIG TIKV testkey="testval"`)
	require.Error(t, err)
	require.Regexp(t, "you need \\(at least one of\\) the CONFIG privilege\\(s\\) for this operation$", err.Error())
	mustExec(t, se, `DROP USER tcd1, tcd2`)
}

func TestShowCreateTable(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER tsct1, tsct2`)
	mustExec(t, se, `GRANT select ON mysql.* to tsct2`)

	// should fail
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tsct1", Hostname: "localhost", AuthUsername: "tsct1", AuthHostname: "%"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), `SHOW CREATE TABLE mysql.user`)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))

	// should pass
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tsct2", Hostname: "localhost", AuthUsername: "tsct2", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, `SHOW CREATE TABLE mysql.user`)
}

func TestReplaceAndInsertOnDuplicate(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER tr_insert`)
	mustExec(t, se, `CREATE USER tr_update`)
	mustExec(t, se, `CREATE USER tr_delete`)
	mustExec(t, se, `CREATE TABLE t1 (a int primary key, b int)`)
	mustExec(t, se, `GRANT INSERT ON t1 TO tr_insert`)
	mustExec(t, se, `GRANT UPDATE ON t1 TO tr_update`)
	mustExec(t, se, `GRANT DELETE ON t1 TO tr_delete`)

	// Restrict the permission to INSERT only.
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tr_insert", Hostname: "localhost", AuthUsername: "tr_insert", AuthHostname: "%"}, nil, nil))

	// REPLACE requires INSERT + DELETE privileges, having INSERT alone is insufficient.
	_, err := se.ExecuteInternal(context.Background(), `REPLACE INTO t1 VALUES (1, 2)`)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	require.EqualError(t, err, "[planner:1142]DELETE command denied to user 'tr_insert'@'%' for table 't1'")

	// INSERT ON DUPLICATE requires INSERT + UPDATE privileges, having INSERT alone is insufficient.
	_, err = se.ExecuteInternal(context.Background(), `INSERT INTO t1 VALUES (3, 4) ON DUPLICATE KEY UPDATE b = 5`)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	require.EqualError(t, err, "[planner:1142]UPDATE command denied to user 'tr_insert'@'%' for table 't1'")

	// Plain INSERT should work.
	mustExec(t, se, `INSERT INTO t1 VALUES (6, 7)`)

	// Also check that having DELETE alone is insufficient for REPLACE.
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tr_delete", Hostname: "localhost", AuthUsername: "tr_delete", AuthHostname: "%"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), `REPLACE INTO t1 VALUES (8, 9)`)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	require.EqualError(t, err, "[planner:1142]INSERT command denied to user 'tr_delete'@'%' for table 't1'")

	// Also check that having UPDATE alone is insufficient for INSERT ON DUPLICATE.
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tr_update", Hostname: "localhost", AuthUsername: "tr_update", AuthHostname: "%"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), `INSERT INTO t1 VALUES (10, 11) ON DUPLICATE KEY UPDATE b = 12`)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	require.EqualError(t, err, "[planner:1142]INSERT command denied to user 'tr_update'@'%' for table 't1'")
}

func TestAnalyzeTable(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	// high privileged user
	mustExec(t, se, "CREATE USER 'asuper'")
	mustExec(t, se, "CREATE USER 'anobody'")
	mustExec(t, se, "GRANT ALL ON *.* TO 'asuper' WITH GRANT OPTION")
	mustExec(t, se, "CREATE DATABASE atest")
	mustExec(t, se, "use atest")
	mustExec(t, se, "CREATE TABLE t1 (a int)")

	require.True(t, se.Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, "analyze table mysql.user")
	// low privileged user
	require.True(t, se.Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), "analyze table t1")
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	require.EqualError(t, err, "[planner:1142]INSERT command denied to user 'anobody'@'%' for table 't1'")

	_, err = se.ExecuteInternal(context.Background(), "select * from t1")
	require.EqualError(t, err, "[planner:1142]SELECT command denied to user 'anobody'@'%' for table 't1'")

	// try again after SELECT privilege granted
	require.True(t, se.Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, "GRANT SELECT ON atest.* TO 'anobody'")
	require.True(t, se.Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "analyze table t1")
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	require.EqualError(t, err, "[planner:1142]INSERT command denied to user 'anobody'@'%' for table 't1'")
	// Add INSERT privilege and it should work.
	require.True(t, se.Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil))
	mustExec(t, se, "GRANT INSERT ON atest.* TO 'anobody'")
	require.True(t, se.Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "analyze table t1")
	require.NoError(t, err)

}

func TestSystemSchema(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	// This test tests no privilege check for INFORMATION_SCHEMA database.
	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER 'u1'@'localhost';`)
	mustExec(t, se, `GRANT SELECT ON *.* TO 'u1'@'localhost';`)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `select * from information_schema.tables`)
	mustExec(t, se, `select * from information_schema.key_column_usage`)
	_, err := se.ExecuteInternal(context.Background(), "create table information_schema.t(a int)")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "denied to user"))
	_, err = se.ExecuteInternal(context.Background(), "drop table information_schema.tables")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "denied to user"))
	_, err = se.ExecuteInternal(context.Background(), "update information_schema.tables set table_name = 'tst' where table_name = 'mysql'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrPrivilegeCheckFail))

	// Test metric_schema.
	mustExec(t, se, `select * from metrics_schema.tidb_query_duration`)
	_, err = se.ExecuteInternal(context.Background(), "drop table metrics_schema.tidb_query_duration")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	_, err = se.ExecuteInternal(context.Background(), "update metrics_schema.tidb_query_duration set instance = 'tst'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrPrivilegeCheckFail))
	_, err = se.ExecuteInternal(context.Background(), "delete from metrics_schema.tidb_query_duration")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	_, err = se.ExecuteInternal(context.Background(), "create table metric_schema.t(a int)")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
}

func TestPerformanceSchema(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	// This test tests no privilege check for INFORMATION_SCHEMA database.
	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER 'u1'@'localhost';`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), "select * from performance_schema.events_statements_summary_by_digest where schema_name = 'tst'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))

	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `GRANT SELECT ON *.* TO 'u1'@'localhost';`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "select * from performance_schema.events_statements_summary_by_digest where schema_name = 'tst'")
	require.NoError(t, err)
	mustExec(t, se, `select * from performance_schema.events_statements_summary_by_digest`)
	_, err = se.ExecuteInternal(context.Background(), "drop table performance_schema.events_statements_summary_by_digest")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	_, err = se.ExecuteInternal(context.Background(), "update performance_schema.events_statements_summary_by_digest set schema_name = 'tst'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrPrivilegeCheckFail))
	_, err = se.ExecuteInternal(context.Background(), "delete from performance_schema.events_statements_summary_by_digest")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
	_, err = se.ExecuteInternal(context.Background(), "create table performance_schema.t(a int)")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
}

func TestMetricsSchema(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER nobody, msprocess, msselect")
	tk.MustExec("GRANT Process ON *.* TO msprocess")
	tk.MustExec("GRANT SELECT ON metrics_schema.* TO msselect")

	tests := []struct {
		stmt     string
		user     string
		checkErr func(err error)
	}{
		{
			"SHOW CREATE DATABASE metrics_schema",
			"nobody",
			func(err error) {
				require.Error(t, err)
				require.True(t, terror.ErrorEqual(err, executor.ErrDBaccessDenied))
			},
		},
		{
			"SHOW CREATE DATABASE metrics_schema",
			"msprocess",
			func(err error) {
				require.NoError(t, err)
			},
		},
		{
			"SHOW CREATE DATABASE metrics_schema",
			"msselect",
			func(err error) {
				require.NoError(t, err)
			},
		},
		{
			"SELECT * FROM metrics_schema.up",
			"nobody",
			func(err error) {
				require.Error(t, err)
				require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
			},
		},
		{
			"SELECT * FROM metrics_schema.up",
			"msprocess",
			func(err error) {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), "pd unavailable"))
			},
		},
		{
			"SELECT * FROM metrics_schema.up",
			"msselect",
			func(err error) {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), "pd unavailable"))
			},
		},
		{
			"SELECT * FROM information_schema.metrics_summary",
			"nobody",
			func(err error) {
				require.Error(t, err)
				require.True(t, terror.ErrorEqual(err, core.ErrSpecificAccessDenied))
			},
		},
		{
			"SELECT * FROM information_schema.metrics_summary",
			"msprocess",
			func(err error) {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), "pd unavailable"))
			},
		},
		{
			"SELECT * FROM information_schema.metrics_summary_by_label",
			"nobody",
			func(err error) {
				require.Error(t, err)
				require.True(t, terror.ErrorEqual(err, core.ErrSpecificAccessDenied))
			},
		},
		{
			"SELECT * FROM information_schema.metrics_summary_by_label",
			"msprocess",
			func(err error) {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), "pd unavailable"))
			},
		},
	}

	for _, test := range tests {
		tk.Session().Auth(&auth.UserIdentity{
			Username: test.user,
			Hostname: "localhost",
		}, nil, nil)

		rs, err := tk.Session().ExecuteInternal(context.Background(), test.stmt)
		if err == nil {
			_, err = session.GetRows4Test(context.Background(), tk.Session(), rs)
		}
		if rs != nil {
			require.NoError(t, rs.Close())
		}
		test.checkErr(err)
	}
}

func TestAdminCommand(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `CREATE USER 'test_admin'@'localhost';`)
	mustExec(t, se, `CREATE TABLE t(a int)`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "test_admin", Hostname: "localhost"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), "ADMIN SHOW DDL JOBS")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrPrivilegeCheckFail))
	_, err = se.ExecuteInternal(context.Background(), "ADMIN CHECK TABLE t")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrPrivilegeCheckFail))

	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "ADMIN SHOW DDL JOBS")
	require.NoError(t, err)
}

func TestTableNotExistNoPermissions(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `CREATE USER 'testnotexist'@'localhost';`)
	mustExec(t, se, `CREATE DATABASE dbexists`)
	mustExec(t, se, `CREATE TABLE dbexists.t1 (a int)`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "testnotexist", Hostname: "localhost"}, nil, nil))

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

	for _, tt := range tests {

		_, err1 := se.ExecuteInternal(context.Background(), fmt.Sprintf(tt.stmt, "dbexists", "t1"))
		_, err2 := se.ExecuteInternal(context.Background(), fmt.Sprintf(tt.stmt, "dbnotexists", "t1"))

		// Check the error is the same whether table exists or not.
		require.True(t, terror.ErrorEqual(err1, err2))

		// Check it is permission denied, not not found.
		require.EqualError(t, err2, fmt.Sprintf("[planner:1142]%s command denied to user 'testnotexist'@'localhost' for table 't1'", tt.stmtType))
	}

}

func TestLoadDataPrivilege(t *testing.T) {
	// Create file.
	path := "/tmp/load_data_priv.csv"
	fp, err := os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)
	defer func() {
		err = fp.Close()
		require.NoError(t, err)
		err = os.Remove(path)
		require.NoError(t, err)
	}()
	_, err = fp.WriteString("1\n")
	require.NoError(t, err)

	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `CREATE USER 'test_load'@'localhost';`)
	mustExec(t, se, `CREATE TABLE t_load(a int)`)

	mustExec(t, se, `GRANT SELECT on *.* to 'test_load'@'localhost'`)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' INTO TABLE t_load")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))

	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `GRANT INSERT on *.* to 'test_load'@'localhost'`)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' INTO TABLE t_load")
	require.NoError(t, err)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, `GRANT INSERT on *.* to 'test_load'@'localhost'`)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil))
	_, err = se.ExecuteInternal(context.Background(), "LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' REPLACE INTO TABLE t_load")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
}

func TestSelectIntoNoPermissions(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER 'nofile'@'localhost';`)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "nofile", Hostname: "localhost"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), `select 1 into outfile '/tmp/doesntmatter-no-permissions'`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrSpecificAccessDenied))
}

func TestGetEncodedPassword(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER 'test_encode_u'@'localhost' identified by 'root';`)
	pc := privilege.GetPrivilegeManager(se)
	require.Equal(t, pc.GetEncodedPassword("test_encode_u", "localhost"), "*81F5E21E35407D884A6CD4A731AEBFB6AF209E1B")
}

func TestAuthHost(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	rootSe := newSession(t, store, dbName)
	se := newSession(t, store, dbName)
	mustExec(t, rootSe, `CREATE USER 'test_auth_host'@'%';`)
	mustExec(t, rootSe, `GRANT ALL ON *.* TO 'test_auth_host'@'%' WITH GRANT OPTION;`)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "test_auth_host", Hostname: "192.168.0.10"}, nil, nil))
	mustExec(t, se, "CREATE USER 'test_auth_host'@'192.168.%';")
	mustExec(t, se, "GRANT SELECT ON *.* TO 'test_auth_host'@'192.168.%';")

	require.True(t, se.Auth(&auth.UserIdentity{Username: "test_auth_host", Hostname: "192.168.0.10"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), "create user test_auth_host_a")
	require.Error(t, err)

	mustExec(t, rootSe, "DROP USER 'test_auth_host'@'192.168.%';")
	mustExec(t, rootSe, "DROP USER 'test_auth_host'@'%';")
}

func TestDefaultRoles(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, `CREATE USER 'testdefault'@'localhost';`)
	mustExec(t, rootSe, `CREATE ROLE 'testdefault_r1'@'localhost', 'testdefault_r2'@'localhost';`)
	mustExec(t, rootSe, `GRANT 'testdefault_r1'@'localhost', 'testdefault_r2'@'localhost' TO 'testdefault'@'localhost';`)

	se := newSession(t, store, dbName)
	pc := privilege.GetPrivilegeManager(se)

	ret := pc.GetDefaultRoles("testdefault", "localhost")
	require.Len(t, ret, 0)

	mustExec(t, rootSe, `SET DEFAULT ROLE ALL TO 'testdefault'@'localhost';`)
	ret = pc.GetDefaultRoles("testdefault", "localhost")
	require.Len(t, ret, 2)

	mustExec(t, rootSe, `SET DEFAULT ROLE NONE TO 'testdefault'@'localhost';`)
	ret = pc.GetDefaultRoles("testdefault", "localhost")
	require.Len(t, ret, 0)
}

func TestUserTableConsistency(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewAsyncTestKit(t, store)
	ctx := tk.OpenSession(context.Background(), dbName)
	tk.MustExec(ctx, "create user superadmin")
	tk.MustExec(ctx, "grant all privileges on *.* to 'superadmin'")

	// GrantPriv is not in AllGlobalPrivs any more, see pingcap/parser#581
	require.Equal(t, len(mysql.Priv2UserCol), len(mysql.AllGlobalPrivs)+1)

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
	tk.MustQuery(ctx, buf.String()).Check(testkit.Rows(res.String()))
}

func TestFieldList(t *testing.T) { // Issue #14237 List fields RPC
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	mustExec(t, se, `CREATE USER 'tableaccess'@'localhost'`)
	mustExec(t, se, `CREATE TABLE fieldlistt1 (a int)`)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "tableaccess", Hostname: "localhost"}, nil, nil))
	_, err := se.FieldList("fieldlistt1")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, core.ErrTableaccessDenied))
}

func mustExec(t *testing.T, se session.Session, sql string) {
	_, err := se.ExecuteInternal(context.Background(), sql)
	require.NoError(t, err)
}

func newStore(t *testing.T) (kv.Storage, func()) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	setUpTest(t, store, dbName)

	clean := func() {
		tearDownTest(t, store, dbName)
		dom.Close()
		err = store.Close()
		require.NoError(t, err)
	}
	return store, clean
}

func newSession(t *testing.T, store kv.Storage, dbName string) session.Session {
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	mustExec(t, se, "create database if not exists "+dbName)
	mustExec(t, se, "use "+dbName)
	return se
}

func TestDynamicPrivs(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, "CREATE USER notsuper")
	mustExec(t, rootSe, "CREATE USER otheruser")
	mustExec(t, rootSe, "CREATE ROLE anyrolename")

	se := newSession(t, store, dbName)
	require.True(t, se.Auth(&auth.UserIdentity{Username: "notsuper", Hostname: "%"}, nil, nil))

	// test SYSTEM_VARIABLES_ADMIN
	_, err := se.ExecuteInternal(context.Background(), "SET GLOBAL wait_timeout = 86400")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	mustExec(t, rootSe, "GRANT SYSTEM_VARIABLES_admin ON *.* TO notsuper")
	mustExec(t, se, "SET GLOBAL wait_timeout = 86400")

	// test ROLE_ADMIN
	_, err = se.ExecuteInternal(context.Background(), "GRANT anyrolename TO otheruser")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or ROLE_ADMIN privilege(s) for this operation")
	mustExec(t, rootSe, "GRANT ROLE_ADMIN ON *.* TO notsuper")
	mustExec(t, se, "GRANT anyrolename TO otheruser")

	// revoke SYSTEM_VARIABLES_ADMIN, confirm it is dropped
	mustExec(t, rootSe, "REVOKE SYSTEM_VARIABLES_AdmIn ON *.* FROM notsuper")
	_, err = se.ExecuteInternal(context.Background(), "SET GLOBAL wait_timeout = 86000")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")

	// grant super, confirm that it is also a substitute for SYSTEM_VARIABLES_ADMIN
	mustExec(t, rootSe, "GRANT SUPER ON *.* TO notsuper")
	mustExec(t, se, "SET GLOBAL wait_timeout = 86400")

	// revoke SUPER, assign SYSTEM_VARIABLES_ADMIN to anyrolename.
	// confirm that a dynamic privilege can be inherited from a role.
	mustExec(t, rootSe, "REVOKE SUPER ON *.* FROM notsuper")
	mustExec(t, rootSe, "GRANT SYSTEM_VARIABLES_AdmIn ON *.* TO anyrolename")
	mustExec(t, rootSe, "GRANT anyrolename TO notsuper")

	// It's not a default role, this should initially fail:
	_, err = se.ExecuteInternal(context.Background(), "SET GLOBAL wait_timeout = 86400")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	mustExec(t, se, "SET ROLE anyrolename")
	mustExec(t, se, "SET GLOBAL wait_timeout = 87000")
}

func TestDynamicGrantOption(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, "CREATE USER varuser1")
	mustExec(t, rootSe, "CREATE USER varuser2")
	mustExec(t, rootSe, "CREATE USER varuser3")

	mustExec(t, rootSe, "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser1")
	mustExec(t, rootSe, "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser2 WITH GRANT OPTION")

	se1 := newSession(t, store, dbName)

	require.True(t, se1.Auth(&auth.UserIdentity{Username: "varuser1", Hostname: "%"}, nil, nil))
	_, err := se1.ExecuteInternal(context.Background(), "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser3")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the GRANT OPTION privilege(s) for this operation")

	se2 := newSession(t, store, dbName)

	require.True(t, se2.Auth(&auth.UserIdentity{Username: "varuser2", Hostname: "%"}, nil, nil))
	mustExec(t, se2, "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser3")
}

func TestSecurityEnhancedModeRestrictedTables(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	// This provides an integration test of the tests in util/security/security_test.go
	cloudAdminSe := newSession(t, store, dbName)
	mustExec(t, cloudAdminSe, "CREATE USER cloudadmin")
	mustExec(t, cloudAdminSe, "GRANT RESTRICTED_TABLES_ADMIN, SELECT ON *.* to cloudadmin")
	mustExec(t, cloudAdminSe, "GRANT CREATE ON mysql.* to cloudadmin")
	mustExec(t, cloudAdminSe, "CREATE USER uroot")
	mustExec(t, cloudAdminSe, "GRANT ALL ON *.* to uroot WITH GRANT OPTION") // A "MySQL" all powerful user.
	require.True(t, cloudAdminSe.Auth(&auth.UserIdentity{Username: "cloudadmin", Hostname: "%"}, nil, nil))
	urootSe := newSession(t, store, dbName)
	require.True(t, urootSe.Auth(&auth.UserIdentity{Username: "uroot", Hostname: "%"}, nil, nil))

	sem.Enable()
	defer sem.Disable()

	_, err := urootSe.ExecuteInternal(context.Background(), "use metrics_schema")
	require.EqualError(t, err, "[executor:1044]Access denied for user 'uroot'@'%' to database 'metrics_schema'")

	_, err = urootSe.ExecuteInternal(context.Background(), "SELECT * FROM metrics_schema.uptime")
	require.EqualError(t, err, "[planner:1142]SELECT command denied to user 'uroot'@'%' for table 'uptime'")

	_, err = urootSe.ExecuteInternal(context.Background(), "CREATE TABLE mysql.abcd (a int)")
	require.EqualError(t, err, "[planner:1142]CREATE command denied to user 'uroot'@'%' for table 'abcd'")

	mustExec(t, cloudAdminSe, "USE metrics_schema")
	mustExec(t, cloudAdminSe, "SELECT * FROM metrics_schema.uptime")
	mustExec(t, cloudAdminSe, "CREATE TABLE mysql.abcd (a int)")
}

func TestSecurityEnhancedModeInfoschema(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("CREATE USER uroot1, uroot2, uroot3")
	tk.MustExec("GRANT SUPER ON *.* to uroot1 WITH GRANT OPTION") // super not process
	tk.MustExec("GRANT SUPER, PROCESS, RESTRICTED_TABLES_ADMIN ON *.* to uroot2 WITH GRANT OPTION")
	tk.Session().Auth(&auth.UserIdentity{
		Username: "uroot1",
		Hostname: "localhost",
	}, nil, nil)

	sem.Enable()
	defer sem.Disable()

	// Even though we have super, we still can't read protected information from tidb_servers_info, cluster_* tables
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.tidb_servers_info WHERE ip IS NOT NULL`).Check(testkit.Rows("0"))
	err := tk.QueryToErr(`SELECT COUNT(*) FROM information_schema.cluster_info WHERE status_address IS NOT NULL`)
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	// That is unless we have the RESTRICTED_TABLES_ADMIN privilege
	tk.Session().Auth(&auth.UserIdentity{
		Username: "uroot2",
		Hostname: "localhost",
	}, nil, nil)

	// flip from is NOT NULL etc
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.tidb_servers_info WHERE ip IS NULL`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.cluster_info WHERE status_address IS NULL`).Check(testkit.Rows("0"))
}

func TestClusterConfigInfoschema(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER ccnobody, ccconfig, ccprocess")
	tk.MustExec("GRANT CONFIG ON *.* TO ccconfig")
	tk.MustExec("GRANT Process ON *.* TO ccprocess")

	// incorrect/no permissions
	tk.Session().Auth(&auth.UserIdentity{
		Username: "ccnobody",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows("GRANT USAGE ON *.* TO 'ccnobody'@'%'"))

	err := tk.QueryToErr("SELECT * FROM information_schema.cluster_config")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CONFIG privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_hardware")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CONFIG privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_info")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_load")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_systeminfo")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_log WHERE time BETWEEN '2021-07-13 00:00:00' AND '2021-07-13 02:00:00' AND message like '%'")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	// With correct/CONFIG permissions
	tk.Session().Auth(&auth.UserIdentity{
		Username: "ccconfig",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows("GRANT CONFIG ON *.* TO 'ccconfig'@'%'"))
	// Needs CONFIG privilege
	tk.MustQuery("SELECT * FROM information_schema.cluster_config")
	tk.MustQuery("SELECT * FROM information_schema.cluster_HARDWARE")
	// Missing Process privilege
	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_INFO")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")
	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_LOAD")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")
	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_SYSTEMINFO")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")
	err = tk.QueryToErr("SELECT * FROM information_schema.cluster_LOG WHERE time BETWEEN '2021-07-13 00:00:00' AND '2021-07-13 02:00:00' AND message like '%'")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	// With correct/Process permissions
	tk.Session().Auth(&auth.UserIdentity{
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
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CONFIG privilege(s) for this operation")
	err = tk.QueryToErr("SELECT * FROM information_schema.CLUSTER_hardware")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CONFIG privilege(s) for this operation")
}

func TestSecurityEnhancedModeStatusVars(t *testing.T) {
	// Without TiKV the status var list does not include tidb_gc_leader_desc
	// So we can only test that the dynamic privilege is grantable.
	// We will have to use an integration test to run SHOW STATUS LIKE 'tidb_gc_leader_desc'
	// and verify if it appears.
	store, clean := newStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER unostatus, ustatus")
	tk.MustExec("GRANT RESTRICTED_STATUS_ADMIN ON *.* to ustatus")
	tk.Session().Auth(&auth.UserIdentity{
		Username: "unostatus",
		Hostname: "localhost",
	}, nil, nil)

}

func TestSecurityEnhancedLocalBackupRestore(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER backuprestore")
	tk.MustExec("GRANT BACKUP_ADMIN,RESTORE_ADMIN ON *.* to backuprestore")
	tk.Session().Auth(&auth.UserIdentity{
		Username: "backuprestore",
		Hostname: "localhost",
	}, nil, nil)

	// Prior to SEM nolocal has permission, the error should be because backup requires tikv
	_, err := tk.Session().ExecuteInternal(context.Background(), "BACKUP DATABASE * TO 'Local:///tmp/test';")
	require.EqualError(t, err, "BACKUP requires tikv store, not unistore")

	_, err = tk.Session().ExecuteInternal(context.Background(), "RESTORE DATABASE * FROM 'LOCAl:///tmp/test';")
	require.EqualError(t, err, "RESTORE requires tikv store, not unistore")

	sem.Enable()
	defer sem.Disable()

	// With SEM enabled nolocal does not have permission, but yeslocal does.
	_, err = tk.Session().ExecuteInternal(context.Background(), "BACKUP DATABASE * TO 'local:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'local storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(context.Background(), "BACKUP DATABASE * TO 'file:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'local storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(context.Background(), "BACKUP DATABASE * TO '/tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'local storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(context.Background(), "RESTORE DATABASE * FROM 'LOCAl:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'local storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(context.Background(), "BACKUP DATABASE * TO 'hdfs:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'hdfs storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(context.Background(), "RESTORE DATABASE * FROM 'HDFS:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'hdfs storage' is not supported when security enhanced mode is enabled")

}

func TestRenameUser(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	rootSe := newSession(t, store, dbName)
	mustExec(t, rootSe, "DROP USER IF EXISTS 'ru1'@'localhost'")
	mustExec(t, rootSe, "DROP USER IF EXISTS ru3")
	mustExec(t, rootSe, "DROP USER IF EXISTS ru6@localhost")
	mustExec(t, rootSe, "CREATE USER 'ru1'@'localhost'")
	mustExec(t, rootSe, "CREATE USER ru3")
	mustExec(t, rootSe, "CREATE USER ru6@localhost")
	se1 := newSession(t, store, dbName)
	require.True(t, se1.Auth(&auth.UserIdentity{Username: "ru1", Hostname: "localhost"}, nil, nil))

	// Check privileges (need CREATE USER)
	_, err := se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru4")
	require.Error(t, err)
	require.Regexp(t, "Access denied; you need .at least one of. the CREATE USER privilege.s. for this operation$", err.Error())
	mustExec(t, rootSe, "GRANT UPDATE ON mysql.user TO 'ru1'@'localhost'")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru4")
	require.Error(t, err)
	require.Regexp(t, "Access denied; you need .at least one of. the CREATE USER privilege.s. for this operation$", err.Error())
	mustExec(t, rootSe, "GRANT CREATE USER ON *.* TO 'ru1'@'localhost'")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru4")
	require.NoError(t, err)

	// Test a few single rename (both Username and Hostname)
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER 'ru4'@'%' TO 'ru3'@'localhost'")
	require.NoError(t, err)
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER 'ru3'@'localhost' TO 'ru3'@'%'")
	require.NoError(t, err)
	// Including negative tests, i.e. non existing from user and existing to user
	_, err = rootSe.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru1@localhost")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Operation RENAME USER failed for ru3@%")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru4 TO ru5@localhost")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Operation RENAME USER failed for ru4@%")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru3")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Operation RENAME USER failed for ru3@%")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru5@localhost, ru4 TO ru7")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Operation RENAME USER failed for ru4@%")
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER ru3 TO ru5@localhost, ru6@localhost TO ru1@localhost")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Operation RENAME USER failed for ru6@localhost")

	// Test multi rename, this is a full swap of ru3 and ru6, i.e. need to read its previous state in the same transaction.
	_, err = se1.ExecuteInternal(context.Background(), "RENAME USER 'ru3' TO 'ru3_tmp', ru6@localhost TO ru3, 'ru3_tmp' to ru6@localhost")
	require.NoError(t, err)

	// Cleanup
	mustExec(t, rootSe, "DROP USER ru6@localhost")
	mustExec(t, rootSe, "DROP USER ru3")
	mustExec(t, rootSe, "DROP USER 'ru1'@'localhost'")
}

func TestSecurityEnhancedModeSysVars(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER svroot1, svroot2")
	tk.MustExec("GRANT SUPER ON *.* to svroot1 WITH GRANT OPTION")
	tk.MustExec("GRANT SUPER, RESTRICTED_VARIABLES_ADMIN ON *.* to svroot2")

	sem.Enable()
	defer sem.Disable()

	// svroot1 has SUPER but in SEM will be restricted
	tk.Session().Auth(&auth.UserIdentity{
		Username:     "svroot1",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil)

	tk.MustQuery(`SHOW VARIABLES LIKE 'tidb_force_priority'`).Check(testkit.Rows())
	tk.MustQuery(`SHOW GLOBAL VARIABLES LIKE 'tidb_enable_telemetry'`).Check(testkit.Rows())

	_, err := tk.Exec("SET tidb_force_priority = 'NO_PRIORITY'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")
	_, err = tk.Exec("SET GLOBAL tidb_enable_telemetry = OFF")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")

	_, err = tk.Exec("SELECT @@session.tidb_force_priority")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")
	_, err = tk.Exec("SELECT @@global.tidb_enable_telemetry")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")

	tk.Session().Auth(&auth.UserIdentity{
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
func TestViewDefiner(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
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

func TestSecurityEnhancedModeRestrictedUsers(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
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
	tk.Session().Auth(&auth.UserIdentity{
		Username:     "ruroot1",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil)

	for _, stmt := range stmts {
		_, err := tk.Exec(stmt)
		require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_USER_ADMIN privilege(s) for this operation")
	}

	// Switch to ruroot2, it should be permitted
	tk.Session().Auth(&auth.UserIdentity{
		Username:     "ruroot2",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil)

	for _, stmt := range stmts {
		tk.MustExec(stmt)
	}
}

func TestDynamicPrivsRegistration(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se := newSession(t, store, dbName)
	pm := privilege.GetPrivilegeManager(se)

	count := len(privileges.GetDynamicPrivileges())

	require.False(t, pm.IsDynamicPrivilege("ACDC_ADMIN"))
	require.Nil(t, privileges.RegisterDynamicPrivilege("ACDC_ADMIN"))
	require.True(t, pm.IsDynamicPrivilege("ACDC_ADMIN"))
	require.Len(t, privileges.GetDynamicPrivileges(), count+1)

	require.False(t, pm.IsDynamicPrivilege("iAmdynamIC"))
	require.Nil(t, privileges.RegisterDynamicPrivilege("IAMdynamic"))
	require.True(t, pm.IsDynamicPrivilege("IAMdyNAMIC"))
	require.Len(t, privileges.GetDynamicPrivileges(), count+2)

	require.Equal(t, "privilege name is longer than 32 characters", privileges.RegisterDynamicPrivilege("THIS_PRIVILEGE_NAME_IS_TOO_LONG_THE_MAX_IS_32_CHARS").Error())
	require.False(t, pm.IsDynamicPrivilege("THIS_PRIVILEGE_NAME_IS_TOO_LONG_THE_MAX_IS_32_CHARS"))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER privassigntest")

	// Check that all privileges registered are assignable to users,
	// including the recently registered ACDC_ADMIN
	for _, priv := range privileges.GetDynamicPrivileges() {
		sqlGrant, err := sqlexec.EscapeSQL("GRANT %n ON *.* TO privassigntest", priv)
		require.NoError(t, err)
		tk.MustExec(sqlGrant)
	}
	// Check that all privileges registered are revokable
	for _, priv := range privileges.GetDynamicPrivileges() {
		sqlGrant, err := sqlexec.EscapeSQL("REVOKE %n ON *.* FROM privassigntest", priv)
		require.NoError(t, err)
		tk.MustExec(sqlGrant)
	}
}

func TestInfoSchemaUserPrivileges(t *testing.T) {
	// Being able to read all privileges from information_schema.user_privileges requires a very specific set of permissions.
	// SUPER user is not sufficient. It was observed in MySQL to require SELECT on mysql.*
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER isnobody, isroot, isselectonmysqluser, isselectonmysql")
	tk.MustExec("GRANT SUPER ON *.* TO isroot")
	tk.MustExec("GRANT SELECT ON mysql.user TO isselectonmysqluser")
	tk.MustExec("GRANT SELECT ON mysql.* TO isselectonmysql")

	// First as Nobody
	tk.Session().Auth(&auth.UserIdentity{
		Username: "isnobody",
		Hostname: "localhost",
	}, nil, nil)

	// I can see myself, but I can not see other users
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows("'isnobody'@'%' def USAGE NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows())

	// Basically the same result as as isselectonmysqluser
	tk.Session().Auth(&auth.UserIdentity{
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
	tk.Session().Auth(&auth.UserIdentity{
		Username: "isroot",
		Hostname: "localhost",
	}, nil, nil)

	// I can see myself, but I can not see other users
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows("'isroot'@'%' def SUPER NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows())

	// Now as isselectonmysqluser
	tk.Session().Auth(&auth.UserIdentity{
		Username: "isselectonmysql",
		Hostname: "localhost",
	}, nil, nil)

	// Now as isselectonmysqluser
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows("'isnobody'@'%' def USAGE NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows("'isroot'@'%' def SUPER NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows("'isselectonmysqluser'@'%' def USAGE NO"))
}

// Issues https://github.com/pingcap/tidb/issues/25972 and https://github.com/pingcap/tidb/issues/26451
func TestGrantOptionAndRevoke(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP USER IF EXISTS u1, u2, u3, ruser")
	tk.MustExec("CREATE USER u1, u2, u3, ruser")
	tk.MustExec("GRANT ALL ON *.* TO ruser WITH GRANT OPTION")
	tk.MustExec("GRANT SELECT ON *.* TO u1 WITH GRANT OPTION")
	tk.MustExec("GRANT UPDATE, DELETE on db.* TO u1")

	tk.Session().Auth(&auth.UserIdentity{
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
func setUpTest(t *testing.T, store kv.Storage, dbName string) {
	se := newSession(t, store, dbName)
	createDBSQL := fmt.Sprintf("create database if not exists %s;", dbName)
	createDB1SQL := fmt.Sprintf("create database if not exists %s1;", dbName)
	useDBSQL := fmt.Sprintf("use %s;", dbName)
	createTableSQL := `CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));`

	mustExec(t, se, createDBSQL)
	mustExec(t, se, createDB1SQL) // create database test1
	mustExec(t, se, useDBSQL)
	mustExec(t, se, createTableSQL)

	createSystemDBSQL := fmt.Sprintf("create database if not exists %s;", mysql.SystemDB)

	mustExec(t, se, createSystemDBSQL)
	mustExec(t, se, session.CreateUserTable)
	mustExec(t, se, session.CreateDBPrivTable)
	mustExec(t, se, session.CreateTablePrivTable)
	mustExec(t, se, session.CreateColumnPrivTable)
}

func tearDownTest(t *testing.T, store kv.Storage, dbName string) {
	// drop db
	se := newSession(t, store, dbName)
	dropDBSQL := fmt.Sprintf("drop database if exists %s;", dbName)
	mustExec(t, se, dropDBSQL)
}

func TestGrantReferences(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE SCHEMA reftestdb")
	tk.MustExec("USE reftestdb")
	tk.MustExec("CREATE TABLE reftest (a int)")
	tk.MustExec("CREATE USER referencesUser")
	tk.MustExec("GRANT REFERENCES ON *.* TO referencesUser")
	tk.MustExec("GRANT REFERENCES ON reftestdb.* TO referencesUser")
	tk.MustExec("GRANT REFERENCES ON reftestdb.reftest TO referencesUser")
	// Must set a session user to avoid null pointer dereferencing
	tk.Session().Auth(&auth.UserIdentity{
		Username: "root",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS FOR referencesUser").Check(testkit.Rows(
		`GRANT REFERENCES ON *.* TO 'referencesUser'@'%'`,
		`GRANT REFERENCES ON reftestdb.* TO 'referencesUser'@'%'`,
		`GRANT REFERENCES ON reftestdb.reftest TO 'referencesUser'@'%'`))
	tk.MustExec("DROP USER referencesUser")
	tk.MustExec("DROP SCHEMA reftestdb")
}

func TestDashboardClientDynamicPriv(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE ROLE dc_r1")
	tk.MustExec("CREATE USER dc_u1")
	tk.MustExec("GRANT dc_r1 TO dc_u1")
	tk.MustExec("SET DEFAULT ROLE dc_r1 TO dc_u1")

	tk1 := testkit.NewTestKit(t, store)
	tk1.Session().Auth(&auth.UserIdentity{
		Username: "dc_u1",
		Hostname: "localhost",
	}, nil, nil)
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
	))
	tk.MustExec("GRANT DASHBOARD_CLIENT ON *.* TO dc_r1")
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
		"GRANT DASHBOARD_CLIENT ON *.* TO 'dc_u1'@'%'",
	))
	tk.MustExec("REVOKE DASHBOARD_CLIENT ON *.* FROM dc_r1")
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
	))
	tk.MustExec("GRANT DASHBOARD_CLIENT ON *.* TO dc_u1")
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
		"GRANT DASHBOARD_CLIENT ON *.* TO 'dc_u1'@'%'",
	))
	tk.MustExec("REVOKE DASHBOARD_CLIENT ON *.* FROM dc_u1")
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
	))
}

// https://github.com/pingcap/tidb/issues/27213
func TestShowGrantsWithRolesAndDynamicPrivs(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE ROLE tsg_r1")
	tk.MustExec("CREATE USER tsg_u1, tsg_u2")
	tk.MustExec("GRANT CONNECTION_ADMIN, ROLE_ADMIN, SYSTEM_VARIABLES_ADMIN, PROCESS ON *.* TO tsg_r1")
	tk.MustExec("GRANT CONNECTION_ADMIN ON *.* TO tsg_u1 WITH GRANT OPTION") // grant a superior privilege to the user
	tk.MustExec("GRANT CONNECTION_ADMIN ON *.* TO tsg_u2 WITH GRANT OPTION") // grant a superior privilege to the user
	tk.MustExec("GRANT ROLE_ADMIN ON *.* TO tsg_u1")
	tk.MustExec("GRANT ROLE_ADMIN ON *.* TO tsg_u2")
	tk.MustExec("GRANT ROLE_ADMIN ON *.* TO tsg_r1 WITH GRANT OPTION") // grant a superior privilege to the role
	tk.MustExec("GRANT CONFIG ON *.* TO tsg_r1")                       // grant a static privilege to the role

	tk.MustExec("GRANT tsg_r1 TO tsg_u1, tsg_u2")    // grant the role to both users
	tk.MustExec("SET DEFAULT ROLE tsg_r1 TO tsg_u1") // u1 has the role by default, but results should be identical.

	// login as tsg_u1
	tk.Session().Auth(&auth.UserIdentity{
		Username: "tsg_u1",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows(
		"GRANT PROCESS,CONFIG ON *.* TO 'tsg_u1'@'%'",
		"GRANT 'tsg_r1'@'%' TO 'tsg_u1'@'%'",
		"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'tsg_u1'@'%'",
		"GRANT CONNECTION_ADMIN,ROLE_ADMIN ON *.* TO 'tsg_u1'@'%' WITH GRANT OPTION",
	))
	tk.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT PROCESS,CONFIG ON *.* TO 'tsg_u1'@'%'",
		"GRANT 'tsg_r1'@'%' TO 'tsg_u1'@'%'",
		"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'tsg_u1'@'%'",
		"GRANT CONNECTION_ADMIN,ROLE_ADMIN ON *.* TO 'tsg_u1'@'%' WITH GRANT OPTION",
	))
	tk.MustQuery("SHOW GRANTS FOR 'tsg_u1'").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'tsg_u1'@'%'",
		"GRANT 'tsg_r1'@'%' TO 'tsg_u1'@'%'",
		"GRANT ROLE_ADMIN ON *.* TO 'tsg_u1'@'%'",
		"GRANT CONNECTION_ADMIN ON *.* TO 'tsg_u1'@'%' WITH GRANT OPTION",
	))

	// login as tsg_u2 + SET ROLE
	tk.Session().Auth(&auth.UserIdentity{
		Username: "tsg_u2",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'tsg_u2'@'%'",
		"GRANT 'tsg_r1'@'%' TO 'tsg_u2'@'%'",
		"GRANT ROLE_ADMIN ON *.* TO 'tsg_u2'@'%'",
		"GRANT CONNECTION_ADMIN ON *.* TO 'tsg_u2'@'%' WITH GRANT OPTION",
	))
	tk.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'tsg_u2'@'%'",
		"GRANT 'tsg_r1'@'%' TO 'tsg_u2'@'%'",
		"GRANT ROLE_ADMIN ON *.* TO 'tsg_u2'@'%'",
		"GRANT CONNECTION_ADMIN ON *.* TO 'tsg_u2'@'%' WITH GRANT OPTION",
	))
	// This should not show the privileges gained from (default) roles
	tk.MustQuery("SHOW GRANTS FOR 'tsg_u2'").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'tsg_u2'@'%'",
		"GRANT 'tsg_r1'@'%' TO 'tsg_u2'@'%'",
		"GRANT ROLE_ADMIN ON *.* TO 'tsg_u2'@'%'",
		"GRANT CONNECTION_ADMIN ON *.* TO 'tsg_u2'@'%' WITH GRANT OPTION",
	))
	tk.MustExec("SET ROLE tsg_r1")
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows(
		"GRANT PROCESS,CONFIG ON *.* TO 'tsg_u2'@'%'",
		"GRANT 'tsg_r1'@'%' TO 'tsg_u2'@'%'",
		"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'tsg_u2'@'%'",
		"GRANT CONNECTION_ADMIN,ROLE_ADMIN ON *.* TO 'tsg_u2'@'%' WITH GRANT OPTION",
	))
	tk.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT PROCESS,CONFIG ON *.* TO 'tsg_u2'@'%'",
		"GRANT 'tsg_r1'@'%' TO 'tsg_u2'@'%'",
		"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'tsg_u2'@'%'",
		"GRANT CONNECTION_ADMIN,ROLE_ADMIN ON *.* TO 'tsg_u2'@'%' WITH GRANT OPTION",
	))
	// This should not show the privileges gained from SET ROLE tsg_r1.
	tk.MustQuery("SHOW GRANTS FOR 'tsg_u2'").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'tsg_u2'@'%'",
		"GRANT 'tsg_r1'@'%' TO 'tsg_u2'@'%'",
		"GRANT ROLE_ADMIN ON *.* TO 'tsg_u2'@'%'",
		"GRANT CONNECTION_ADMIN ON *.* TO 'tsg_u2'@'%' WITH GRANT OPTION",
	))
}

func TestGrantLockTables(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE lock_tables_db")
	tk.MustExec("USE lock_tables_db")
	tk.MustExec("CREATE TABLE lock_tables_table (a int)")
	tk.MustExec("CREATE USER lock_tables_user")
	tk.MustExec("GRANT LOCK TABLES ON *.* TO lock_tables_user")
	tk.MustExec("GRANT LOCK TABLES ON lock_tables_db.* TO lock_tables_user")
	// Must set a session user to avoid null pointer dereferencing
	tk.Session().Auth(&auth.UserIdentity{
		Username: "root",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS FOR lock_tables_user").Check(testkit.Rows(
		`GRANT LOCK TABLES ON *.* TO 'lock_tables_user'@'%'`,
		`GRANT LOCK TABLES ON lock_tables_db.* TO 'lock_tables_user'@'%'`))
	tk.MustExec("DROP USER lock_tables_user")
	tk.MustExec("DROP DATABASE lock_tables_db")
}

// https://github.com/pingcap/tidb/issues/27560
func TestShowGrantsForCurrentUserUsingRole(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("DROP USER IF EXISTS joe, engineering, notgranted, otherrole, delete_stuff_privilege")
	tk.MustExec("CREATE USER joe;")
	tk.MustExec("CREATE ROLE engineering;")
	tk.MustExec("CREATE ROLE admins;")
	tk.MustExec("CREATE ROLE notgranted;")
	tk.MustExec("CREATE ROLE otherrole;")
	tk.MustExec("GRANT INSERT ON test.* TO engineering;")
	tk.MustExec("GRANT DELETE ON test.* TO admins;")
	tk.MustExec("GRANT SELECT on test.* to joe;")
	tk.MustExec("GRANT engineering TO joe;")
	tk.MustExec("GRANT admins TO joe;")
	tk.MustExec("SET DEFAULT ROLE admins TO joe;")
	tk.MustExec("GRANT otherrole TO joe;")
	tk.MustExec("GRANT UPDATE ON role.* TO otherrole;")
	tk.MustExec("GRANT SELECT ON mysql.user TO otherrole;")
	tk.MustExec("CREATE ROLE delete_stuff_privilege;")
	tk.MustExec("GRANT DELETE ON mysql.user TO delete_stuff_privilege;")
	tk.MustExec("GRANT delete_stuff_privilege TO otherrole;")

	tk.Session().Auth(&auth.UserIdentity{
		Username: "joe",
		Hostname: "%",
	}, nil, nil)

	err := tk.QueryToErr("SHOW GRANTS FOR CURRENT_USER() USING notgranted")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, executor.ErrRoleNotGranted))

	tk.MustQuery("SHOW GRANTS FOR current_user() USING otherrole;").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'joe'@'%'",
		"GRANT SELECT ON test.* TO 'joe'@'%'",
		"GRANT UPDATE ON role.* TO 'joe'@'%'",
		"GRANT SELECT,DELETE ON mysql.user TO 'joe'@'%'",
		"GRANT 'admins'@'%', 'engineering'@'%', 'otherrole'@'%' TO 'joe'@'%'",
	))
	tk.MustQuery("SHOW GRANTS FOR joe USING otherrole;").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'joe'@'%'",
		"GRANT SELECT ON test.* TO 'joe'@'%'",
		"GRANT UPDATE ON role.* TO 'joe'@'%'",
		"GRANT SELECT,DELETE ON mysql.user TO 'joe'@'%'",
		"GRANT 'admins'@'%', 'engineering'@'%', 'otherrole'@'%' TO 'joe'@'%'",
	))
}

func TestGrantPlacementAdminDynamicPriv(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE placement_db")
	tk.MustExec("USE placement_db")
	tk.MustExec("CREATE TABLE placement_table (a int)")
	tk.MustExec("CREATE USER placement_user")
	tk.MustExec("GRANT PLACEMENT_ADMIN ON *.* TO placement_user")
	// Must set a session user to avoid null pointer dereferencing
	tk.Session().Auth(&auth.UserIdentity{
		Username: "root",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS FOR placement_user").Check(testkit.Rows(
		`GRANT USAGE ON *.* TO 'placement_user'@'%'`,
		`GRANT PLACEMENT_ADMIN ON *.* TO 'placement_user'@'%'`))
	tk.MustExec("DROP USER placement_user")
	tk.MustExec("DROP DATABASE placement_db")
}

func TestPlacementPolicyStmt(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	se := newSession(t, store, dbName)
	mustExec(t, se, "drop placement policy if exists x")
	createStmt := "create placement policy x PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1\""
	dropStmt := "drop placement policy if exists x"

	// high privileged user setting password for other user (passes)
	mustExec(t, se, "CREATE USER super_user, placement_user, empty_user")
	mustExec(t, se, "GRANT ALL ON *.* TO super_user")
	mustExec(t, se, "GRANT PLACEMENT_ADMIN ON *.* TO placement_user")

	require.True(t, se.Auth(&auth.UserIdentity{Username: "empty_user", Hostname: "localhost"}, nil, nil))
	_, err := se.ExecuteInternal(context.Background(), createStmt)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or PLACEMENT_ADMIN privilege(s) for this operation")
	_, err = se.ExecuteInternal(context.Background(), dropStmt)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or PLACEMENT_ADMIN privilege(s) for this operation")

	require.True(t, se.Auth(&auth.UserIdentity{Username: "super_user", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, createStmt)
	mustExec(t, se, dropStmt)

	require.True(t, se.Auth(&auth.UserIdentity{Username: "placement_user", Hostname: "localhost"}, nil, nil))
	mustExec(t, se, createStmt)
	mustExec(t, se, dropStmt)

}

func TestDBNameCaseSensitivityInTableLevel(t *testing.T) {
	store, clean := newStore(t)
	defer clean()
	se := newSession(t, store, dbName)
	mustExec(t, se, "CREATE USER test_user")
	mustExec(t, se, "grant select on metrics_schema.up to test_user;")
}

func TestGrantCreateTmpTables(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE create_tmp_table_db")
	tk.MustExec("USE create_tmp_table_db")
	tk.MustExec("CREATE USER u1")
	tk.MustExec("CREATE TABLE create_tmp_table_table (a int)")
	tk.MustExec("GRANT CREATE TEMPORARY TABLES on create_tmp_table_db.* to u1")
	tk.MustExec("GRANT CREATE TEMPORARY TABLES on *.* to u1")
	tk.MustGetErrCode("GRANT CREATE TEMPORARY TABLES on create_tmp_table_db.tmp to u1", mysql.ErrIllegalGrantForTable)
	// Must set a session user to avoid null pointer dereference
	tk.Session().Auth(&auth.UserIdentity{
		Username: "root",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS FOR u1").Check(testkit.Rows(
		`GRANT CREATE TEMPORARY TABLES ON *.* TO 'u1'@'%'`,
		`GRANT CREATE TEMPORARY TABLES ON create_tmp_table_db.* TO 'u1'@'%'`))
	tk.MustExec("DROP USER u1")
	tk.MustExec("DROP DATABASE create_tmp_table_db")
}

func TestCreateTmpTablesPriv(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	createStmt := "CREATE TEMPORARY TABLE test.tmp(id int)"
	dropStmt := "DROP TEMPORARY TABLE IF EXISTS test.tmp"

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(dropStmt)
	tk.MustExec("CREATE TABLE test.t(id int primary key)")
	tk.MustExec("CREATE SEQUENCE test.tmp")
	tk.MustExec("CREATE USER vcreate, vcreate_tmp, vcreate_tmp_all")
	tk.MustExec("GRANT CREATE, USAGE ON test.* TO vcreate")
	tk.MustExec("GRANT CREATE TEMPORARY TABLES, USAGE ON test.* TO vcreate_tmp")
	tk.MustExec("GRANT CREATE TEMPORARY TABLES, USAGE ON *.* TO vcreate_tmp_all")

	tk.Session().Auth(&auth.UserIdentity{Username: "vcreate", Hostname: "localhost"}, nil, nil)
	err := tk.ExecToErr(createStmt)
	require.EqualError(t, err, "[planner:1044]Access denied for user 'vcreate'@'%' to database 'test'")
	tk.Session().Auth(&auth.UserIdentity{Username: "vcreate_tmp", Hostname: "localhost"}, nil, nil)
	tk.MustExec(createStmt)
	tk.MustExec(dropStmt)
	tk.Session().Auth(&auth.UserIdentity{Username: "vcreate_tmp_all", Hostname: "localhost"}, nil, nil)
	// TODO: issue #29280 to be fixed.
	//err = tk.ExecToErr(createStmt)
	//require.EqualError(t, err, "[planner:1044]Access denied for user 'vcreate_tmp_all'@'%' to database 'test'")

	tests := []struct {
		sql     string
		errcode int
	}{
		{
			sql: "create temporary table tmp(id int primary key)",
		},
		{
			sql: "insert into tmp value(1)",
		},
		{
			sql: "insert into tmp value(1) on duplicate key update id=1",
		},
		{
			sql: "replace tmp values(1)",
		},
		{
			sql:     "insert into tmp select * from t",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "update tmp set id=1 where id=1",
		},
		{
			sql:     "update tmp t1, t t2 set t1.id=t2.id where t1.id=t2.id",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "delete from tmp where id=1",
		},
		{
			sql:     "delete t1 from tmp t1 join t t2 where t1.id=t2.id",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "select * from tmp where id=1",
		},
		{
			sql: "select * from tmp where id in (1,2)",
		},
		{
			sql: "select * from tmp",
		},
		{
			sql:     "select * from tmp join t where tmp.id=t.id",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "(select * from tmp) union (select * from t)",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "create temporary table tmp1 like t",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "create table tmp(id int primary key)",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "create table t(id int primary key)",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "analyze table tmp",
		},
		{
			sql:     "analyze table tmp, t",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "show create table tmp",
		},
		// TODO: issue #29281 to be fixed.
		//{
		//	sql: "show create table t",
		//	errcode: mysql.ErrTableaccessDenied,
		//},
		{
			sql:     "drop sequence tmp",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "alter table tmp add column c1 char(10)",
			errcode: errno.ErrUnsupportedDDLOperation,
		},
		{
			sql: "truncate table tmp",
		},
		{
			sql:     "drop temporary table t",
			errcode: mysql.ErrBadTable,
		},
		{
			sql:     "drop table t",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "drop table t, tmp",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "drop temporary table tmp",
		},
	}

	tk.Session().Auth(&auth.UserIdentity{Username: "vcreate_tmp", Hostname: "localhost"}, nil, nil)
	tk.MustExec("use test")
	tk.MustExec(dropStmt)
	for _, test := range tests {
		if test.errcode == 0 {
			tk.MustExec(test.sql)
		} else {
			tk.MustGetErrCode(test.sql, test.errcode)
		}
	}

	// TODO: issue #29282 to be fixed.
	//for i, test := range tests {
	//	preparedStmt := fmt.Sprintf("prepare stmt%d from '%s'", i, test.sql)
	//	executeStmt := fmt.Sprintf("execute stmt%d", i)
	//	tk.MustExec(preparedStmt)
	//	if test.errcode == 0 {
	//		tk.MustExec(executeStmt)
	//	} else {
	//		tk.MustGetErrCode(executeStmt, test.errcode)
	//	}
	//}
}

func TestRevokeSecondSyntax(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.Session().Auth(&auth.UserIdentity{
		Username: "root",
		Hostname: "localhost",
	}, nil, nil)

	tk.MustExec(`drop user if exists ss1;`)
	tk.MustExec(`create user ss1;`)
	tk.MustExec(`revoke all privileges, grant option from ss1;`)
	tk.MustQuery("show grants for ss1").Check(testkit.Rows("GRANT USAGE ON *.* TO 'ss1'@'%'"))
}

func TestGrantEvent(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE event_db")
	tk.MustExec("USE event_db")
	tk.MustExec("CREATE USER u1")
	tk.MustExec("CREATE TABLE event_table (a int)")
	tk.MustExec("GRANT EVENT on event_db.* to u1")
	tk.MustExec("GRANT EVENT on *.* to u1")
	// Must set a session user to avoid null pointer dereferencing
	tk.Session().Auth(&auth.UserIdentity{
		Username: "root",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS FOR u1").Check(testkit.Rows(
		`GRANT EVENT ON *.* TO 'u1'@'%'`,
		`GRANT EVENT ON event_db.* TO 'u1'@'%'`))
	tk.MustExec("DROP USER u1")
	tk.MustExec("DROP DATABASE event_db")
}

func TestGrantRoutine(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE routine_db")
	tk.MustExec("USE routine_db")
	tk.MustExec("CREATE USER u1")
	tk.MustExec("CREATE TABLE routine_table (a int)")
	tk.MustExec("GRANT CREATE ROUTINE on routine_db.* to u1")
	tk.MustExec("GRANT CREATE ROUTINE on *.* to u1")
	tk.MustExec("GRANT ALTER ROUTINE on routine_db.* to u1")
	tk.MustExec("GRANT ALTER ROUTINE on *.* to u1")
	// Must set a session user to avoid null pointer dereferencing
	tk.Session().Auth(&auth.UserIdentity{
		Username: "root",
		Hostname: "localhost",
	}, nil, nil)
	tk.MustQuery("SHOW GRANTS FOR u1").Check(testkit.Rows(
		`GRANT CREATE ROUTINE,ALTER ROUTINE ON *.* TO 'u1'@'%'`,
		`GRANT CREATE ROUTINE,ALTER ROUTINE ON routine_db.* TO 'u1'@'%'`))
	tk.MustExec("DROP USER u1")
	tk.MustExec("DROP DATABASE routine_db")
}

func TestIssue28675(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`DROP VIEW IF EXISTS test.v`)
	tk.MustExec(`create user test_user`)
	tk.MustExec("create view test.v as select 1")
	tk.MustExec("grant show view on test.v to test_user")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "test_user", Hostname: "localhost"}, nil, nil))
	tk.MustQuery("select count(*) from information_schema.columns where table_schema='test' and table_name='v'").Check(testkit.Rows("0"))
	tk.ExecToErr("desc test.v")
	tk.ExecToErr("explain test.v")

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	tk.MustExec("grant update on test.v to test_user")
	tk.MustExec("grant select on test.v to test_user")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "test_user", Hostname: "localhost"}, nil, nil))
	tk.MustQuery("select count(*) from information_schema.columns where table_schema='test' and table_name='v'").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from information_schema.columns where table_schema='Test' and table_name='V'").Check(testkit.Rows("1"))
	tk.MustQuery("select privileges from information_schema.columns where table_schema='test' and table_name='v'").Check(testkit.Rows("select,update"))
	tk.MustQuery("select privileges from information_schema.columns where table_schema='Test' and table_name='V'").Check(testkit.Rows("select,update"))
	require.Equal(t, 1, len(tk.MustQuery("desc test.v").Rows()))
	require.Equal(t, 1, len(tk.MustQuery("explain test.v").Rows()))
}

func TestSkipGrantTable(t *testing.T) {
	save := config.GetGlobalConfig()
	config.UpdateGlobal(func(c *config.Config) { c.Security.SkipGrantTable = true })
	defer config.StoreGlobalConfig(save)

	store, clean := newStore(t)
	defer clean()

	// Issue 29317
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER 'test1'@'%';`)
	tk.MustExec(`GRANT BACKUP_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTORE_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RELOAD ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SHUTDOWN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_VARIABLES_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_STATUS_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_CONNECTION_ADMIN, CONNECTION_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_USER_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_TABLES_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT PROCESS ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SHUTDOWN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SELECT, INSERT, UPDATE, DELETE ON mysql.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SELECT ON information_schema.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SELECT ON performance_schema.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT ALL PRIVILEGES ON *.* TO root;`)
	tk.MustExec(`revoke SHUTDOWN on *.* from root;`)
	tk.MustExec(`revoke CONFIG on *.* from root;`)

	tk.MustExec(`CREATE USER 'test2'@'%' IDENTIFIED BY '12345';`)
	tk.MustExec(`GRANT PROCESS, CONFIG ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT SHOW DATABASES ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT DASHBOARD_CLIENT ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_VARIABLES_ADMIN ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_STATUS_ADMIN ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_TABLES_ADMIN ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_USER_ADMIN ON *.* TO 'test2'@'%';`)
}

func TestIssue29823(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create user u1")
	tk.MustExec("create role r1")
	tk.MustExec("create table t1 (c1 int)")
	tk.MustExec("grant select on t1 to r1")
	tk.MustExec("grant r1 to u1")

	tk2 := testkit.NewTestKit(t, store)
	require.True(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil))
	tk2.MustExec("set role all")
	tk2.MustQuery("select current_role()").Check(testkit.Rows("`r1`@`%`"))
	tk2.MustQuery("select * from test.t1").Check(testkit.Rows())
	tk2.MustQuery("show databases like 'test'").Check(testkit.Rows("test"))
	tk2.MustQuery("show tables from test").Check(testkit.Rows("t1"))

	tk.MustExec("revoke r1 from u1")
	tk2.MustQuery("select current_role()").Check(testkit.Rows("`r1`@`%`"))
	err := tk2.ExecToErr("select * from test.t1")
	require.EqualError(t, err, "[planner:1142]SELECT command denied to user 'u1'@'%' for table 't1'")
	tk2.MustQuery("show databases like 'test'").Check(testkit.Rows())
	err = tk2.QueryToErr("show tables from test")
	require.EqualError(t, err, "[executor:1044]Access denied for user 'u1'@'%' to database 'test'")
}
