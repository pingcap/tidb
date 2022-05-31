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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestDo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("do 1, @a:=1")
	tk.MustQuery("select @a").Check(testkit.Rows("1"))

	tk.MustExec("use test")
	tk.MustExec("create table t (i int)")
	tk.MustExec("insert into t values (1)")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("do @a := (select * from t where i = 1)")
	tk2.MustExec("insert into t values (2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))
}

func TestDoWithAggFunc(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DO sum(1)")
	tk.MustExec("DO avg(@e+@f)")
	tk.MustExec("DO GROUP_CONCAT(NULLIF(ELT(1, @e), 2.0) ORDER BY 1)")
}

func TestSetRoleAllCorner(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// For user with no role, `SET ROLE ALL` should active
	// a empty slice, rather than nil.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user set_role_all")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "set_role_all", Hostname: "localhost"}, nil, nil))
	ctx := context.Background()
	_, err = se.Execute(ctx, `set role all`)
	require.NoError(t, err)
	_, err = se.Execute(ctx, `select current_role`)
	require.NoError(t, err)
}

func TestCreateRole(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user testCreateRole;")
	tk.MustExec("grant CREATE USER on *.* to testCreateRole;")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "testCreateRole", Hostname: "localhost"}, nil, nil))

	ctx := context.Background()
	_, err = se.Execute(ctx, `create role test_create_role;`)
	require.NoError(t, err)
	tk.MustExec("revoke CREATE USER on *.* from testCreateRole;")
	tk.MustExec("drop role test_create_role;")
	tk.MustExec("grant CREATE ROLE on *.* to testCreateRole;")
	_, err = se.Execute(ctx, `create role test_create_role;`)
	require.NoError(t, err)
	tk.MustExec("drop role test_create_role;")
	_, err = se.Execute(ctx, `create user test_create_role;`)
	require.Error(t, err)
	tk.MustExec("drop user testCreateRole;")
}

func TestDropRole(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user testCreateRole;")
	tk.MustExec("create user test_create_role;")
	tk.MustExec("grant CREATE USER on *.* to testCreateRole;")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "testCreateRole", Hostname: "localhost"}, nil, nil))

	ctx := context.Background()
	_, err = se.Execute(ctx, `drop role test_create_role;`)
	require.NoError(t, err)
	tk.MustExec("revoke CREATE USER on *.* from testCreateRole;")
	tk.MustExec("create role test_create_role;")
	tk.MustExec("grant DROP ROLE on *.* to testCreateRole;")
	_, err = se.Execute(ctx, `drop role test_create_role;`)
	require.NoError(t, err)
	tk.MustExec("create user test_create_role;")
	_, err = se.Execute(ctx, `drop user test_create_role;`)
	require.Error(t, err)
	tk.MustExec("drop user testCreateRole;")
	tk.MustExec("drop user test_create_role;")
}

func TestTransaction(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("begin")
	ctx := tk.Session()
	require.True(t, inTxn(ctx))
	tk.MustExec("commit")
	require.False(t, inTxn(ctx))
	tk.MustExec("begin")
	require.True(t, inTxn(ctx))
	tk.MustExec("rollback")
	require.False(t, inTxn(ctx))

	// Test that begin implicitly commits previous transaction.
	tk.MustExec("use test")
	tk.MustExec("create table txn (a int)")
	tk.MustExec("begin")
	tk.MustExec("insert txn values (1)")
	tk.MustExec("begin")
	tk.MustExec("rollback")
	tk.MustQuery("select * from txn").Check(testkit.Rows("1"))

	// Test that DDL implicitly commits previous transaction.
	tk.MustExec("begin")
	tk.MustExec("insert txn values (2)")
	tk.MustExec("create table txn2 (a int)")
	tk.MustExec("rollback")
	tk.MustQuery("select * from txn").Check(testkit.Rows("1", "2"))
}

func inTxn(ctx sessionctx.Context) bool {
	return (ctx.GetSessionVars().Status & mysql.ServerStatusInTrans) > 0
}

func TestRole(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	// Make sure user test not in mysql.User.
	result := tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test" and Host="localhost"`)
	result.Check(nil)

	// Test for DROP ROLE.
	createRoleSQL := `CREATE ROLE 'test'@'localhost';`
	tk.MustExec(createRoleSQL)
	// Make sure user test in mysql.User.
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("")))
	// Insert relation into mysql.role_edges
	tk.MustExec("insert into mysql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('localhost','test','%','root')")
	tk.MustExec("insert into mysql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('localhost','test1','localhost','test1')")
	// Insert relation into mysql.default_roles
	tk.MustExec("insert into mysql.default_roles (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) values ('%','root','localhost','test')")
	tk.MustExec("insert into mysql.default_roles (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) values ('localhost','test','%','test1')")

	dropUserSQL := `DROP ROLE IF EXISTS 'test'@'localhost' ;`
	_, err := tk.Exec(dropUserSQL)
	require.NoError(t, err)

	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test" and Host="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM mysql.role_edges WHERE TO_USER="test" and TO_HOST="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM mysql.role_edges WHERE FROM_USER="test" and FROM_HOST="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM mysql.default_roles WHERE USER="test" and HOST="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM mysql.default_roles WHERE DEFAULT_ROLE_USER="test" and DEFAULT_ROLE_HOST="localhost"`)
	result.Check(nil)

	// Test for GRANT ROLE
	createRoleSQL = `CREATE ROLE 'r_1'@'localhost', 'r_2'@'localhost', 'r_3'@'localhost';`
	tk.MustExec(createRoleSQL)
	grantRoleSQL := `GRANT 'r_1'@'localhost' TO 'r_2'@'localhost';`
	tk.MustExec(grantRoleSQL)
	result = tk.MustQuery(`SELECT TO_USER FROM mysql.role_edges WHERE FROM_USER="r_1" and FROM_HOST="localhost"`)
	result.Check(testkit.Rows("r_2"))

	grantRoleSQL = `GRANT 'r_1'@'localhost' TO 'r_3'@'localhost', 'r_4'@'localhost';`
	_, err = tk.Exec(grantRoleSQL)
	require.Error(t, err)

	// Test grant role for current_user();
	sessionVars := tk.Session().GetSessionVars()
	originUser := sessionVars.User
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "%"}
	tk.MustExec("grant 'r_1'@'localhost' to current_user();")
	tk.MustExec("revoke 'r_1'@'localhost' from 'root'@'%';")
	sessionVars.User = originUser

	result = tk.MustQuery(`SELECT FROM_USER FROM mysql.role_edges WHERE TO_USER="r_3" and TO_HOST="localhost"`)
	result.Check(nil)

	dropRoleSQL := `DROP ROLE IF EXISTS 'r_1'@'localhost' ;`
	tk.MustExec(dropRoleSQL)
	dropRoleSQL = `DROP ROLE IF EXISTS 'r_2'@'localhost' ;`
	tk.MustExec(dropRoleSQL)
	dropRoleSQL = `DROP ROLE IF EXISTS 'r_3'@'localhost' ;`
	tk.MustExec(dropRoleSQL)

	// Test for revoke role
	createRoleSQL = `CREATE ROLE 'test'@'localhost', r_1, r_2;`
	tk.MustExec(createRoleSQL)
	tk.MustExec("insert into mysql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('localhost','test','%','root')")
	tk.MustExec("insert into mysql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('%','r_1','%','root')")
	tk.MustExec("insert into mysql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('%','r_2','%','root')")
	tk.MustExec("flush privileges")
	tk.MustExec("SET DEFAULT ROLE r_1, r_2 TO root")
	_, err = tk.Exec("revoke test@localhost, r_1 from root;")
	require.NoError(t, err)
	_, err = tk.Exec("revoke `r_2`@`%` from root, u_2;")
	require.Error(t, err)
	_, err = tk.Exec("revoke `r_2`@`%` from root;")
	require.NoError(t, err)
	_, err = tk.Exec("revoke `r_1`@`%` from root;")
	require.NoError(t, err)
	result = tk.MustQuery(`SELECT * FROM mysql.default_roles WHERE DEFAULT_ROLE_USER="test" and DEFAULT_ROLE_HOST="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM mysql.default_roles WHERE USER="root" and HOST="%"`)
	result.Check(nil)
	dropRoleSQL = `DROP ROLE 'test'@'localhost', r_1, r_2;`
	tk.MustExec(dropRoleSQL)

	ctx := tk.Session().(sessionctx.Context)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "test1", Hostname: "localhost"}
	require.NotNil(t, tk.ExecToErr("SET ROLE role1, role2"))
	tk.MustExec("SET ROLE ALL")
	tk.MustExec("SET ROLE ALL EXCEPT role1, role2")
	tk.MustExec("SET ROLE DEFAULT")
	tk.MustExec("SET ROLE NONE")
}

func TestRoleAdmin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER 'testRoleAdmin';")
	tk.MustExec("CREATE ROLE 'targetRole';")

	// Create a new session.
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "testRoleAdmin", Hostname: "localhost"}, nil, nil))

	ctx := context.Background()
	_, err = se.Execute(ctx, "GRANT `targetRole` TO `testRoleAdmin`;")
	require.Error(t, err)

	tk.MustExec("GRANT SUPER ON *.* TO `testRoleAdmin`;")
	_, err = se.Execute(ctx, "GRANT `targetRole` TO `testRoleAdmin`;")
	require.NoError(t, err)
	_, err = se.Execute(ctx, "REVOKE `targetRole` FROM `testRoleAdmin`;")
	require.NoError(t, err)

	tk.MustExec("DROP USER 'testRoleAdmin';")
	tk.MustExec("DROP ROLE 'targetRole';")
}

func TestDefaultRole(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	createRoleSQL := `CREATE ROLE r_1, r_2, r_3, u_1;`
	tk.MustExec(createRoleSQL)

	tk.MustExec("insert into mysql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('%','r_1','%','u_1')")
	tk.MustExec("insert into mysql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('%','r_2','%','u_1')")

	tk.MustExec("flush privileges;")

	setRoleSQL := `SET DEFAULT ROLE r_3 TO u_1;`
	_, err := tk.Exec(setRoleSQL)
	require.Error(t, err)

	setRoleSQL = `SET DEFAULT ROLE r_1 TO u_1000;`
	_, err = tk.Exec(setRoleSQL)
	require.Error(t, err)

	setRoleSQL = `SET DEFAULT ROLE r_1, r_3 TO u_1;`
	_, err = tk.Exec(setRoleSQL)
	require.Error(t, err)

	setRoleSQL = `SET DEFAULT ROLE r_1 TO u_1;`
	_, err = tk.Exec(setRoleSQL)
	require.NoError(t, err)
	result := tk.MustQuery(`SELECT DEFAULT_ROLE_USER FROM mysql.default_roles WHERE USER="u_1"`)
	result.Check(testkit.Rows("r_1"))
	setRoleSQL = `SET DEFAULT ROLE r_2 TO u_1;`
	_, err = tk.Exec(setRoleSQL)
	require.NoError(t, err)
	result = tk.MustQuery(`SELECT DEFAULT_ROLE_USER FROM mysql.default_roles WHERE USER="u_1"`)
	result.Check(testkit.Rows("r_2"))

	setRoleSQL = `SET DEFAULT ROLE ALL TO u_1;`
	_, err = tk.Exec(setRoleSQL)
	require.NoError(t, err)
	result = tk.MustQuery(`SELECT DEFAULT_ROLE_USER FROM mysql.default_roles WHERE USER="u_1"`)
	result.Check(testkit.Rows("r_1", "r_2"))

	setRoleSQL = `SET DEFAULT ROLE NONE TO u_1;`
	_, err = tk.Exec(setRoleSQL)
	require.NoError(t, err)
	result = tk.MustQuery(`SELECT DEFAULT_ROLE_USER FROM mysql.default_roles WHERE USER="u_1"`)
	result.Check(nil)

	dropRoleSQL := `DROP USER r_1, r_2, r_3, u_1;`
	tk.MustExec(dropRoleSQL)
}

func TestSetDefaultRoleAll(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user test_all;")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "test_all", Hostname: "localhost"}, nil, nil))

	ctx := context.Background()
	_, err = se.Execute(ctx, "set default role all to test_all;")
	require.NoError(t, err)
}

func TestUser(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	// Make sure user test not in mysql.User.
	result := tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test" and Host="localhost"`)
	result.Check(nil)
	// Create user test.
	createUserSQL := `CREATE USER 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	// Make sure user test in mysql.User.
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("123")))
	// Create duplicate user with IfNotExists will be success.
	createUserSQL = `CREATE USER IF NOT EXISTS 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)

	// Create duplicate user without IfNotExists will cause error.
	createUserSQL = `CREATE USER 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustGetErrCode(createUserSQL, mysql.ErrCannotUser)
	createUserSQL = `CREATE USER IF NOT EXISTS 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|3163|User 'test'@'localhost' already exists."))
	dropUserSQL := `DROP USER IF EXISTS 'test'@'localhost' ;`
	tk.MustExec(dropUserSQL)
	// Create user test.
	createUserSQL = `CREATE USER 'test1'@'localhost';`
	tk.MustExec(createUserSQL)
	// Make sure user test in mysql.User.
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("")))
	dropUserSQL = `DROP USER IF EXISTS 'test1'@'localhost' ;`
	tk.MustExec(dropUserSQL)

	// Test alter user.
	createUserSQL = `CREATE USER 'test1'@'localhost' IDENTIFIED BY '123', 'test2'@'localhost' IDENTIFIED BY '123', 'test3'@'localhost' IDENTIFIED BY '123', 'test4'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	alterUserSQL := `ALTER USER 'test1'@'localhost' IDENTIFIED BY '111';`
	tk.MustExec(alterUserSQL)
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("111")))
	alterUserSQL = `ALTER USER 'test_not_exist'@'localhost' IDENTIFIED BY '111';`
	tk.MustGetErrCode(alterUserSQL, mysql.ErrCannotUser)
	alterUserSQL = `ALTER USER 'test1'@'localhost' IDENTIFIED BY '222', 'test_not_exist'@'localhost' IDENTIFIED BY '111';`
	tk.MustGetErrCode(alterUserSQL, mysql.ErrCannotUser)
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("222")))
	alterUserSQL = `ALTER USER 'test4'@'localhost' IDENTIFIED WITH 'auth_socket';`
	tk.MustExec(alterUserSQL)
	result = tk.MustQuery(`SELECT plugin FROM mysql.User WHERE User="test4" and Host="localhost"`)
	result.Check(testkit.Rows("auth_socket"))

	alterUserSQL = `ALTER USER IF EXISTS 'test2'@'localhost' IDENTIFIED BY '222', 'test_not_exist'@'localhost' IDENTIFIED BY '1';`
	tk.MustExec(alterUserSQL)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|3162|User 'test_not_exist'@'localhost' does not exist."))
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test2" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("222")))
	alterUserSQL = `ALTER USER IF EXISTS'test_not_exist'@'localhost' IDENTIFIED BY '1', 'test3'@'localhost' IDENTIFIED BY '333';`
	tk.MustExec(alterUserSQL)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|3162|User 'test_not_exist'@'localhost' does not exist."))
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test3" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("333")))

	// Test alter user user().
	alterUserSQL = `ALTER USER USER() IDENTIFIED BY '1';`
	_, err := tk.Exec(alterUserSQL)
	require.Truef(t, terror.ErrorEqual(err, errors.New("Session user is empty")), "err %v", err)
	sess, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tk.SetSession(sess)
	ctx := tk.Session().(sessionctx.Context)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "test1", Hostname: "localhost", AuthHostname: "localhost"}
	tk.MustExec(alterUserSQL)
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("1")))
	dropUserSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
	tk.MustExec(dropUserSQL)

	// Test drop user if exists.
	createUserSQL = `CREATE USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustExec(createUserSQL)
	dropUserSQL = `DROP USER IF EXISTS 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost' ;`
	tk.MustExec(dropUserSQL)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|3162|User test2@localhost does not exist."))

	// Test negative cases without IF EXISTS.
	createUserSQL = `CREATE USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustExec(createUserSQL)
	dropUserSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
	tk.MustGetErrCode(dropUserSQL, mysql.ErrCannotUser)
	dropUserSQL = `DROP USER 'test3'@'localhost';`
	tk.MustExec(dropUserSQL)
	dropUserSQL = `DROP USER 'test1'@'localhost';`
	tk.MustExec(dropUserSQL)
	// Test positive cases without IF EXISTS.
	createUserSQL = `CREATE USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustExec(createUserSQL)
	dropUserSQL = `DROP USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustExec(dropUserSQL)

	// Test 'identified by password'
	createUserSQL = `CREATE USER 'test1'@'localhost' identified by password 'xxx';`
	_, err = tk.Exec(createUserSQL)
	require.Truef(t, terror.ErrorEqual(executor.ErrPasswordFormat, err), "err %v", err)
	createUserSQL = `CREATE USER 'test1'@'localhost' identified by password '*3D56A309CD04FA2EEF181462E59011F075C89548';`
	tk.MustExec(createUserSQL)
	dropUserSQL = `DROP USER 'test1'@'localhost';`
	tk.MustExec(dropUserSQL)

	// Test drop user meet error
	_, err = tk.Exec(dropUserSQL)
	require.Truef(t, terror.ErrorEqual(err, executor.ErrCannotUser.GenWithStackByArgs("DROP USER", "")), "err %v", err)

	createUserSQL = `CREATE USER 'test1'@'localhost'`
	tk.MustExec(createUserSQL)
	createUserSQL = `CREATE USER 'test2'@'localhost'`
	tk.MustExec(createUserSQL)

	dropUserSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
	_, err = tk.Exec(dropUserSQL)
	require.Truef(t, terror.ErrorEqual(err, executor.ErrCannotUser.GenWithStackByArgs("DROP USER", "")), "err %v", err)

	// Close issue #17639
	dropUserSQL = `DROP USER if exists test3@'%'`
	tk.MustExec(dropUserSQL)
	createUserSQL = `create user test3@'%' IDENTIFIED WITH 'mysql_native_password' AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';`
	tk.MustExec(createUserSQL)
	querySQL := `select authentication_string from mysql.user where user="test3" ;`
	tk.MustQuery(querySQL).Check(testkit.Rows("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9"))
	alterUserSQL = `alter user test3@'%' IDENTIFIED WITH 'mysql_native_password' AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';`
	tk.MustExec(alterUserSQL)
	tk.MustQuery(querySQL).Check(testkit.Rows("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9"))

	createUserSQL = `create user userA@LOCALHOST;`
	tk.MustExec(createUserSQL)
	querySQL = `select user,host from mysql.user where user = 'userA';`
	tk.MustQuery(querySQL).Check(testkit.Rows("userA localhost"))

	createUserSQL = `create user userB@DEMO.com;`
	tk.MustExec(createUserSQL)
	querySQL = `select user,host from mysql.user where user = 'userB';`
	tk.MustQuery(querySQL).Check(testkit.Rows("userB demo.com"))

	createUserSQL = `create user userC@localhost;`
	tk.MustExec(createUserSQL)
	renameUserSQL := `rename user 'userC'@'localhost' to 'userD'@'Demo.com';`
	tk.MustExec(renameUserSQL)
	querySQL = `select user,host from mysql.user where user = 'userD';`
	tk.MustQuery(querySQL).Check(testkit.Rows("userD demo.com"))

	createUserSQL = `create user foo@localhost identified with 'foobar';`
	_, err = tk.Exec(createUserSQL)
	require.Truef(t, terror.ErrorEqual(err, executor.ErrPluginIsNotLoaded), "err %v", err)

	tk.MustExec(`create user joan;`)
	tk.MustExec(`create user sally;`)
	tk.MustExec(`create role engineering;`)
	tk.MustExec(`create role consultants;`)
	tk.MustExec(`create role qa;`)
	tk.MustExec(`grant engineering to joan;`)
	tk.MustExec(`grant engineering to sally;`)
	tk.MustExec(`grant engineering, consultants to joan, sally;`)
	tk.MustExec(`grant qa to consultants;`)
	tk.MustExec("CREATE ROLE `engineering`@`US`;")
	tk.MustExec("create role `engineering`@`INDIA`;")
	tk.MustExec("grant `engineering`@`US` TO `engineering`@`INDIA`;")

	tk.MustQuery("select user,host from mysql.user where user='engineering' and host = 'india'").
		Check(testkit.Rows("engineering india"))
	tk.MustQuery("select user,host from mysql.user where user='engineering' and host = 'us'").
		Check(testkit.Rows("engineering us"))

	tk.MustExec("drop role engineering@INDIA;")
	tk.MustExec("drop role engineering@US;")

	tk.MustQuery("select user from mysql.user where user='engineering' and host = 'india'").Check(testkit.Rows())
	tk.MustQuery("select user from mysql.user where user='engineering' and host = 'us'").Check(testkit.Rows())
}

func TestSetPwd(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	createUserSQL := `CREATE USER 'testpwd'@'localhost' IDENTIFIED BY '';`
	tk.MustExec(createUserSQL)
	result := tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Rows(""))

	// set password for
	tk.MustExec(`SET PASSWORD FOR 'testpwd'@'localhost' = 'password';`)
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("password")))

	tk.MustExec(`CREATE USER 'testpwdsock'@'localhost' IDENTIFIED WITH 'auth_socket';`)
	tk.MustExec(`SET PASSWORD FOR 'testpwdsock'@'localhost' = 'password';`)
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Note 1699 SET PASSWORD has no significance for user 'testpwdsock'@'localhost' as authentication plugin does not support it."))

	// set password
	setPwdSQL := `SET PASSWORD = 'pwd'`
	// Session user is empty.
	_, err := tk.Exec(setPwdSQL)
	require.Error(t, err)
	sess, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tk.SetSession(sess)
	ctx := tk.Session().(sessionctx.Context)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "testpwd1", Hostname: "localhost", AuthUsername: "testpwd1", AuthHostname: "localhost"}
	// Session user doesn't exist.
	_, err = tk.Exec(setPwdSQL)
	require.Truef(t, terror.ErrorEqual(err, executor.ErrPasswordNoMatch), "err %v", err)
	// normal
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "testpwd", Hostname: "localhost", AuthUsername: "testpwd", AuthHostname: "localhost"}
	tk.MustExec(setPwdSQL)
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("pwd")))

}

func TestKillStmt(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.EnableGlobalKill = false
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	sm := &mockSessionManager{
		serverID: 0,
	}
	tk.Session().SetSessionManager(sm)
	tk.MustExec("kill 1")
	result := tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] connectionID' instead"))

	newCfg2 := *originCfg
	newCfg2.EnableGlobalKill = true
	config.StoreGlobalConfig(&newCfg2)

	// ZERO serverID, treated as truncated.
	tk.MustExec("kill 1")
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."))

	// truncated
	sm.SetServerID(1)
	tk.MustExec("kill 101")
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."))

	// excceed int64
	tk.MustExec("kill 9223372036854775808") // 9223372036854775808 == 2^63
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Parse ConnectionID failed: Unexpected connectionID excceeds int64"))

	// local kill
	connID := util.NewGlobalConnID(1, true)
	tk.MustExec("kill " + strconv.FormatUint(connID.ID(), 10))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows())

	// remote kill is tested in `tests/globalkilltest`
}

func TestFlushPrivileges(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`CREATE USER 'testflush'@'localhost' IDENTIFIED BY '';`)
	tk.MustExec(`UPDATE mysql.User SET Select_priv='Y' WHERE User="testflush" and Host="localhost"`)

	// Create a new session.
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "testflush", Hostname: "localhost"}, nil, nil))

	ctx := context.Background()
	// Before flush.
	_, err = se.Execute(ctx, `SELECT authentication_string FROM mysql.User WHERE User="testflush" and Host="localhost"`)
	require.Error(t, err)

	tk.MustExec("FLUSH PRIVILEGES")

	// After flush.
	_, err = se.Execute(ctx, `SELECT authentication_string FROM mysql.User WHERE User="testflush" and Host="localhost"`)
	require.NoError(t, err)

}

func TestFlushPrivilegesPanic(t *testing.T) {
	// Run in a separate suite because this test need to set SkipGrantTable config.
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Security.SkipGrantTable = true
	})

	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("FLUSH PRIVILEGES")
}

func TestDropPartitionStats(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	// Use the testSerialSuite to fix the unstable test
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database if not exists test_drop_gstats`)
	tk.MustExec("use test_drop_gstats")
	tk.MustExec("drop table if exists test_drop_gstats;")
	tk.MustExec(`create table test_drop_gstats (
	a int,
	key(a)
)
partition by range (a) (
	partition p0 values less than (10),
	partition p1 values less than (20),
	partition global values less than (30)
)`)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("insert into test_drop_gstats values (1), (5), (11), (15), (21), (25)")
	require.Nil(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))

	checkPartitionStats := func(names ...string) {
		rs := tk.MustQuery("show stats_meta").Rows()
		require.Equal(t, len(names), len(rs))
		for i := range names {
			require.Equal(t, names[i], rs[i][2].(string))
		}
	}

	tk.MustExec("analyze table test_drop_gstats")
	checkPartitionStats("global", "p0", "p1", "global")

	tk.MustExec("drop stats test_drop_gstats partition p0")
	checkPartitionStats("global", "p1", "global")

	err := tk.ExecToErr("drop stats test_drop_gstats partition abcde")
	require.Error(t, err)
	require.Equal(t, "can not found the specified partition name abcde in the table definition", err.Error())

	tk.MustExec("drop stats test_drop_gstats partition global")
	checkPartitionStats("global", "p1")

	tk.MustExec("drop stats test_drop_gstats global")
	checkPartitionStats("p1")

	tk.MustExec("analyze table test_drop_gstats")
	checkPartitionStats("global", "p0", "p1", "global")

	tk.MustExec("drop stats test_drop_gstats partition p0, p1, global")
	checkPartitionStats("global")

	tk.MustExec("analyze table test_drop_gstats")
	checkPartitionStats("global", "p0", "p1", "global")

	tk.MustExec("drop stats test_drop_gstats")
	checkPartitionStats()
}

func TestDropStats(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := dom.StatsHandle()
	h.Clear()
	testKit.MustExec("analyze table t")
	statsTbl := h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	testKit.MustExec("drop stats t")
	require.Nil(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.True(t, statsTbl.Pseudo)

	testKit.MustExec("analyze table t")
	statsTbl = h.GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	h.SetLease(1)
	testKit.MustExec("drop stats t")
	require.Nil(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.True(t, statsTbl.Pseudo)
	h.SetLease(0)
}

func TestDropStatsFromKV(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 varchar(20), c2 varchar(20))")
	tk.MustExec(`insert into t values("1","1"),("2","2"),("3","3"),("4","4")`)
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	tk.MustExec("analyze table t with 2 topn")
	tblID := tk.MustQuery(`select tidb_table_id from information_schema.tables where table_name = "t" and table_schema = "test"`).Rows()[0][0].(string)
	tk.MustQuery("select modify_count, count from mysql.stats_meta where table_id = " + tblID).Check(
		testkit.Rows("0 16"))
	tk.MustQuery("select hist_id from mysql.stats_histograms where table_id = " + tblID).Check(
		testkit.Rows("1", "2"))
	ret := tk.MustQuery("select hist_id, bucket_id from mysql.stats_buckets where table_id = " + tblID)
	require.True(t, len(ret.Rows()) > 0)
	ret = tk.MustQuery("select hist_id from mysql.stats_top_n where table_id = " + tblID)
	require.True(t, len(ret.Rows()) > 0)

	tk.MustExec("drop stats t")
	tk.MustQuery("select modify_count, count from mysql.stats_meta where table_id = " + tblID).Check(
		testkit.Rows("0 16"))
	tk.MustQuery("select hist_id from mysql.stats_histograms where table_id = " + tblID).Check(
		testkit.Rows())
	tk.MustQuery("select hist_id, bucket_id from mysql.stats_buckets where table_id = " + tblID).Check(
		testkit.Rows())
	tk.MustQuery("select hist_id from mysql.stats_top_n where table_id = " + tblID).Check(
		testkit.Rows())
}

func TestFlushTables(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	_, err := tk.Exec("FLUSH TABLES")
	require.NoError(t, err)

	_, err = tk.Exec("FLUSH TABLES WITH READ LOCK")
	require.Error(t, err)

}

func TestUseDB(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Exec("USE test")
	require.NoError(t, err)

	_, err = tk.Exec("USE ``")
	require.Truef(t, terror.ErrorEqual(core.ErrNoDB, err), "err %v", err)
}

func TestStmtAutoNewTxn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// Some statements are like DDL, they commit the previous txn automically.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Fix issue https://github.com/pingcap/tidb/issues/10705
	tk.MustExec("begin")
	tk.MustExec("create user 'xxx'@'%';")
	tk.MustExec("grant all privileges on *.* to 'xxx'@'%';")

	tk.MustExec("create table auto_new (id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into auto_new values (1)")
	tk.MustExec("revoke all privileges on *.* from 'xxx'@'%'")
	tk.MustExec("rollback") // insert statement has already committed
	tk.MustQuery("select * from auto_new").Check(testkit.Rows("1"))

	// Test the behavior when autocommit is false.
	tk.MustExec("set autocommit = 0")
	tk.MustExec("insert into auto_new values (2)")
	tk.MustExec("create user 'yyy'@'%'")
	tk.MustExec("rollback")
	tk.MustQuery("select * from auto_new").Check(testkit.Rows("1", "2"))

	tk.MustExec("drop user 'yyy'@'%'")
	tk.MustExec("insert into auto_new values (3)")
	tk.MustExec("rollback")
	tk.MustQuery("select * from auto_new").Check(testkit.Rows("1", "2"))
}

func TestIssue9111(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// CREATE USER / DROP USER fails if admin doesn't have insert privilege on `mysql.user` table.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user 'user_admin'@'localhost';")
	tk.MustExec("grant create user on *.* to 'user_admin'@'localhost';")

	// Create a new session.
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "user_admin", Hostname: "localhost"}, nil, nil))

	ctx := context.Background()
	_, err = se.Execute(ctx, `create user test_create_user`)
	require.NoError(t, err)
	_, err = se.Execute(ctx, `drop user test_create_user`)
	require.NoError(t, err)

	tk.MustExec("revoke create user on *.* from 'user_admin'@'localhost';")
	tk.MustExec("grant insert, delete on mysql.user to 'user_admin'@'localhost';")

	_, err = se.Execute(ctx, `create user test_create_user`)
	require.NoError(t, err)
	_, err = se.Execute(ctx, `drop user test_create_user`)
	require.NoError(t, err)

	_, err = se.Execute(ctx, `create role test_create_user`)
	require.NoError(t, err)
	_, err = se.Execute(ctx, `drop role test_create_user`)
	require.NoError(t, err)

	tk.MustExec("drop user 'user_admin'@'localhost';")
}

func TestRoleAtomic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create role r2;")
	_, err := tk.Exec("create role r1, r2, r3")
	require.Error(t, err)
	// Check atomic create role.
	result := tk.MustQuery(`SELECT user FROM mysql.User WHERE user in ('r1', 'r2', 'r3')`)
	result.Check(testkit.Rows("r2"))
	// Check atomic drop role.
	_, err = tk.Exec("drop role r1, r2, r3")
	require.Error(t, err)
	result = tk.MustQuery(`SELECT user FROM mysql.User WHERE user in ('r1', 'r2', 'r3')`)
	result.Check(testkit.Rows("r2"))
	tk.MustExec("drop role r2;")
}

func TestExtendedStatsPrivileges(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create user 'u1'@'%'")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.True(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil))
	ctx := context.Background()
	_, err = se.Execute(ctx, "set session tidb_enable_extended_stats = on")
	require.NoError(t, err)
	_, err = se.Execute(ctx, "alter table test.t add stats_extended s1 correlation(a,b)")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]ALTER command denied to user 'u1'@'%' for table 't'", err.Error())
	tk.MustExec("grant alter on test.* to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter table test.t add stats_extended s1 correlation(a,b)")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]ADD STATS_EXTENDED command denied to user 'u1'@'%' for table 't'", err.Error())
	tk.MustExec("grant select on test.* to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter table test.t add stats_extended s1 correlation(a,b)")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]ADD STATS_EXTENDED command denied to user 'u1'@'%' for table 'stats_extended'", err.Error())
	tk.MustExec("grant insert on mysql.stats_extended to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter table test.t add stats_extended s1 correlation(a,b)")
	require.NoError(t, err)

	_, err = se.Execute(ctx, "use test")
	require.NoError(t, err)
	_, err = se.Execute(ctx, "alter table t drop stats_extended s1")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]DROP STATS_EXTENDED command denied to user 'u1'@'%' for table 'stats_extended'", err.Error())
	tk.MustExec("grant update on mysql.stats_extended to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter table t drop stats_extended s1")
	require.NoError(t, err)
	tk.MustExec("drop user 'u1'@'%'")
}

func TestIssue17247(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user 'issue17247'")
	tk.MustExec("grant CREATE USER on *.* to 'issue17247'")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "issue17247", Hostname: "%"}, nil, nil))
	tk1.MustExec("ALTER USER USER() IDENTIFIED BY 'xxx'")
	tk1.MustExec("ALTER USER CURRENT_USER() IDENTIFIED BY 'yyy'")
	tk1.MustExec("ALTER USER CURRENT_USER IDENTIFIED BY 'zzz'")
	tk.MustExec("ALTER USER 'issue17247'@'%' IDENTIFIED BY 'kkk'")
	tk.MustExec("ALTER USER 'issue17247'@'%' IDENTIFIED BY PASSWORD '*B50FBDB37F1256824274912F2A1CE648082C3F1F'")
	// Wrong grammar
	_, err := tk1.Exec("ALTER USER USER() IDENTIFIED BY PASSWORD '*B50FBDB37F1256824274912F2A1CE648082C3F1F'")
	require.Error(t, err)
}

// Close issue #23649.
// See https://github.com/pingcap/tidb/issues/23649
func TestIssue23649(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP USER IF EXISTS issue23649;")
	tk.MustExec("CREATE USER issue23649;")
	_, err := tk.Exec("GRANT bogusrole to issue23649;")
	require.Equal(t, "[executor:3523]Unknown authorization ID `bogusrole`@`%`", err.Error())
	_, err = tk.Exec("GRANT bogusrole to nonexisting;")
	require.Equal(t, "[executor:3523]Unknown authorization ID `bogusrole`@`%`", err.Error())
}

func TestSetCurrentUserPwd(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER issue28534;")
	defer func() {
		tk.MustExec("DROP USER IF EXISTS issue28534;")
	}()

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "issue28534", Hostname: "localhost", CurrentUser: true, AuthUsername: "issue28534", AuthHostname: "%"}, nil, nil))
	tk.MustExec(`SET PASSWORD FOR CURRENT_USER() = "43582eussi"`)

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	result := tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="issue28534"`)
	result.Check(testkit.Rows(auth.EncodePassword("43582eussi")))
}

func TestShowGrantsAfterDropRole(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER u29473")
	defer tk.MustExec("DROP USER IF EXISTS u29473")

	tk.MustExec("CREATE ROLE r29473")
	tk.MustExec("GRANT r29473 TO u29473")
	tk.MustExec("GRANT CREATE USER ON *.* TO u29473")

	tk.Session().Auth(&auth.UserIdentity{Username: "u29473", Hostname: "%"}, nil, nil)
	tk.MustExec("SET ROLE r29473")
	tk.MustExec("DROP ROLE r29473")
	tk.MustQuery("SHOW GRANTS").Check(testkit.Rows("GRANT CREATE USER ON *.* TO 'u29473'@'%'"))
}

func TestDropRoleAfterRevoke(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	// issue 29781
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil)

	tk.MustExec("create role r1, r2, r3;")
	defer tk.MustExec("drop role if exists r1, r2, r3;")
	tk.MustExec("grant r1,r2,r3 to current_user();")
	tk.MustExec("set role all;")
	tk.MustExec("revoke r1, r3 from root;")
	tk.MustExec("drop role r1;")
}

func TestUserWithSetNames(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set names gbk;")

	tk.MustExec("drop user if exists '\xd2\xbb'@'localhost';")
	tk.MustExec("create user '\xd2\xbb'@'localhost' IDENTIFIED BY '\xd2\xbb';")

	result := tk.MustQuery("SELECT authentication_string FROM mysql.User WHERE User='\xd2\xbb' and Host='localhost';")
	result.Check(testkit.Rows(auth.EncodePassword("一")))

	tk.MustExec("ALTER USER '\xd2\xbb'@'localhost' IDENTIFIED BY '\xd2\xbb\xd2\xbb';")
	result = tk.MustQuery("SELECT authentication_string FROM mysql.User WHERE User='\xd2\xbb' and Host='localhost';")
	result.Check(testkit.Rows(auth.EncodePassword("一一")))

	tk.MustExec("RENAME USER '\xd2\xbb'@'localhost' to '\xd2\xbb'")

	tk.MustExec("drop user '\xd2\xbb';")
}

func TestStatementsCauseImplicitCommit(t *testing.T) {
	// Test some of the implicit commit statements.
	// See https://dev.mysql.com/doc/refman/5.7/en/implicit-commit.html
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table ic (id int primary key)")

	cases := []string{
		"create table xx (id int)",
		"create user 'xx'@'127.0.0.1'",
		"grant SELECT on test.ic to 'xx'@'127.0.0.1'",
		"flush privileges",
		"analyze table ic",
	}
	for i, sql := range cases {
		tk.MustExec("begin")
		tk.MustExec("insert into ic values (?)", i)
		tk.MustExec(sql)
		tk.MustQuery("select * from ic where id = ?", i).Check(testkit.Rows(strconv.FormatInt(int64(i), 10)))
		// Clean up data
		tk.MustExec("delete from ic")
	}
}
