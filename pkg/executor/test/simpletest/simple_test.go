// Copyright 2022 PingCAP, Inc.
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

package simpletest

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

func TestExtendedStatsPrivileges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create user 'u1'@'%'")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
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

func TestUserWithSetNames(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestTransaction(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	return ctx.GetSessionVars().InTxn()
}

func TestRole(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	err := tk.ExecToErr(dropUserSQL)
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
	err = tk.ExecToErr(grantRoleSQL)
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
	err = tk.ExecToErr("revoke test@localhost, r_1 from root;")
	require.NoError(t, err)
	err = tk.ExecToErr("revoke `r_2`@`%` from root, u_2;")
	require.Error(t, err)
	err = tk.ExecToErr("revoke `r_2`@`%` from root;")
	require.NoError(t, err)
	err = tk.ExecToErr("revoke `r_1`@`%` from root;")
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

func TestUser(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

	// Test create/alter user with `tidb_auth_token`
	tk.MustExec(`CREATE USER token_user IDENTIFIED WITH 'tidb_auth_token' REQUIRE token_issuer 'issuer-abc'`)
	tk.MustQuery(`SELECT plugin, token_issuer FROM mysql.user WHERE user = 'token_user'`).Check(testkit.Rows("tidb_auth_token issuer-abc"))
	tk.MustExec(`ALTER USER token_user REQUIRE token_issuer 'issuer-123'`)
	tk.MustQuery(`SELECT plugin, token_issuer FROM mysql.user WHERE user = 'token_user'`).Check(testkit.Rows("tidb_auth_token issuer-123"))
	tk.MustExec(`ALTER USER token_user IDENTIFIED WITH 'tidb_auth_token'`)
	tk.MustExec(`CREATE USER token_user1 IDENTIFIED WITH 'tidb_auth_token'`)
	tk.MustQuery(`show warnings`).Check(testkit.RowsWithSep("|", "Warning|1105|TOKEN_ISSUER is needed for 'tidb_auth_token' user, please use 'alter user' to declare it"))
	tk.MustExec(`CREATE USER temp_user IDENTIFIED WITH 'mysql_native_password' BY '1234' REQUIRE token_issuer 'issuer-abc'`)
	tk.MustQuery(`show warnings`).Check(testkit.RowsWithSep("|", "Warning|1105|TOKEN_ISSUER is not needed for 'mysql_native_password' user"))
	tk.MustExec(`ALTER USER temp_user IDENTIFIED WITH 'tidb_auth_token' REQUIRE token_issuer 'issuer-abc'`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`ALTER USER temp_user IDENTIFIED WITH 'mysql_native_password' REQUIRE token_issuer 'issuer-abc'`)
	tk.MustQuery(`show warnings`).Check(testkit.RowsWithSep("|", "Warning|1105|TOKEN_ISSUER is not needed for the auth plugin"))
	tk.MustExec(`ALTER USER temp_user IDENTIFIED WITH 'tidb_auth_token'`)
	tk.MustQuery(`show warnings`).Check(testkit.RowsWithSep("|", "Warning|1105|Auth plugin 'tidb_auth_plugin' needs TOKEN_ISSUER"))
	tk.MustExec(`ALTER USER token_user REQUIRE SSL`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`ALTER USER token_user IDENTIFIED WITH 'mysql_native_password' BY '1234'`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`ALTER USER token_user IDENTIFIED WITH 'tidb_auth_token' REQUIRE token_issuer 'issuer-abc'`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())

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
	result.Check(testkit.Rows(auth.EncodePassword("111")))
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
	err := tk.ExecToErr(alterUserSQL)
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
	err = tk.ExecToErr(createUserSQL)
	require.Truef(t, terror.ErrorEqual(exeerrors.ErrPasswordFormat, err), "err %v", err)
	createUserSQL = `CREATE USER 'test1'@'localhost' identified by password '*3D56A309CD04FA2EEF181462E59011F075C89548';`
	tk.MustExec(createUserSQL)
	dropUserSQL = `DROP USER 'test1'@'localhost';`
	tk.MustExec(dropUserSQL)

	// Test drop user meet error
	err = tk.ExecToErr(dropUserSQL)
	require.Truef(t, terror.ErrorEqual(err, exeerrors.ErrCannotUser.GenWithStackByArgs("DROP USER", "")), "err %v", err)

	createUserSQL = `CREATE USER 'test1'@'localhost'`
	tk.MustExec(createUserSQL)
	createUserSQL = `CREATE USER 'test2'@'localhost'`
	tk.MustExec(createUserSQL)

	dropUserSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
	err = tk.ExecToErr(dropUserSQL)
	require.Truef(t, terror.ErrorEqual(err, exeerrors.ErrCannotUser.GenWithStackByArgs("DROP USER", "")), "err %v", err)

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
	err = tk.ExecToErr(createUserSQL)
	require.Truef(t, terror.ErrorEqual(err, exeerrors.ErrPluginIsNotLoaded), "err %v", err)

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
	store := testkit.CreateMockStore(t)
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
	err := tk.ExecToErr(setPwdSQL)
	require.Error(t, err)
	sess, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tk.SetSession(sess)
	ctx := tk.Session().(sessionctx.Context)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "testpwd1", Hostname: "localhost", AuthUsername: "testpwd1", AuthHostname: "localhost"}
	// Session user doesn't exist.
	err = tk.ExecToErr(setPwdSQL)
	require.Truef(t, terror.ErrorEqual(err, exeerrors.ErrPasswordNoMatch), "err %v", err)
	// normal
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "testpwd", Hostname: "localhost", AuthUsername: "testpwd", AuthHostname: "localhost"}
	tk.MustExec(setPwdSQL)
	result = tk.MustQuery(`SELECT authentication_string FROM mysql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("pwd")))
}

func TestFlushPrivilegesPanic(t *testing.T) {
	defer view.Stop()
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	require.Nil(t, dom.StatsHandle().DumpStatsDeltaToKV(true))

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
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1681|'DROP STATS ... PARTITION ...' is deprecated and will be removed in a future release."))
	checkPartitionStats("global", "p1", "global")

	err := tk.ExecToErr("drop stats test_drop_gstats partition abcde")
	require.Error(t, err)
	require.Equal(t, "can not found the specified partition name abcde in the table definition", err.Error())

	tk.MustExec("drop stats test_drop_gstats partition global")
	checkPartitionStats("global", "p1")

	tk.MustExec("drop stats test_drop_gstats global")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1287|'DROP STATS ... GLOBAL' is deprecated and will be removed in a future release. Please use DROP STATS ... instead"))
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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

func TestDropStatsForMultipleTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("create table t2 (c1 int, c2 int)")

	is := dom.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo1 := tbl1.Meta()

	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo2 := tbl2.Meta()

	h := dom.StatsHandle()
	h.Clear()
	testKit.MustExec("analyze table t1, t2")
	statsTbl1 := h.GetTableStats(tableInfo1)
	require.False(t, statsTbl1.Pseudo)
	statsTbl2 := h.GetTableStats(tableInfo2)
	require.False(t, statsTbl2.Pseudo)

	testKit.MustExec("drop stats t1, t2")
	require.Nil(t, h.Update(is))
	statsTbl1 = h.GetTableStats(tableInfo1)
	require.True(t, statsTbl1.Pseudo)
	statsTbl2 = h.GetTableStats(tableInfo2)
	require.True(t, statsTbl2.Pseudo)

	testKit.MustExec("analyze table t1, t2")
	statsTbl1 = h.GetTableStats(tableInfo1)
	require.False(t, statsTbl1.Pseudo)
	statsTbl2 = h.GetTableStats(tableInfo2)
	require.False(t, statsTbl2.Pseudo)

	h.SetLease(1)
	testKit.MustExec("drop stats t1, t2")
	require.Nil(t, h.Update(is))
	statsTbl1 = h.GetTableStats(tableInfo1)
	require.True(t, statsTbl1.Pseudo)
	statsTbl2 = h.GetTableStats(tableInfo2)
	require.True(t, statsTbl2.Pseudo)
	h.SetLease(0)
}

func TestKillStmt(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)

	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.EnableGlobalKill = false
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	connID := conn1.ID()

	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("kill %d", connID))
	result := tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead"))

	newCfg2 := *originCfg
	newCfg2.EnableGlobalKill = true
	config.StoreGlobalConfig(&newCfg2)

	// ZERO serverID, treated as truncated.
	tk.MustExec("kill 1")
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."))

	// truncated
	tk.MustExec("kill 101")
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."))

	// excceed int64
	tk.MustExec("kill 9223372036854775808") // 9223372036854775808 == 2^63
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Parse ConnectionID failed: unexpected connectionID exceeds int64"))

	// local kill
	connIDAllocator := globalconn.NewGlobalAllocator(dom.ServerID, false)
	killConnID := connIDAllocator.NextID()
	tk.MustExec("kill " + strconv.FormatUint(killConnID, 10))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows())

	tk.MustExecToErr("kill rand()", "Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead")
	// remote kill is tested in `tests/globalkilltest`
}
