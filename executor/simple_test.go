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
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

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
	result.Check(testkit.Rows("Warning 1105 Parse ConnectionID failed: Unexpected connectionID excceeds int64"))

	// local kill
	killConnID := util.NewGlobalConnID(connID, true)
	tk.MustExec("kill " + strconv.FormatUint(killConnID.ID(), 10))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows())

	tk.MustExecToErr("kill rand()", "Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead")
	// remote kill is tested in `tests/globalkilltest`
}

func TestUserAttributes(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	rootTK := testkit.NewTestKit(t, store)
	ctx := context.WithValue(context.Background(), tikvutil.RequestSourceKey, tikvutil.RequestSource{RequestSourceInternal: true})

	// https://dev.mysql.com/doc/refman/8.0/en/create-user.html#create-user-comments-attributes
	rootTK.MustExec(`CREATE USER testuser COMMENT '1234'`)
	rootTK.MustExec(`CREATE USER testuser1 ATTRIBUTE '{"name": "Tom", "age": 19}'`)
	_, err := rootTK.Exec(`CREATE USER testuser2 ATTRIBUTE '{"name": "Tom", age: 19}'`)
	rootTK.MustExec(`CREATE USER testuser2`)
	require.Error(t, err)
	rootTK.MustQuery(`SELECT user_attributes FROM mysql.user WHERE user = 'testuser'`).Check(testkit.Rows(`{"metadata": {"comment": "1234"}}`))
	rootTK.MustQuery(`SELECT user_attributes FROM mysql.user WHERE user = 'testuser1'`).Check(testkit.Rows(`{"metadata": {"age": 19, "name": "Tom"}}`))
	rootTK.MustQuery(`SELECT user_attributes FROM mysql.user WHERE user = 'testuser2'`).Check(testkit.Rows(`<nil>`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser'`).Check(testkit.Rows(`{"comment": "1234"}`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 19, "name": "Tom"}`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute->>"$.age" AS age, attribute->>"$.name" AS name FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`19 Tom`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser2'`).Check(testkit.Rows(`<nil>`))

	// https://dev.mysql.com/doc/refman/8.0/en/alter-user.html#alter-user-comments-attributes
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"age": 20, "sex": "male"}'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom", "sex": "male"}`))
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"hobby": "soccer"}'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "hobby": "soccer", "name": "Tom", "sex": "male"}`))
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"sex": null, "hobby": null}'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom"}`))
	rootTK.MustExec(`ALTER USER testuser1 COMMENT '5678'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "comment": "5678", "name": "Tom"}`))
	rootTK.MustExec(`ALTER USER testuser1 COMMENT ''`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "comment": "", "name": "Tom"}`))
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"comment": null}'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom"}`))

	// Non-root users could access COMMENT or ATTRIBUTE of all users via the view,
	// but not via the mysql.user table.
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "testuser1"}, nil, nil))
	tk.MustQueryWithContext(ctx, `SELECT user, host, attribute FROM information_schema.user_attributes ORDER BY user`).Check(
		testkit.Rows("root % <nil>", "testuser % {\"comment\": \"1234\"}", "testuser1 % {\"age\": 20, \"name\": \"Tom\"}", "testuser2 % <nil>"))
	tk.MustGetErrCode(`SELECT user, host, user_attributes FROM mysql.user ORDER BY user`, mysql.ErrTableaccessDenied)

	// https://github.com/pingcap/tidb/issues/39207
	rootTK.MustExec("create user usr1@'%' identified by 'passord'")
	rootTK.MustExec("alter user usr1 comment 'comment1'")
	rootTK.MustQuery("select user_attributes from mysql.user where user = 'usr1'").Check(testkit.Rows(`{"metadata": {"comment": "comment1"}}`))
}

func TestValidatePassword(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	subtk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil)
	require.NoError(t, err)
	tk.MustExec("CREATE USER ''@'localhost'")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.* TO ''@'localhost';")
	err = subtk.Session().Auth(&auth.UserIdentity{Hostname: "localhost"}, nil, nil)
	require.NoError(t, err)

	authPlugins := []string{mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password}
	tk.MustQuery("SELECT @@global.validate_password.enable").Check(testkit.Rows("0"))
	tk.MustExec("SET GLOBAL validate_password.enable = 1")
	tk.MustQuery("SELECT @@global.validate_password.enable").Check(testkit.Rows("1"))

	for _, authPlugin := range authPlugins {
		tk.MustExec("DROP USER IF EXISTS testuser")
		tk.MustExec(fmt.Sprintf("CREATE USER testuser IDENTIFIED WITH %s BY '!Abc12345678'", authPlugin))

		tk.MustExec("SET GLOBAL validate_password.policy = 'LOW'")
		// check user name
		tk.MustQuery("SELECT @@global.validate_password.check_user_name").Check(testkit.Rows("1"))
		tk.MustContainErrMsg("ALTER USER testuser IDENTIFIED BY '!Abcdroot1234'", "Password Contains User Name")
		tk.MustContainErrMsg("ALTER USER testuser IDENTIFIED BY '!Abcdtoor1234'", "Password Contains Reversed User Name")
		tk.MustExec("SET PASSWORD FOR 'testuser' = 'testuser'") // password the same as the user name, but run by root
		tk.MustExec("ALTER USER testuser IDENTIFIED BY 'testuser'")
		tk.MustExec("SET GLOBAL validate_password.check_user_name = 0")
		tk.MustExec("ALTER USER testuser IDENTIFIED BY '!Abcdroot1234'")
		tk.MustExec("ALTER USER testuser IDENTIFIED BY '!Abcdtoor1234'")
		tk.MustExec("SET GLOBAL validate_password.check_user_name = 1")

		// LOW: Length
		tk.MustExec("SET GLOBAL validate_password.length = 8")
		tk.MustQuery("SELECT @@global.validate_password.length").Check(testkit.Rows("8"))
		tk.MustContainErrMsg("ALTER USER testuser IDENTIFIED BY '1234567'", "Require Password Length: 8")
		tk.MustExec("SET GLOBAL validate_password.length = 12")
		tk.MustContainErrMsg("ALTER USER testuser IDENTIFIED BY '!Abcdefg123'", "Require Password Length: 12")
		tk.MustExec("ALTER USER testuser IDENTIFIED BY '!Abcdefg1234'")
		tk.MustExec("SET GLOBAL validate_password.length = 8")

		// MEDIUM: Length; numeric, lowercase/uppercase, and special characters
		tk.MustExec("SET GLOBAL validate_password.policy = 'MEDIUM'")
		tk.MustExec("ALTER USER testuser IDENTIFIED BY '!Abc1234567'")
		tk.MustContainErrMsg("ALTER USER testuser IDENTIFIED BY '!ABC1234567'", "Require Password Lowercase Count: 1")
		tk.MustContainErrMsg("ALTER USER testuser IDENTIFIED BY '!abc1234567'", "Require Password Uppercase Count: 1")
		tk.MustContainErrMsg("ALTER USER testuser IDENTIFIED BY '!ABCDabcd'", "Require Password Digit Count: 1")
		tk.MustContainErrMsg("ALTER USER testuser IDENTIFIED BY 'Abc1234567'", "Require Password Non-alphanumeric Count: 1")
		tk.MustExec("SET GLOBAL validate_password.special_char_count = 0")
		tk.MustExec("ALTER USER testuser IDENTIFIED BY 'Abc1234567'")
		tk.MustExec("SET GLOBAL validate_password.special_char_count = 1")
		tk.MustExec("SET GLOBAL validate_password.length = 3")
		tk.MustQuery("SELECT @@GLOBAL.validate_password.length").Check(testkit.Rows("4"))

		// STRONG: Length; numeric, lowercase/uppercase, and special characters; dictionary file
		tk.MustExec("SET GLOBAL validate_password.policy = 'STRONG'")
		tk.MustExec("ALTER USER testuser IDENTIFIED BY '!Abc1234567'")
		tk.MustExec(fmt.Sprintf("SET GLOBAL validate_password.dictionary = '%s'", "1234;5678"))
		tk.MustExec("ALTER USER testuser IDENTIFIED BY '!Abc123567'")
		tk.MustExec("ALTER USER testuser IDENTIFIED BY '!Abc43218765'")
		tk.MustContainErrMsg("ALTER USER testuser IDENTIFIED BY '!Abc1234567'", "Password contains word in the dictionary")
		tk.MustExec("SET GLOBAL validate_password.dictionary = ''")
		tk.MustExec("ALTER USER testuser IDENTIFIED BY '!Abc1234567'")

		// "IDENTIFIED AS 'xxx'" is not affected by validation
		tk.MustExec(fmt.Sprintf("ALTER USER testuser IDENTIFIED WITH '%s' AS ''", authPlugin))
	}
	tk.MustContainErrMsg("CREATE USER 'testuser1'@'localhost'", "Your password does not satisfy the current policy requirements")
	tk.MustContainErrMsg("CREATE USER 'testuser1'@'localhost' IDENTIFIED WITH 'caching_sha2_password'", "Your password does not satisfy the current policy requirements")
	tk.MustContainErrMsg("CREATE USER 'testuser1'@'localhost' IDENTIFIED WITH 'caching_sha2_password' AS ''", "Your password does not satisfy the current policy requirements")

	// if the username is '', all password can pass the check_user_name
	subtk.MustQuery("SELECT user(), current_user()").Check(testkit.Rows("@localhost @localhost"))
	subtk.MustQuery("SELECT @@global.validate_password.check_user_name").Check(testkit.Rows("1"))
	subtk.MustQuery("SELECT @@global.validate_password.enable").Check(testkit.Rows("1"))
	tk.MustExec("SET GLOBAL validate_password.number_count = 0")
	tk.MustExec("SET GLOBAL validate_password.special_char_count = 0")
	tk.MustExec("SET GLOBAL validate_password.mixed_case_count = 0")
	tk.MustExec("SET GLOBAL validate_password.length = 0")
	subtk.MustExec("ALTER USER ''@'localhost' IDENTIFIED BY ''")
	subtk.MustExec("ALTER USER ''@'localhost' IDENTIFIED BY 'abcd'")

	// CREATE ROLE is not affected by password validation
	tk.MustExec("SET GLOBAL validate_password.enable = 1")
	tk.MustExec("SET GLOBAL validate_password.number_count = default")
	tk.MustExec("SET GLOBAL validate_password.special_char_count = default")
	tk.MustExec("SET GLOBAL validate_password.mixed_case_count = default")
	tk.MustExec("SET GLOBAL validate_password.length = default")
	tk.MustExec("CREATE ROLE role1")
}

func expectedPasswordExpiration(t *testing.T, tk *testkit.TestKit, testuser, expired string, lifetime string) {
	res := tk.MustQuery(fmt.Sprintf("SELECT password_expired, password_last_changed, password_lifetime FROM mysql.user WHERE user = '%s'", testuser))
	rows := res.Rows()
	require.NotEmpty(t, rows)
	row := rows[0]
	require.Equal(t, 3, len(row))
	require.Equal(t, expired, row[0].(string), testuser)
	require.True(t, len(row[1].(string)) > 0, testuser)
	require.Equal(t, lifetime, row[2].(string), testuser)
}

func TestPasswordExpiration(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// CREATE USER
	tk.MustExec(`CREATE USER testuser`)
	expectedPasswordExpiration(t, tk, "testuser", "N", "<nil>")
	tk.MustExec(`CREATE USER testuser1 PASSWORD EXPIRE`)
	expectedPasswordExpiration(t, tk, "testuser1", "Y", "<nil>")
	tk.MustExec(`CREATE USER testuser2 PASSWORD EXPIRE DEFAULT`)
	expectedPasswordExpiration(t, tk, "testuser2", "N", "<nil>")
	tk.MustExec(`CREATE USER testuser3 PASSWORD EXPIRE NEVER`)
	expectedPasswordExpiration(t, tk, "testuser3", "N", "0")
	tk.MustExec(`CREATE USER testuser4 PASSWORD EXPIRE INTERVAL 3 DAY`)
	expectedPasswordExpiration(t, tk, "testuser4", "N", "3")
	tk.MustExec(`CREATE ROLE role1`)
	expectedPasswordExpiration(t, tk, "role1", "Y", "<nil>")

	// ALTER USER
	testcases := []struct {
		user    string
		expired string
	}{
		{"testuser", "N"},
		{"testuser1", "Y"},
		{"testuser2", "N"},
		{"testuser3", "N"},
		{"testuser4", "N"},
		{"role1", "Y"},
	}
	for _, testcase := range testcases {
		tk.MustExec(fmt.Sprintf("ALTER USER %s PASSWORD EXPIRE NEVER", testcase.user))
		expectedPasswordExpiration(t, tk, testcase.user, testcase.expired, "0")
		tk.MustExec(fmt.Sprintf("ALTER USER %s PASSWORD EXPIRE DEFAULT", testcase.user))
		expectedPasswordExpiration(t, tk, testcase.user, testcase.expired, "<nil>")
		tk.MustExec(fmt.Sprintf("ALTER USER %s PASSWORD EXPIRE INTERVAL 3 DAY", testcase.user))
		expectedPasswordExpiration(t, tk, testcase.user, testcase.expired, "3")
		tk.MustExec(fmt.Sprintf("ALTER USER %s PASSWORD EXPIRE", testcase.user))
		expectedPasswordExpiration(t, tk, testcase.user, "Y", "3")
		tk.MustExec(fmt.Sprintf("ALTER USER %s IDENTIFIED BY '' PASSWORD EXPIRE", testcase.user))
		expectedPasswordExpiration(t, tk, testcase.user, "Y", "3")
		tk.MustExec(fmt.Sprintf("ALTER USER %s IDENTIFIED WITH 'mysql_native_password' AS ''", testcase.user))
		expectedPasswordExpiration(t, tk, testcase.user, "N", "3")
		tk.MustExec(fmt.Sprintf("ALTER USER %s IDENTIFIED BY ''", testcase.user))
		expectedPasswordExpiration(t, tk, testcase.user, "N", "3")
	}

	// SET PASSWORD
	tk.MustExec("ALTER USER testuser PASSWORD EXPIRE")
	expectedPasswordExpiration(t, tk, "testuser", "Y", "3")
	tk.MustExec("SET PASSWORD FOR testuser = '1234'")
	expectedPasswordExpiration(t, tk, "testuser", "N", "3")

	tk.MustGetErrCode(`CREATE USER ''@localhost IDENTIFIED BY 'pass' PASSWORD EXPIRE`, mysql.ErrPasswordExpireAnonymousUser)
	tk.MustExec(`CREATE USER ''@localhost IDENTIFIED BY 'pass'`)
	tk.MustGetErrCode(`ALTER USER ''@localhost PASSWORD EXPIRE`, mysql.ErrPasswordExpireAnonymousUser)

	// different cleartext authentication plugin
	for _, authplugin := range []string{mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password} {
		tk.MustExec("DROP USER IF EXISTS 'u1'@'localhost'")
		tk.MustExec(fmt.Sprintf("CREATE USER 'u1'@'localhost' IDENTIFIED WITH '%s'", authplugin))
		tk.MustExec("ALTER USER 'u1'@'localhost' IDENTIFIED BY 'pass'")
		tk.MustExec("ALTER USER 'u1'@'localhost' PASSWORD EXPIRE")
		tk.MustQuery("SELECT password_expired FROM mysql.user WHERE user = 'u1'").Check(testkit.Rows("Y"))
	}
}
