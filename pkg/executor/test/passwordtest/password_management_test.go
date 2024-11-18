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

package passwordtest

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/stretchr/testify/require"
)

func TestValidatePassword(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	subtk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)
	tk.MustExec("CREATE USER ''@'localhost'")
	tk.MustExec("GRANT ALL PRIVILEGES ON mysql.* TO ''@'localhost';")
	err = subtk.Session().Auth(&auth.UserIdentity{Hostname: "localhost"}, nil, nil, nil)
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
	tk.MustGetErrCode("CREATE USER 'testuser1'@'localhost'", errno.ErrNotValidPassword)
	tk.MustGetErrCode("CREATE USER 'testuser1'@'localhost' IDENTIFIED WITH 'caching_sha2_password'", errno.ErrNotValidPassword)
	tk.MustGetErrCode("CREATE USER 'testuser1'@'localhost' IDENTIFIED WITH 'caching_sha2_password' AS ''", errno.ErrNotValidPassword)

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

// Test cases that related to PASSWORD VALIDATION, PASSWORD EXPIRATION, PASSWORD REUSE POLICY, and PASSWORD FAILED-LOGIN TRACK.
func TestPasswordManagement(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("SET GLOBAL validate_password.enable = 1")

	// PASSWORD VALIDATION can work with user-specified PASSWORD REUSE POLICY.
	tk.MustExec("CREATE USER u1 IDENTIFIED BY '!Abc1234' password history 1")
	tk.MustGetErrCode("ALTER USER u1 IDENTIFIED BY '!Abc1234'", errno.ErrExistsInHistoryPassword)
	tk.MustGetErrCode("ALTER USER u1 IDENTIFIED BY '!abc1234'", errno.ErrNotValidPassword)

	// PASSWORD VALIDATION can work with global PASSWORD REUSE POLICY.
	tk.MustExec("SET GLOBAL password_history = 1")
	tk.MustExec("DROP USER u1")
	tk.MustExec("CREATE USER u1 IDENTIFIED BY '!Abc1234'")
	tk.MustGetErrCode("ALTER USER u1 IDENTIFIED BY '!Abc1234'", errno.ErrExistsInHistoryPassword)
	tk.MustGetErrCode("ALTER USER u1 IDENTIFIED BY '!abc1234'", errno.ErrNotValidPassword)

	// PASSWORD EXPIRATION can work with ACCOUNT LOCK.
	// PASSWORD EXPIRE NEVER and ACCOUNT UNLOCK take effect.
	tk.MustExec(`ALTER USER u1 ACCOUNT LOCK PASSWORD EXPIRE NEVER PASSWORD EXPIRE NEVER ACCOUNT UNLOCK ACCOUNT LOCK ACCOUNT LOCK ACCOUNT UNLOCK;`)
	tk.MustQuery(`SELECT password_expired, password_lifetime, account_locked FROM mysql.user WHERE USER='u1';`).Check(
		testkit.Rows("N 0 N"))

	// PASSWORD EXPIRATION can work with PASSWORD REUSE POLICY
	tk.MustExec(`create user u2 identified by '!Abc1234' password expire password reuse interval default password expire never password
		reuse interval 3 day password history 5 password history default password expire default`)
	tk.MustQuery(`select password_expired, password_lifetime, password_reuse_history, password_reuse_time from mysql.user where user = 'u2'`).Check(
		testkit.Rows("N <nil> <nil> 3"))
	tk.MustExec(`alter user u2 password expire default password reuse interval 3 day password history default
		password expire never password expire interval 5 day password reuse interval default password expire password history 5`)
	tk.MustQuery(`select password_expired, password_lifetime, password_reuse_history, password_reuse_time from mysql.user where user = 'u2'`).Check(
		testkit.Rows("Y <nil> 5 <nil>"))
	tk.MustExec(`alter user u2 identified by '!Abc12345'`)
	tk.MustQuery(`select password_expired, password_lifetime, password_reuse_history, password_reuse_time from mysql.user where user = 'u2'`).Check(
		testkit.Rows("N <nil> 5 <nil>"))

	// PASSWORD FAILED-LOGIN TRACK can work with USER COMMENT and USER ATTRIBUTE
	tk.MustExec(`CREATE USER u3 IDENTIFIED BY '!Abc12345' FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME 3 COMMENT 'Some statements to test create user'`)
	tk.MustQuery(`select user_attributes->>"$.metadata" from mysql.user where user = 'u3'`).Check(testkit.Rows(`{"comment": "Some statements to test create user"}`))
	tk.MustQuery(`select user_attributes->>"$.Password_locking" from mysql.user where user = 'u3'`).Check(testkit.Rows(`{"failed_login_attempts": 4, "password_lock_time_days": 3}`))
	tk.MustExec(`ALTER USER u3 FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME unbounded FAILED_LOGIN_ATTEMPTS 5 PASSWORD_LOCK_TIME 5 ATTRIBUTE '{"name": "John", "age": 19}'`)
	tk.MustQuery(`select user_attributes->>"$.metadata" from mysql.user where user = 'u3'`).Check(testkit.Rows(`{"age": 19, "comment": "Some statements to test create user", "name": "John"}`))
	tk.MustQuery(`select user_attributes->>"$.Password_locking" from mysql.user where user = 'u3'`).Check(testkit.Rows(`{"failed_login_attempts": 5, "password_lock_time_days": 5}`))

	tk.MustExec("SET GLOBAL validate_password.enable = 0")

	rootTK := testkit.NewTestKit(t, store)
	// Password Strength Check.
	rootTK.MustExec(`set global validate_password.enable = ON`)
	rootTK.MustExec(`drop user u2`)
	rootTK.MustGetErrCode(`create user u2 identified by 'u2' PASSWORD EXPIRE INTERVAL 2 DAY password history 2
		password reuse interval 2 day FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1`, 1819)
	rootTK.MustGetErrCode(`create user u2`, 1819)
	rootTK.MustGetErrCode(`create user u2 identified by 'u2222222' PASSWORD EXPIRE INTERVAL 2 DAY password history 2
		password reuse interval 2 day FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1`, 1819)
	rootTK.MustGetErrCode(`create user u2 identified by 'Uu2222222' PASSWORD EXPIRE INTERVAL 2 DAY password history 2
		password reuse interval 2 day FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1`, 1819)
	rootTK.MustGetErrCode(`create user u2 identified by 'Uu3222222' PASSWORD EXPIRE INTERVAL 2 DAY password history 2
		password reuse interval 2 day FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1`, 1819)
	rootTK.MustExec(`create user u2 identified by 'Uu3@22222' PASSWORD EXPIRE INTERVAL 2 DAY password history 2
		password reuse interval 2 day FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1`)
	rootTK.MustQuery(`Select count(*) from mysql.password_history where user = 'u2' and host = '%'`).Check(testkit.Rows("1"))
	result := rootTK.MustQuery(`Select authentication_string from mysql.user where user = 'u2' and host = '%'`)
	result.Check(testkit.Rows(auth.EncodePassword("Uu3@22222")))
	// Disable password reuse.
	rootTK.MustGetErrCode(`Alter user u2 identified by 'Uu3@22222'`, 3638)
	rootTK.MustGetErrCode(`Set password for 'u2' = 'Uu3@22222'`, 3638)
	// Password Strength Check.
	rootTK.MustGetErrCode(`Alter user u2 identified by 'U2'`, 1819)
	rootTK.MustGetErrCode(`Set password for 'u2' = 'U2'`, 1819)
	// Did not modify successfully.
	result = rootTK.MustQuery(`Select authentication_string from mysql.user where user = 'u2' and host = '%'`)
	result.Check(testkit.Rows(auth.EncodePassword("Uu3@22222")))
	// Auto-lock in effect.
	err := tk.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "%"}, sha1Password("<wrong-password>"), nil, nil)
	require.ErrorContains(t, err, "Account is blocked for 1 day(s) (1 day(s) remaining) due to 1 consecutive failed logins.")
	result = rootTK.MustQuery(`SELECT
		JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_count')),
		JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.auto_account_locked')) from mysql.user where user = 'u2' and host = '%'`)
	result.Check(testkit.Rows(`1 Y`))
	rootTK.MustExec(`ALTER user u2 account unlock`)

	// Unlock in effect.
	result = rootTK.MustQuery(`SELECT
		JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_count')),
		JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.auto_account_locked')) from mysql.user where user = 'u2' and host = '%'`)
	result.Check(testkit.Rows(`0 N`))

	rootTK.MustExec(`set global validate_password.enable = OFF`)
	rootTK.MustExec(`update mysql.user set Password_last_changed = date_sub(Password_last_changed,interval '3 0:0:1' DAY_SECOND)  where user = 'u2' and host = '%'`)
	err = domain.GetDomain(rootTK.Session()).NotifyUpdatePrivilege()
	require.NoError(t, err)
	// Password expires and takes effect.
	err = tk.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "%"}, sha1Password("Uu3@22222"), nil, nil)
	require.ErrorContains(t, err, "Your password has expired.")
	variable.IsSandBoxModeEnabled.Store(true)
	err = tk.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "%"}, sha1Password("Uu3@22222"), nil, nil)
	require.NoError(t, err)
	require.True(t, tk.Session().InSandBoxMode())

	rootTK.MustExec(`set global validate_password.enable = ON`)
	// Forbid other users to change password.
	tk.MustGetErrCode(`Alter user root identified by 'Uu3@22222'`, 1820)
	// Disable password reuse.
	tk.MustGetErrCode(`Alter user u2 identified by 'Uu3@22222'`, 3638)
	tk.MustGetErrCode(`set password = 'Uu3@22222'`, 3638)
	// Password Strength Check.
	tk.MustGetErrCode(`Alter user u2 identified by 'U2'`, 1819)
	tk.MustGetErrCode(`set password = 'U2'`, 1819)
	tk.MustExec(`Set password = 'Uu3@22223'`)
	require.False(t, tk.Session().InSandBoxMode())
	rootTK.MustQuery(`Select count(*) from mysql.password_history where user = 'u2' and host = '%'`).Check(testkit.Rows("2"))
	result = rootTK.MustQuery(`Select authentication_string from mysql.user where user = 'u2' and host = '%'`)
	result.Check(testkit.Rows(auth.EncodePassword("Uu3@22223")))
	tk = testkit.NewTestKit(t, store)
	err = tk.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "%"}, sha1Password("Uu3@22223"), nil, nil)
	require.NoError(t, err)
}

// Test basic CREATE/ALTER USER with failed-login track.
func TestFailedLoginTrackingBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	createUserTestCases := []struct {
		sql    string
		rsJSON string
		user   string
	}{
		{"CREATE USER 'u1'@'localhost' IDENTIFIED BY 'password' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3;",
			"{\"failed_login_attempts\": 3, \"password_lock_time_days\": 3}", "u1"},
		{"CREATE USER 'u2'@'localhost' IDENTIFIED BY 'password' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME UNBOUNDED;",
			"{\"failed_login_attempts\": 3, \"password_lock_time_days\": -1}", "u2"},
		{"CREATE USER 'u3'@'localhost' IDENTIFIED BY 'password' FAILED_LOGIN_ATTEMPTS 3;",
			"{\"failed_login_attempts\": 3, \"password_lock_time_days\": 0}", "u3"},
		{"CREATE USER 'u4'@'localhost' IDENTIFIED BY 'password' PASSWORD_LOCK_TIME 3;",
			"{\"failed_login_attempts\": 0, \"password_lock_time_days\": 3}", "u4"},
		{"CREATE USER 'u5'@'localhost' IDENTIFIED BY 'password' PASSWORD_LOCK_TIME UNBOUNDED;",
			"{\"failed_login_attempts\": 0, \"password_lock_time_days\": -1}", "u5"},
	}
	for _, tc := range createUserTestCases {
		tk.MustExec(tc.sql)
		sql := fmt.Sprintf("SELECT user_attributes->>\"$.Password_locking\" from mysql.user WHERE USER = '%s' AND HOST = 'localhost' for update", tc.user)
		tk.MustQuery(sql).Check(testkit.Rows(tc.rsJSON))
	}

	alterUserTestCases := []struct {
		sql                  string
		user                 string
		failedLoginAttempts  int64
		passwordLockTimeDays int64
		failedLoginCount     int64
		comment              string
	}{
		{"ALTER USER 'u1'@'localhost' FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME 6;", "u1",
			4, 6, 0, ""},
		{"ALTER USER 'u2'@'localhost' FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME UNBOUNDED;",
			"u2", 4, -1, 0, ""},
		{"ALTER USER 'u3'@'localhost' PASSWORD_LOCK_TIME 6;",
			"u3", 3, 6, 0, ""},
		{"ALTER USER 'u4'@'localhost' FAILED_LOGIN_ATTEMPTS 4;",
			"u4", 4, 3, 0, ""},
		{"ALTER USER 'u4'@'localhost' PASSWORD_LOCK_TIME UNBOUNDED;",
			"u4", 4, -1, 0, ""},
		{"ALTER USER 'u5'@'localhost' ACCOUNT UNLOCK FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 6;",
			"u5", 3, 6, 0, ""},
		{"ALTER USER 'u5'@'localhost' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 6 COMMENT 'Something';",
			"u5", 3, 6, 0, "Something"},
	}
	for _, tc := range alterUserTestCases {
		tk.MustExec(tc.sql)
		sql := fmt.Sprintf("SELECT user_attributes from mysql.user WHERE USER = '%s' AND HOST = 'localhost' for update", tc.user)
		rs := tk.MustQuery(sql)
		buf := bytes.NewBufferString("")
		for _, row := range rs.Rows() {
			_, err := fmt.Fprintf(buf, "%s\n", row)
			require.NoError(t, err)
		}
		str := buf.String()
		var ua []userAttributes
		err := json.Unmarshal([]byte(str), &ua)
		require.NoError(t, err)
		require.Equal(t, tc.failedLoginAttempts, ua[0].PasswordLocking.FailedLoginAttempts, tc.sql, str)
		require.Equal(t, tc.passwordLockTimeDays, ua[0].PasswordLocking.PasswordLockTimeDays, tc.sql, str)
		require.Equal(t, tc.failedLoginCount, ua[0].PasswordLocking.FailedLoginCount, tc.sql, str)
		require.Equal(t, tc.comment, ua[0].Metadata.Comment, tc.sql, str)
	}

	tk.MustExec("CREATE USER 'u6'@'localhost' IDENTIFIED BY 'password' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3;")
	tk.MustQuery(" SHOW CREATE USER 'u6'@'localhost';").Check(
		testkit.Rows("CREATE USER 'u6'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3"))

	tk.MustExec("CREATE USER 'u7'@'localhost' IDENTIFIED BY 'password';")
	tk.MustQuery(" SHOW CREATE USER 'u7'@'localhost';").Check(
		testkit.Rows("CREATE USER 'u7'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT"))

	tk.MustExec("CREATE USER 'u8'@'localhost' IDENTIFIED BY 'password' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME UNBOUNDED;")
	tk.MustQuery(" SHOW CREATE USER 'u8'@'localhost';").Check(
		testkit.Rows("CREATE USER 'u8'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME UNBOUNDED"))

	tk.MustExec("ALTER USER 'u4'@'localhost' PASSWORD_LOCK_TIME 0 FAILED_LOGIN_ATTEMPTS 0")
	tk.MustQuery("select user_attributes from mysql.user where user = 'u4' and host = 'localhost'").Check(testkit.Rows(`<nil>`))
	tk.MustExec("ALTER USER 'u4'@'localhost' account unlock")
	tk.MustQuery("select user_attributes from mysql.user where user = 'u4' and host = 'localhost'").Check(testkit.Rows(`<nil>`))
	tk.MustExec("ALTER USER 'u4'@'localhost' PASSWORD_LOCK_TIME 6")
	tk.MustQuery("select user_attributes from mysql.user where user = 'u4' and host = 'localhost'").Check(testkit.Rows(`{"Password_locking": {"failed_login_attempts": 0, "password_lock_time_days": 6}}`))
}

func sha1Password(s string) []byte {
	crypt := sha1.New()
	crypt.Write([]byte(s))
	hashStage1 := crypt.Sum(nil)
	crypt.Reset()
	crypt.Write(hashStage1)
	hashStage2 := crypt.Sum(nil)
	crypt.Reset()
	crypt.Write(hashStage2)
	hashStage3 := crypt.Sum(nil)
	for i := range hashStage3 {
		hashStage3[i] ^= hashStage1[i]
	}
	return hashStage3
}

func TestFailedLoginTracking(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Set FAILED_LOGIN_ATTEMPTS to 1, and check error messages after  login failure once.
	createAndCheck(tk, "CREATE USER 'testu1'@'localhost' IDENTIFIED BY 'testu1' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1",
		"{\"Password_locking\": {\"failed_login_attempts\": 1, \"password_lock_time_days\": 1}}", "testu1")
	err := tk.Session().Auth(&auth.UserIdentity{Username: "testu1", Hostname: "localhost"}, sha1Password("password"), nil, nil)
	lds := strconv.FormatInt(1, 10)
	errTarget := privileges.GenerateAccountAutoLockErr(1, "testu1", "localhost", lds, lds)
	require.Equal(t, err.Error(), errTarget.Error())
	checkAuthUser(t, tk, "testu1", 1, "Y")

	// Check the login error message after the account is locked.
	err = tk.Session().Auth(&auth.UserIdentity{Username: "testu1", Hostname: "localhost"}, sha1Password("password"), nil, nil)
	require.Equal(t, err.Error(), errTarget.Error())
	checkAuthUser(t, tk, "testu1", 1, "Y")

	// Set FAILED_LOGIN_ATTEMPTS to 1 and PASSWORD_LOCK_TIME to UNBOUNDED. Check error messages after failed login once.
	createAndCheck(tk, "CREATE USER 'testu2'@'localhost' IDENTIFIED BY 'testu2' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME UNBOUNDED",
		"{\"Password_locking\": {\"failed_login_attempts\": 1, \"password_lock_time_days\": -1}}", "testu2")
	err = tk.Session().Auth(&auth.UserIdentity{Username: "testu2", Hostname: "localhost"}, sha1Password("password"), nil, nil)
	errTarget = privileges.GenerateAccountAutoLockErr(1, "testu2", "localhost", "unlimited", "unlimited")
	require.Equal(t, err.Error(), errTarget.Error())
	checkAuthUser(t, tk, "testu2", 1, "Y")

	// Check the login error message after the account is locked.
	err = tk.Session().Auth(&auth.UserIdentity{Username: "testu2", Hostname: "localhost"}, sha1Password("password"), nil, nil)
	require.Equal(t, err.Error(), errTarget.Error())
	checkAuthUser(t, tk, "testu2", 1, "Y")

	// Set FAILED_LOGIN_ATTEMPTS to 0 or PASSWORD_LOCK_TIME to 0. Check error messages after failed login once.
	createAndCheck(tk, "CREATE USER 'testu3'@'localhost' IDENTIFIED BY 'testu3' FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME UNBOUNDED",
		"{\"Password_locking\": {\"failed_login_attempts\": 0, \"password_lock_time_days\": -1}}", "testu3")
	err = tk.Session().Auth(&auth.UserIdentity{Username: "testu3", Hostname: "localhost"}, sha1Password("password"), nil, nil)
	require.ErrorContains(t, err, "Access denied for user 'testu3'@'localhost' (using password: YES)")
	checkAuthUser(t, tk, "testu3", 0, "")
	createAndCheck(tk, "CREATE USER 'testu4'@'localhost' IDENTIFIED BY 'testu4' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 0",
		"{\"Password_locking\": {\"failed_login_attempts\": 1, \"password_lock_time_days\": 0}}", "testu4")
	err = tk.Session().Auth(&auth.UserIdentity{Username: "testu4", Hostname: "localhost"}, sha1Password("password"), nil, nil)
	require.ErrorContains(t, err, "Access denied for user 'testu4'@'localhost' (using password: YES)")
	checkAuthUser(t, tk, "testu4", 0, "")
	tk.MustExec("CREATE USER 'testu5'@'localhost' IDENTIFIED BY 'testu5' FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 0")
	err = tk.Session().Auth(&auth.UserIdentity{Username: "testu5", Hostname: "localhost"}, sha1Password("password"), nil, nil)
	require.ErrorContains(t, err, "Access denied for user 'testu5'@'localhost' (using password: YES)")
	tk.MustQuery("select user_attributes from mysql.user where user= 'testu5' and host = 'localhost'").Check(testkit.Rows("{}"))

	tk.MustExec("DROP USER 'testu1'@'localhost', 'testu2'@'localhost', 'testu3'@'localhost', 'testu4'@'localhost', 'testu5'@'localhost'")

	// Create user specifying only comment.
	tk.MustExec("CREATE USER 'testu1'@'localhost' IDENTIFIED BY 'testu1' comment 'testcomment' ")
	tk.MustQuery("select user_attributes from mysql.user where user= 'testu1' and host = 'localhost'").
		Check(testkit.Rows("{\"metadata\": {\"comment\": \"testcomment\"}}"))

	// Create user specifying only attribute.
	tk.MustExec("create user testu2@'localhost' identified by 'testu2' ATTRIBUTE '{\"attribute\":\"testattribute\"}'")
	tk.MustQuery("select user_attributes from mysql.user where user= 'testu2' and host = 'localhost'").
		Check(testkit.Rows("{\"metadata\": {\"attribute\": \"testattribute\"}}"))

	// Create user specified comment and FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME.
	tk.MustExec("create user testu3@'localhost' identified by 'testu3' FAILED_LOGIN_ATTEMPTS 1 " +
		"PASSWORD_LOCK_TIME 1 comment 'testcomment'")
	checkUserUserAttributes(tk, "testu3", "localhost", "1 <nil> <nil> 1 {\"comment\": \"testcomment\"}")

	// Create user specified attribute and FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME.
	tk.MustExec("create user testu4@'localhost' identified by 'testu4' FAILED_LOGIN_ATTEMPTS 1 " +
		"PASSWORD_LOCK_TIME 1 ATTRIBUTE '{\"attribute\":\"testattribute\"}'")
	checkUserUserAttributes(tk, "testu4", "localhost", "1 <nil> <nil> 1 {\"attribute\": \"testattribute\"}")

	// Create user specified comment, FAILED_LOGIN_ATTEMPTS, and PASSWORD_LOCK_TIME,
	// and confirm the user_attributes column value after login fails.
	tk.MustExec("create user testu5@'localhost' identified by 'testu5' FAILED_LOGIN_ATTEMPTS 2 " +
		"PASSWORD_LOCK_TIME 1 comment 'testcomment'")
	checkUserUserAttributes(tk, "testu5", "localhost", "2 <nil> <nil> 1 {\"comment\": \"testcomment\"}")

	// Confirm the user_attributes value after login failure once.
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu5", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu5", "localhost", "2 \"N\" 1 1 {\"comment\": \"testcomment\"}")

	// After the number of failed login attempts reaches FAILED_LOGIN_ATTEMPTS, check the account lock status.
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu5", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu5", "localhost", "2 \"Y\" 2 1 {\"comment\": \"testcomment\"}")
	// After the account is locked, manually unlock the account and check the user_attributes value.
	tk.MustExec("alter user testu5@'localhost' account  unlock")
	checkUserUserAttributes(tk, "testu5", "localhost", "2 \"N\" 0 1 {\"comment\": \"testcomment\"}")

	// Create user specified comment, FAILED_LOGIN_ATTEMPTS, and PASSWORD_LOCK_TIME,
	// and confirm the user_attributes column value after login fails.
	tk.MustExec("create user testu6@'localhost' identified by '' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1 " +
		"comment 'testcomment'")
	// Confirm the user_attributes value after login failure once.
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu6", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu6", "localhost", "2 \"N\" 1 1 {\"comment\": \"testcomment\"}")

	// After the number of failed login attempts reaches FAILED_LOGIN_ATTEMPTS, check the account lock status.
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu6", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu6", "localhost", "2 \"Y\" 2 1 {\"comment\": \"testcomment\"}")

	// After the account is automatically locked, change the lock time and check
	// the user_attributes value after logging in successfully.
	changeAutoLockedLastChanged(tk, "-72h1s", "testu6")
	sk1 := testkit.NewTestKit(t, store)
	require.NoError(t, sk1.Session().Auth(&auth.UserIdentity{Username: "testu6", Hostname: "localhost"}, nil, nil, nil))
	checkUserUserAttributes(tk, "testu6", "localhost", "3 \"N\" 0 3 {\"comment\": \"testcomment\"}")

	// Create user specified attributes, FAILED_LOGIN_ATTEMPTS, and PASSWORD_LOCK_TIME,
	// and confirm the user_attributes column value after login fails.
	tk.MustExec("create user testu7@'localhost' identified by 'testu7' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1 " +
		"ATTRIBUTE '{\"attribute\":\"testattribute\"}'")
	checkUserUserAttributes(tk, "testu7", "localhost", "2 <nil> <nil> 1 {\"attribute\": \"testattribute\"}")

	// Confirm the user_attributes value after login failure once.
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu7", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu7", "localhost", "2 \"N\" 1 1 {\"attribute\": \"testattribute\"}")

	// After the number of failed login attempts reaches FAILED_LOGIN_ATTEMPTS, check the account lock status.
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu7", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu7", "localhost", "2 \"Y\" 2 1 {\"attribute\": \"testattribute\"}")

	// After the account is locked, manually unlock the account and check the user_attributes value.
	tk.MustExec("alter user testu7@'localhost' account  unlock")
	checkUserUserAttributes(tk, "testu7", "localhost", "2 \"N\" 0 1 {\"attribute\": \"testattribute\"}")

	tk.MustExec("create user testu8@'localhost' identified by '' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1" +
		" ATTRIBUTE '{\"attribute\":\"testattribute\"}'")
	// Confirm the user_attributes value after login failure once.
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu8", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu8", "localhost", "2 \"N\" 1 1 {\"attribute\": \"testattribute\"}")

	// After the number of failed login attempts reaches FAILED_LOGIN_ATTEMPTS, check the account lock status.
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu8", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu8", "localhost", "2 \"Y\" 2 1 {\"attribute\": \"testattribute\"}")

	// After the account is automatically locked, change the lock time and check
	// the user_attributes value after logging in successfully.
	changeAutoLockedLastChanged(tk, "-72h1s", "testu8")
	sk2 := testkit.NewTestKit(t, store)
	require.NoError(t, sk2.Session().Auth(&auth.UserIdentity{Username: "testu8", Hostname: "localhost"}, nil, nil, nil))
	checkUserUserAttributes(tk, "testu8", "localhost", "3 \"N\" 0 3 {\"attribute\": \"testattribute\"}")

	// FAILED_LOGIN_ATTEMPTS is set to 2 . check user_attributes value after
	// the user login fails once ,and login success at second time.
	tk.MustExec("create user testu9@'localhost' identified by '' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1" +
		" comment 'testcomment'")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu9", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu9", "localhost", "2 \"N\" 1 1 {\"comment\": \"testcomment\"}")
	sk3 := testkit.NewTestKit(t, store)
	require.NoError(t, sk3.Session().Auth(&auth.UserIdentity{Username: "testu9", Hostname: "localhost"},
		nil, nil, nil))
	checkUserUserAttributes(tk, "testu9", "localhost", "2 \"N\" 0 1 {\"comment\": \"testcomment\"}")

	// FAILED_LOGIN_ATTEMPTS or PASSWORD_LOCK_TIME is set to 0. Check user_attributes value after login fail.
	tk.MustExec("create user testu10@'localhost' identified by '' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 0 " +
		"comment 'testcomment'")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu10", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu10", "localhost", "2 <nil> <nil> 0 {\"comment\": \"testcomment\"}")

	tk.MustExec("create user testu11@'localhost' identified by '' FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 2 " +
		"comment 'testcomment'")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu11", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu11", "localhost", "0 <nil> <nil> 2 {\"comment\": \"testcomment\"}")

	// The account is automatically locked after the user specifies FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME.
	// Change FAILED_LOGIN_ATTEMPTS or PASSWORD_LOCK_TIME to 0, and check whether the user can login.
	tk.MustExec("create user testu12@'localhost' identified by '' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1 " +
		"comment 'testcomment'")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu12", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu12", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu12", "localhost", "2 \"Y\" 2 1 {\"comment\": \"testcomment\"}")
	tk.MustExec("alter user testu12@'localhost' FAILED_LOGIN_ATTEMPTS 0")
	checkUserUserAttributes(tk, "testu12", "localhost", "0 \"Y\" 2 1 {\"comment\": \"testcomment\"}")
	sk4 := testkit.NewTestKit(t, store)
	require.NoError(t, sk4.Session().Auth(&auth.UserIdentity{Username: "testu12", Hostname: "localhost"},
		nil, nil, nil))

	rootk := testkit.NewTestKit(t, store)
	createAndCheck(tk, "CREATE USER 'u6'@'localhost' IDENTIFIED BY '' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3",
		"{\"Password_locking\": {\"failed_login_attempts\": 3, \"password_lock_time_days\": 3}}", "u6")
	createAndCheck(tk, "CREATE USER 'u5'@'localhost' IDENTIFIED BY '' FAILED_LOGIN_ATTEMPTS 60 PASSWORD_LOCK_TIME 3",
		"{\"Password_locking\": {\"failed_login_attempts\": 60, \"password_lock_time_days\": 3}}", "u5")

	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 1, "N")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, nil, nil, nil))
	checkAuthUser(t, rootk, "u6", 0, "N")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))

	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 1, "N")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 2, "N")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 3, "Y")

	changeAutoLockedLastChanged(rootk, "-72h1s", "u6")
	loadUser(t, tk, 1, rootk)
	checkAuthUser(t, rootk, "u6", 3, "Y")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, nil, nil, nil))
	checkAuthUser(t, rootk, "u6", 0, "N")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))

	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 1, "N")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 2, "N")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 3, "Y")
	alterAndCheck(t, rootk, "ALTER USER 'u6'@'localhost' ACCOUNT UNLOCK;", "u6", 3, 3, 0)
	loadUser(t, tk, 2, rootk)
	checkAuthUser(t, rootk, "u6", 0, "N")

	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 1, "N")
	alterAndCheck(t, rootk, "ALTER USER 'u6'@'localhost' ACCOUNT UNLOCK;", "u6", 3, 3, 0)
	checkAuthUser(t, rootk, "u6", 0, "N")

	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 1, "N")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 2, "N")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 3, "Y")
	changeAutoLockedLastChanged(rootk, "-72h1s", "u6")
	loadUser(t, tk, 3, rootk)
	checkAuthUser(t, rootk, "u6", 3, "Y")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u6", 1, "N")

	createAndCheck(rootk, "CREATE USER 'u1'@'localhost' IDENTIFIED BY '' FAILED_LOGIN_ATTEMPTS 3",
		"{\"Password_locking\": {\"failed_login_attempts\": 3, \"password_lock_time_days\": 0}}", "u1")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u6", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u1", 0, "")
	alterAndCheck(t, rootk, "ALTER USER 'u1'@'localhost' PASSWORD_LOCK_TIME 6;", "u1", 3, 6, 0)
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u1", 1, "N")
}

func TestFailedLoginTrackingAlterUser(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Create user specifying only comment.
	tk.MustExec("CREATE USER 'testu1'@'localhost' IDENTIFIED BY 'testu1' comment 'testcomment' ")
	tk.MustQuery("select user_attributes from mysql.user where user= 'testu1' and host = 'localhost'").
		Check(testkit.Rows("{\"metadata\": {\"comment\": \"testcomment\"}}"))
	tk.MustExec("Alter USER 'testu1'@'localhost' comment ''")
	tk.MustQuery("select user_attributes from mysql.user where user= 'testu1' and host = 'localhost'").
		Check(testkit.Rows("{\"metadata\": {\"comment\": \"\"}}"))

	// Create user specifying only attribute.
	tk.MustExec("CREATE USER 'testu2'@'localhost' IDENTIFIED BY 'testu2' ATTRIBUTE '{\"attribute\":\"testattribute\"}'")
	tk.MustQuery("select user_attributes from mysql.user where user= 'testu2' and host = 'localhost'").
		Check(testkit.Rows("{\"metadata\": {\"attribute\": \"testattribute\"}}"))
	tk.MustExec("Alter USER 'testu2'@'localhost' ATTRIBUTE '{\"attribute\":\"test\"}'")
	tk.MustQuery("select user_attributes from mysql.user where user= 'testu2' and host = 'localhost'").
		Check(testkit.Rows("{\"metadata\": {\"attribute\": \"test\"}}"))

	// Create a user and specify FAILED_LOGIN_ATTEMPTS, PASSWORD_LOCK_TIME, and COMMENT.
	// Check the user_attributes value after alter user.
	tk.MustExec("CREATE USER 'testu3'@'localhost' IDENTIFIED BY 'testu3' FAILED_LOGIN_ATTEMPTS 1 " +
		"PASSWORD_LOCK_TIME 1 comment 'testcomment'")
	checkUserUserAttributes(tk, "testu3", "localhost", "1 <nil> <nil> 1 {\"comment\": \"testcomment\"}")
	tk.MustExec("alter user 'testu3'@'localhost' FAILED_LOGIN_ATTEMPTS 0")
	checkUserUserAttributes(tk, "testu3", "localhost", "0 <nil> <nil> 1 {\"comment\": \"testcomment\"}")
	tk.MustExec("alter user 'testu3'@'localhost' PASSWORD_LOCK_TIME 0")
	tk.MustQuery("select JSON_EXTRACT(user_attributes, '$.Password_locking')," +
		"JSON_EXTRACT(user_attributes, '$.metadata')from mysql.user where user= 'testu3' and host = 'localhost'").
		Check(testkit.Rows("<nil> {\"comment\": \"testcomment\"}"))

	// Create a user and specify FAILED_LOGIN_ATTEMPTS, PASSWORD_LOCK_TIME, and ATTRIBUTE.
	// Check the user_attributes value after alter user.
	tk.MustExec("CREATE USER 'testu4'@'localhost' IDENTIFIED BY 'testu4' FAILED_LOGIN_ATTEMPTS 1 " +
		"PASSWORD_LOCK_TIME 1 ATTRIBUTE '{\"attribute\":\"testattribute\"}'")
	checkUserUserAttributes(tk, "testu4", "localhost", "1 <nil> <nil> 1 {\"attribute\": \"testattribute\"}")
	tk.MustExec("alter user 'testu4'@'localhost' FAILED_LOGIN_ATTEMPTS 0")
	checkUserUserAttributes(tk, "testu4", "localhost", "0 <nil> <nil> 1 {\"attribute\": \"testattribute\"}")
	tk.MustExec("alter user 'testu4'@'localhost' PASSWORD_LOCK_TIME 0")
	tk.MustQuery("select JSON_EXTRACT(user_attributes, '$.Password_locking')," +
		"JSON_EXTRACT(user_attributes, '$.metadata')from mysql.user where user= 'testu4' and host = 'localhost'").
		Check(testkit.Rows("<nil> {\"attribute\": \"testattribute\"}"))

	// Create a user and specify FAILED_LOGIN_ATTEMPTS, PASSWORD_LOCK_TIME, and ATTRIBUTE.
	// Check the user_attributes value after alter user.
	tk.MustExec("CREATE USER 'testu5'@'localhost' IDENTIFIED BY 'testu5' FAILED_LOGIN_ATTEMPTS 1 " +
		"PASSWORD_LOCK_TIME 1 ATTRIBUTE '{\"attribute\":\"testattribute\"}'")
	checkUserUserAttributes(tk, "testu5", "localhost", "1 <nil> <nil> 1 {\"attribute\": \"testattribute\"}")
	tk.MustExec("alter user 'testu5'@'localhost' FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 0  ATTRIBUTE '{\"attribute\":\"test\"}'")
	tk.MustQuery("select JSON_EXTRACT(user_attributes, '$.Password_locking')," +
		"JSON_EXTRACT(user_attributes, '$.metadata')from mysql.user where user= 'testu5' and host = 'localhost'").
		Check(testkit.Rows("<nil> {\"attribute\": \"test\"}"))

	// Create a user to specify a comment, modify the user to add an ATTRIBUTE,
	// modify the user to delete a comment, and check the user_attributes value.
	tk.MustExec("CREATE USER 'testu6'@'localhost' IDENTIFIED BY 'testu6' FAILED_LOGIN_ATTEMPTS 1 " +
		"PASSWORD_LOCK_TIME 1 comment 'testcomment'")
	checkUserUserAttributes(tk, "testu6", "localhost", "1 <nil> <nil> 1 {\"comment\": \"testcomment\"}")
	tk.MustExec("alter user 'testu6'@'localhost' ATTRIBUTE '{\"attribute\": \"testattribute\"}'")
	checkUserUserAttributes(tk, "testu6", "localhost", "1 <nil> <nil> 1 {\"attribute\": \"testattribute\", \"comment\": \"testcomment\"}")
	tk.MustExec("alter user 'testu6'@'localhost' ATTRIBUTE '{\"comment\": null}'")
	checkUserUserAttributes(tk, "testu6", "localhost", "1 <nil> <nil> 1 {\"attribute\": \"testattribute\"}")

	// After consecutive login failures and the account is locked,
	// change the values of FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME to 0 and check the user_attributes value
	tk.MustExec("CREATE USER 'testu7'@'localhost' IDENTIFIED BY 'testu7' FAILED_LOGIN_ATTEMPTS 1 " +
		"PASSWORD_LOCK_TIME 1 comment 'testcomment'")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu7", Hostname: "localhost"},
		sha1Password("password"), nil, nil))
	checkUserUserAttributes(tk, "testu7", "localhost", "1 \"Y\" 1 1 {\"comment\": \"testcomment\"}")
	tk.MustExec("alter user 'testu7'@'localhost' FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 0")
	tk.MustQuery("select JSON_EXTRACT(user_attributes, '$.Password_locking'),JSON_EXTRACT(user_attributes,'$.metadata') " +
		"from mysql.user where user='testu7' and host ='localhost'").Check(testkit.Rows("<nil> {\"comment\": \"testcomment\"}"))

	// Create a user and specify FAILED_LOGIN_ATTEMPTS, PASSWORD_LOCK_TIME.
	// Check the user_attributes value after alter user.
	tk.MustExec("CREATE USER 'testu8'@'localhost' IDENTIFIED BY 'testu5' FAILED_LOGIN_ATTEMPTS 1 " +
		"PASSWORD_LOCK_TIME 1")
	checkUserUserAttributes(tk, "testu8", "localhost", "1 <nil> <nil> 1 <nil>")
	tk.MustExec("alter user 'testu8'@'localhost' FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 0")
	tk.MustQuery("select user_attributes from mysql.user where user= 'testu8' and host = 'localhost'").
		Check(testkit.Rows("<nil>"))

	// Specify only FAILED_LOGIN_ATTEMPTS one attribute when creating user.
	// Change the value to 0 and check the user_attributes value.
	tk.MustExec("CREATE USER 'testu9'@'localhost' IDENTIFIED BY 'testu9' FAILED_LOGIN_ATTEMPTS 1 ")
	tk.MustQuery("select JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_attempts') " +
		"from mysql.user where user='testu9' and host ='localhost'").Check(testkit.Rows("1"))
	tk.MustExec("ALTER USER 'testu9'@'localhost' FAILED_LOGIN_ATTEMPTS 0")
	tk.MustQuery("select user_attributes from mysql.user where user='testu9' and host ='localhost'").Check(testkit.Rows("<nil>"))

	// Specify only PASSWORD_LOCK_TIME one attribute when creating user.
	// Change the value to 0 and check the user_attributes value.
	tk.MustExec("CREATE USER 'testu10'@'localhost' IDENTIFIED BY 'testu10' PASSWORD_LOCK_TIME 1 ")
	tk.MustQuery("select JSON_EXTRACT(user_attributes, '$.Password_locking.password_lock_time_days') " +
		"from mysql.user where user='testu10' and host ='localhost'").Check(testkit.Rows("1"))
	tk.MustExec("ALTER USER 'testu10'@'localhost' PASSWORD_LOCK_TIME 0")
	tk.MustQuery("select user_attributes from mysql.user where user='testu10' and host ='localhost'").Check(testkit.Rows("<nil>"))

	// Specify FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME attributes when creating user ,
	// change the values of the two attributes to 0, and check the value of user_attributes.
	tk.MustExec("CREATE USER 'testu11'@'localhost' IDENTIFIED BY 'testu11' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1 ")
	tk.MustQuery("select JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_attempts')," +
		"JSON_EXTRACT(user_attributes, '$.Password_locking.password_lock_time_days') " +
		"from mysql.user where user='testu11' and host ='localhost'").Check(testkit.Rows("1 1"))
	tk.MustExec("ALTER USER 'testu11'@'localhost' PASSWORD_LOCK_TIME 0")
	tk.MustQuery("select JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_attempts')," +
		"JSON_EXTRACT(user_attributes, '$.Password_locking.password_lock_time_days') " +
		"from mysql.user where user='testu11' and host ='localhost'").Check(testkit.Rows("1 0"))
	tk.MustExec("ALTER USER 'testu11'@'localhost' FAILED_LOGIN_ATTEMPTS 0")
	tk.MustQuery("select user_attributes " +
		"from mysql.user where user='testu11' and host ='localhost'").Check(testkit.Rows("<nil>"))

	rootTK := testkit.NewTestKit(t, store)
	sql := new(strings.Builder)
	checkUserAttributes := "select JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_attempts')," +
		"JSON_EXTRACT(user_attributes, '$.Password_locking.auto_account_locked')," +
		"JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_count')," +
		"JSON_EXTRACT(user_attributes, '$.Password_locking.password_lock_time_days')," +
		"JSON_EXTRACT(user_attributes, '$.metadata')from mysql.user where user= %? and host = %?"
	err := domain.GetDomain(rootTK.Session()).NotifyUpdatePrivilege()
	require.NoError(t, err)
	rootTK.MustExec(`CREATE USER test1 IDENTIFIED BY '1234' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3 COMMENT 'test'`)
	err = tk.Session().Auth(&auth.UserIdentity{Username: "test1", Hostname: "%"}, sha1Password("1234"), nil, nil)
	require.NoError(t, err)
	sqlescape.MustFormatSQL(sql, checkUserAttributes, "test1", "%")
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`3 <nil> <nil> 3 {"comment": "test"}`))
	tk = testkit.NewTestKit(t, store)
	err = tk.Session().Auth(&auth.UserIdentity{Username: "test1", Hostname: "%"}, sha1Password("<wrong-password>"), nil, nil)
	require.Error(t, err)

	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`3 "N" 1 3 {"comment": "test"}`))
	rootTK.MustExec(`Alter user test1  FAILED_LOGIN_ATTEMPTS 4 `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`4 "N" 1 3 {"comment": "test"}`))
	rootTK.MustExec(`Alter user test1  PASSWORD_LOCK_TIME 4 `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`4 "N" 1 4 {"comment": "test"}`))
	rootTK.MustExec(`Alter user test1  COMMENT 'test1' `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`4 "N" 1 4 {"comment": "test1"}`))
	rootTK.MustExec(`Alter user test1 FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3 COMMENT 'test'`)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`3 "N" 1 3 {"comment": "test"}`))

	err = tk.Session().Auth(&auth.UserIdentity{Username: "test1", Hostname: "%"}, sha1Password("<wrong-password>"), nil, nil)
	require.Error(t, err)
	err = tk.Session().Auth(&auth.UserIdentity{Username: "test1", Hostname: "%"}, sha1Password("<wrong-password>"), nil, nil)
	require.Error(t, err)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`3 "Y" 3 3 {"comment": "test"}`))
	rootTK.MustExec(`Alter user test1  FAILED_LOGIN_ATTEMPTS 4 `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`4 "Y" 3 3 {"comment": "test"}`))
	rootTK.MustExec(`Alter user test1  PASSWORD_LOCK_TIME 4 `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`4 "Y" 3 4 {"comment": "test"}`))
	rootTK.MustExec(`Alter user test1  COMMENT 'test2' `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`4 "Y" 3 4 {"comment": "test2"}`))
	rootTK.MustExec(`Alter user test1  account unlock `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`4 "N" 0 4 {"comment": "test2"}`))

	rootTK.MustExec(`Alter user test1  FAILED_LOGIN_ATTEMPTS 0 `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`0 "N" 0 4 {"comment": "test2"}`))
	rootTK.MustExec(`Alter user test1  PASSWORD_LOCK_TIME 0 `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`<nil> <nil> <nil> <nil> {"comment": "test2"}`))

	rootTK.MustExec(`Alter user test1  account unlock `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`<nil> <nil> <nil> <nil> {"comment": "test2"}`))
	rootTK.MustExec(`Alter user test1  FAILED_LOGIN_ATTEMPTS 4 `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`4 <nil> <nil> 0 {"comment": "test2"}`))
	rootTK.MustExec(`Alter user test1  account unlock `)
	rootTK.MustQuery(sql.String()).Check(testkit.Rows(`4 "N" 0 0 {"comment": "test2"}`))
}

func TestFailedLoginTrackingCheckPrivilges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createAndCheck(tk, "CREATE USER 'testu1'@'localhost' IDENTIFIED BY '' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1",
		"{\"Password_locking\": {\"failed_login_attempts\": 1, \"password_lock_time_days\": 1}}", "testu1")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "testu1", Hostname: "localhost"}, nil, nil, nil))
	// Specify FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME attributes when creating user ,
	// Check user privileges  after successful login.
	tk.MustQuery(`show grants`).Check(testkit.Rows("GRANT USAGE ON *.* TO 'testu1'@'localhost'"))
	tk.MustQuery(`select user()`).Check(testkit.Rows("testu1@localhost"))
}

func TestUserPassword(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set global validate_password.enable = ON`)

	testcases := []struct {
		errSQL         string
		sucSQL         string
		user           string
		host           string
		rsJSON         string
		simplePassword string
		strongPassword string
	}{
		{
			"CREATE USER 'u1'@'localhost' IDENTIFIED BY 'qwe123' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 4;",
			"CREATE USER 'u1'@'localhost' IDENTIFIED BY '!@#HASHhs123' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 4;",
			"u1",
			"localhost",
			"{\"Password_locking\": {\"failed_login_attempts\": 3, \"password_lock_time_days\": 4}}",
			"qwe123",
			"!@#HASHhs123",
		},
		{
			`CREATE USER 'u2'@'localhost' IDENTIFIED BY 'qwe123' FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME 3 COMMENT 'Some statements to test create user'`,
			`CREATE USER 'u2'@'localhost' IDENTIFIED BY '!@#HASHhs123' FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME 3 COMMENT 'Some statements to test create user'`,
			"u2",
			"localhost",
			"{\"Password_locking\": {\"failed_login_attempts\": 4, \"password_lock_time_days\": 3}, \"metadata\": {\"comment\": \"Some statements to test create user\"}}",
			"qwe123",
			"!@#HASHhs123",
		},
	}
	for _, tc := range testcases {
		tk := testkit.NewTestKit(t, store)
		rootk := testkit.NewTestKit(t, store)
		createAndCheckToErr(t, rootk, tc.errSQL, tc.user)
		require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: tc.user, Hostname: tc.host}, sha1Password(tc.simplePassword), nil, nil))
		createAndCheck(rootk, tc.sucSQL, tc.rsJSON, tc.user)
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: tc.user, Hostname: tc.host}, sha1Password(tc.strongPassword), nil, nil))
	}
}

func TestPasswordExpiredAndTacking(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	user := "u3"
	host := "localhost"
	tk.MustExec(`set global validate_password.enable = ON`)
	tk = testkit.NewTestKit(t, store)
	createAndCheckToErr(t, tk, `CREATE USER 'u3'@'localhost' IDENTIFIED BY 'qwe123' PASSWORD EXPIRE INTERVAL 3 DAY FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME 3 COMMENT 'Some statements to test create user'`, user)
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: user, Hostname: host}, sha1Password("qwe123"), nil, nil))
	tk = testkit.NewTestKit(t, store)
	createAndCheck(tk, `CREATE USER 'u3'@'localhost' IDENTIFIED BY '!@#HASHhs123' PASSWORD EXPIRE INTERVAL 3 DAY  FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME 3 COMMENT 'Some statements to test create user'`,
		"{\"Password_locking\": {\"failed_login_attempts\": 4, \"password_lock_time_days\": 3}, \"metadata\": {\"comment\": \"Some statements to test create user\"}}", user)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: user, Hostname: host}, sha1Password("!@#HASHhs123"), nil, nil))

	tk = testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("ALTER USER '%s'@'%s' PASSWORD EXPIRE NEVER", user, host))
	tk = testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: user, Hostname: host}, sha1Password("!@#HASHhs123"), nil, nil))

	loginFailedAncCheck(t, store, user, host, "password", 1, "N")
	loginSucAncCheck(t, store, user, host, "!@#HASHhs123", 0, "N")
	loginFailedAncCheck(t, store, user, host, "password", 1, "N")
	loginFailedAncCheck(t, store, user, host, "password", 2, "N")
	loginFailedAncCheck(t, store, user, host, "password", 3, "N")
	loginFailedAncCheck(t, store, user, host, "password", 4, "Y")

	tk = testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("ALTER USER '%s'@'%s' PASSWORD EXPIRE", user, host))
	tk = testkit.NewTestKit(t, store)
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: user, Hostname: host}, sha1Password("!@#HASHhs123"), nil, nil))
}

// TestPasswordMySQLCompatibility is to test compatibility with the output of what MySQL outputs on SHOW CREATE USER.
func TestPasswordMySQLCompatibility(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// MySQL 8.0.37
	//
	// This is using mysql_native_password as that's common with this version, however it is not the default.
	//
	// CREATE USER 'test80037'@'%' IDENTIFIED WITH 'mysql_native_password' BY 'secret';
	// SHOW CREATE USER 'test80037'@'%';
	tk.MustExec(
		"CREATE USER `test80037`@`%` " +
			"IDENTIFIED WITH 'mysql_native_password' AS '*14E65567ABDB5135D0CFD9A70B3032C179A49EE7' " +
			"REQUIRE NONE " +
			"PASSWORD EXPIRE DEFAULT " +
			"ACCOUNT UNLOCK " +
			"PASSWORD HISTORY DEFAULT " +
			"PASSWORD REUSE INTERVAL DEFAULT " +
			"PASSWORD REQUIRE CURRENT DEFAULT",
	)

	// MySQL 8.4.0
	//
	// NOT using mysql_native_password here as that is disabled by default in this version.
	//
	// CREATE USER 'test80400'@'%';
	// SHOW CREATE USER 'test80400'@'%';
	tk.MustExec(
		"CREATE USER `test80400`@`%` " +
			"IDENTIFIED WITH 'caching_sha2_password' " +
			"REQUIRE NONE " +
			"PASSWORD EXPIRE DEFAULT " +
			"ACCOUNT UNLOCK " +
			"PASSWORD HISTORY DEFAULT " +
			"PASSWORD REUSE INTERVAL DEFAULT " +
			"PASSWORD REQUIRE CURRENT DEFAULT",
	)
}

func loginFailedAncCheck(t *testing.T, store kv.Storage, user, host, password string, failedLoginCount int64, autoAccountLocked string) {
	tk := testkit.NewTestKit(t, store)
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: user, Hostname: host}, sha1Password(password), nil, nil))
	checkAuthUser(t, tk, user, failedLoginCount, autoAccountLocked)
}

func loginSucAncCheck(t *testing.T, store kv.Storage, user, host, password string, failedLoginCount int64, autoAccountLocked string) {
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: user, Hostname: host}, sha1Password(password), nil, nil))
	tk = testkit.NewTestKit(t, store)
	checkAuthUser(t, tk, user, failedLoginCount, autoAccountLocked)
}

func loadUser(t *testing.T, tk *testkit.TestKit, useCount int64, rootk *testkit.TestKit) {
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u5", Hostname: "localhost"}, sha1Password("password"), nil, nil))
	checkAuthUser(t, rootk, "u5", useCount, "N")
}

func changeAutoLockedLastChanged(tk *testkit.TestKit, ds, user string) {
	SQL := "UPDATE `mysql`.`User` SET user_attributes=json_merge_patch(user_attributes, '{\"Password_locking\": {\"failed_login_attempts\": 3," +
		"\"password_lock_time_days\": 3,\"auto_account_locked\": \"Y\",\"failed_login_count\": 3,\"auto_locked_last_changed\": \"%s\"}}') " +
		"WHERE Host='localhost' and User='%s'"
	d, _ := time.ParseDuration(ds)
	changeTime := time.Now().Add(d).Format(time.UnixDate)
	SQL = fmt.Sprintf(SQL, changeTime, user)
	tk.MustExec(SQL)
	domain.GetDomain(tk.Session()).NotifyUpdatePrivilege()
}

func checkUserUserAttributes(tk *testkit.TestKit, user, host, row string) {
	sqlTemplate := "select JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_attempts')," +
		"JSON_EXTRACT(user_attributes, '$.Password_locking.auto_account_locked')," +
		"JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_count')," +
		"JSON_EXTRACT(user_attributes, '$.Password_locking.password_lock_time_days')," +
		"JSON_EXTRACT(user_attributes, '$.metadata')from mysql.user where user= %? and host = %?"
	userAttributesSQL := new(strings.Builder)
	sqlescape.MustFormatSQL(userAttributesSQL, sqlTemplate, user, host)
	tk.MustQuery(userAttributesSQL.String()).Check(testkit.Rows(row))
}

func alterAndCheck(t *testing.T, tk *testkit.TestKit, sql string, user string, failedLoginAttempts, passwordLockTimeDays, failedLoginCount int64) {
	tk.MustExec(sql)
	userAttributesSQL := selectSQL(user)
	resBuff := bytes.NewBufferString("")
	rs := tk.MustQuery(userAttributesSQL)
	for _, row := range rs.Rows() {
		_, err := fmt.Fprintf(resBuff, "%s\n", row)
		require.NoError(t, err)
	}
	err := checkUser(t, resBuff.String(), failedLoginAttempts, passwordLockTimeDays, failedLoginCount)
	require.NoError(t, err)
}

func checkUser(t *testing.T, rs string, failedLoginAttempts, passwordLockTimeDays, failedLoginCount int64) error {
	var ua []userAttributes
	if err := json.Unmarshal([]byte(rs), &ua); err != nil {
		return err
	}
	require.Equal(t, failedLoginAttempts, ua[0].PasswordLocking.FailedLoginAttempts)
	require.Equal(t, passwordLockTimeDays, ua[0].PasswordLocking.PasswordLockTimeDays)
	require.Equal(t, failedLoginCount, ua[0].PasswordLocking.FailedLoginCount)
	return nil
}

func createAndCheck(tk *testkit.TestKit, sql, rsJSON, user string) {
	tk.MustExec(sql)
	sql = selectSQL(user)
	tk.MustQuery(sql).Check(testkit.Rows(rsJSON))
}

func createAndCheckToErr(t *testing.T, tk *testkit.TestKit, sql, user string) {
	tk.MustExecToErr(sql)
	sql = selectSQL(user)
	require.Equal(t, 0, len(tk.MustQuery(sql).Rows()))
}

func checkAuthUser(t *testing.T, tk *testkit.TestKit, user string, failedLoginCount int64, autoAccountLocked string) {
	userAttributesSQL := selectSQL(user)
	resBuff := bytes.NewBufferString("")
	rs := tk.MustQuery(userAttributesSQL)
	for _, row := range rs.Rows() {
		_, err := fmt.Fprintf(resBuff, "%s\n", row)
		require.NoError(t, err)
	}
	var ua []userAttributes
	err := json.Unmarshal(resBuff.Bytes(), &ua)
	require.NoError(t, err)
	require.Equal(t, failedLoginCount, ua[0].PasswordLocking.FailedLoginCount)
	require.Equal(t, autoAccountLocked, ua[0].PasswordLocking.AutoAccountLocked)
}

func selectSQL(user string) string {
	userAttributesSQL := new(strings.Builder)
	sqlescape.MustFormatSQL(userAttributesSQL, "SELECT user_attributes from mysql.user WHERE USER = %? AND HOST = 'localhost' for update", user)
	return userAttributesSQL.String()
}

type passwordLocking struct {
	FailedLoginAttempts   int64  `json:"failed_login_attempts"`
	PasswordLockTimeDays  int64  `json:"password_lock_time_days"`
	AutoAccountLocked     string `json:"auto_account_locked"`
	FailedLoginCount      int64  `json:"failed_login_count"`
	AutoLockedLastChanged string `json:"auto_locked_last_changed"`
}

type metadata struct {
	Comment string `json:"comment"`
}

type userAttributes struct {
	PasswordLocking passwordLocking `json:"Password_locking"`
	Metadata        metadata        `json:"metadata"`
}
