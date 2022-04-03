// Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestGrantGlobal(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testGlobal'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	// Make sure all the global privs for new user is "N".
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal\" and host=\"localhost\";", mysql.Priv2UserCol[v])
		r := tk.MustQuery(sql)
		r.Check(testkit.Rows("N"))
	}

	// Grant each priv to the user.
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("GRANT %s ON *.* TO 'testGlobal'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		sql = fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal\" and host=\"localhost\"", mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("Y"))
	}

	// Create a new user.
	createUserSQL = `CREATE USER 'testGlobal1'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec("GRANT ALL ON *.* TO 'testGlobal1'@'localhost';")
	// Make sure all the global privs for granted user is "Y".
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal1\" and host=\"localhost\"", mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("Y"))
	}
	// with grant option
	tk.MustExec("GRANT ALL ON *.* TO 'testGlobal1'@'localhost' WITH GRANT OPTION;")
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal1\" and host=\"localhost\"", mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("Y"))
	}
}

func TestGrantDBScope(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testDB'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	// Make sure all the db privs for new user is empty.
	sql := `SELECT * FROM mysql.db WHERE User="testDB" and host="localhost"`
	tk.MustQuery(sql).Check(testkit.Rows())

	// Grant each priv to the user.
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("GRANT %s ON test.* TO 'testDB'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		sql = fmt.Sprintf("SELECT %s FROM mysql.DB WHERE User=\"testDB\" and host=\"localhost\" and db=\"test\"", mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("Y"))
	}

	// Create a new user.
	createUserSQL = `CREATE USER 'testDB1'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec("USE test;")
	tk.MustExec("GRANT ALL ON * TO 'testDB1'@'localhost';")
	// Make sure all the db privs for granted user is "Y".
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.DB WHERE User=\"testDB1\" and host=\"localhost\" and db=\"test\";", mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("Y"))
	}

	// Grant in wrong scope.
	_, err := tk.Exec(` grant create user on test.* to 'testDB1'@'localhost';`)
	require.True(t, terror.ErrorEqual(err, executor.ErrWrongUsage.GenWithStackByArgs("DB GRANT", "GLOBAL PRIVILEGES")))

	_, err = tk.Exec("GRANT SUPER ON test.* TO 'testDB1'@'localhost';")
	require.True(t, terror.ErrorEqual(err, executor.ErrWrongUsage.GenWithStackByArgs("DB GRANT", "NON-DB PRIVILEGES")))
}

func TestWithGrantOption(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testWithGrant'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	// Make sure all the db privs for new user is empty.
	sql := `SELECT * FROM mysql.db WHERE User="testWithGrant" and host="localhost"`
	tk.MustQuery(sql).Check(testkit.Rows())

	// Grant select priv to the user, with grant option.
	tk.MustExec("GRANT select ON test.* TO 'testWithGrant'@'localhost' WITH GRANT OPTION;")
	tk.MustQuery("SELECT grant_priv FROM mysql.DB WHERE User=\"testWithGrant\" and host=\"localhost\" and db=\"test\"").Check(testkit.Rows("Y"))

	tk.MustExec("CREATE USER 'testWithGrant1'")
	tk.MustQuery("SELECT grant_priv FROM mysql.user WHERE User=\"testWithGrant1\"").Check(testkit.Rows("N"))
	tk.MustExec("GRANT ALL ON *.* TO 'testWithGrant1'")
	tk.MustQuery("SELECT grant_priv FROM mysql.user WHERE User=\"testWithGrant1\"").Check(testkit.Rows("N"))
	tk.MustExec("GRANT ALL ON *.* TO 'testWithGrant1' WITH GRANT OPTION")
	tk.MustQuery("SELECT grant_priv FROM mysql.user WHERE User=\"testWithGrant1\"").Check(testkit.Rows("Y"))
}

func TestGrantTableScope(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testTbl'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec(`CREATE TABLE test.test1(c1 int);`)
	// Make sure all the table privs for new user is empty.
	tk.MustQuery(`SELECT * FROM mysql.Tables_priv WHERE User="testTbl" and host="localhost" and db="test" and Table_name="test1"`).Check(testkit.Rows())

	// Grant each priv to the user.
	for _, v := range mysql.AllTablePrivs {
		sql := fmt.Sprintf("GRANT %s ON test.test1 TO 'testTbl'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		rows := tk.MustQuery(`SELECT Table_priv FROM mysql.Tables_priv WHERE User="testTbl" and host="localhost" and db="test" and Table_name="test1";`).Rows()
		require.Len(t, rows, 1)
		row := rows[0]
		require.Len(t, rows, 1)
		p := fmt.Sprintf("%v", row[0])
		require.Greater(t, strings.Index(p, mysql.Priv2SetStr[v]), -1)
	}
	// Create a new user.
	createUserSQL = `CREATE USER 'testTbl1'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec("USE test;")
	tk.MustExec(`CREATE TABLE test2(c1 int);`)
	// Grant all table scope privs.
	tk.MustExec("GRANT ALL ON test2 TO 'testTbl1'@'localhost' WITH GRANT OPTION;")
	// Make sure all the table privs for granted user are in the Table_priv set.
	for _, v := range mysql.AllTablePrivs {
		rows := tk.MustQuery(`SELECT Table_priv FROM mysql.Tables_priv WHERE User="testTbl1" and host="localhost" and db="test" and Table_name="test2";`).Rows()
		require.Len(t, rows, 1)
		row := rows[0]
		require.Len(t, rows, 1)
		p := fmt.Sprintf("%v", row[0])
		require.Greater(t, strings.Index(p, mysql.Priv2SetStr[v]), -1)
	}

	_, err := tk.Exec("GRANT SUPER ON test2 TO 'testTbl1'@'localhost';")
	require.EqualError(t, err, "[executor:1144]Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used")
}

func TestGrantColumnScope(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testCol'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec(`CREATE TABLE test.test3(c1 int, c2 int);`)

	// Make sure all the column privs for new user is empty.
	tk.MustQuery(`SELECT * FROM mysql.Columns_priv WHERE User="testCol" and host="localhost" and db="test" and Table_name="test3" and Column_name="c1"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM mysql.Columns_priv WHERE User="testCol" and host="localhost" and db="test" and Table_name="test3" and Column_name="c2"`).Check(testkit.Rows())

	// Grant each priv to the user.
	for _, v := range mysql.AllColumnPrivs {
		sql := fmt.Sprintf("GRANT %s(c1) ON test.test3 TO 'testCol'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		rows := tk.MustQuery(`SELECT Column_priv FROM mysql.Columns_priv WHERE User="testCol" and host="localhost" and db="test" and Table_name="test3" and Column_name="c1";`).Rows()
		require.Len(t, rows, 1)
		row := rows[0]
		require.Len(t, rows, 1)
		p := fmt.Sprintf("%v", row[0])
		require.Greater(t, strings.Index(p, mysql.Priv2SetStr[v]), -1)
	}

	// Create a new user.
	createUserSQL = `CREATE USER 'testCol1'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec("USE test;")
	// Grant all column scope privs.
	tk.MustExec("GRANT ALL(c2) ON test3 TO 'testCol1'@'localhost';")
	// Make sure all the column privs for granted user are in the Column_priv set.
	for _, v := range mysql.AllColumnPrivs {
		rows := tk.MustQuery(`SELECT Column_priv FROM mysql.Columns_priv WHERE User="testCol1" and host="localhost" and db="test" and Table_name="test3" and Column_name="c2";`).Rows()
		require.Len(t, rows, 1)
		row := rows[0]
		require.Len(t, rows, 1)
		p := fmt.Sprintf("%v", row[0])
		require.Greater(t, strings.Index(p, mysql.Priv2SetStr[v]), -1)
	}

	_, err := tk.Exec("GRANT SUPER(c2) ON test3 TO 'testCol1'@'localhost';")
	require.EqualError(t, err, "[executor:1221]Incorrect usage of COLUMN GRANT and NON-COLUMN PRIVILEGES")
}

func TestIssue2456(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER 'dduser'@'%' IDENTIFIED by '123456';")
	tk.MustExec("CREATE DATABASE `dddb_%`;")
	tk.MustExec("CREATE table `dddb_%`.`te%` (id int);")
	tk.MustExec("GRANT ALL PRIVILEGES ON `dddb_%`.* TO 'dduser'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `dddb_%`.`te%` to 'dduser'@'%';")
}

func TestNoAutoCreateUser(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
	tk.MustExec(`SET sql_mode='NO_AUTO_CREATE_USER'`)
	_, err := tk.Exec(`GRANT ALL PRIVILEGES ON *.* to 'test'@'%' IDENTIFIED BY 'xxx'`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, executor.ErrCantCreateUserWithGrant))
}

func TestCreateUserWhenGrant(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
	// This only applies to sql_mode:NO_AUTO_CREATE_USER off
	tk.MustExec(`SET SQL_MODE=''`)
	tk.MustExec(`GRANT ALL PRIVILEGES ON *.* to 'test'@'%' IDENTIFIED BY 'xxx'`)
	// Make sure user is created automatically when grant to a non-exists one.
	tk.MustQuery(`SELECT user FROM mysql.user WHERE user='test' and host='%'`).Check(
		testkit.Rows("test"),
	)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
}

func TestCreateUserWithTooLongName(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	err := tk.ExecToErr("CREATE USER '1234567890abcdefGHIKL1234567890abcdefGHIKL@localhost'")
	require.Truef(t, terror.ErrorEqual(err, executor.ErrWrongStringLength), "ERROR 1470 (HY000): String '1234567890abcdefGHIKL1234567890abcdefGHIKL' is too long for user name (should be no longer than 32)")
	err = tk.ExecToErr("CREATE USER 'some_user_name@host_1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij1234567890X'")
	require.Truef(t, terror.ErrorEqual(err, executor.ErrWrongStringLength), "ERROR 1470 (HY000): String 'host_1234567890abcdefghij1234567890abcdefghij1234567890abcdefghij12345' is too long for host name (should be no longer than 255)")
}

func TestGrantPrivilegeAtomic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`drop role if exists r1, r2, r3, r4;`)
	tk.MustExec(`create role r1, r2, r3;`)
	tk.MustExec(`create table test.testatomic(x int);`)

	_, err := tk.Exec(`grant update, select, insert, delete on *.* to r1, r2, r4;`)
	require.True(t, terror.ErrorEqual(err, executor.ErrCantCreateUserWithGrant))
	tk.MustQuery(`select Update_priv, Select_priv, Insert_priv, Delete_priv from mysql.user where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Rows(
		"N N N N",
		"N N N N",
		"N N N N",
	))
	tk.MustExec(`grant update, select, insert, delete on *.* to r1, r2, r3;`)
	_, err = tk.Exec(`revoke all on *.* from r1, r2, r4, r3;`)
	require.Error(t, err)
	tk.MustQuery(`select Update_priv, Select_priv, Insert_priv, Delete_priv from mysql.user where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Rows(
		"Y Y Y Y",
		"Y Y Y Y",
		"Y Y Y Y",
	))

	_, err = tk.Exec(`grant update, select, insert, delete on test.* to r1, r2, r4;`)
	require.True(t, terror.ErrorEqual(err, executor.ErrCantCreateUserWithGrant))
	tk.MustQuery(`select Update_priv, Select_priv, Insert_priv, Delete_priv from mysql.db where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Rows())
	tk.MustExec(`grant update, select, insert, delete on test.* to r1, r2, r3;`)
	_, err = tk.Exec(`revoke all on *.* from r1, r2, r4, r3;`)
	require.Error(t, err)
	tk.MustQuery(`select Update_priv, Select_priv, Insert_priv, Delete_priv from mysql.db where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Rows(
		"Y Y Y Y",
		"Y Y Y Y",
		"Y Y Y Y",
	))

	_, err = tk.Exec(`grant update, select, insert, delete on test.testatomic to r1, r2, r4;`)
	require.True(t, terror.ErrorEqual(err, executor.ErrCantCreateUserWithGrant))
	tk.MustQuery(`select Table_priv from mysql.tables_priv where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Rows())
	tk.MustExec(`grant update, select, insert, delete on test.testatomic to r1, r2, r3;`)
	_, err = tk.Exec(`revoke all on *.* from r1, r2, r4, r3;`)
	require.Error(t, err)
	tk.MustQuery(`select Table_priv from mysql.tables_priv where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Rows(
		"Select,Insert,Update,Delete",
		"Select,Insert,Update,Delete",
		"Select,Insert,Update,Delete",
	))

	tk.MustExec(`drop role if exists r1, r2, r3, r4;`)
	tk.MustExec(`drop table test.testatomic;`)

}

func TestIssue2654(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
	tk.MustExec(`CREATE USER 'test'@'%' IDENTIFIED BY 'test'`)
	tk.MustExec("GRANT SELECT ON test.* to 'test'")
	rows := tk.MustQuery(`SELECT user,host FROM mysql.user WHERE user='test' and host='%'`)
	rows.Check(testkit.Rows(`test %`))
}

func TestGrantUnderANSIQuotes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// Fix a bug that the GrantExec fails in ANSI_QUOTES sql mode
	// The bug is caused by the improper usage of double quotes like:
	// INSERT INTO mysql.user ... VALUES ("..", "..", "..")
	tk.MustExec(`SET SQL_MODE='ANSI_QUOTES'`)
	tk.MustExec(`GRANT ALL PRIVILEGES ON video_ulimit.* TO web@'%' IDENTIFIED BY 'eDrkrhZ>l2sV'`)
	tk.MustExec(`REVOKE ALL PRIVILEGES ON video_ulimit.* FROM web@'%';`)
	tk.MustExec(`DROP USER IF EXISTS 'web'@'%'`)
}

func TestMaintainRequire(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	// test create with require
	tk.MustExec(`CREATE USER 'ssl_auser'@'%' require issuer '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'`)
	tk.MustExec(`CREATE USER 'ssl_buser'@'%' require subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'`)
	tk.MustExec(`CREATE USER 'ssl_cuser'@'%' require cipher 'AES128-GCM-SHA256'`)
	tk.MustExec(`CREATE USER 'ssl_duser'@'%'`)
	tk.MustExec(`CREATE USER 'ssl_euser'@'%' require none`)
	tk.MustExec(`CREATE USER 'ssl_fuser'@'%' require ssl`)
	tk.MustExec(`CREATE USER 'ssl_guser'@'%' require x509`)
	tk.MustQuery("select * from mysql.global_priv where `user` like 'ssl_%'").Check(testkit.Rows(
		"% ssl_auser {\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\",\"x509_issuer\":\"/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US\",\"x509_subject\":\"/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH\"}",
		"% ssl_buser {\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\",\"x509_subject\":\"/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH\"}",
		"% ssl_cuser {\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\"}",
		"% ssl_duser {}",
		"% ssl_euser {}",
		"% ssl_fuser {\"ssl_type\":1}",
		"% ssl_guser {\"ssl_type\":2}",
	))

	// test grant with require
	tk.MustExec("CREATE USER 'u1'@'%'")
	tk.MustExec("GRANT ALL ON *.* TO 'u1'@'%' require issuer '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' and subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH'") // add new require.
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Rows("{\"ssl_type\":3,\"x509_issuer\":\"/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US\",\"x509_subject\":\"/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH\"}"))
	tk.MustExec("GRANT ALL ON *.* TO 'u1'@'%' require cipher 'AES128-GCM-SHA256'") // modify always overwrite.
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Rows("{\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\"}"))
	tk.MustExec("GRANT select ON *.* TO 'u1'@'%'") // modify without require should not modify old require.
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Rows("{\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\"}"))
	tk.MustExec("GRANT ALL ON *.* TO 'u1'@'%' require none") // use require none to clean up require.
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Rows("{}"))

	// test alter with require
	tk.MustExec("CREATE USER 'u2'@'%'")
	tk.MustExec("alter user 'u2'@'%' require ssl")
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Rows("{\"ssl_type\":1}"))
	tk.MustExec("alter user 'u2'@'%' require x509")
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Rows("{\"ssl_type\":2}"))
	tk.MustExec("alter user 'u2'@'%' require issuer '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'")
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Rows("{\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\",\"x509_issuer\":\"/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US\",\"x509_subject\":\"/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH\"}"))
	tk.MustExec("alter user 'u2'@'%' require none")
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Rows("{}"))

	// test show create user
	tk.MustExec(`CREATE USER 'u3'@'%' require issuer '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'`)
	tk.MustQuery("show create user 'u3'").Check(testkit.Rows("CREATE USER 'u3'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE CIPHER 'AES128-GCM-SHA256' ISSUER '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' SUBJECT '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	// check issuer/subject/cipher value
	_, err := tk.Exec(`CREATE USER 'u4'@'%' require issuer 'CN=TiDB,OU=PingCAP'`)
	require.Error(t, err)
	_, err = tk.Exec(`CREATE USER 'u5'@'%' require subject '/CN=TiDB\OU=PingCAP'`)
	require.Error(t, err)
	_, err = tk.Exec(`CREATE USER 'u6'@'%' require subject '/CN=TiDB\NC=PingCAP'`)
	require.Error(t, err)
	_, err = tk.Exec(`CREATE USER 'u7'@'%' require cipher 'AES128-GCM-SHA1'`)
	require.Error(t, err)
	_, err = tk.Exec(`CREATE USER 'u8'@'%' require subject '/CN'`)
	require.Error(t, err)
	_, err = tk.Exec(`CREATE USER 'u9'@'%' require cipher 'TLS_AES_256_GCM_SHA384' cipher 'RC4-SHA'`)
	require.EqualError(t, err, "Duplicate require CIPHER clause")
	_, err = tk.Exec(`CREATE USER 'u9'@'%' require issuer 'CN=TiDB,OU=PingCAP' issuer 'CN=TiDB,OU=PingCAP2'`)
	require.EqualError(t, err, "Duplicate require ISSUER clause")
	_, err = tk.Exec(`CREATE USER 'u9'@'%' require subject '/CN=TiDB\OU=PingCAP' subject '/CN=TiDB\OU=PingCAP2'`)
	require.EqualError(t, err, "Duplicate require SUBJECT clause")
	_, err = tk.Exec(`CREATE USER 'u9'@'%' require ssl ssl`)
	require.Error(t, err)
	_, err = tk.Exec(`CREATE USER 'u9'@'%' require x509 x509`)
	require.Error(t, err)
}

func TestMaintainAuthString(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER 'maint_auth_str1'@'%' IDENTIFIED BY 'foo'`)
	tk.MustQuery("SELECT authentication_string FROM mysql.user WHERE `Host` = '%' and `User` = 'maint_auth_str1'").Check(testkit.Rows("*F3A2A51A9B0F2BE2468926B4132313728C250DBF"))
	tk.MustExec(`ALTER USER 'maint_auth_str1'@'%' REQUIRE SSL`)
	tk.MustQuery("SELECT authentication_string FROM mysql.user WHERE `Host` = '%' and `User` = 'maint_auth_str1'").Check(testkit.Rows("*F3A2A51A9B0F2BE2468926B4132313728C250DBF"))
}

func TestGrantOnNonExistTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user genius")
	tk.MustExec("use test")
	_, err := tk.Exec("select * from nonexist")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotExists))
	_, err = tk.Exec("grant Select,Insert on nonexist to 'genius'")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotExists))

	tk.MustExec("create table if not exists xx (id int)")
	// Case sensitive
	_, err = tk.Exec("grant Select,Insert on XX to 'genius'")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotExists))

	_, err = tk.Exec("grant Select,Insert on xx to 'genius'")
	require.NoError(t, err)
	_, err = tk.Exec("grant Select,Update on test.xx to 'genius'")
	require.NoError(t, err)

	// issue #29268
	tk.MustExec("CREATE DATABASE d29268")
	defer tk.MustExec("DROP DATABASE IF EXISTS d29268")
	tk.MustExec("USE d29268")
	tk.MustExec("CREATE USER u29268")
	defer tk.MustExec("DROP USER u29268")

	// without create privilege
	err = tk.ExecToErr("GRANT SELECT ON t29268 TO u29268")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotExists))
	err = tk.ExecToErr("GRANT DROP, INSERT ON t29268 TO u29268")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotExists))
	err = tk.ExecToErr("GRANT UPDATE, CREATE VIEW, SHOW VIEW ON t29268 TO u29268")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotExists))
	err = tk.ExecToErr("GRANT DELETE, REFERENCES, ALTER ON t29268 TO u29268")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotExists))

	// with create privilege
	tk.MustExec("GRANT CREATE ON t29268 TO u29268")
	tk.MustExec("GRANT CREATE, SELECT ON t29268 TO u29268")
	tk.MustExec("GRANT CREATE, DROP, INSERT ON t29268 TO u29268")

	// check privilege
	tk.Session().Auth(&auth.UserIdentity{Username: "u29268", Hostname: "localhost"}, nil, nil)
	tk.MustExec("USE d29268")
	tk.MustExec("CREATE TABLE t29268 (c1 int)")
	tk.MustExec("INSERT INTO t29268 VALUES (1), (2)")
	tk.MustQuery("SELECT c1 FROM t29268").Check(testkit.Rows("1", "2"))
	tk.MustExec("DROP TABLE t29268")

	// check grant all
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil)
	tk.MustExec("GRANT ALL ON t29268 TO u29268")
}

func TestIssue22721(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists xx (id int)")
	tk.MustExec("CREATE USER 'sync_ci_data'@'%' IDENTIFIED BY 'sNGNQo12fEHe0n3vU';")
	tk.MustExec("GRANT USAGE ON *.* TO 'sync_ci_data'@'%';")
	tk.MustExec("GRANT USAGE ON sync_ci_data.* TO 'sync_ci_data'@'%';")
	tk.MustExec("GRANT USAGE ON test.* TO 'sync_ci_data'@'%';")
	tk.MustExec("GRANT USAGE ON test.xx TO 'sync_ci_data'@'%';")
}

func TestPerformanceSchemaPrivGrant(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create user issue27867;")
	defer func() {
		tk.MustExec("drop user issue27867;")
	}()
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	err := tk.ExecToErr("grant all on performance_schema.* to issue27867;")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1044]Access denied for user 'root'@'%' to database 'performance_schema'")
	// Check case insensitivity
	err = tk.ExecToErr("grant all on PERFormanCE_scHemA.* to issue27867;")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1044]Access denied for user 'root'@'%' to database 'PERFormanCE_scHemA'")
	// Check other database privileges
	tk.MustExec("grant select on performance_schema.* to issue27867;")
	tk.MustExec("grant insert on performance_schema.* to issue27867;")
	tk.MustExec("grant update on performance_schema.* to issue27867;")
	tk.MustExec("grant delete on performance_schema.* to issue27867;")
	tk.MustExec("grant drop on performance_schema.* to issue27867;")
	tk.MustExec("grant lock tables on performance_schema.* to issue27867;")
	err = tk.ExecToErr("grant create on performance_schema.* to issue27867;")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1044]Access denied for user 'root'@'%' to database 'performance_schema'")
	err = tk.ExecToErr("grant references on performance_schema.* to issue27867;")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1044]Access denied for user 'root'@'%' to database 'performance_schema'")
	err = tk.ExecToErr("grant alter on PERFormAnCE_scHemA.* to issue27867;")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1044]Access denied for user 'root'@'%' to database 'PERFormAnCE_scHemA'")
	err = tk.ExecToErr("grant execute on performance_schema.* to issue27867;")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1044]Access denied for user 'root'@'%' to database 'performance_schema'")
	err = tk.ExecToErr("grant index on PERFormanCE_scHemA.* to issue27867;")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1044]Access denied for user 'root'@'%' to database 'PERFormanCE_scHemA'")
	err = tk.ExecToErr("grant create view on performance_schema.* to issue27867;")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1044]Access denied for user 'root'@'%' to database 'performance_schema'")
	err = tk.ExecToErr("grant show view on performance_schema.* to issue27867;")
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1044]Access denied for user 'root'@'%' to database 'performance_schema'")
}

func TestGrantDynamicPrivs(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user dyn")

	_, err := tk.Exec("GRANT BACKUP_ADMIN ON test.* TO dyn")
	require.True(t, terror.ErrorEqual(err, executor.ErrIllegalPrivilegeLevel))
	_, err = tk.Exec("GRANT BOGUS_GRANT ON *.* TO dyn")
	require.True(t, terror.ErrorEqual(err, executor.ErrDynamicPrivilegeNotRegistered))

	tk.MustExec("GRANT BACKUP_Admin ON *.* TO dyn") // grant one priv
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(testkit.Rows("dyn % BACKUP_ADMIN N"))

	tk.MustExec("GRANT SYSTEM_VARIABLES_ADMIN, BACKUP_ADMIN ON *.* TO dyn") // grant multiple
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(
		testkit.Rows("dyn % BACKUP_ADMIN N", "dyn % SYSTEM_VARIABLES_ADMIN N"),
	)

	tk.MustExec("GRANT ROLE_ADMIN, BACKUP_ADMIN ON *.* TO dyn WITH GRANT OPTION") // grant multiple with GRANT option.
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(
		testkit.Rows("dyn % BACKUP_ADMIN Y", "dyn % ROLE_ADMIN Y", "dyn % SYSTEM_VARIABLES_ADMIN N"),
	)

	tk.MustExec("GRANT SYSTEM_VARIABLES_ADMIN, Select, ROLE_ADMIN ON *.* TO dyn") // grant mixed dynamic/non dynamic
	tk.MustQuery("SELECT Grant_Priv FROM mysql.user WHERE `Host` = '%' AND `User` = 'dyn'").Check(testkit.Rows("N"))
	tk.MustQuery("SELECT WITH_GRANT_OPTION FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' AND Priv='SYSTEM_VARIABLES_ADMIN'").Check(testkit.Rows("N"))

	tk.MustExec("GRANT CONNECTION_ADMIN, Insert ON *.* TO dyn WITH GRANT OPTION") // grant mixed dynamic/non dynamic with GRANT option.
	tk.MustQuery("SELECT Grant_Priv FROM mysql.user WHERE `Host` = '%' AND `User` = 'dyn'").Check(testkit.Rows("Y"))
	tk.MustQuery("SELECT WITH_GRANT_OPTION FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' AND Priv='CONNECTION_ADMIN'").Check(testkit.Rows("Y"))
}

func TestNonExistTableIllegalGrant(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user u29302")
	defer tk.MustExec("drop user u29302")
	// Table level, not existing table, illegal privilege
	tk.MustGetErrCode("grant create temporary tables on NotExistsD29302.NotExistsT29302 to u29302", mysql.ErrIllegalGrantForTable)
	tk.MustGetErrCode("grant lock tables on test.NotExistsT29302 to u29302", mysql.ErrIllegalGrantForTable)
	// Column level, not existing table, illegal privilege
	tk.MustGetErrCode("grant create temporary tables (NotExistsCol) on NotExistsD29302.NotExistsT29302 to u29302;", mysql.ErrWrongUsage)
}
