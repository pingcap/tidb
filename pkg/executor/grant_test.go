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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func TestGrantGlobal(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	err := tk.ExecToErr(` grant create user on test.* to 'testDB1'@'localhost';`)
	require.True(t, terror.ErrorEqual(err, exeerrors.ErrWrongUsage.GenWithStackByArgs("DB GRANT", "GLOBAL PRIVILEGES")))

	err = tk.ExecToErr("GRANT SUPER ON test.* TO 'testDB1'@'localhost';")
	require.True(t, terror.ErrorEqual(err, exeerrors.ErrWrongUsage.GenWithStackByArgs("DB GRANT", "NON-DB PRIVILEGES")))
}

func TestGrantTableScope(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

	tk.MustGetErrMsg("GRANT SUPER ON test2 TO 'testTbl1'@'localhost';",
		"[executor:1144]Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used")
}

func TestGrantColumnScope(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

	tk.MustGetErrMsg("GRANT SUPER(c2) ON test3 TO 'testCol1'@'localhost';",
		"[executor:1221]Incorrect usage of COLUMN GRANT and NON-COLUMN PRIVILEGES")
}
