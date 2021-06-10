// Copyright 2017 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestRevokeGlobal(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	_, err := tk.Exec(`REVOKE ALL PRIVILEGES ON *.* FROM 'nonexistuser'@'host'`)
	c.Assert(err, NotNil)

	// Create a new user.
	createUserSQL := `CREATE USER 'testGlobalRevoke'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	grantPrivSQL := `GRANT ALL PRIVILEGES ON *.* to 'testGlobalRevoke'@'localhost';`
	tk.MustExec(grantPrivSQL)

	// Make sure all the global privs for new user is "Y".
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf(`SELECT %s FROM mysql.User WHERE User="testGlobalRevoke" and host="localhost";`, mysql.Priv2UserCol[v])
		r := tk.MustQuery(sql)
		r.Check(testkit.Rows("Y"))
	}

	// Revoke each priv from the user.
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("REVOKE %s ON *.* FROM 'testGlobalRevoke'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		sql = fmt.Sprintf(`SELECT %s FROM mysql.User WHERE User="testGlobalRevoke" and host="localhost"`, mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("N"))
	}
}

func (s *testSuite1) TestRevokeDBScope(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	tk.MustExec(`CREATE USER 'testDBRevoke'@'localhost' IDENTIFIED BY '123';`)
	tk.MustExec(`GRANT ALL ON test.* TO 'testDBRevoke'@'localhost';`)

	_, err := tk.Exec(`REVOKE ALL PRIVILEGES ON nonexistdb.* FROM 'testDBRevoke'@'localhost'`)
	c.Assert(err, NotNil)

	// Revoke each priv from the user.
	for _, v := range mysql.AllDBPrivs {
		check := fmt.Sprintf(`SELECT %s FROM mysql.DB WHERE User="testDBRevoke" and host="localhost" and db="test"`, mysql.Priv2UserCol[v])
		sql := fmt.Sprintf("REVOKE %s ON test.* FROM 'testDBRevoke'@'localhost';", mysql.Priv2Str[v])

		tk.MustQuery(check).Check(testkit.Rows("Y"))
		tk.MustExec(sql)
		tk.MustQuery(check).Check(testkit.Rows("N"))
	}
}

func (s *testSuite1) TestRevokeTableScope(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	tk.MustExec(`CREATE USER 'testTblRevoke'@'localhost' IDENTIFIED BY '123';`)
	tk.MustExec(`CREATE TABLE test.test1(c1 int);`)
	tk.MustExec(`GRANT ALL PRIVILEGES ON test.test1 TO 'testTblRevoke'@'localhost';`)

	_, err := tk.Exec(`REVOKE ALL PRIVILEGES ON test.nonexisttable FROM 'testTblRevoke'@'localhost'`)
	c.Assert(err, NotNil)

	// Make sure all the table privs for new user is Y.
	res := tk.MustQuery(`SELECT Table_priv FROM mysql.tables_priv WHERE User="testTblRevoke" and host="localhost" and db="test" and Table_name="test1"`)
	res.Check(testkit.Rows("Select,Insert,Update,Delete,Create,Drop,Index,Alter,Create View,Show View"))

	// Revoke each priv from the user.
	for _, v := range mysql.AllTablePrivs {
		sql := fmt.Sprintf("REVOKE %s ON test.test1 FROM 'testTblRevoke'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		rows := tk.MustQuery(`SELECT Table_priv FROM mysql.tables_priv WHERE User="testTblRevoke" and host="localhost" and db="test" and Table_name="test1";`).Rows()
		c.Assert(rows, HasLen, 1)
		row := rows[0]
		c.Assert(row, HasLen, 1)
		op := mysql.Priv2SetStr[v]
		found := false
		for _, v := range strings.Split(fmt.Sprintf("%v", row[0]), ",") {
			if v == op {
				found = true
				break
			}
		}
		c.Assert(found, IsFalse)
	}

	// Revoke all table scope privs.
	tk.MustExec("REVOKE ALL ON test.test1 FROM 'testTblRevoke'@'localhost';")
	tk.MustQuery(`SELECT Table_priv FROM mysql.Tables_priv WHERE User="testTblRevoke" and host="localhost" and db="test" and Table_name="test1"`).Check(testkit.Rows(""))
}

func (s *testSuite1) TestRevokeColumnScope(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	tk.MustExec(`CREATE USER 'testColRevoke'@'localhost' IDENTIFIED BY '123';`)
	tk.MustExec(`CREATE TABLE test.test3(c1 int, c2 int);`)
	tk.MustQuery(`SELECT * FROM mysql.Columns_priv WHERE User="testColRevoke" and host="localhost" and db="test" and Table_name="test3" and Column_name="c2"`).Check(testkit.Rows())

	// Grant and Revoke each priv on the user.
	for _, v := range mysql.AllColumnPrivs {
		grantSQL := fmt.Sprintf("GRANT %s(c1) ON test.test3 TO 'testColRevoke'@'localhost';", mysql.Priv2Str[v])
		revokeSQL := fmt.Sprintf("REVOKE %s(c1) ON test.test3 FROM 'testColRevoke'@'localhost';", mysql.Priv2Str[v])
		checkSQL := `SELECT Column_priv FROM mysql.Columns_priv WHERE User="testColRevoke" and host="localhost" and db="test" and Table_name="test3" and Column_name="c1"`

		tk.MustExec(grantSQL)
		rows := tk.MustQuery(checkSQL).Rows()
		c.Assert(rows, HasLen, 1)
		row := rows[0]
		c.Assert(row, HasLen, 1)
		p := fmt.Sprintf("%v", row[0])
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)

		tk.MustExec(revokeSQL)
		tk.MustQuery(checkSQL).Check(testkit.Rows(""))
	}

	// Create a new user.
	tk.MustExec("CREATE USER 'testCol1Revoke'@'localhost' IDENTIFIED BY '123';")
	tk.MustExec("USE test;")
	// Grant all column scope privs.
	tk.MustExec("GRANT ALL(c2) ON test3 TO 'testCol1Revoke'@'localhost';")
	// Make sure all the column privs for granted user are in the Column_priv set.
	for _, v := range mysql.AllColumnPrivs {
		rows := tk.MustQuery(`SELECT Column_priv FROM mysql.Columns_priv WHERE User="testCol1Revoke" and host="localhost" and db="test" and Table_name="test3" and Column_name="c2";`).Rows()
		c.Assert(rows, HasLen, 1)
		row := rows[0]
		c.Assert(row, HasLen, 1)
		p := fmt.Sprintf("%v", row[0])
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)
	}
	tk.MustExec("REVOKE ALL(c2) ON test3 FROM 'testCol1Revoke'@'localhost'")
	tk.MustQuery(`SELECT Column_priv FROM mysql.Columns_priv WHERE User="testCol1Revoke" and host="localhost" and db="test" and Table_name="test3"`).Check(testkit.Rows(""))
}

func (s *testSuite1) TestRevokeDynamicPrivs(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("DROP USER if exists dyn")
	tk.MustExec("create user dyn")

	tk.MustExec("GRANT BACKUP_Admin ON *.* TO dyn") // grant one priv
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(testkit.Rows("dyn % BACKUP_ADMIN N"))

	// try revoking only on test.* - should fail:
	_, err := tk.Exec("REVOKE BACKUP_Admin,system_variables_admin ON test.* FROM dyn")
	c.Assert(terror.ErrorEqual(err, executor.ErrIllegalPrivilegeLevel), IsTrue)

	// privs should still be intact:
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(testkit.Rows("dyn % BACKUP_ADMIN N"))
	// with correct usage, the privilege is revoked
	tk.MustExec("REVOKE BACKUP_Admin ON *.* FROM dyn")
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(testkit.Rows())

	// Revoke bogus is a warning in MySQL
	tk.MustExec("REVOKE bogus ON *.* FROM dyn")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 3929 Dynamic privilege 'BOGUS' is not registered with the server."))

	// grant and revoke two dynamic privileges at once.
	tk.MustExec("GRANT BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN ON *.* TO dyn")
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(testkit.Rows("dyn % BACKUP_ADMIN N", "dyn % SYSTEM_VARIABLES_ADMIN N"))
	tk.MustExec("REVOKE BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN ON *.* FROM dyn")
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(testkit.Rows())

	// revoke a combination of dynamic + non-dynamic
	tk.MustExec("GRANT BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN, SELECT, INSERT ON *.* TO dyn")
	tk.MustExec("REVOKE BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN, SELECT, INSERT ON *.* FROM dyn")
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(testkit.Rows())

	// revoke grant option from privileges
	tk.MustExec("GRANT BACKUP_ADMIN, SYSTEM_VARIABLES_ADMIN, SELECT ON *.* TO dyn WITH GRANT OPTION")
	tk.MustExec("REVOKE BACKUP_ADMIN, SELECT, GRANT OPTION ON *.* FROM dyn")
	tk.MustQuery("SELECT * FROM mysql.global_grants WHERE `Host` = '%' AND `User` = 'dyn' ORDER BY user,host,priv,with_grant_option").Check(testkit.Rows("dyn % SYSTEM_VARIABLES_ADMIN Y"))
}
