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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSuite) TestCharsetDatabase(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	testSQL := `create database if not exists cd_test_utf8 CHARACTER SET utf8 COLLATE utf8_bin;`
	tk.MustExec(testSQL)

	testSQL = `create database if not exists cd_test_latin1 CHARACTER SET latin1 COLLATE latin1_swedish_ci;`
	tk.MustExec(testSQL)

	testSQL = `use cd_test_utf8;`
	tk.MustExec(testSQL)
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Rows("utf8"))
	tk.MustQuery(`select @@collation_database;`).Check(testkit.Rows("utf8_bin"))

	testSQL = `use cd_test_latin1;`
	tk.MustExec(testSQL)
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Rows("latin1"))
	tk.MustQuery(`select @@collation_database;`).Check(testkit.Rows("latin1_swedish_ci"))
}

func (s *testSuite) TestSetVar(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	testSQL := "SET @a = 1;"
	tk.MustExec(testSQL)

	testSQL = `SET @a = "1";`
	tk.MustExec(testSQL)

	testSQL = "SET @a = null;"
	tk.MustExec(testSQL)

	testSQL = "SET @@global.autocommit = 1;"
	tk.MustExec(testSQL)

	// TODO: this test case should returns error.
	// testSQL = "SET @@global.autocommit = null;"
	// _, err := tk.Exec(testSQL)
	// c.Assert(err, NotNil)

	testSQL = "SET @@autocommit = 1;"
	tk.MustExec(testSQL)

	testSQL = "SET @@autocommit = null;"
	_, err := tk.Exec(testSQL)
	c.Assert(err, NotNil)

	errTestSql := "SET @@date_format = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET @@rewriter_enabled = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET xxx = abcd;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET @@global.a = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET @@global.timestamp = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	// For issue 998
	testSQL = "SET @issue998a=1, @issue998b=5;"
	tk.MustExec(testSQL)
	tk.MustQuery(`select @issue998a, @issue998b;`).Check(testkit.Rows("1 5"))
	testSQL = "SET @@autocommit=0, @issue998a=2;"
	tk.MustExec(testSQL)
	tk.MustQuery(`select @issue998a, @@autocommit;`).Check(testkit.Rows("2 0"))
	testSQL = "SET @@global.autocommit=1, @issue998b=6;"
	tk.MustExec(testSQL)
	tk.MustQuery(`select @issue998b, @@global.autocommit;`).Check(testkit.Rows("6 1"))

	// Set default
	// {ScopeGlobal | ScopeSession, "low_priority_updates", "OFF"},
	// For global var
	tk.MustQuery(`select @@global.low_priority_updates;`).Check(testkit.Rows("OFF"))
	tk.MustExec(`set @@global.low_priority_updates="ON";`)
	tk.MustQuery(`select @@global.low_priority_updates;`).Check(testkit.Rows("ON"))
	tk.MustExec(`set @@global.low_priority_updates=DEFAULT;`) // It will be set to compiled-in default value.
	tk.MustQuery(`select @@global.low_priority_updates;`).Check(testkit.Rows("OFF"))
	// For session
	tk.MustQuery(`select @@session.low_priority_updates;`).Check(testkit.Rows("OFF"))
	tk.MustExec(`set @@global.low_priority_updates="ON";`)
	tk.MustExec(`set @@session.low_priority_updates=DEFAULT;`) // It will be set to global var value.
	tk.MustQuery(`select @@session.low_priority_updates;`).Check(testkit.Rows("ON"))
}

func (s *testSuite) TestSetCharset(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`SET NAMES latin1`)

	ctx := tk.Se.(context.Context)
	sessionVars := variable.GetSessionVars(ctx)
	for _, v := range variable.SetNamesVariables {
		sVar := sessionVars.GetSystemVar(v)
		c.Assert(sVar.GetString() != "utf8", IsTrue)
	}
	tk.MustExec(`SET NAMES utf8`)
	for _, v := range variable.SetNamesVariables {
		sVar := sessionVars.GetSystemVar(v)
		c.Assert(sVar.GetString(), Equals, "utf8")
	}
	sVar := sessionVars.GetSystemVar(variable.CollationConnection)
	c.Assert(sVar.GetString(), Equals, "utf8_general_ci")

	// Issue 1523
	tk.MustExec(`SET NAMES binary`)
}

func (s *testSuite) TestDo(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("do 1, 2")
}

func (s *testSuite) TestTransaction(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("begin")
	ctx := tk.Se.(context.Context)
	c.Assert(inTxn(ctx), IsTrue)
	tk.MustExec("commit")
	c.Assert(inTxn(ctx), IsFalse)
	tk.MustExec("begin")
	c.Assert(inTxn(ctx), IsTrue)
	tk.MustExec("rollback")
	c.Assert(inTxn(ctx), IsFalse)
}

func inTxn(ctx context.Context) bool {
	return (variable.GetSessionVars(ctx).Status & mysql.ServerStatusInTrans) > 0
}

func (s *testSuite) TestCreateUser(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	// Make sure user test not in mysql.User.
	result := tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="test" and Host="localhost"`)
	result.Check(nil)
	// Create user test.
	createUserSQL := `CREATE USER 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	// Make sure user test in mysql.User.
	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="test" and Host="localhost"`)
	rowStr := fmt.Sprintf("%v", []byte(util.EncodePassword("123")))
	result.Check(testkit.Rows(rowStr))
	// Create duplicate user with IfNotExists will be success.
	createUserSQL = `CREATE USER IF NOT EXISTS 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)

	// Create duplicate user without IfNotExists will cause error.
	createUserSQL = `CREATE USER 'test'@'localhost' IDENTIFIED BY '123';`
	_, err := tk.Exec(createUserSQL)
	c.Check(err, NotNil)
	dropUserSQL := `DROP USER IF EXISTS 'test'@'localhost' ;`
	tk.MustExec(dropUserSQL)
	// Create user test.
	createUserSQL = `CREATE USER 'test1'@'localhost';`
	tk.MustExec(createUserSQL)
	// Make sure user test in mysql.User.
	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="test1" and Host="localhost"`)
	rowStr = fmt.Sprintf("%v", []byte(util.EncodePassword("")))
	result.Check(testkit.Rows(rowStr))
	dropUserSQL = `DROP USER IF EXISTS 'test1'@'localhost' ;`
	tk.MustExec(dropUserSQL)

	// Test drop user if exists.
	createUserSQL = `CREATE USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustExec(createUserSQL)
	dropUserSQL = `DROP USER IF EXISTS 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost' ;`
	tk.MustExec(dropUserSQL)
	// Test negative cases without IF EXISTS.
	createUserSQL = `CREATE USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustExec(createUserSQL)
	dropUserSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
	_, err = tk.Exec(dropUserSQL)
	c.Check(err, NotNil)
	dropUserSQL = `DROP USER 'test3'@'localhost';`
	_, err = tk.Exec(dropUserSQL)
	c.Check(err, NotNil)
	dropUserSQL = `DROP USER 'test1'@'localhost';`
	_, err = tk.Exec(dropUserSQL)
	c.Check(err, NotNil)
	// Test positive cases without IF EXISTS.
	createUserSQL = `CREATE USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustExec(createUserSQL)
	dropUserSQL = `DROP USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustExec(dropUserSQL)
}

func (s *testSuite) TestSetPwd(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	createUserSQL := `CREATE USER 'testpwd'@'localhost' IDENTIFIED BY '';`
	tk.MustExec(createUserSQL)

	result := tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="testpwd" and Host="localhost"`)
	rowStr := fmt.Sprintf("%v", []byte(""))
	result.Check(testkit.Rows(rowStr))

	tk.MustExec(`SET PASSWORD FOR 'testpwd'@'localhost' = 'password';`)

	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="testpwd" and Host="localhost"`)
	rowStr = fmt.Sprintf("%v", []byte(util.EncodePassword("password")))
	result.Check(testkit.Rows(rowStr))
}

func (s *testSuite) TestAnalyzeTable(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`ANALYZE TABLE mysql.GLOBAL_VARIABLES`)
	ctx := tk.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("mysql"), model.NewCIStr("GLOBAL_VARIABLES"))
	c.Check(err, IsNil)
	tableID := t.Meta().ID

	txn, err := ctx.GetTxn(true)
	c.Check(err, IsNil)
	meta := meta.NewMeta(txn)
	tpb, err := meta.GetTableStats(tableID)
	c.Check(err, IsNil)
	c.Check(tpb, NotNil)
	tStats, err := statistics.TableFromPB(t.Meta(), tpb)
	c.Check(err, IsNil)
	c.Check(tStats, NotNil)
}
