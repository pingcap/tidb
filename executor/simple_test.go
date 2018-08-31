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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/testkit"
	"golang.org/x/net/context"
)

func (s *testSuite) TestCharsetDatabase(c *C) {
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

func (s *testSuite) TestDo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("do 1, @a:=1")
	tk.MustQuery("select @a").Check(testkit.Rows("1"))
}

func (s *testSuite) TestTransaction(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("begin")
	ctx := tk.Se.(sessionctx.Context)
	c.Assert(inTxn(ctx), IsTrue)
	tk.MustExec("commit")
	c.Assert(inTxn(ctx), IsFalse)
	tk.MustExec("begin")
	c.Assert(inTxn(ctx), IsTrue)
	tk.MustExec("rollback")
	c.Assert(inTxn(ctx), IsFalse)

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

func (s *testSuite) TestUser(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Make sure user test not in mysql.User.
	result := tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="test" and Host="localhost"`)
	result.Check(nil)
	// Create user test.
	createUserSQL := `CREATE USER 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	// Make sure user test in mysql.User.
	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="test" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("123")))
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
	result.Check(testkit.Rows(auth.EncodePassword("")))
	dropUserSQL = `DROP USER IF EXISTS 'test1'@'localhost' ;`
	tk.MustExec(dropUserSQL)

	// Test alter user.
	createUserSQL = `CREATE USER 'test1'@'localhost' IDENTIFIED BY '123', 'test2'@'localhost' IDENTIFIED BY '123', 'test3'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	alterUserSQL := `ALTER USER 'test1'@'localhost' IDENTIFIED BY '111';`
	tk.MustExec(alterUserSQL)
	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("111")))
	alterUserSQL = `ALTER USER IF EXISTS 'test2'@'localhost' IDENTIFIED BY '222', 'test_not_exist'@'localhost' IDENTIFIED BY '1';`
	_, err = tk.Exec(alterUserSQL)
	c.Check(err, NotNil)
	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="test2" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("222")))
	alterUserSQL = `ALTER USER IF EXISTS'test_not_exist'@'localhost' IDENTIFIED BY '1', 'test3'@'localhost' IDENTIFIED BY '333';`
	_, err = tk.Exec(alterUserSQL)
	c.Check(err, NotNil)
	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="test3" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("333")))
	// Test alter user user().
	alterUserSQL = `ALTER USER USER() IDENTIFIED BY '1';`
	_, err = tk.Exec(alterUserSQL)
	c.Check(err, NotNil)
	tk.Se, err = session.CreateSession4Test(s.store)
	c.Check(err, IsNil)
	ctx := tk.Se.(sessionctx.Context)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "test1", Hostname: "localhost"}
	tk.MustExec(alterUserSQL)
	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("1")))
	dropUserSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
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

	// Test 'identified by password'
	createUserSQL = `CREATE USER 'test1'@'localhost' identified by password 'xxx';`
	_, err = tk.Exec(createUserSQL)
	c.Assert(terror.ErrorEqual(executor.ErrPasswordFormat, err), IsTrue, Commentf("err %v", err))
	createUserSQL = `CREATE USER 'test1'@'localhost' identified by password '*3D56A309CD04FA2EEF181462E59011F075C89548';`
	tk.MustExec(createUserSQL)
	dropUserSQL = `DROP USER 'test1'@'localhost';`
	tk.MustExec(dropUserSQL)
	tk.MustQuery("select * from mysql.db").Check(testkit.Rows(
		"localhost test testDB Y Y Y Y Y Y Y N Y Y N N N N N N Y N N",
		"localhost test testDB1 Y Y Y Y Y Y Y N Y Y N N N N N N Y N N",
		"% dddb_% dduser Y Y Y Y Y Y Y N Y Y N N N N N N Y N N",
		"% test test Y N N N N N N N N N N N N N N N N N N",
		"localhost test testDBRevoke N N N N N N N N N N N N N N N N N N N",
	))

	// Test drop user meet error
	_, err = tk.Exec(dropUserSQL)
	c.Assert(terror.ErrorEqual(err, executor.ErrCannotUser.GenByArgs("DROP USER", "")), IsTrue, Commentf("err %v", err))

	createUserSQL = `CREATE USER 'test1'@'localhost'`
	tk.MustExec(createUserSQL)
	createUserSQL = `CREATE USER 'test2'@'localhost'`
	tk.MustExec(createUserSQL)

	dropUserSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
	_, err = tk.Exec(dropUserSQL)
	c.Assert(terror.ErrorEqual(err, executor.ErrCannotUser.GenByArgs("DROP USER", "")), IsTrue, Commentf("err %v", err))
}

func (s *testSuite) TestSetPwd(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	createUserSQL := `CREATE USER 'testpwd'@'localhost' IDENTIFIED BY '';`
	tk.MustExec(createUserSQL)
	result := tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Rows(""))

	// set password for
	tk.MustExec(`SET PASSWORD FOR 'testpwd'@'localhost' = 'password';`)
	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("password")))

	// set password
	setPwdSQL := `SET PASSWORD = 'pwd'`
	// Session user is empty.
	_, err := tk.Exec(setPwdSQL)
	c.Check(err, NotNil)
	tk.Se, err = session.CreateSession4Test(s.store)
	c.Check(err, IsNil)
	ctx := tk.Se.(sessionctx.Context)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "testpwd1", Hostname: "localhost"}
	// Session user doesn't exist.
	_, err = tk.Exec(setPwdSQL)
	c.Check(terror.ErrorEqual(err, executor.ErrPasswordNoMatch), IsTrue, Commentf("err %v", err))
	// normal
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "testpwd", Hostname: "localhost"}
	tk.MustExec(setPwdSQL)
	result = tk.MustQuery(`SELECT Password FROM mysql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Rows(auth.EncodePassword("pwd")))
}

func (s *testSuite) TestKillStmt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("kill 1")

	result := tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] connectionID' instead"))
}

func (s *testSuite) TestFlushPrivileges(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec(`CREATE USER 'testflush'@'localhost' IDENTIFIED BY '';`)
	tk.MustExec(`FLUSH PRIVILEGES;`)
	tk.MustExec(`UPDATE mysql.User SET Select_priv='Y' WHERE User="testflush" and Host="localhost"`)

	// Create a new session.
	se, err := session.CreateSession4Test(s.store)
	c.Check(err, IsNil)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "testflush", Hostname: "localhost"}, nil, nil), IsTrue)

	ctx := context.Background()
	// Before flush.
	_, err = se.Execute(ctx, `SELECT Password FROM mysql.User WHERE User="testflush" and Host="localhost"`)
	c.Check(err, NotNil)

	tk.MustExec("FLUSH PRIVILEGES")

	// After flush.
	_, err = se.Execute(ctx, `SELECT Password FROM mysql.User WHERE User="testflush" and Host="localhost"`)
	c.Check(err, IsNil)
}

func (s *testSuite) TestDropStats(c *C) {
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	do := domain.GetDomain(testKit.Se)
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	h.Clear()
	testKit.MustExec("analyze table t")
	statsTbl := h.GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	testKit.MustExec("drop stats t")
	h.Update(is)
	statsTbl = h.GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)

	testKit.MustExec("analyze table t")
	statsTbl = h.GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	h.Lease = 1
	testKit.MustExec("drop stats t")
	h.Update(is)
	statsTbl = h.GetTableStats(tableInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
	h.Lease = 0
}
