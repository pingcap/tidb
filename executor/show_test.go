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
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testSuite2) TestShowVisibility(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database showdatabase")
	tk.MustExec("use showdatabase")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create table t2 (id int)")
	tk.MustExec(`create user 'show'@'%'`)
	tk.MustExec(`flush privileges`)

	tk1 := testkit.NewTestKit(c, s.store)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "show", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se

	// No ShowDatabases privilege, this user would see nothing except INFORMATION_SCHEMA.
	tk.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// After grant, the user can see the database.
	tk.MustExec(`grant select on showdatabase.t1 to 'show'@'%'`)
	tk.MustExec(`flush privileges`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA", "showdatabase"))

	// The user can see t1 but not t2.
	tk1.MustExec("use showdatabase")
	tk1.MustQuery("show tables").Check(testkit.Rows("t1"))

	// After revoke, show database result should be just except INFORMATION_SCHEMA.
	tk.MustExec(`revoke select on showdatabase.t1 from 'show'@'%'`)
	tk.MustExec(`flush privileges`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// Grant any global privilege would make show databases available.
	tk.MustExec(`grant CREATE on *.* to 'show'@'%'`)
	tk.MustExec(`flush privileges`)
	rows := tk1.MustQuery("show databases").Rows()
	c.Assert(len(rows), GreaterEqual, 2) // At least INFORMATION_SCHEMA and showdatabase

	tk.MustExec(`drop user 'show'@'%'`)
	tk.MustExec("drop database showdatabase")
}

func (s *testSuite2) TestShowDatabasesInfoSchemaFirst(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA"))
	tk.MustExec(`create user 'show'@'%'`)
	tk.MustExec(`flush privileges`)

	tk.MustExec(`create database AAAA`)
	tk.MustExec(`create database BBBB`)
	tk.MustExec(`grant select on AAAA.* to 'show'@'%'`)
	tk.MustExec(`grant select on BBBB.* to 'show'@'%'`)
	tk.MustExec(`flush privileges`)

	tk1 := testkit.NewTestKit(c, s.store)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "show", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA", "AAAA", "BBBB"))

	tk.MustExec(`drop user 'show'@'%'`)
	tk.MustExec(`drop database AAAA`)
	tk.MustExec(`drop database BBBB`)
}

// mockSessionManager is a mocked session manager that wraps one session
// it returns only this session's current process info as processlist for test.
type mockSessionManager struct {
	session.Session
}

// Kill implements the SessionManager.Kill interface.
func (msm *mockSessionManager) Kill(cid uint64, query bool) {
}

func (s *testSuite2) TestShowWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_warnings (a int)`
	tk.MustExec(testSQL)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert show_warnings values ('a')")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1265|Data Truncated"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1265|Data Truncated"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))

	// Test Warning level 'Error'
	testSQL = `create table show_warnings (a int)`
	tk.Exec(testSQL)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Error|1050|Table 'test.show_warnings' already exists"))
	tk.MustQuery("select @@error_count").Check(testutil.RowsWithSep("|", "1"))

	// Test Warning level 'Note'
	testSQL = `create table show_warnings_2 (a int)`
	tk.MustExec(testSQL)
	testSQL = `create table if not exists show_warnings_2 like show_warnings`
	tk.Exec(testSQL)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1050|Table 'test.show_warnings_2' already exists"))
	tk.MustQuery("select @@warning_count").Check(testutil.RowsWithSep("|", "1"))
	tk.MustQuery("select @@warning_count").Check(testutil.RowsWithSep("|", "0"))
}

func (s *testSuite2) TestShowErrors(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_errors (a int)`
	tk.MustExec(testSQL)
	testSQL = `create table show_errors (a int)`
	tk.Exec(testSQL)

	tk.MustQuery("show errors").Check(testutil.RowsWithSep("|", "Error|1050|Table 'test.show_errors' already exists"))
}

func (s *testSuite2) TestIssue3641(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	_, err := tk.Exec("show tables;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
	_, err = tk.Exec("show table status;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
}

// TestShow2 is moved from session_test
func (s *testSuite2) TestShow2(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("set global autocommit=0")
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Rows("autocommit 0"))
	tk.MustExec("set global autocommit = 1")
	tk2 := testkit.NewTestKit(c, s.store)
	// TODO: In MySQL, the result is "autocommit ON".
	tk2.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Rows("autocommit 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table if not exists t (c int) comment '注释'`)
	tk.MustExec("create or replace view v as select * from t")
	tk.MustQuery(`show columns from t`).Check(testutil.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`describe t`).Check(testutil.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`show columns from v`).Check(testutil.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`describe v`).Check(testutil.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery("show collation where Charset = 'utf8' and Collation = 'utf8_bin'").Check(testutil.RowsWithSep(",", "utf8_bin,utf8,83,,Yes,1"))

	tk.MustQuery("show tables").Check(testkit.Rows("t", "v"))
	tk.MustQuery("show full tables").Check(testkit.Rows("t BASE TABLE", "v VIEW"))
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	createTime := model.TSConvert2Time(tblInfo.Meta().UpdateTS).Format("2006-01-02 15:04:05")

	// The Hostname is the actual host
	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	r := tk.MustQuery("show table status from test like 't'")
	r.Check(testkit.Rows(fmt.Sprintf("t InnoDB 10 Compact 0 0 0 0 0 0 0 %s <nil> <nil> utf8mb4_bin   注释", createTime)))

	tk.MustQuery("show databases like 'test'").Check(testkit.Rows("test"))

	tk.MustExec(`grant all on *.* to 'root'@'%'`)
	tk.MustQuery("show grants").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'`))

	tk.MustQuery("show grants for current_user()").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'`))
	tk.MustQuery("show grants for current_user").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'`))
}

func (s *testSuite2) TestShow3(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	createUserSQL := `CREATE USER 'test_show_create_user'@'%' IDENTIFIED BY 'root';`
	tk.MustExec(createUserSQL)
	tk.MustQuery("show create user 'test_show_create_user'@'%'").Check(testkit.Rows(`CREATE USER 'test_show_create_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*81F5E21E35407D884A6CD4A731AEBFB6AF209E1B' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK`))
}

func (s *testSuite2) TestUnprivilegedShow(c *C) {

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE DATABASE testshow")
	tk.MustExec("USE testshow")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")

	tk.MustExec(`CREATE USER 'lowprivuser'`) // no grants
	tk.MustExec(`FLUSH PRIVILEGES`)

	tk.Se.Auth(&auth.UserIdentity{Username: "lowprivuser", Hostname: "192.168.0.1", AuthUsername: "lowprivuser", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	rs, err := tk.Exec("SHOW TABLE STATUS FROM testshow")
	c.Assert(err, IsNil)
	c.Assert(rs, NotNil)

	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tk.MustExec("GRANT ALL ON testshow.t1 TO 'lowprivuser'")
	tk.MustExec(`FLUSH PRIVILEGES`)
	tk.Se.Auth(&auth.UserIdentity{Username: "lowprivuser", Hostname: "192.168.0.1", AuthUsername: "lowprivuser", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("testshow"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	createTime := model.TSConvert2Time(tblInfo.Meta().UpdateTS).Format("2006-01-02 15:04:05")

	tk.MustQuery("show table status from testshow").Check(testkit.Rows(fmt.Sprintf("t1 InnoDB 10 Compact 0 0 0 0 0 0 0 %s <nil> <nil> utf8mb4_bin   ", createTime)))

}

func (s *testSuite2) TestCollation(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	rs, err := tk.Exec("show collation;")
	c.Assert(err, IsNil)
	fields := rs.Fields()
	c.Assert(fields[0].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[1].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[2].Column.Tp, Equals, mysql.TypeLonglong)
	c.Assert(fields[3].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[4].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[5].Column.Tp, Equals, mysql.TypeLonglong)
}

func (s *testSuite2) TestShowTableStatus(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint);`)

	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	// It's not easy to test the result contents because every time the test runs, "Create_time" changed.
	tk.MustExec("show table status;")
	rs, err := tk.Exec("show table status;")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err := session.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(errors.ErrorStack(err), Equals, "")
	err = rs.Close()
	c.Assert(errors.ErrorStack(err), Equals, "")

	for i := range rows {
		row := rows[i]
		c.Assert(row.GetString(0), Equals, "t")
		c.Assert(row.GetString(1), Equals, "InnoDB")
		c.Assert(row.GetInt64(2), Equals, int64(10))
		c.Assert(row.GetString(3), Equals, "Compact")
	}
	tk.MustExec(`drop table if exists tp;`)
	tk.MustExec(`create table tp (a int)
 		partition by range(a)
 		( partition p0 values less than (10),
		  partition p1 values less than (20),
		  partition p2 values less than (maxvalue)
  		);`)
	rs, err = tk.Exec("show table status from test like 'tp';")
	c.Assert(errors.ErrorStack(err), Equals, "")
	rows, err = session.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rows[0].GetString(16), Equals, "partitioned")
}

func (s *testSuite2) TestShowSlow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// The test result is volatile, because
	// 1. Slow queries is stored in domain, which may be affected by other tests.
	// 2. Collecting slow queries is a asynchronous process, check immediately may not get the expected result.
	// 3. Make slow query like "select sleep(1)" would slow the CI.
	// So, we just cover the code but do not check the result.
	tk.MustQuery(`admin show slow recent 3`)
	tk.MustQuery(`admin show slow top 3`)
	tk.MustQuery(`admin show slow top internal 3`)
	tk.MustQuery(`admin show slow top all 3`)
}

func (s *testSuite2) TestShowCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int,b int)")
	tk.MustExec("drop view if exists v1")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select * from t1")
	tk.MustQuery("show create table v1").Check(testutil.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a`, `b`) AS select * from t1  "))

	tk.MustExec("drop view v1")
	tk.MustExec("drop table t1")
}

func (s *testSuite2) TestShowEscape(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists `t``abl\"e`")
	tk.MustExec("create table `t``abl\"e`(`c``olum\"n` int(11) primary key)")
	tk.MustQuery("show create table `t``abl\"e`").Check(testutil.RowsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE `t``abl\"e` (\n"+
			"  `c``olum\"n` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`c``olum\"n`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// ANSI_QUOTES will change the SHOW output
	tk.MustExec("set @old_sql_mode=@@sql_mode")
	tk.MustExec("set sql_mode=ansi_quotes")
	tk.MustQuery("show create table \"t`abl\"\"e\"").Check(testutil.RowsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE \"t`abl\"\"e\" (\n"+
			"  \"c`olum\"\"n\" int(11) NOT NULL,\n"+
			"  PRIMARY KEY (\"c`olum\"\"n\")\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("rename table \"t`abl\"\"e\" to t")
	tk.MustExec("set sql_mode=@old_sql_mode")
}
