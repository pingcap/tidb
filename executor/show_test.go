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
	"github.com/pingcap/tidb/executor"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testSuite5) TestShowVisibility(c *C) {
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

func (s *testSuite5) TestShowDatabasesInfoSchemaFirst(c *C) {
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

func (s *testSuite5) TestShowWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_warnings (a int)`
	tk.MustExec(testSQL)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert show_warnings values ('a')")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect FLOAT value: 'a'"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect FLOAT value: 'a'"))
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

func (s *testSuite5) TestShowErrors(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_errors (a int)`
	tk.MustExec(testSQL)
	testSQL = `create table show_errors (a int)`
	tk.Exec(testSQL)

	tk.MustQuery("show errors").Check(testutil.RowsWithSep("|", "Error|1050|Table 'test.show_errors' already exists"))
}

func (s *testSuite5) TestShowGrantsPrivilege(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create user show_grants")
	tk.MustExec("show grants for show_grants")
	tk1 := testkit.NewTestKit(c, s.store)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "show_grants", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se
	err = tk1.QueryToErr("show grants for root")
	c.Assert(err.Error(), Equals, executor.ErrDBaccessDenied.GenWithStackByArgs("show_grants", "%", mysql.SystemDB).Error())
}

func (s *testSuite5) TestIssue3641(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	_, err := tk.Exec("show tables;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
	_, err = tk.Exec("show table status;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
}

func (s *testSuite5) TestIssue10549(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE DATABASE newdb;")
	tk.MustExec("CREATE ROLE 'app_developer';")
	tk.MustExec("GRANT ALL ON newdb.* TO 'app_developer';")
	tk.MustExec("CREATE USER 'dev';")
	tk.MustExec("GRANT 'app_developer' TO 'dev';")
	tk.MustExec("SET DEFAULT ROLE app_developer TO 'dev';")

	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "dev", Hostname: "%", AuthUsername: "dev", AuthHostname: "%"}, nil, nil), IsTrue)
	tk.MustQuery("SHOW DATABASES;").Check(testkit.Rows("INFORMATION_SCHEMA", "newdb"))
	tk.MustQuery("SHOW GRANTS;").Check(testkit.Rows("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT ALL PRIVILEGES ON newdb.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
	tk.MustQuery("SHOW GRANTS FOR CURRENT_USER").Check(testkit.Rows("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
}

func (s *testSuite5) TestIssue11165(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE ROLE 'r_manager';")
	tk.MustExec("CREATE USER 'manager'@'localhost';")
	tk.MustExec("GRANT 'r_manager' TO 'manager'@'localhost';")

	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "manager", Hostname: "localhost", AuthUsername: "manager", AuthHostname: "localhost"}, nil, nil), IsTrue)
	tk.MustExec("SET DEFAULT ROLE ALL TO 'manager'@'localhost';")
	tk.MustExec("SET DEFAULT ROLE NONE TO 'manager'@'localhost';")
	tk.MustExec("SET DEFAULT ROLE 'r_manager' TO 'manager'@'localhost';")
}

// TestShow2 is moved from session_test
func (s *testSuite5) TestShow2(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("set global autocommit=0")
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Rows("autocommit 0"))
	tk.MustExec("set global autocommit = 1")
	tk2 := testkit.NewTestKit(c, s.store)
	// TODO: In MySQL, the result is "autocommit ON".
	tk2.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Rows("autocommit 1"))

	// TODO: Specifying the charset for national char/varchar should not be supported.
	tk.MustExec("drop table if exists test_full_column")
	tk.MustExec(`create table test_full_column(
					c_int int,
					c_float float,
					c_bit bit,
					c_bool bool,
					c_char char(1) charset ascii collate ascii_bin,
					c_nchar national char(1) charset ascii collate ascii_bin,
					c_binary binary,
					c_varchar varchar(1) charset ascii collate ascii_bin,
					c_varchar_default varchar(20) charset ascii collate ascii_bin default 'cUrrent_tImestamp',
					c_nvarchar national varchar(1) charset ascii collate ascii_bin,
					c_varbinary varbinary(1),
					c_year year,
					c_date date,
					c_time time,
					c_datetime datetime,
					c_datetime_default datetime default current_timestamp,
					c_datetime_default_2 datetime(2) default current_timestamp(2),
					c_timestamp timestamp,
					c_timestamp_default timestamp default current_timestamp,
					c_timestamp_default_3 timestamp(3) default current_timestamp(3),
					c_timestamp_default_4 timestamp(3) default current_timestamp(3) on update current_timestamp(3),
					c_blob blob,
					c_tinyblob tinyblob,
					c_mediumblob mediumblob,
					c_longblob longblob,
					c_text text charset ascii collate ascii_bin,
					c_tinytext tinytext charset ascii collate ascii_bin,
					c_mediumtext mediumtext charset ascii collate ascii_bin,
					c_longtext longtext charset ascii collate ascii_bin,
					c_json json,
					c_enum enum('1') charset ascii collate ascii_bin,
					c_set set('1') charset ascii collate ascii_bin
				);`)

	tk.MustQuery(`show full columns from test_full_column`).Check(testkit.Rows(
		"" +
			"c_int int(11) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_float float <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_bit bit(1) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_bool tinyint(1) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_char char(1) ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_nchar char(1) ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_binary binary(1) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_varchar varchar(1) ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_varchar_default varchar(20) ascii_bin YES  cUrrent_tImestamp  select,insert,update,references ]\n" +
			"[c_nvarchar varchar(1) ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_varbinary varbinary(1) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_year year(4) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_date date <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_time time <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_datetime datetime <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_datetime_default datetime <nil> YES  CURRENT_TIMESTAMP  select,insert,update,references ]\n" +
			"[c_datetime_default_2 datetime(2) <nil> YES  CURRENT_TIMESTAMP(2)  select,insert,update,references ]\n" +
			"[c_timestamp timestamp <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_timestamp_default timestamp <nil> YES  CURRENT_TIMESTAMP  select,insert,update,references ]\n" +
			"[c_timestamp_default_3 timestamp(3) <nil> YES  CURRENT_TIMESTAMP(3)  select,insert,update,references ]\n" +
			"[c_timestamp_default_4 timestamp(3) <nil> YES  CURRENT_TIMESTAMP(3) DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3) select,insert,update,references ]\n" +
			"[c_blob blob <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_tinyblob tinyblob <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_mediumblob mediumblob <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_longblob longblob <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_text text ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_tinytext tinytext ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_mediumtext mediumtext ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_longtext longtext ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_json json <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_enum enum('1') ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_set set('1') ascii_bin YES  <nil>  select,insert,update,references "))

	tk.MustExec("drop table if exists test_full_column")

	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table if not exists t (c int) comment '注释'`)
	tk.MustExec("create or replace definer='root'@'localhost' view v as select * from t")
	tk.MustQuery(`show columns from t`).Check(testutil.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`describe t`).Check(testutil.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`show columns from v`).Check(testutil.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`describe v`).Check(testutil.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery("show collation where Charset = 'utf8' and Collation = 'utf8_bin'").Check(testutil.RowsWithSep(",", "utf8_bin,utf8,83,Yes,Yes,1"))
	tk.MustExec(`drop sequence if exists seq`)
	tk.MustExec(`create sequence seq`)
	tk.MustQuery("show tables").Check(testkit.Rows("seq", "t", "v"))
	tk.MustQuery("show full tables").Check(testkit.Rows("seq SEQUENCE", "t BASE TABLE", "v VIEW"))
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"), false)
	c.Assert(err, IsNil)
	createTime := model.TSConvert2Time(tblInfo.Meta().UpdateTS).Format("2006-01-02 15:04:05")

	// The Hostname is the actual host
	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	r := tk.MustQuery("show table status from test like 't'")
	r.Check(testkit.Rows(fmt.Sprintf("t InnoDB 10 Compact 0 0 0 0 0 0 <nil> %s <nil> <nil> utf8mb4_bin   注释", createTime)))

	tk.MustQuery("show databases like 'test'").Check(testkit.Rows("test"))

	tk.MustExec(`grant all on *.* to 'root'@'%'`)
	tk.MustQuery("show grants").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))

	tk.MustQuery("show grants for current_user()").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))
	tk.MustQuery("show grants for current_user").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))
}

func (s *testSuite5) TestShowCreateUser(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	tk.MustExec(`CREATE USER 'test_show_create_user'@'%' IDENTIFIED BY 'root';`)
	tk.MustQuery("show create user 'test_show_create_user'@'%'").
		Check(testkit.Rows(`CREATE USER 'test_show_create_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*81F5E21E35407D884A6CD4A731AEBFB6AF209E1B' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK`))

	tk.MustExec(`CREATE USER 'test_show_create_user'@'localhost' IDENTIFIED BY 'test';`)
	tk.MustQuery("show create user 'test_show_create_user'@'localhost';").
		Check(testkit.Rows(`CREATE USER 'test_show_create_user'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK`))

	// Case: the user exists but the host portion doesn't match
	err := tk.QueryToErr("show create user 'test_show_create_user'@'asdf';")
	c.Assert(err.Error(), Equals, executor.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER", "'test_show_create_user'@'asdf'").Error())

	// Case: a user that doesn't exist
	err = tk.QueryToErr("show create user 'aaa'@'localhost';")
	c.Assert(err.Error(), Equals, executor.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER", "'aaa'@'localhost'").Error())

	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, nil)
	rows := tk.MustQuery("show create user current_user")
	rows.Check(testkit.Rows("CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	rows = tk.MustQuery("show create user current_user()")
	rows.Check(testkit.Rows("CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	tk.MustExec("create user 'check_priv'")

	// "show create user" for other user requires the SELECT privilege on mysql database.
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use mysql")
	succ := tk1.Se.Auth(&auth.UserIdentity{Username: "check_priv", Hostname: "127.0.0.1", AuthUsername: "test_show", AuthHostname: "asdf"}, nil, nil)
	c.Assert(succ, IsTrue)
	err = tk1.QueryToErr("show create user 'root'@'%'")
	c.Assert(err, NotNil)

	// "show create user" for current user doesn't check privileges.
	rows = tk1.MustQuery("show create user current_user")
	rows.Check(testkit.Rows("CREATE USER 'check_priv'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))
}

func (s *testSuite5) TestUnprivilegedShow(c *C) {

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
	tblInfo, err := is.TableByName(model.NewCIStr("testshow"), model.NewCIStr("t1"), false)
	c.Assert(err, IsNil)
	createTime := model.TSConvert2Time(tblInfo.Meta().UpdateTS).Format("2006-01-02 15:04:05")

	tk.MustQuery("show table status from testshow").Check(testkit.Rows(fmt.Sprintf("t1 InnoDB 10 Compact 0 0 0 0 0 0 <nil> %s <nil> <nil> utf8mb4_bin   ", createTime)))

}

func (s *testSuite5) TestCollation(c *C) {
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

func (s *testSuite5) TestShowTableStatus(c *C) {
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

func (s *testSuite5) TestShowSlow(c *C) {
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

func (s *testSuite5) TestShowOpenTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("show open tables")
	tk.MustQuery("show open tables in test")
}

func (s *testSuite5) TestShowCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int,b int)")
	tk.MustExec("drop view if exists v1")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select * from t1")
	tk.MustQuery("show create table v1").Check(testutil.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a`, `b`) AS SELECT `test`.`t1`.`a`,`test`.`t1`.`b` FROM `test`.`t1`  "))
	tk.MustQuery("show create view v1").Check(testutil.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a`, `b`) AS SELECT `test`.`t1`.`a`,`test`.`t1`.`b` FROM `test`.`t1`  "))
	tk.MustExec("drop view v1")
	tk.MustExec("drop table t1")

	tk.MustExec("drop view if exists v")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v as select JSON_MERGE('{}', '{}') as col;")
	tk.MustQuery("show create view v").Check(testutil.RowsWithSep("|", "v|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v` (`col`) AS SELECT JSON_MERGE('{}', '{}') AS `col`  "))
	tk.MustExec("drop view if exists v")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int,b int)")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select avg(a),t1.* from t1 group by a")
	tk.MustQuery("show create view v1").Check(testutil.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`avg(a)`, `a`, `b`) AS SELECT AVG(`a`),`test`.`t1`.`a`,`test`.`t1`.`b` FROM `test`.`t1` GROUP BY `a`  "))
	tk.MustExec("drop view v1")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select a+b, t1.* , a as c from t1")
	tk.MustQuery("show create view v1").Check(testutil.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a+b`, `a`, `b`, `c`) AS SELECT `a`+`b`,`test`.`t1`.`a`,`test`.`t1`.`b`,`a` AS `c` FROM `test`.`t1`  "))
	tk.MustExec("drop table t1")
	tk.MustExec("drop view v1")

	// For issue #9211
	tk.MustExec("create table t(c int, b int as (c + 1))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) GENERATED ALWAYS AS (`c` + 1) VIRTUAL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(c int, b int as (c + 1) not null)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) GENERATED ALWAYS AS (`c` + 1) VIRTUAL NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t")
	tk.MustExec("create table t ( a char(10) charset utf8 collate utf8_bin, b char(10) as (rtrim(a)));")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` char(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"+
			"  `b` char(10) GENERATED ALWAYS AS (rtrim(`a`)) VIRTUAL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t")

	tk.MustExec(`drop table if exists different_charset`)
	tk.MustExec(`create table different_charset(ch1 varchar(10) charset utf8, ch2 varchar(10) charset binary);`)
	tk.MustQuery(`show create table different_charset`).Check(testutil.RowsWithSep("|",
		""+
			"different_charset CREATE TABLE `different_charset` (\n"+
			"  `ch1` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"+
			"  `ch2` varbinary(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table `t` (\n" +
		"`a` timestamp not null default current_timestamp,\n" +
		"`b` timestamp(3) default current_timestamp(3),\n" +
		"`c` datetime default current_timestamp,\n" +
		"`d` datetime(4) default current_timestamp(4),\n" +
		"`e` varchar(20) default 'cUrrent_tImestamp',\n" +
		"`f` datetime(2) default current_timestamp(2) on update current_timestamp(2),\n" +
		"`g` timestamp(2) default current_timestamp(2) on update current_timestamp(2))")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"+
			"  `b` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),\n"+
			"  `c` datetime DEFAULT CURRENT_TIMESTAMP,\n"+
			"  `d` datetime(4) DEFAULT CURRENT_TIMESTAMP(4),\n"+
			"  `e` varchar(20) DEFAULT 'cUrrent_tImestamp',\n"+
			"  `f` datetime(2) DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2),\n"+
			"  `g` timestamp(2) DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t")

	tk.MustExec("create table t (a int, b int) shard_row_id_bits = 4 pre_split_regions=3;")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin/*!90000 SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 */",
	))
	tk.MustExec("drop table t")

	tk.MustExec("CREATE TABLE `log` (" +
		"`LOG_ID` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT," +
		"`ROUND_ID` bigint(20) UNSIGNED NOT NULL," +
		"`USER_ID` int(10) UNSIGNED NOT NULL," +
		"`USER_IP` int(10) UNSIGNED DEFAULT NULL," +
		"`END_TIME` datetime NOT NULL," +
		"`USER_TYPE` int(11) DEFAULT NULL," +
		"`APP_ID` int(11) DEFAULT NULL," +
		"PRIMARY KEY (`LOG_ID`,`END_TIME`)," +
		"KEY `IDX_EndTime` (`END_TIME`)," +
		"KEY `IDX_RoundId` (`ROUND_ID`)," +
		"KEY `IDX_UserId_EndTime` (`USER_ID`,`END_TIME`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=505488 " +
		"PARTITION BY RANGE ( month(`end_time`) ) (" +
		"PARTITION `p1` VALUES LESS THAN (2)," +
		"PARTITION `p2` VALUES LESS THAN (3)," +
		"PARTITION `p3` VALUES LESS THAN (4)," +
		"PARTITION `p4` VALUES LESS THAN (5)," +
		"PARTITION `p5` VALUES LESS THAN (6)," +
		"PARTITION `p6` VALUES LESS THAN (7)," +
		"PARTITION `p7` VALUES LESS THAN (8)," +
		"PARTITION `p8` VALUES LESS THAN (9)," +
		"PARTITION `p9` VALUES LESS THAN (10)," +
		"PARTITION `p10` VALUES LESS THAN (11)," +
		"PARTITION `p11` VALUES LESS THAN (12)," +
		"PARTITION `p12` VALUES LESS THAN (MAXVALUE))")
	tk.MustQuery("show create table log").Check(testutil.RowsWithSep("|",
		"log CREATE TABLE `log` (\n"+
			"  `LOG_ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n"+
			"  `ROUND_ID` bigint(20) unsigned NOT NULL,\n"+
			"  `USER_ID` int(10) unsigned NOT NULL,\n"+
			"  `USER_IP` int(10) unsigned DEFAULT NULL,\n"+
			"  `END_TIME` datetime NOT NULL,\n"+
			"  `USER_TYPE` int(11) DEFAULT NULL,\n"+
			"  `APP_ID` int(11) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`LOG_ID`,`END_TIME`),\n"+
			"  KEY `IDX_EndTime` (`END_TIME`),\n"+
			"  KEY `IDX_RoundId` (`ROUND_ID`),\n"+
			"  KEY `IDX_UserId_EndTime` (`USER_ID`,`END_TIME`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=505488\n"+
			"PARTITION BY RANGE ( month(`end_time`) ) (\n"+
			"  PARTITION `p1` VALUES LESS THAN (2),\n"+
			"  PARTITION `p2` VALUES LESS THAN (3),\n"+
			"  PARTITION `p3` VALUES LESS THAN (4),\n"+
			"  PARTITION `p4` VALUES LESS THAN (5),\n"+
			"  PARTITION `p5` VALUES LESS THAN (6),\n"+
			"  PARTITION `p6` VALUES LESS THAN (7),\n"+
			"  PARTITION `p7` VALUES LESS THAN (8),\n"+
			"  PARTITION `p8` VALUES LESS THAN (9),\n"+
			"  PARTITION `p9` VALUES LESS THAN (10),\n"+
			"  PARTITION `p10` VALUES LESS THAN (11),\n"+
			"  PARTITION `p11` VALUES LESS THAN (12),\n"+
			"  PARTITION `p12` VALUES LESS THAN (MAXVALUE)\n"+
			")"))

	// for issue #11831
	tk.MustExec("create table ttt4(a varchar(123) default null collate utf8mb4_unicode_ci)engine=innodb default charset=utf8mb4 collate=utf8mb4_unicode_ci;")
	tk.MustQuery("show create table `ttt4`").Check(testutil.RowsWithSep("|",
		""+
			"ttt4 CREATE TABLE `ttt4` (\n"+
			"  `a` varchar(123) COLLATE utf8mb4_unicode_ci DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
	))
	tk.MustExec("create table ttt5(a varchar(123) default null)engine=innodb default charset=utf8mb4 collate=utf8mb4_bin;")
	tk.MustQuery("show create table `ttt5`").Check(testutil.RowsWithSep("|",
		""+
			"ttt5 CREATE TABLE `ttt5` (\n"+
			"  `a` varchar(123) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// for expression index
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b real);")
	tk.MustExec("alter table t add index expr_idx((a*b+1));")
	tk.MustQuery("show create table t;").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` double DEFAULT NULL,\n"+
			"  KEY `expr_idx` ((`a` * `b` + 1))\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Fix issue #15175, show create table sequence_name.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustQuery("show create table seq;").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))
}

func (s *testAutoRandomSuite) TestShowCreateTableAutoRandom(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	testutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer testutil.ConfigTestUtils.RestoreAutoRandomTestConfig()

	// Basic show create table.
	tk.MustExec("create table auto_random_tbl1 (a bigint primary key auto_random(3), b varchar(255))")
	tk.MustQuery("show create table `auto_random_tbl1`").Check(testutil.RowsWithSep("|",
		""+
			"auto_random_tbl1 CREATE TABLE `auto_random_tbl1` (\n"+
			"  `a` bigint(20) NOT NULL /*T!30100 AUTO_RANDOM(3) */,\n"+
			"  `b` varchar(255) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Implicit auto_random value should be shown explicitly.
	tk.MustExec("create table auto_random_tbl2 (a bigint auto_random primary key, b char)")
	tk.MustQuery("show create table auto_random_tbl2").Check(testutil.RowsWithSep("|",
		""+
			"auto_random_tbl2 CREATE TABLE `auto_random_tbl2` (\n"+
			"  `a` bigint(20) NOT NULL /*T!30100 AUTO_RANDOM(5) */,\n"+
			"  `b` char(1) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Special version comment can be shown in TiDB with new version.
	tk.MustExec("create table auto_random_tbl3 (a bigint /*T!30100 auto_random */ primary key)")
	tk.MustQuery("show create table auto_random_tbl3").Check(testutil.RowsWithSep("|",
		""+
			"auto_random_tbl3 CREATE TABLE `auto_random_tbl3` (\n"+
			"  `a` bigint(20) NOT NULL /*T!30100 AUTO_RANDOM(5) */,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
}

func (s *testSuite5) TestShowEscape(c *C) {
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

func (s *testSuite5) TestShowBuiltin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	res := tk.MustQuery("show builtins;")
	c.Assert(res, NotNil)
	rows := res.Rows()
	c.Assert(268, Equals, len(rows))
	c.Assert("abs", Equals, rows[0][0].(string))
	c.Assert("yearweek", Equals, rows[267][0].(string))
}
